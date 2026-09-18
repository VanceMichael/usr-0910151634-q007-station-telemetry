package store_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/vancemichael/station-telemetry-service/internal/contracts"
	"github.com/vancemichael/station-telemetry-service/internal/domain"
	"github.com/vancemichael/station-telemetry-service/internal/store"
)

func testDSN() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return "host=/tmp port=5433 user=telemetry dbname=stations_test connect_timeout=3"
}

// truncateAll 在每个用例前清空数据（迁移保留）。
func truncateAll(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE telemetry_events, source_watermarks, sequence_gaps, station_projection RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("清空数据失败: %v", err)
	}
}

func openStore(t *testing.T) (*store.Store, *sql.DB, contracts.Policy) {
	t.Helper()
	db, err := sql.Open("postgres", testDSN())
	if err != nil {
		t.Skipf("无法连接 PostgreSQL，跳过集成测试: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("PostgreSQL 不可用，跳过集成测试: %v", err)
	}
	if err := store.Migrate(context.Background(), db); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	policy, err := contracts.Load()
	if err != nil {
		t.Fatal(err)
	}
	truncateAll(t, db)
	return store.New(db, policy), db, policy
}

func goodMetrics() *domain.Metrics {
	sats := 8
	pdop := 1.5
	return &domain.Metrics{Satellites: &sats, PDOP: &pdop}
}

func badMetrics() *domain.Metrics {
	sats := 2
	pdop := 9.0
	return &domain.Metrics{Satellites: &sats, PDOP: &pdop}
}

type spec struct {
	eventID    string
	stationID  string
	sourceID   string
	seq        int64
	observedAt time.Time
	receivedAt time.Time
	metrics    *domain.Metrics
}

func ingestSpec(t *testing.T, s *store.Store, sp spec) *store.IngestResult {
	t.Helper()
	e := domain.Event{
		EventID: sp.eventID, StationID: sp.stationID, SourceID: sp.sourceID,
		SourceSeq: sp.seq, ObservedAt: sp.observedAt, ReceivedAt: sp.receivedAt,
		Metrics: sp.metrics,
	}
	status := domain.QualityStatus(e.Metrics, s.Policy)
	res, err := s.Ingest(context.Background(), e, []byte(`{}`), status == domain.QualityQualified, status)
	if err != nil {
		t.Fatalf("Ingest(%s) 失败: %v", sp.eventID, err)
	}
	return res
}

// 场景：跳号产生缺口，晚到补传回填缺口、推进水位，但不回退投影。
func TestIngestGapFillAndProjectionGuard(t *testing.T) {
	s, db, _ := openStore(t)
	defer db.Close()

	station, source := "st-gap", "link-a"
	base := time.Date(2026, 9, 18, 1, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }
	now := time.Now().UTC()

	// seq 1,2 正常到账。
	r1 := ingestSpec(t, s, spec{"e1", station, source, 1, at(0), now, goodMetrics()})
	if r1.Watermark != 1 {
		t.Fatalf("水位应为 1，实际 %d", r1.Watermark)
	}
	ingestSpec(t, s, spec{"e2", station, source, 2, at(10), now.Add(10 * time.Second), goodMetrics()})

	// seq 4 先到（人为留下序号 3 的缺口）。
	r4 := ingestSpec(t, s, spec{"e4", station, source, 4, at(30), now.Add(30 * time.Second), goodMetrics()})
	if r4.Watermark != 2 {
		t.Fatalf("缺口未补前水位应停在 2，实际 %d", r4.Watermark)
	}
	if r4.HighestSeen != 4 {
		t.Fatalf("highest_seen 应为 4，实际 %d", r4.HighestSeen)
	}

	h, err := s.Handoff(context.Background(), station)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Sources) != 1 || len(h.Sources[0].MissingRanges) != 1 ||
		h.Sources[0].MissingRanges[0].Start != 3 || h.Sources[0].MissingRanges[0].End != 3 {
		t.Fatalf("交班缺口区间错误: %+v", h.Sources)
	}

	// seq 3 跨链路晚到补传：observed_at 停留在 30 秒前，received_at 是现在。
	r3 := ingestSpec(t, s, spec{"e3-late", station, source, 3, at(20), now.Add(20 * time.Minute), goodMetrics()})
	if r3.Status != store.StatusAccepted || r3.AppliedToProjection {
		t.Fatalf("晚到补传应入账但不影响投影: %+v", r3)
	}
	if r3.Watermark != 4 {
		t.Fatalf("补齐后水位应推进到 4，实际 %d", r3.Watermark)
	}

	h2, err := s.Handoff(context.Background(), station)
	if err != nil {
		t.Fatal(err)
	}
	if len(h2.Sources[0].OpenGaps) != 0 || len(h2.Sources[0].MissingRanges) != 0 {
		t.Fatalf("补齐后不应再有缺口: %+v", h2.Sources[0])
	}
	// 当前投影表头仍应是 seq 4（晚到的 seq 3 不得回退它）。
	if h2.Projection.LastSample.SourceSeq != 4 {
		t.Fatalf("投影表头被晚到数据回退: %+v", h2.Projection.LastSample)
	}
}

// event_id 重送不重复入账。
func TestDuplicateEventID(t *testing.T) {
	s, db, _ := openStore(t)
	defer db.Close()
	base := time.Date(2026, 9, 18, 2, 0, 0, 0, time.UTC)
	sp := spec{"dup-1", "st-dup", "l", 1, base, base, goodMetrics()}
	r1 := ingestSpec(t, s, sp)
	if r1.Status != store.StatusAccepted {
		t.Fatalf("首次应 accepted: %s", r1.Status)
	}
	r2 := ingestSpec(t, s, sp)
	if r2.Status != store.StatusDupEvent {
		t.Fatalf("重送应识别为 duplicate_event: %s", r2.Status)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM telemetry_events WHERE event_id='dup-1'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("重送不得重复入账，实际行数 %d", count)
	}
}

// 两个接收端争用同一序号，只保留一条。
func TestConcurrentSeqContention(t *testing.T) {
	s, db, _ := openStore(t)
	defer db.Close()
	base := time.Date(2026, 9, 18, 3, 0, 0, 0, time.UTC)

	const n = 12
	var wg sync.WaitGroup
	results := make([]string, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			sp := spec{
				eventID: "race-" + string(rune('a'+i)), stationID: "st-race", sourceID: "l",
				seq: 5, observedAt: base, receivedAt: base, metrics: goodMetrics(),
			}
			e := domain.Event{
				EventID: sp.eventID, StationID: sp.stationID, SourceID: sp.sourceID,
				SourceSeq: sp.seq, ObservedAt: sp.observedAt, ReceivedAt: sp.receivedAt,
				Metrics: sp.metrics,
			}
			q := domain.QualityStatus(e.Metrics, s.Policy)
			res, err := s.Ingest(context.Background(), e, []byte(`{}`), true, q)
			if err != nil {
				errs[i] = err
				return
			}
			results[i] = res.Status
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("并发入账出错: %v", err)
		}
	}
	var accepted, conflicts int
	for _, st := range results {
		switch st {
		case store.StatusAccepted:
			accepted++
		case store.StatusConflictSeq:
			conflicts++
		}
	}
	if accepted != 1 || conflicts != n-1 {
		t.Fatalf("应有 1 条入账、%d 条冲突，实际 accepted=%d conflict=%d: %v", n-1, accepted, conflicts, results)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM telemetry_events WHERE station_id='st-race' AND source_seq=5`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("数据库中该序号只能有一条，实际 %d", rows)
	}
}

// 事件流断点续读：记下 next_cursor 后从中断处继续。
func TestEventStreamResume(t *testing.T) {
	s, db, _ := openStore(t)
	defer db.Close()
	base := time.Date(2026, 9, 18, 4, 0, 0, 0, time.UTC)
	for i := int64(1); i <= 5; i++ {
		ingestSpec(t, s, spec{"stream-" + string(rune('a'+i-1)), "st-stream", "l", i,
			base.Add(time.Duration(i) * time.Second), base.Add(time.Duration(i) * time.Second), goodMetrics()})
	}

	first, hasMore, err := s.ReadEvents(context.Background(), 0, 2)
	if err != nil || len(first) != 2 || !hasMore {
		t.Fatalf("首页读取错误: n=%d hasMore=%v err=%v", len(first), hasMore, err)
	}
	cursor := first[len(first)-1].ID
	second, hasMore2, err := s.ReadEvents(context.Background(), cursor, 2)
	if err != nil || len(second) != 2 || second[0].EventID != "stream-c" || !hasMore2 {
		t.Fatalf("断点续读错误: %+v hasMore=%v err=%v", second, hasMore2, err)
	}
	rest, hasMore3, err := s.ReadEvents(context.Background(), second[len(second)-1].ID, 2)
	if err != nil || len(rest) != 1 || rest[0].EventID != "stream-e" || hasMore3 {
		t.Fatalf("尾页错误: %+v hasMore=%v err=%v", rest, hasMore3, err)
	}
}

// 恢复契约：suspect/offline 必须连续 3 个合格样本才回 healthy，
// 交班依据锚定真正恢复的那个样本。
func TestRecoveryContractEndToEnd(t *testing.T) {
	s, db, _ := openStore(t)
	defer db.Close()
	station := "st-recovery"
	base := time.Date(2026, 9, 18, 5, 0, 0, 0, time.UTC)
	now := time.Now().UTC()

	// 健康基线（3 连）。
	for i := int64(1); i <= 3; i++ {
		ingestSpec(t, s, spec{"ok-" + string(rune('1'+i-1)), station, "l", i,
			base.Add(time.Duration(i) * 10 * time.Second), now.Add(time.Duration(i) * 10 * time.Second), goodMetrics()})
	}
	h, _ := s.Handoff(context.Background(), station)
	if h.Projection.State != domain.StateHealthy || h.Projection.Recovered.EventID != "ok-3" {
		t.Fatalf("3 连后应 healthy 且锚定 ok-3: %+v", h.Projection)
	}

	// 一个差质量样本打断。
	ingestSpec(t, s, spec{"bad", station, "l", 4, base.Add(40 * time.Second), now.Add(40 * time.Second), badMetrics()})
	h, _ = s.Handoff(context.Background(), station)
	if h.Projection.State != domain.StateSuspect || h.Projection.HealthyStreak != 0 {
		t.Fatalf("差质量后应 suspect 且计数清零: %+v", h.Projection)
	}

	// 再连续 2 个好样本仍不能恢复。
	ingestSpec(t, s, spec{"re-1", station, "l", 5, base.Add(50 * time.Second), now.Add(50 * time.Second), goodMetrics()})
	ingestSpec(t, s, spec{"re-2", station, "l", 6, base.Add(60 * time.Second), now.Add(60 * time.Second), goodMetrics()})
	h, _ = s.Handoff(context.Background(), station)
	if h.Projection.State != domain.StateSuspect || h.Projection.HealthyStreak != 2 {
		t.Fatalf("2/3 时不得恢复: %+v", h.Projection)
	}

	// 第 3 个好样本：恢复真正发生在这一帧。
	ingestSpec(t, s, spec{"re-3", station, "l", 7, base.Add(70 * time.Second), now.Add(70 * time.Second), goodMetrics()})
	h, _ = s.Handoff(context.Background(), station)
	if h.Projection.State != domain.StateHealthy {
		t.Fatalf("3/3 后应 healthy: %+v", h.Projection)
	}
	if h.Projection.Recovered.EventID != "re-3" || h.Projection.Recovered.SourceSeq != 7 {
		t.Fatalf("恢复必须锚定 re-3(seq=7): %+v", h.Projection.Recovered)
	}
	if h.Projection.RecoveredStreak != 3 {
		t.Fatalf("恢复时计数应为 3: %d", h.Projection.RecoveredStreak)
	}
}

// 超时静默翻 offline；offline 后同样需要完整 3 连才能回 healthy。
func TestOfflineSweepAndRecovery(t *testing.T) {
	s, db, _ := openStore(t)
	defer db.Close()
	station := "st-offline"
	// 最后观测在 1 小时前。
	old := time.Now().UTC().Add(-time.Hour)
	ingestSpec(t, s, spec{"old-1", station, "l", 1, old, old, goodMetrics()})

	flipped, err := s.SweepStaleOffline(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range flipped {
		if id == station {
			found = true
		}
	}
	if !found {
		t.Fatalf("静默站点应被翻为 offline: %v", flipped)
	}
	h, _ := s.Handoff(context.Background(), station)
	if h.Projection.State != domain.StateOffline {
		t.Fatalf("交班状态应为 offline: %s", h.Projection.State)
	}

	// 恢复来 2 个样本：仍 suspect。
	now := time.Now().UTC()
	for i, id := range []string{"back-1", "back-2"} {
		ingestSpec(t, s, spec{id, station, "l", int64(i + 2), now.Add(time.Duration(i) * 10 * time.Second), now.Add(time.Duration(i) * 10 * time.Second), goodMetrics()})
	}
	h, _ = s.Handoff(context.Background(), station)
	if h.Projection.State != domain.StateSuspect || h.Projection.HealthyStreak != 2 {
		t.Fatalf("offline 后 2 连仍应为 suspect: %+v", h.Projection)
	}
	ingestSpec(t, s, spec{"back-3", station, "l", 4, now.Add(20 * time.Second), now.Add(20 * time.Second), goodMetrics()})
	h, _ = s.Handoff(context.Background(), station)
	if h.Projection.State != domain.StateHealthy || h.Projection.Recovered.EventID != "back-3" {
		t.Fatalf("offline 恢复应锚定 back-3: %+v", h.Projection.Recovered)
	}
}
