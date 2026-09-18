// Package store 负责 PostgreSQL 落地:不可变事件、缺口集合、来源状态与站点投影
// 在同一事务内写入;另提供交班查询、可续读事件流与 offline 清扫。
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vancemichael/station-telemetry-service/internal/continuity"
)

// Store 包装连接池,所有写路径都以事务执行。
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Ping 供健康检查使用。
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Observation 是一个待入账的观测;Qualified/LateByTime 已按合约预先判定。
type Observation struct {
	EventID    string
	StationID  string
	SourceID   string
	SourceSeq  int64
	ObservedAt time.Time
	ReceivedAt time.Time
	Metrics    json.RawMessage
	Qualified  bool
	LateByTime bool
}

// IngestOutcome 汇报一次入账的结果;重复投递不视为错误。
type IngestOutcome struct {
	Recorded         bool
	Duplicate        bool
	Classification   string
	StreamID         int64  // 事件流中的位置(新入账或已存在的事件)
	OccupyingEventID string // duplicate_seq 时占据该序号的事件
	Decision         continuity.Decision
}

// Ingest 在单一事务内完成:幂等去重 → 状态机判定 → 事件落库 →
// 缺口集合维护 → 来源状态更新 → 站点投影刷新。
// event_id 重放与 (station, source, seq) 争用都只保留先到的一条。
func (s *Store) Ingest(ctx context.Context, obs Observation, healthyStreak int) (IngestOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestOutcome{}, fmt.Errorf("开启事务: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 确保来源状态行存在,然后行级锁串行化同一来源的并发入账。
	if _, err := tx.Exec(ctx,
		`INSERT INTO source_state (station_id, source_id, status, status_reason)
		 VALUES ($1, $2, 'suspect', 'awaiting_first_sample')
		 ON CONFLICT DO NOTHING`,
		obs.StationID, obs.SourceID); err != nil {
		return IngestOutcome{}, fmt.Errorf("确保来源状态行: %w", err)
	}

	var st continuity.SourceState
	var status string
	if err := tx.QueryRow(ctx,
		`SELECT status, watermark, max_seen_seq, streak
		 FROM source_state WHERE station_id = $1 AND source_id = $2
		 FOR UPDATE`,
		obs.StationID, obs.SourceID).
		Scan(&status, &st.Watermark, &st.MaxSeen, &st.Streak); err != nil {
		return IngestOutcome{}, fmt.Errorf("锁定来源状态: %w", err)
	}
	st.Status = continuity.Status(status)

	decision := continuity.Evaluate(st, continuity.Input{
		Seq:        obs.SourceSeq,
		Qualified:  obs.Qualified,
		LateByTime: obs.LateByTime,
	}, healthyStreak)

	var streamID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO telemetry_events
		   (event_id, station_id, source_id, source_seq, observed_at, received_at,
		    metrics, qualified, late_by_time, classification)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT DO NOTHING
		 RETURNING id`,
		obs.EventID, obs.StationID, obs.SourceID, obs.SourceSeq,
		obs.ObservedAt, obs.ReceivedAt, obs.Metrics,
		obs.Qualified, obs.LateByTime, decision.Classification).
		Scan(&streamID)
	if errors.Is(err, pgx.ErrNoRows) {
		// 唯一约束冲突:event_id 重放,或另一接收端已占据该序号。
		dup, derr := s.classifyDuplicate(ctx, tx, obs)
		if derr != nil {
			return IngestOutcome{}, derr
		}
		return dup, nil // 事务随 defer 回滚,不产生任何状态变更
	}
	if err != nil {
		return IngestOutcome{}, fmt.Errorf("写入事件: %w", err)
	}

	if decision.OpenGap {
		if _, err := tx.Exec(ctx,
			`INSERT INTO source_gaps (station_id, source_id, seq)
			 SELECT $1, $2, g.seq
			 FROM generate_series($3::bigint, $4::bigint) AS g(seq)
			 WHERE NOT EXISTS (
			   SELECT 1 FROM telemetry_events e
			   WHERE e.station_id = $1 AND e.source_id = $2 AND e.source_seq = g.seq)
			 ON CONFLICT DO NOTHING`,
			obs.StationID, obs.SourceID, decision.OpenGapFrom, decision.OpenGapTo); err != nil {
			return IngestOutcome{}, fmt.Errorf("登记缺口: %w", err)
		}
	}
	if decision.FillGap {
		if _, err := tx.Exec(ctx,
			`DELETE FROM source_gaps WHERE station_id = $1 AND source_id = $2 AND seq = $3`,
			obs.StationID, obs.SourceID, decision.FillSeq); err != nil {
			return IngestOutcome{}, fmt.Errorf("补齐缺口: %w", err)
		}
	}

	isProjection := decision.Classification == continuity.ClassProjection
	isBackfill := decision.Classification == continuity.ClassBackfill
	if _, err := tx.Exec(ctx,
		`UPDATE source_state SET
		   status = $3,
		   streak = $4,
		   watermark = CASE WHEN $5::boolean THEN $6 ELSE watermark END,
		   max_seen_seq = GREATEST(max_seen_seq, $7),
		   contiguous_watermark = COALESCE(
		       (SELECT MIN(seq) FROM source_gaps g
		         WHERE g.station_id = $1 AND g.source_id = $2),
		       GREATEST(max_seen_seq, $7) + 1
		     ) - 1,
		   status_reason = CASE WHEN $8::boolean THEN $9 ELSE status_reason END,
		   status_seq = CASE WHEN $8::boolean THEN $10 ELSE status_seq END,
		   status_since = CASE WHEN $8::boolean THEN now() ELSE status_since END,
		   last_seq = CASE WHEN $11::boolean THEN $7 ELSE last_seq END,
		   last_observed_at = CASE WHEN $11::boolean THEN $12 ELSE last_observed_at END,
		   last_received_at = CASE WHEN $11::boolean THEN $13 ELSE last_received_at END,
		   recovered_seq = CASE WHEN $14::boolean THEN $7 ELSE recovered_seq END,
		   recovered_at = CASE WHEN $14::boolean THEN $13 ELSE recovered_at END,
		   last_backfill_seq = CASE WHEN $15::boolean THEN $7 ELSE last_backfill_seq END,
		   last_backfill_at = CASE WHEN $15::boolean THEN $13 ELSE last_backfill_at END,
		   revision = revision + 1,
		   updated_at = now()
		 WHERE station_id = $1 AND source_id = $2`,
		obs.StationID, obs.SourceID,
		string(decision.NewStatus), decision.NewStreak,
		decision.Advance, decision.NewWatermark,
		obs.SourceSeq,
		decision.StatusChanged, decision.Reason, decision.ReasonSeq,
		isProjection, obs.ObservedAt, obs.ReceivedAt,
		decision.Recovered,
		isBackfill); err != nil {
		return IngestOutcome{}, fmt.Errorf("更新来源状态: %w", err)
	}

	if err := refreshStationProjection(ctx, tx, obs.StationID); err != nil {
		return IngestOutcome{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return IngestOutcome{}, fmt.Errorf("提交事务: %w", err)
	}
	return IngestOutcome{
		Recorded:       true,
		Classification: decision.Classification,
		StreamID:       streamID,
		Decision:       decision,
	}, nil
}

// classifyDuplicate 区分 event_id 重放与序号争用;两者都只读,事务随后回滚。
func (s *Store) classifyDuplicate(ctx context.Context, tx pgx.Tx, obs Observation) (IngestOutcome, error) {
	var (
		streamID    int64
		existingEvt string
	)
	err := tx.QueryRow(ctx,
		`SELECT id, event_id FROM telemetry_events
		 WHERE event_id = $1 OR (station_id = $2 AND source_id = $3 AND source_seq = $4)
		 ORDER BY (event_id = $1) DESC
		 LIMIT 1`,
		obs.EventID, obs.StationID, obs.SourceID, obs.SourceSeq).
		Scan(&streamID, &existingEvt)
	if err != nil {
		return IngestOutcome{}, fmt.Errorf("判定重复类型: %w", err)
	}
	out := IngestOutcome{Duplicate: true, StreamID: streamID}
	if existingEvt == obs.EventID {
		out.Classification = continuity.ClassDuplicateEvent
	} else {
		out.Classification = continuity.ClassDuplicateSeq
		out.OccupyingEventID = existingEvt
	}
	return out, nil
}

// refreshStationProjection 由该站点全部来源状态聚合出站点投影( worst 优先:
// offline > suspect > healthy ),必须与触发它的事件处于同一事务。
func refreshStationProjection(ctx context.Context, tx pgx.Tx, stationID string) error {
	if _, err := tx.Exec(ctx,
		`INSERT INTO station_projection (station_id, status, basis, revision, updated_at)
		 SELECT $1,
		   CASE
		     WHEN COUNT(*) FILTER (WHERE status = 'offline') > 0 THEN 'offline'
		     WHEN COUNT(*) FILTER (WHERE status = 'suspect') > 0 THEN 'suspect'
		     ELSE 'healthy'
		   END,
		   jsonb_agg(
		     jsonb_build_object(
		       'source_id', source_id,
		       'status', status,
		       'watermark', watermark,
		       'contiguous_watermark', contiguous_watermark,
		       'streak', streak,
		       'status_reason', status_reason,
		       'status_seq', status_seq
		     ) ORDER BY source_id
		   ),
		   1,
		   now()
		 FROM source_state
		 WHERE station_id = $1
		 ON CONFLICT (station_id) DO UPDATE SET
		   status = EXCLUDED.status,
		   basis = EXCLUDED.basis,
		   revision = station_projection.revision + 1,
		   updated_at = EXCLUDED.updated_at`,
		stationID); err != nil {
		return fmt.Errorf("刷新站点投影: %w", err)
	}
	return nil
}

// SweepOffline 把静默超过 offlineAfter 的来源标记为 offline(连续合格计数清零),
// 并在同一事务内刷新受影响站点的投影。返回转为 offline 的来源数。
func (s *Store) SweepOffline(ctx context.Context, offlineAfter time.Duration) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("开启事务: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx,
		`UPDATE source_state SET
		   status = 'offline',
		   streak = 0,
		   status_reason = 'silence_timeout',
		   status_seq = NULL,
		   status_since = now(),
		   revision = revision + 1,
		   updated_at = now()
		 WHERE status <> 'offline'
		   AND last_received_at IS NOT NULL
		   AND last_received_at < now() - ($1::float8 * interval '1 second')
		 RETURNING station_id`,
		offlineAfter.Seconds())
	if err != nil {
		return 0, fmt.Errorf("清扫静默来源: %w", err)
	}
	stations := map[string]struct{}{}
	var swept int
	for rows.Next() {
		var stationID string
		if err := rows.Scan(&stationID); err != nil {
			rows.Close()
			return 0, fmt.Errorf("读取清扫结果: %w", err)
		}
		swept++
		stations[stationID] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("读取清扫结果: %w", err)
	}

	for stationID := range stations {
		if err := refreshStationProjection(ctx, tx, stationID); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("提交事务: %w", err)
	}
	return swept, nil
}

// GapRange 是一段仍待补齐的连续序号区间(两端含)。
type GapRange struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// SourceHandoff 是单个来源的交班视图。
type SourceHandoff struct {
	SourceID            string
	Status              string
	StatusReason        string
	StatusSeq           *int64
	StatusSince         *time.Time
	Watermark           int64
	ContiguousWatermark int64
	MaxSeenSeq          int64
	Streak              int
	LastSeq             *int64
	LastObservedAt      *time.Time
	LastReceivedAt      *time.Time
	RecoveredSeq        *int64
	RecoveredAt         *time.Time
	RecoveredObservedAt *time.Time
	LastBackfillSeq     *int64
	LastBackfillAt      *time.Time
	PendingGaps         []GapRange
}

// Handoff 是站点级交班视图:当前状态、依据与各来源待补齐区间。
type Handoff struct {
	StationID string
	Status    string
	Revision  int64
	UpdatedAt time.Time
	Sources   []SourceHandoff
}

// Handoff 读取站点投影与全部来源状态;站点不存在时 found=false。
func (s *Store) Handoff(ctx context.Context, stationID string) (Handoff, bool, error) {
	var h Handoff
	err := s.pool.QueryRow(ctx,
		`SELECT station_id, status, revision, updated_at
		 FROM station_projection WHERE station_id = $1`,
		stationID).Scan(&h.StationID, &h.Status, &h.Revision, &h.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Handoff{}, false, nil
	}
	if err != nil {
		return Handoff{}, false, fmt.Errorf("读取站点投影: %w", err)
	}

	rows, err := s.pool.Query(ctx,
		`SELECT s.source_id, s.status, s.status_reason, s.status_seq, s.status_since,
		        s.watermark, s.contiguous_watermark, s.max_seen_seq, s.streak,
		        s.last_seq, s.last_observed_at, s.last_received_at,
		        s.recovered_seq, s.recovered_at, r.observed_at,
		        s.last_backfill_seq, s.last_backfill_at
		 FROM source_state s
		 LEFT JOIN telemetry_events r
		   ON r.station_id = s.station_id
		  AND r.source_id = s.source_id
		  AND r.source_seq = s.recovered_seq
		 WHERE s.station_id = $1
		 ORDER BY s.source_id`,
		stationID)
	if err != nil {
		return Handoff{}, false, fmt.Errorf("读取来源状态: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sh SourceHandoff
		if err := rows.Scan(
			&sh.SourceID, &sh.Status, &sh.StatusReason, &sh.StatusSeq, &sh.StatusSince,
			&sh.Watermark, &sh.ContiguousWatermark, &sh.MaxSeenSeq, &sh.Streak,
			&sh.LastSeq, &sh.LastObservedAt, &sh.LastReceivedAt,
			&sh.RecoveredSeq, &sh.RecoveredAt, &sh.RecoveredObservedAt,
			&sh.LastBackfillSeq, &sh.LastBackfillAt,
		); err != nil {
			return Handoff{}, false, fmt.Errorf("解析来源状态: %w", err)
		}
		sh.PendingGaps = []GapRange{}
		h.Sources = append(h.Sources, sh)
	}
	if err := rows.Err(); err != nil {
		return Handoff{}, false, fmt.Errorf("解析来源状态: %w", err)
	}

	gaps, err := s.gapRanges(ctx, stationID)
	if err != nil {
		return Handoff{}, false, err
	}
	for i := range h.Sources {
		h.Sources[i].PendingGaps = gaps[h.Sources[i].SourceID]
		if h.Sources[i].PendingGaps == nil {
			h.Sources[i].PendingGaps = []GapRange{}
		}
	}
	return h, true, nil
}

// gapRanges 把逐条缺口记录聚合成连续区间,按来源分组。
func (s *Store) gapRanges(ctx context.Context, stationID string) (map[string][]GapRange, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT source_id, MIN(seq), MAX(seq) FROM (
		   SELECT source_id, seq,
		          seq - ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY seq) AS grp
		   FROM source_gaps
		   WHERE station_id = $1
		 ) t
		 GROUP BY source_id, grp
		 ORDER BY source_id, MIN(seq)`,
		stationID)
	if err != nil {
		return nil, fmt.Errorf("读取缺口区间: %w", err)
	}
	defer rows.Close()
	out := map[string][]GapRange{}
	for rows.Next() {
		var sourceID string
		var r GapRange
		if err := rows.Scan(&sourceID, &r.From, &r.To); err != nil {
			return nil, fmt.Errorf("解析缺口区间: %w", err)
		}
		out[sourceID] = append(out[sourceID], r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("解析缺口区间: %w", err)
	}
	return out, nil
}

// Event 是一条不可变事件(事件流中的元素)。
type Event struct {
	ID             int64
	EventID        string
	StationID      string
	SourceID       string
	SourceSeq      int64
	ObservedAt     time.Time
	ReceivedAt     time.Time
	Metrics        json.RawMessage
	Qualified      bool
	LateByTime     bool
	Classification string
	RecordedAt     time.Time
}

// ListEvents 按 id 升序返回 afterID 之后的事件,支持按站点/来源过滤。
// 多取一条用于判定 hasMore,调用方以最后一条的 id 作为续读游标。
func (s *Store) ListEvents(ctx context.Context, stationID, sourceID string, afterID int64, limit int) ([]Event, bool, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, event_id, station_id, source_id, source_seq,
		        observed_at, received_at, metrics, qualified, late_by_time,
		        classification, recorded_at
		 FROM telemetry_events
		 WHERE id > $1
		   AND ($2::text = '' OR station_id = $2)
		   AND ($3::text = '' OR source_id = $3)
		 ORDER BY id
		 LIMIT $4`,
		afterID, stationID, sourceID, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("读取事件流: %w", err)
	}
	defer rows.Close()

	events := []Event{}
	for rows.Next() {
		var e Event
		if err := rows.Scan(
			&e.ID, &e.EventID, &e.StationID, &e.SourceID, &e.SourceSeq,
			&e.ObservedAt, &e.ReceivedAt, &e.Metrics, &e.Qualified, &e.LateByTime,
			&e.Classification, &e.RecordedAt,
		); err != nil {
			return nil, false, fmt.Errorf("解析事件: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("解析事件: %w", err)
	}

	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	return events, hasMore, nil
}
