package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/vancemichael/station-telemetry-service/internal/contracts"
	"github.com/vancemichael/station-telemetry-service/internal/domain"
	"github.com/vancemichael/station-telemetry-service/internal/sequence"
)

// projectionColumns 必须与 migrations/001_initial.sql 中 station_projection 的
// 列保持一致，scanProjectionRow 按此顺序扫描（不含 station_id）。
const projectionColumns = `
	state, state_reason, revision, healthy_streak,
	streak_start_event_id, streak_start_source_id, streak_start_seq, streak_start_observed_at,
	last_event_id, last_source_id, last_seq, last_observed_at,
	last_qualified_event_id, last_qualified_source_id, last_qualified_seq, last_qualified_observed_at,
	last_quality_status, offline_since,
	recovered_event_id, recovered_source_id, recovered_seq, recovered_observed_at, recovered_streak,
	updated_at`

type projectionRow struct {
	State       string
	StateReason string
	Revision    int64
	Streak      int

	StreakStart *domain.SampleRef
	Last        *domain.SampleRef
	LastGood    *domain.SampleRef
	LastQuality sql.NullString

	OfflineSince    sql.NullTime
	Recovered       *domain.SampleRef
	RecoveredStreak int
	UpdatedAt       time.Time
}

type rowScanner interface {
	Scan(dest ...any) error
}

// scanProjectionRow 从已定位到单行的 scanner 读取投影。
// extra 追加在投影列之后扫描（ListStations 把 station_id 放在末尾）。
func scanProjectionRow(scanner rowScanner, extra ...any) (*projectionRow, error) {
	var r projectionRow
	var streakStartEvent, streakStartSource, lastEvent, lastSource, lastGoodEvent, lastGoodSource sql.NullString
	var streakStartSeq, lastSeq, lastGoodSeq sql.NullInt64
	var streakStartAt, lastAt, lastGoodAt sql.NullTime
	var recoveredEvent, recoveredSource sql.NullString
	var recoveredSeq sql.NullInt64
	var recoveredAt sql.NullTime

	dest := []any{
		&r.State, &r.StateReason, &r.Revision, &r.Streak,
		&streakStartEvent, &streakStartSource, &streakStartSeq, &streakStartAt,
		&lastEvent, &lastSource, &lastSeq, &lastAt,
		&lastGoodEvent, &lastGoodSource, &lastGoodSeq, &lastGoodAt,
		&r.LastQuality, &r.OfflineSince,
		&recoveredEvent, &recoveredSource, &recoveredSeq, &recoveredAt, &r.RecoveredStreak,
		&r.UpdatedAt,
	}
	dest = append(dest, extra...)
	err := scanner.Scan(dest...)
	if err != nil {
		return nil, err
	}
	r.StreakStart = refFrom(streakStartEvent, streakStartSource, streakStartSeq, streakStartAt)
	r.Last = refFrom(lastEvent, lastSource, lastSeq, lastAt)
	r.LastGood = refFrom(lastGoodEvent, lastGoodSource, lastGoodSeq, lastGoodAt)
	r.Recovered = refFrom(recoveredEvent, recoveredSource, recoveredSeq, recoveredAt)
	return &r, nil
}

func refFrom(eventID, sourceID sql.NullString, seq sql.NullInt64, at sql.NullTime) *domain.SampleRef {
	if !eventID.Valid {
		return nil
	}
	return &domain.SampleRef{
		EventID:    eventID.String,
		SourceID:   sourceID.String,
		SourceSeq:  seq.Int64,
		ObservedAt: at.Time,
	}
}

func toDomainProjection(r *projectionRow) domain.Projection {
	if r == nil {
		return domain.Projection{}
	}
	p := domain.Projection{
		State:           r.State,
		StateReason:     r.StateReason,
		Revision:        r.Revision,
		Streak:          r.Streak,
		StreakStart:     r.StreakStart,
		LastSample:      r.Last,
		LastGood:        r.LastGood,
		Recovered:       r.Recovered,
		RecoveredStreak: r.RecoveredStreak,
	}
	if r.OfflineSince.Valid {
		t := r.OfflineSince.Time
		p.OfflineSince = &t
	}
	return p
}

// upsertProjection 把状态机结论写回当前投影。revision 已由 domain.Decide 递增，
// 这里直接落值，不再二次 +1。
func upsertProjection(ctx context.Context, tx *sql.Tx, e domain.Event, p domain.Projection, qualified bool, qualityStatus string) error {
	var lastGood *domain.SampleRef
	switch {
	case qualified:
		ref := domain.SampleRef{EventID: e.EventID, SourceID: e.SourceID, SourceSeq: e.SourceSeq, ObservedAt: e.ObservedAt}
		lastGood = &ref
	default:
		lastGood = p.LastGood // 不合格样本保留此前的最后合格样本指针
	}

	args := []any{e.StationID, p.State, p.StateReason, p.Revision, p.Streak}
	args = append(args, refArgs(p.StreakStart)...)
	args = append(args, e.EventID, e.SourceID, e.SourceSeq, e.ObservedAt)
	args = append(args, refArgs(lastGood)...)
	args = append(args, qualityStatus, p.OfflineSince)
	args = append(args, refArgs(p.Recovered)...)
	args = append(args, p.RecoveredStreak)

	_, err := tx.ExecContext(ctx, upsertProjectionSQL, args...)
	return err
}

// refArgs 把样本引用展开成 (event_id, source_id, seq, observed_at) 四个标量参数；
// 空引用展开成四个 nil。
func refArgs(r *domain.SampleRef) []any {
	if r == nil {
		return []any{nil, nil, nil, nil}
	}
	return []any{r.EventID, r.SourceID, r.SourceSeq, r.ObservedAt}
}

const upsertProjectionSQL = `
		INSERT INTO station_projection (
			station_id, state, state_reason, revision, healthy_streak,
			streak_start_event_id, streak_start_source_id, streak_start_seq, streak_start_observed_at,
			last_event_id, last_source_id, last_seq, last_observed_at,
			last_qualified_event_id, last_qualified_source_id, last_qualified_seq, last_qualified_observed_at,
			last_quality_status, offline_since,
			recovered_event_id, recovered_source_id, recovered_seq, recovered_observed_at, recovered_streak,
			updated_at
		) VALUES (
			$1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12,$13, $14,$15,$16,$17, $18,$19, $20,$21,$22,$23,$24, now()
		)
		ON CONFLICT (station_id) DO UPDATE SET
			state = EXCLUDED.state,
			state_reason = EXCLUDED.state_reason,
			revision = EXCLUDED.revision,
			healthy_streak = EXCLUDED.healthy_streak,
			streak_start_event_id = EXCLUDED.streak_start_event_id,
			streak_start_source_id = EXCLUDED.streak_start_source_id,
			streak_start_seq = EXCLUDED.streak_start_seq,
			streak_start_observed_at = EXCLUDED.streak_start_observed_at,
			last_event_id = EXCLUDED.last_event_id,
			last_source_id = EXCLUDED.last_source_id,
			last_seq = EXCLUDED.last_seq,
			last_observed_at = EXCLUDED.last_observed_at,
			last_qualified_event_id = EXCLUDED.last_qualified_event_id,
			last_qualified_source_id = EXCLUDED.last_qualified_source_id,
			last_qualified_seq = EXCLUDED.last_qualified_seq,
			last_qualified_observed_at = EXCLUDED.last_qualified_observed_at,
			last_quality_status = EXCLUDED.last_quality_status,
			offline_since = EXCLUDED.offline_since,
			recovered_event_id = EXCLUDED.recovered_event_id,
			recovered_source_id = EXCLUDED.recovered_source_id,
			recovered_seq = EXCLUDED.recovered_seq,
			recovered_observed_at = EXCLUDED.recovered_observed_at,
			recovered_streak = EXCLUDED.recovered_streak,
			updated_at = now()`

// ProjectionView 是投影对外的只读形状（接收回执与交班接口共用）。
type ProjectionView struct {
	StationID       string            `json:"station_id"`
	State           string            `json:"state"`
	StateReason     string            `json:"state_reason"`
	StateBasis      string            `json:"state_basis"`
	Revision        int64             `json:"revision"`
	HealthyStreak   int               `json:"healthy_streak"`
	RequiredStreak  int               `json:"required_streak"`
	StreakStart     *domain.SampleRef `json:"streak_start,omitempty"`
	LastSample      *domain.SampleRef `json:"last_sample,omitempty"`
	LastQualified   *domain.SampleRef `json:"last_qualified_sample,omitempty"`
	OfflineSince    *time.Time        `json:"offline_since,omitempty"`
	Recovered       *domain.SampleRef `json:"recovered_at_sample,omitempty"`
	RecoveredStreak int               `json:"recovered_streak,omitempty"`
	UpdatedAt       time.Time         `json:"updated_at"`
}

func viewFromRow(r *projectionRow, policy contracts.Policy) *ProjectionView {
	v := &ProjectionView{
		State:           r.State,
		StateReason:     r.StateReason,
		Revision:        r.Revision,
		HealthyStreak:   r.Streak,
		RequiredStreak:  policy.HealthyStreak,
		StreakStart:     r.StreakStart,
		LastSample:      r.Last,
		LastQualified:   r.LastGood,
		Recovered:       r.Recovered,
		RecoveredStreak: r.RecoveredStreak,
		UpdatedAt:       r.UpdatedAt,
	}
	if r.OfflineSince.Valid {
		t := r.OfflineSince.Time
		v.OfflineSince = &t
	}
	v.StateBasis = basisText(r, policy)
	return v
}

// basisText 生成交班界面上的“当前状态依据”说明：锚定具体样本与计数，
// 避免几分钟前的正常数据被误认作站点刚刚恢复。
func basisText(r *projectionRow, p contracts.Policy) string {
	switch r.State {
	case domain.StateHealthy:
		if r.Recovered != nil {
			return fmt.Sprintf(
				"healthy：连续合格段始于 %s，第 %d 个合格样本为 %s（%s seq=%d，observed_at=%s），达到契约 %s 要求的 %d 连——真正恢复发生在该样本",
				refLabel(r.StreakStart), r.RecoveredStreak,
				r.Recovered.EventID, r.Recovered.SourceID, r.Recovered.SourceSeq,
				r.Recovered.ObservedAt.Format(time.RFC3339), p.Version, p.HealthyStreak)
		}
		return "healthy：连续合格样本满足契约要求"
	case domain.StateSuspect:
		switch r.StateReason {
		case domain.ReasonLowQuality:
			return fmt.Sprintf("suspect：最新样本 %s 质量不合格（%s），连续合格计数已清零，需重新取得 %d 个连续合格样本",
				refLabel(r.Last), r.LastQuality.String, p.HealthyStreak)
		case domain.ReasonLinkGap:
			return fmt.Sprintf("suspect：来源序号跳号，缺口样本质量无法验证，连续合格段自 %s 重新计数，当前 %d/%d；缺口补齐前旧正常数据不能证明站点已恢复",
				refLabel(r.StreakStart), r.Streak, p.HealthyStreak)
		default:
			return fmt.Sprintf("suspect：恢复计数 %d/%d，连续合格段始于 %s，还需 %d 个合格样本；本段之前的正常数据不计入恢复计数",
				r.Streak, p.HealthyStreak, refLabel(r.StreakStart), p.HealthyStreak-r.Streak)
		}
	case domain.StateOffline:
		return fmt.Sprintf("offline：观测静默超过 %d 秒（最后样本 %s），自 %s 起离线；恢复须重新取得 %d 个连续合格样本，历史正常数据不算恢复",
			p.OfflineAfterSeconds, refLabel(r.Last),
			r.OfflineSince.Time.Format(time.RFC3339), p.HealthyStreak)
	default:
		return r.StateReason
	}
}

func refLabel(r *domain.SampleRef) string {
	if r == nil {
		return "未知样本"
	}
	return fmt.Sprintf("%s（%s seq=%d @%s）", r.EventID, r.SourceID, r.SourceSeq, r.ObservedAt.Format(time.RFC3339))
}

// SourceStatus 是交班视图中单个来源的水位与缺口情况。
type SourceStatus struct {
	SourceID      string           `json:"source_id"`
	ContiguousSeq int64            `json:"contiguous_seq"`
	HighestSeen   int64            `json:"highest_seen"`
	OpenGaps      []int64          `json:"open_gaps"`
	MissingRanges []sequence.Range `json:"missing_ranges"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

// HandoffReport 是交班接口的完整结论。
type HandoffReport struct {
	StationID  string          `json:"station_id"`
	Projection *ProjectionView `json:"projection"`
	Sources    []SourceStatus  `json:"sources"`
	Now        time.Time       `json:"generated_at"`
}

// ErrStationNotFound 表示交班查询的站点尚无任何数据。
var ErrStationNotFound = errors.New("station not found")

// Handoff 组装交班结论：当前投影（含状态依据与恢复锚点）+ 各来源水位 +
// 仍待补齐的序号区间。
func (s *Store) Handoff(ctx context.Context, stationID string) (*HandoffReport, error) {
	row, err := scanProjectionRow(s.DB.QueryRowContext(ctx,
		`SELECT `+projectionColumns+` FROM station_projection WHERE station_id=$1`, stationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrStationNotFound
	}
	if err != nil {
		return nil, err
	}

	rows, err := s.DB.QueryContext(ctx, `
		SELECT w.source_id, w.contiguous_seq, w.highest_seen, w.updated_at, g.missing_seq
		  FROM source_watermarks w
		  LEFT JOIN sequence_gaps g
		    ON g.station_id = w.station_id
		   AND g.source_id = w.source_id
		   AND g.filled_event_id IS NULL
		 WHERE w.station_id = $1
		 ORDER BY w.source_id, g.missing_seq`, stationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sources := make([]SourceStatus, 0)
	index := map[string]int{}
	for rows.Next() {
		var src SourceStatus
		var missing sql.NullInt64
		if err := rows.Scan(&src.SourceID, &src.ContiguousSeq, &src.HighestSeen, &src.UpdatedAt, &missing); err != nil {
			return nil, err
		}
		pos, ok := index[src.SourceID]
		if !ok {
			pos = len(sources)
			index[src.SourceID] = pos
			src.OpenGaps = []int64{}
			sources = append(sources, src)
		}
		if missing.Valid {
			sources[pos].OpenGaps = append(sources[pos].OpenGaps, missing.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range sources {
		ranges := sequence.Ranges(sources[i].OpenGaps)
		if ranges == nil {
			ranges = []sequence.Range{}
		}
		sources[i].MissingRanges = ranges
	}

	view := viewFromRow(row, s.Policy)
	view.StationID = stationID
	return &HandoffReport{
		StationID:  stationID,
		Projection: view,
		Sources:    sources,
		Now:        time.Now().UTC(),
	}, nil
}

// ListStations 返回有投影的全部站点当前状态。
func (s *Store) ListStations(ctx context.Context) ([]ProjectionView, error) {
	// station_id 放末尾，前面的列与 projectionColumns 完全一致。
	rows, err := s.DB.QueryContext(ctx,
		`SELECT `+projectionColumns+`, station_id FROM station_projection ORDER BY station_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProjectionView
	for rows.Next() {
		var stationID string
		r, err := scanProjectionRow(rows, &stationID)
		if err != nil {
			return nil, err
		}
		v := viewFromRow(r, s.Policy)
		v.StationID = stationID
		out = append(out, *v)
	}
	return out, rows.Err()
}
