package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/vancemichael/station-telemetry-service/internal/contracts"
	"github.com/vancemichael/station-telemetry-service/internal/domain"
)

// 约束名与 migrations/001_initial.sql 中的 CONSTRAINT 保持一致，
// 用于区分两个接收端争用同序号与 event_id 重送。
const (
	constraintEventUniq = "telemetry_events_event_uniq"
	constraintSeqUniq   = "telemetry_events_seq_uniq"
)

// Ingest 状态。
const (
	StatusAccepted    = "accepted"        // 新入账
	StatusDupEvent    = "duplicate_event" // event_id 重送，不重复入账
	StatusConflictSeq = "conflict_seq"    // 序号已被另一事件占用
)

// Store 封装所有数据库访问。
type Store struct {
	DB     *sql.DB
	Policy contracts.Policy
}

func New(db *sql.DB, policy contracts.Policy) *Store {
	return &Store{DB: db, Policy: policy}
}

// IngestResult 是一次接收的结果。
type IngestResult struct {
	Status              string
	ExistingEventID     string // 序号冲突/重送时，已占用该序号的 event_id
	Watermark           int64  // 该来源事务后的连续水位
	HighestSeen         int64
	AppliedToProjection bool // 是否影响了当前投影（晚到历史样本为 false）
	Projection          *ProjectionView
}

// Ingest 在单一事务内完成：不可变事件入账、来源水位推进、缺口集合维护、
// 站点投影更新。任何一步失败整体回滚。
func (s *Store) Ingest(ctx context.Context, e domain.Event, payload []byte, qualified bool, qualityStatus string) (*IngestResult, error) {
	late := e.IsLate(s.Policy)

	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	// 事件入账：event_id 与 (station,source,seq) 两个唯一约束在数据库侧仲裁。
	_, err = tx.ExecContext(ctx, `
		INSERT INTO telemetry_events
			(event_id, station_id, source_id, source_seq, observed_at, received_at,
			 is_qualified, quality_status, is_late, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		e.EventID, e.StationID, e.SourceID, e.SourceSeq, e.ObservedAt, e.ReceivedAt,
		qualified, qualityStatus, late, payload)
	if err != nil {
		_ = tx.Rollback()
		var pqErr *pq.Error
		if errors.As(err, &pqErr) {
			switch pqErr.Constraint {
			case constraintEventUniq:
				return &IngestResult{Status: StatusDupEvent, ExistingEventID: e.EventID}, nil
			case constraintSeqUniq:
				winner := s.DB.QueryRowContext(ctx, `
					SELECT event_id FROM telemetry_events
					WHERE station_id=$1 AND source_id=$2 AND source_seq=$3`,
					e.StationID, e.SourceID, e.SourceSeq)
				var eventID string
				if scanErr := winner.Scan(&eventID); scanErr != nil {
					return nil, scanErr
				}
				return &IngestResult{Status: StatusConflictSeq, ExistingEventID: eventID}, nil
			}
		}
		return nil, fmt.Errorf("store: 事件入账失败: %w", err)
	}

	// 锁定来源水位行（不存在则创建），串行化同来源的并发接收。
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO source_watermarks (station_id, source_id)
		VALUES ($1, $2) ON CONFLICT DO NOTHING`, e.StationID, e.SourceID); err != nil {
		return nil, rollback(tx, err)
	}
	var watermark, highest int64
	if err := tx.QueryRowContext(ctx, `
		SELECT contiguous_seq, highest_seen FROM source_watermarks
		WHERE station_id=$1 AND source_id=$2 FOR UPDATE`,
		e.StationID, e.SourceID).Scan(&watermark, &highest); err != nil {
		return nil, rollback(tx, err)
	}

	// 缺口集合维护：
	//  - 跳号到达：登记中间所有序号为待补缺口；
	//  - 晚到补传：回填对应缺口（保留 first_seen/filled 审计痕迹）；
	// 水位只随“已到账事件的连续前缀”向前推进，任何情况下都不回退。
	if e.SourceSeq <= highest {
		if _, err := tx.ExecContext(ctx, `
			UPDATE sequence_gaps
			   SET filled_event_id = $4, filled_at = now()
			 WHERE station_id=$1 AND source_id=$2 AND missing_seq=$3
			   AND filled_event_id IS NULL`,
			e.StationID, e.SourceID, e.SourceSeq, e.EventID); err != nil {
			return nil, rollback(tx, err)
		}
	} else {
		for missing := watermark + 1; missing < e.SourceSeq; missing++ {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO sequence_gaps
					(station_id, source_id, missing_seq, first_seen_event_id)
				VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
				e.StationID, e.SourceID, missing, e.EventID); err != nil {
				return nil, rollback(tx, err)
			}
		}
		highest = e.SourceSeq
	}

	newWatermark := watermark
	for {
		next := newWatermark + 1
		if next > highest {
			break
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM telemetry_events
				 WHERE station_id=$1 AND source_id=$2 AND source_seq=$3)`,
			e.StationID, e.SourceID, next).Scan(&exists); err != nil {
			return nil, rollback(tx, err)
		}
		if !exists {
			break
		}
		newWatermark = next
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE source_watermarks
		   SET contiguous_seq=$3, highest_seen=$4, updated_at=now()
		 WHERE station_id=$1 AND source_id=$2`,
		e.StationID, e.SourceID, newWatermark, highest); err != nil {
		return nil, rollback(tx, err)
	}

	// 站点投影：只有“观测时间严格更新”的样本才能影响当前投影。
	// 晚到历史样本进入事件表与缺口集合，但不会回退或推进投影。
	//
	// 序号跳号（本帧到达后该来源仍存在未闭合缺口）意味着缺失样本的质量
	// 无法验证，此前的连续合格段必须作废，从本帧重新计数。
	linkJump := e.SourceSeq > watermark+1
	applied, view, err := s.applyProjection(ctx, tx, e, qualified, qualityStatus, linkJump)
	if err != nil {
		return nil, rollback(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &IngestResult{
		Status:              StatusAccepted,
		Watermark:           newWatermark,
		HighestSeen:         highest,
		AppliedToProjection: applied,
		Projection:          view,
	}, nil
}

// applyProjection 在同一事务内按 observed_at 守卫更新站点投影。
func (s *Store) applyProjection(ctx context.Context, tx *sql.Tx, e domain.Event, qualified bool, qualityStatus string, linkJump bool) (bool, *ProjectionView, error) {
	prev, err := scanProjectionRow(tx.QueryRowContext(ctx,
		`SELECT `+projectionColumns+` FROM station_projection WHERE station_id=$1 FOR UPDATE`,
		e.StationID))
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, nil, err
	}

	if prev != nil && !e.ObservedAt.After(prev.Last.ObservedAt) {
		// 历史/同刻样本：投影原样返回，不发生变化。
		return false, viewFromRow(prev, s.Policy), nil
	}

	breakReason := ""
	switch {
	case prev != nil && e.ObservedAt.Sub(prev.Last.ObservedAt) > time.Duration(s.Policy.OfflineAfterSeconds)*time.Second:
		breakReason = domain.ReasonFreshnessTimeout
	case linkJump:
		breakReason = domain.ReasonLinkGap
	}
	next := domain.Decide(toDomainProjection(prev), e, qualified, breakReason, s.Policy.HealthyStreak)
	if err := upsertProjection(ctx, tx, e, next, qualified, qualityStatus); err != nil {
		return false, nil, err
	}

	row, err := scanProjectionRow(tx.QueryRowContext(ctx,
		`SELECT `+projectionColumns+` FROM station_projection WHERE station_id=$1`, e.StationID))
	if err != nil {
		return false, nil, err
	}
	return true, viewFromRow(row, s.Policy), nil
}

func rollback(tx *sql.Tx, err error) error {
	_ = tx.Rollback()
	return err
}

// SweepStaleOffline 把观测静默超过 offline_after_seconds 的非 offline 站点
// 翻为 offline。交班接口读取前也会对单站调用一次，保证结论实时。
// 返回发生状态翻转的站点。
func (s *Store) SweepStaleOffline(ctx context.Context) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, `
		UPDATE station_projection
		   SET state = 'offline',
		       state_reason = 'freshness_timeout',
		       healthy_streak = 0,
		       streak_start_event_id = NULL,
		       streak_start_source_id = NULL,
		       streak_start_seq = NULL,
		       streak_start_observed_at = NULL,
		       offline_since = COALESCE(offline_since, last_observed_at + make_interval(secs => $1)),
		       recovered_event_id = NULL,
		       recovered_source_id = NULL,
		       recovered_seq = NULL,
		       recovered_observed_at = NULL,
		       recovered_streak = 0,
		       revision = revision + 1,
		       updated_at = now()
		 WHERE state <> 'offline'
		   AND last_observed_at IS NOT NULL
		   AND last_observed_at < now() - make_interval(secs => $1)
		 RETURNING station_id`,
		float64(s.Policy.OfflineAfterSeconds))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var stations []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		stations = append(stations, id)
	}
	return stations, rows.Err()
}

// StreamEvent 是事件流中的一帧。
type StreamEvent struct {
	ID            int64           `json:"id"`
	EventID       string          `json:"event_id"`
	StationID     string          `json:"station_id"`
	SourceID      string          `json:"source_id"`
	SourceSeq     int64           `json:"source_seq"`
	ObservedAt    time.Time       `json:"observed_at"`
	ReceivedAt    time.Time       `json:"received_at"`
	IsQualified   bool            `json:"is_qualified"`
	QualityStatus string          `json:"quality_status"`
	IsLate        bool            `json:"is_late"`
	Payload       json.RawMessage `json:"payload"`
	RecordedAt    time.Time       `json:"recorded_at"`
}

// ReadEvents 以事件自增 id 为游标支持断点续读：调用方记下最后一个 id，
// 重连后用 after=<last id> 续读；事件不可变，重放结果稳定。
func (s *Store) ReadEvents(ctx context.Context, after int64, limit int) ([]StreamEvent, bool, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id, event_id, station_id, source_id, source_seq, observed_at, received_at,
		       is_qualified, quality_status, is_late, payload, recorded_at
		  FROM telemetry_events
		 WHERE id > $1
		 ORDER BY id ASC
		 LIMIT $2`, after, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	events := make([]StreamEvent, 0, limit)
	for rows.Next() {
		var ev StreamEvent
		if err := rows.Scan(&ev.ID, &ev.EventID, &ev.StationID, &ev.SourceID, &ev.SourceSeq,
			&ev.ObservedAt, &ev.ReceivedAt, &ev.IsQualified, &ev.QualityStatus, &ev.IsLate,
			&ev.Payload, &ev.RecordedAt); err != nil {
			return nil, false, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	return events, hasMore, nil
}
