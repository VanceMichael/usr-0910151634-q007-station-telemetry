// Package api 提供 REST 接口:观测入账、交班视图、可续读事件流与合约查询。
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/vancemichael/station-telemetry-service/internal/continuity"
	"github.com/vancemichael/station-telemetry-service/internal/contract"
	"github.com/vancemichael/station-telemetry-service/internal/store"
)

// Server 持有 handler 所需的依赖。
type Server struct {
	st     *store.Store
	policy contract.Policy
}

// NewServer 构造 API 服务。
func NewServer(st *store.Store, policy contract.Policy) *Server {
	return &Server{st: st, policy: policy}
}

// Handler 装配路由(Go 1.22+ 方法模式)。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/observations", s.postObservation)
	mux.HandleFunc("GET /v1/stations/{stationID}/handoff", s.getHandoff)
	mux.HandleFunc("GET /v1/events", s.getEvents)
	mux.HandleFunc("GET /v1/contract", s.getContract)
	mux.HandleFunc("GET /health", s.getHealth)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// observationRequest 是 POST /v1/observations 的请求体。
type observationRequest struct {
	EventID    string          `json:"event_id"`
	StationID  string          `json:"station_id"`
	SourceID   string          `json:"source_id"`
	SourceSeq  int64           `json:"source_seq"`
	ObservedAt time.Time       `json:"observed_at"`
	ReceivedAt *time.Time      `json:"received_at"` // 缺省取服务端当前时间
	Metrics    json.RawMessage `json:"metrics"`
}

func (r *observationRequest) validate() string {
	switch {
	case r.EventID == "":
		return "event_id 不能为空"
	case r.StationID == "":
		return "station_id 不能为空"
	case r.SourceID == "":
		return "source_id 不能为空"
	case r.SourceSeq < 1:
		return "source_seq 必须 >= 1"
	case r.ObservedAt.IsZero():
		return "observed_at 缺失或格式非法(需 RFC3339)"
	}
	return ""
}

func (s *Server) postObservation(w http.ResponseWriter, r *http.Request) {
	var req observationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON: "+err.Error())
		return
	}
	if msg := req.validate(); msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}
	var metrics contract.Metrics
	if len(req.Metrics) > 0 {
		if err := json.Unmarshal(req.Metrics, &metrics); err != nil {
			writeError(w, http.StatusBadRequest, "metrics 字段非法: "+err.Error())
			return
		}
	} else {
		req.Metrics = json.RawMessage(`{}`)
	}
	receivedAt := time.Now().UTC()
	if req.ReceivedAt != nil {
		receivedAt = req.ReceivedAt.UTC()
	}

	obs := store.Observation{
		EventID:    req.EventID,
		StationID:  req.StationID,
		SourceID:   req.SourceID,
		SourceSeq:  req.SourceSeq,
		ObservedAt: req.ObservedAt.UTC(),
		ReceivedAt: receivedAt,
		Metrics:    req.Metrics,
		Qualified:  s.policy.Qualified(metrics),
		LateByTime: s.policy.LateByTime(req.ObservedAt.UTC(), receivedAt),
	}

	outcome, err := s.st.Ingest(r.Context(), obs, s.policy.HealthyStreak)
	if err != nil {
		slog.Error("入账失败", "error", err, "event_id", req.EventID)
		writeError(w, http.StatusInternalServerError, "入账失败")
		return
	}

	resp := map[string]any{
		"recorded":       outcome.Recorded,
		"duplicate":      outcome.Duplicate,
		"classification": outcome.Classification,
		"stream_id":      outcome.StreamID,
	}
	switch {
	case outcome.Duplicate:
		if outcome.Classification == continuity.ClassDuplicateSeq {
			resp["occupying_event_id"] = outcome.OccupyingEventID
		}
	default:
		d := outcome.Decision
		resp["qualified"] = obs.Qualified
		resp["late_by_time"] = obs.LateByTime
		resp["status"] = string(d.NewStatus)
		resp["streak"] = d.NewStreak
		resp["recovered"] = d.Recovered
		if d.Advance {
			resp["watermark"] = d.NewWatermark
		}
		if d.OpenGap {
			resp["gap_opened"] = map[string]int64{"from": d.OpenGapFrom, "to": d.OpenGapTo}
		}
		if d.FillGap {
			resp["gap_filled"] = d.FillSeq
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// sampleRef 指向某个关键样本(最近推进投影的样本、真正恢复发生的样本)。
type sampleRef struct {
	SourceSeq  int64     `json:"source_seq"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
}

type sourceHandoffJSON struct {
	SourceID              string           `json:"source_id"`
	Status                string           `json:"status"`
	StatusReason          string           `json:"status_reason"`
	StatusSeq             *int64           `json:"status_seq,omitempty"`
	StatusSince           *time.Time       `json:"status_since,omitempty"`
	Watermark             int64            `json:"watermark"`
	ContiguousWatermark   int64            `json:"contiguous_watermark"`
	MaxSeenSeq            int64            `json:"max_seen_seq"`
	Streak                int              `json:"streak"`
	HealthyStreakRequired int              `json:"healthy_streak_required"`
	PendingGaps           []store.GapRange `json:"pending_gaps"`
	LastSample            *sampleRef       `json:"last_sample,omitempty"`
	LastRecovery          *sampleRef       `json:"last_recovery,omitempty"`
	LastBackfill          *backfillRef     `json:"last_backfill,omitempty"`
}

type backfillRef struct {
	SourceSeq  int64     `json:"source_seq"`
	ReceivedAt time.Time `json:"received_at"`
}

func (s *Server) getHandoff(w http.ResponseWriter, r *http.Request) {
	stationID := r.PathValue("stationID")
	h, found, err := s.st.Handoff(r.Context(), stationID)
	if err != nil {
		slog.Error("交班查询失败", "error", err, "station_id", stationID)
		writeError(w, http.StatusInternalServerError, "交班查询失败")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "站点不存在或尚无数据: "+stationID)
		return
	}

	sources := make([]sourceHandoffJSON, 0, len(h.Sources))
	for _, src := range h.Sources {
		sj := sourceHandoffJSON{
			SourceID:              src.SourceID,
			Status:                src.Status,
			StatusReason:          src.StatusReason,
			StatusSeq:             src.StatusSeq,
			StatusSince:           src.StatusSince,
			Watermark:             src.Watermark,
			ContiguousWatermark:   src.ContiguousWatermark,
			MaxSeenSeq:            src.MaxSeenSeq,
			Streak:                src.Streak,
			HealthyStreakRequired: s.policy.HealthyStreak,
			PendingGaps:           src.PendingGaps,
		}
		if src.LastSeq != nil && src.LastObservedAt != nil && src.LastReceivedAt != nil {
			sj.LastSample = &sampleRef{
				SourceSeq:  *src.LastSeq,
				ObservedAt: *src.LastObservedAt,
				ReceivedAt: *src.LastReceivedAt,
			}
		}
		if src.RecoveredSeq != nil && src.RecoveredAt != nil {
			ref := sampleRef{SourceSeq: *src.RecoveredSeq, ReceivedAt: *src.RecoveredAt}
			if src.RecoveredObservedAt != nil {
				ref.ObservedAt = *src.RecoveredObservedAt
			}
			sj.LastRecovery = &ref
		}
		if src.LastBackfillSeq != nil && src.LastBackfillAt != nil {
			sj.LastBackfill = &backfillRef{SourceSeq: *src.LastBackfillSeq, ReceivedAt: *src.LastBackfillAt}
		}
		sources = append(sources, sj)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"station_id":            h.StationID,
		"generated_at":          time.Now().UTC(),
		"status":                h.Status,
		"projection_revision":   h.Revision,
		"projection_updated_at": h.UpdatedAt,
		"contract": map[string]any{
			"version":                  s.policy.Version,
			"healthy_streak":           s.policy.HealthyStreak,
			"offline_after_seconds":    s.policy.OfflineAfterSeconds,
			"allowed_lateness_seconds": s.policy.AllowedLatenessSeconds,
		},
		"sources": sources,
	})
}

func (s *Server) getEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var afterID int64
	if raw := q.Get("after"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "after 必须是非负整数游标")
			return
		}
		afterID = v
	}
	limit := 100
	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > 1000 {
			writeError(w, http.StatusBadRequest, "limit 必须在 1..1000 之间")
			return
		}
		limit = v
	}

	events, hasMore, err := s.st.ListEvents(r.Context(), q.Get("station_id"), q.Get("source_id"), afterID, limit)
	if err != nil {
		slog.Error("事件流查询失败", "error", err)
		writeError(w, http.StatusInternalServerError, "事件流查询失败")
		return
	}

	type eventJSON struct {
		StreamID       int64           `json:"stream_id"`
		EventID        string          `json:"event_id"`
		StationID      string          `json:"station_id"`
		SourceID       string          `json:"source_id"`
		SourceSeq      int64           `json:"source_seq"`
		ObservedAt     time.Time       `json:"observed_at"`
		ReceivedAt     time.Time       `json:"received_at"`
		Metrics        json.RawMessage `json:"metrics"`
		Qualified      bool            `json:"qualified"`
		LateByTime     bool            `json:"late_by_time"`
		Classification string          `json:"classification"`
		RecordedAt     time.Time       `json:"recorded_at"`
	}
	out := make([]eventJSON, 0, len(events))
	var nextCursor = afterID
	for _, e := range events {
		nextCursor = e.ID
		out = append(out, eventJSON{
			StreamID: e.ID, EventID: e.EventID, StationID: e.StationID,
			SourceID: e.SourceID, SourceSeq: e.SourceSeq,
			ObservedAt: e.ObservedAt, ReceivedAt: e.ReceivedAt,
			Metrics: e.Metrics, Qualified: e.Qualified, LateByTime: e.LateByTime,
			Classification: e.Classification, RecordedAt: e.RecordedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"events":      out,
		"count":       len(out),
		"next_cursor": nextCursor,
		"has_more":    hasMore,
	})
}

func (s *Server) getContract(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.policy)
}

func (s *Server) getHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.st.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "数据库不可达")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
