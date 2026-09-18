// Package httpapi 提供遥测接收、交班与事件流 HTTP 接口。
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/vancemichael/station-telemetry-service/internal/contracts"
	"github.com/vancemichael/station-telemetry-service/internal/domain"
	"github.com/vancemichael/station-telemetry-service/internal/store"
)

type Handler struct {
	Store  *store.Store
	Policy contracts.Policy
}

func New(s *store.Store, p contracts.Policy) *Handler {
	return &Handler{Store: s, Policy: p}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", h.ingest)
	mux.HandleFunc("GET /v1/stations", h.listStations)
	mux.HandleFunc("GET /v1/stations/{station_id}/handoff", h.handoff)
	mux.HandleFunc("GET /v1/events", h.stream)
	mux.HandleFunc("GET /health", h.health)
	return logRequests(mux)
}

// ingestRequest 与 domain.Event 分开定义，保证时间按 RFC3339 在边界层解析。
type ingestRequest struct {
	EventID    string          `json:"event_id"`
	StationID  string          `json:"station_id"`
	SourceID   string          `json:"source_id"`
	SourceSeq  int64           `json:"source_seq"`
	ObservedAt time.Time       `json:"observed_at"`
	ReceivedAt time.Time       `json:"received_at"`
	Metrics    *domain.Metrics `json:"metrics"`
}

type ingestResponse struct {
	Status              string                `json:"status"`
	EventID             string                `json:"event_id,omitempty"`
	WinnerEventID       string                `json:"winner_event_id,omitempty"`
	Watermark           int64                 `json:"watermark,omitempty"`
	HighestSeen         int64                 `json:"highest_seen,omitempty"`
	AppliedToProjection bool                  `json:"applied_to_projection"`
	Projection          *store.ProjectionView `json:"projection,omitempty"`
}

func (h *Handler) ingest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "请求体不是合法 JSON: "+err.Error())
		return
	}
	if req.ReceivedAt.IsZero() {
		req.ReceivedAt = time.Now().UTC()
	}

	e := domain.Event{
		EventID:    req.EventID,
		StationID:  req.StationID,
		SourceID:   req.SourceID,
		SourceSeq:  req.SourceSeq,
		ObservedAt: req.ObservedAt.UTC(),
		ReceivedAt: req.ReceivedAt.UTC(),
		Metrics:    req.Metrics,
	}
	if err := e.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	qualityStatus := domain.QualityStatus(e.Metrics, h.Policy)
	qualified := qualityStatus == domain.QualityQualified

	// 原始 payload 原样留存（已通过 Decode 校验合法性）。
	raw := map[string]any{
		"event_id":    e.EventID,
		"station_id":  e.StationID,
		"source_id":   e.SourceID,
		"source_seq":  e.SourceSeq,
		"observed_at": e.ObservedAt,
		"received_at": e.ReceivedAt,
		"metrics":     req.Metrics,
	}
	payload, _ := json.Marshal(raw)

	result, err := h.Store.Ingest(r.Context(), e, payload, qualified, qualityStatus)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := ingestResponse{
		Status:              result.Status,
		Watermark:           result.Watermark,
		HighestSeen:         result.HighestSeen,
		AppliedToProjection: result.AppliedToProjection,
		Projection:          result.Projection,
	}
	switch result.Status {
	case store.StatusDupEvent:
		// 重送是幂等成功，而非错误。
		resp.EventID = e.EventID
		writeJSON(w, http.StatusOK, resp)
	case store.StatusConflictSeq:
		// 两个接收端争用同一序号：本帧未落账，指出获胜事件。
		resp.WinnerEventID = result.ExistingEventID
		writeJSON(w, http.StatusConflict, resp)
	default:
		resp.EventID = e.EventID
		writeJSON(w, http.StatusCreated, resp)
	}
}

func (h *Handler) listStations(w http.ResponseWriter, r *http.Request) {
	stations, err := h.Store.ListStations(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stations": stations})
}

func (h *Handler) handoff(w http.ResponseWriter, r *http.Request) {
	stationID := r.PathValue("station_id")
	// 先把已超时静默的站点翻为 offline，交班结论才不会停留在旧状态。
	if _, err := h.Store.SweepStaleOffline(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	report, err := h.Store.Handoff(r.Context(), stationID)
	if errors.Is(err, store.ErrStationNotFound) {
		writeError(w, http.StatusNotFound, "站点暂无数据: "+stationID)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, report)
}

type streamResponse struct {
	Events     []store.StreamEvent `json:"events"`
	NextCursor int64               `json:"next_cursor"`
	HasMore    bool                `json:"has_more"`
}

func (h *Handler) stream(w http.ResponseWriter, r *http.Request) {
	var after int64
	if raw := r.URL.Query().Get("after"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "after 必须为非负整数游标")
			return
		}
		after = v
	}
	limit := 200
	if raw := r.URL.Query().Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			writeError(w, http.StatusBadRequest, "limit 必须为正整数")
			return
		}
		limit = v
	}
	events, hasMore, err := h.Store.ReadEvents(r.Context(), after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	var next int64 = after
	if n := len(events); n > 0 {
		next = events[n-1].ID
	}
	writeJSON(w, http.StatusOK, streamResponse{
		Events:     events,
		NextCursor: next,
		HasMore:    hasMore,
	})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if err := h.Store.DB.PingContext(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "db_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
