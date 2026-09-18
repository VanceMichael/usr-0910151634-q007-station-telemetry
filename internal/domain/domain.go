// Package domain 承载遥测连续性的核心规则：质量合格判定、晚到判定与
// 站点状态（healthy/suspect/offline）的推进规则。
//
// 这里的规则不直接访问数据库，store 层在单一事务内读取旧投影、调用
// Decide 得到新投影并落库，保证投影与事件、水位、缺口集合原子一致。
package domain

import (
	"errors"
	"time"

	"github.com/vancemichael/station-telemetry-service/internal/contracts"
)

// 质量判定结果取值。
const (
	QualityQualified      = "qualified"
	QualityLowSatellites  = "low_satellites"
	QualityHighPDOP       = "high_pdop"
	QualityMetricsMissing = "metrics_missing"
)

// 站点投影状态取值。
const (
	StateHealthy = "healthy"
	StateSuspect = "suspect"
	StateOffline = "offline"
)

// 状态依据（state_reason）取值，交班接口直接展示。
const (
	ReasonInitial          = "initial"           // 站点首帧，计数刚开始
	ReasonStreakComplete   = "streak_complete"   // 已取得连续合格样本
	ReasonStreakIncomplete = "recovery_streak"   // 恢复计数未满
	ReasonLowQuality       = "quality_failed"    // 最新样本质量不合格
	ReasonFreshnessTimeout = "freshness_timeout" // 超过 offline_after_seconds 无新观测
	ReasonLinkGap          = "link_gap"          // 来源序号跳号，缺失样本的质量无法验证
)

// Metrics 是上游随观测上报的质量指标。
type Metrics struct {
	Satellites *int     `json:"satellites"`
	PDOP       *float64 `json:"pdop"`
}

// Event 是接收端收到的一帧遥测。
type Event struct {
	EventID    string    `json:"event_id"`
	StationID  string    `json:"station_id"`
	SourceID   string    `json:"source_id"`
	SourceSeq  int64     `json:"source_seq"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
	Metrics    *Metrics  `json:"metrics"`
}

// Validate 校验必填字段。时间由 HTTP 层按 RFC3339 解析。
func (e Event) Validate() error {
	switch {
	case e.EventID == "":
		return errors.New("event_id 不能为空")
	case e.StationID == "":
		return errors.New("station_id 不能为空")
	case e.SourceID == "":
		return errors.New("source_id 不能为空")
	case e.SourceSeq < 1:
		return errors.New("source_seq 必须为正整数")
	case e.ObservedAt.IsZero():
		return errors.New("observed_at 不能为空")
	case e.ReceivedAt.IsZero():
		return errors.New("received_at 不能为空")
	default:
		return nil
	}
}

// IsLate 按契约 allowed_lateness_seconds 判定是否为跨链路补传的晚到观测：
// 接收到的时刻比观测时刻晚超过允许窗口。
func (e Event) IsLate(policy contracts.Policy) bool {
	return e.ReceivedAt.Sub(e.ObservedAt) > time.Duration(policy.AllowedLatenessSecs)*time.Second
}

// QualityStatus 按契约质量门槛判定样本是否合格。
// 判定顺序固定：指标缺失 -> 卫星数不足 -> PDOP 超限。
func QualityStatus(m *Metrics, policy contracts.Policy) string {
	if m == nil || m.Satellites == nil || m.PDOP == nil {
		return QualityMetricsMissing
	}
	if *m.Satellites < policy.Quality.MinimumSatellites {
		return QualityLowSatellites
	}
	if *m.PDOP > policy.Quality.MaximumPDOP {
		return QualityHighPDOP
	}
	return QualityQualified
}

// SampleRef 指向某一个具体样本，用于在交班结果中说明“依据是哪个样本”。
type SampleRef struct {
	EventID    string    `json:"event_id"`
	SourceID   string    `json:"source_id"`
	SourceSeq  int64     `json:"source_seq"`
	ObservedAt time.Time `json:"observed_at"`
}

// Projection 是站点当前投影。可空字段用指针表示“尚无依据”。
type Projection struct {
	State       string
	StateReason string
	Revision    int64
	Streak      int

	StreakStart *SampleRef
	LastSample  *SampleRef
	LastGood    *SampleRef

	OfflineSince *time.Time

	// Recovered 锚定“真正恢复”的那个样本（计数达到阈值的样本）。
	Recovered       *SampleRef
	RecoveredStreak int
}

// Decide 是状态机：只对“向前的”新样本调用；晚到历史样本不得调用本函数
// （见 store 层的 observed_at 守卫），因此当前投影不会被历史数据回退或推进。
//
//	prev        该站点此前的投影（首帧时为零值）
//	qualified   本样本质量是否合格
//	breakReason 此前的连续样本段为何不能继续计数：
//	            "" 表示连续；ReasonFreshnessTimeout 表示观测静默超过窗口；
//	            ReasonLinkGap 表示来源序号跳号（缺失样本质量无从验证）。
//	required    契约要求的连续合格样本数
func Decide(prev Projection, e Event, qualified bool, breakReason string, required int) Projection {
	next := prev
	next.Revision++
	next.OfflineSince = nil // 已收到向前的新观测，离线计时清零

	ref := SampleRef{EventID: e.EventID, SourceID: e.SourceID, SourceSeq: e.SourceSeq, ObservedAt: e.ObservedAt}
	next.LastSample = &ref

	isFirst := prev.LastSample == nil
	broken := breakReason != ""

	switch {
	case !qualified:
		// 质量不合格：连续段中断，必须重新攒满；上一个 healthy 周期的恢复
		// 锚点同时作废，交班不得再拿旧周期的样本当作当前依据。
		next.Streak = 0
		next.StreakStart = nil
		next.LastGood = nil
		next.Recovered = nil
		next.RecoveredStreak = 0
		next.State = StateSuspect
		next.StateReason = ReasonLowQuality
	case isFirst || broken:
		// 全新站点，或此前的连续性（时间/序号）已被打断：重新计数。
		next.Streak = 1
		start := ref
		next.StreakStart = &start
		next.LastGood = &ref
		next.Recovered = nil
		next.RecoveredStreak = 0
		next.applyStreakState(required)
		if next.Streak < required && breakReason == ReasonLinkGap {
			next.StateReason = ReasonLinkGap
		}
		if isFirst {
			next.StateReason = ReasonStreakIncomplete
		}
	default:
		if prev.Streak == 0 {
			start := ref
			next.StreakStart = &start
			next.Streak = 1
		} else {
			next.Streak = prev.Streak + 1
		}
		next.LastGood = &ref
		next.applyStreakState(required)
	}
	return next
}

// applyStreakState 在 streak 计数变化后决定状态，并在跨过阈值的那一帧
// 锚定真正恢复的样本。
func (p *Projection) applyStreakState(required int) {
	if p.Streak >= required {
		if p.State != StateHealthy {
			// 仅在“非 healthy -> healthy”的这一帧记录恢复锚点：
			// 前 required-1 个合格样本都不算恢复。
			anchor := *p.LastGood
			p.Recovered = &anchor
			p.RecoveredStreak = p.Streak
		}
		p.State = StateHealthy
		p.StateReason = ReasonStreakComplete
		return
	}
	p.State = StateSuspect
	p.StateReason = ReasonStreakIncomplete
}
