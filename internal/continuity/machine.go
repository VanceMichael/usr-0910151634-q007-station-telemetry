// Package continuity 是纯函数状态机:给定来源当前状态与一个已去重的观测,
// 判定它走"投影推进"还是"历史补录"路径,并给出全部状态变更。
// 不触碰数据库,便于单测;SQL 落地由 store 层按 Decision 执行。
package continuity

import (
	"github.com/vancemichael/station-telemetry-service/internal/sequence"
)

// Status 是来源/站点的健康状态。
type Status string

const (
	StatusHealthy Status = "healthy"
	StatusSuspect Status = "suspect"
	StatusOffline Status = "offline"
)

// 状态进入依据(status_reason 的取值)。
const (
	ReasonAwaitingFirstSample   = "awaiting_first_sample"
	ReasonInitialQualified      = "initial_sample_qualified"
	ReasonInitialUnqualified    = "initial_sample_unqualified"
	ReasonGapOpened             = "gap_opened"
	ReasonQualityBelowContract  = "quality_below_contract"
	ReasonHealthyStreakRestored = "healthy_streak_restored"
	ReasonSilenceTimeout        = "silence_timeout"
)

// 入账分类(classification 的取值)。
const (
	ClassProjection     = "projection"      // 新鲜且序号超前,推进投影
	ClassBackfill       = "backfill"        // 迟到(超时或序号已过水位),只进历史
	ClassDuplicateEvent = "duplicate_event" // event_id 重放
	ClassDuplicateSeq   = "duplicate_seq"   // 同序号被另一接收端抢先入账
)

// SourceState 是状态机读取的来源状态快照(对应 source_state 表)。
type SourceState struct {
	Status    Status
	Watermark int64 // 投影水位:最近一次推进投影的样本序号
	MaxSeen   int64 // 已入账的最大序号(含补录)
	Streak    int   // 当前连续合格(新鲜且按序)样本数
}

// FirstContact 在来源尚无任何已入账事件时为真(水位与 MaxSeen 均为 0)。
func (s SourceState) FirstContact() bool {
	return s.Watermark == 0 && s.MaxSeen == 0
}

// Input 是一个已通过幂等/争用去重的观测。
type Input struct {
	Seq        int64
	Qualified  bool // 合约质量判定结果
	LateByTime bool // received_at - observed_at 超过合约允许延迟
}

// Decision 描述一次入账引起的全部变更,store 层在同一事务内落地。
type Decision struct {
	Classification string

	// 缺口集合变更
	OpenGapFrom int64 // 新缺口区间起点(含),OpenGap 为真时有效
	OpenGapTo   int64 // 新缺口区间终点(含)
	OpenGap     bool
	FillSeq     int64 // 补录命中缺口时要移除的序号
	FillGap     bool

	// 投影变更(仅 projection 路径;backfill 一律不回退投影)
	NewStatus     Status
	StatusChanged bool
	Reason        string
	ReasonSeq     int64 // 触发当前状态的样本序号
	NewStreak     int
	Advance       bool  // 是否推进水位/存活时间
	NewWatermark  int64 // Advance 为真时有效
	Recovered     bool  // 本次样本完成了合约要求的连续合格,转回 healthy
}

// Evaluate 判定一个观测如何影响来源状态。healthyStreak 来自合约。
//
// 规则:
//   - 超时迟到或序号不高于水位 → backfill:进历史、补缺,不碰投影;
//   - 首次接触 → 以该样本初始化水位,不追溯其前的序号;
//   - 序号跳变 → 开出缺口区间,连续合格计数清零,healthy 降级为 suspect;
//   - 质量不合格 → 连续合格计数清零,healthy 降级为 suspect;
//   - suspect/offline 只有连续 healthyStreak 个新鲜合格样本才转回 healthy,
//     恢复样本即"真正恢复发生的位置"。
func Evaluate(st SourceState, in Input, healthyStreak int) Decision {
	d := Decision{
		Classification: ClassProjection,
		NewStatus:      st.Status,
		NewStreak:      st.Streak,
	}

	lateBySeq := !st.FirstContact() && in.Seq <= st.Watermark
	if in.LateByTime || lateBySeq {
		d.Classification = ClassBackfill
		if st.FirstContact() {
			// 首次接触即使是迟到的补传,也以它初始化水位。
			d.Advance, d.NewWatermark = true, in.Seq
			return d
		}
		if lateBySeq {
			d.FillGap, d.FillSeq = true, in.Seq
		} else if missing := sequence.Missing(st.Watermark, in.Seq); len(missing) > 0 {
			// 超时迟到但序号超前:不推进投影,但跳过的序号仍属待补齐。
			d.OpenGap, d.OpenGapFrom, d.OpenGapTo = true, missing[0], missing[len(missing)-1]
		}
		return d
	}

	d.Advance, d.NewWatermark = true, in.Seq

	if st.FirstContact() {
		// 首次接触总是记录状态依据,即使状态字符串未变。
		d.StatusChanged = true
		if in.Qualified {
			d.NewStatus, d.NewStreak = StatusHealthy, 1
			d.Reason, d.ReasonSeq = ReasonInitialQualified, in.Seq
		} else {
			d.NewStatus, d.NewStreak = StatusSuspect, 0
			d.Reason, d.ReasonSeq = ReasonInitialUnqualified, in.Seq
		}
		return d
	}

	if missing := sequence.Missing(st.Watermark, in.Seq); len(missing) > 0 {
		d.OpenGap, d.OpenGapFrom, d.OpenGapTo = true, missing[0], missing[len(missing)-1]
	}

	switch {
	case d.OpenGap:
		d.NewStreak = 0
		if st.Status == StatusHealthy {
			d.NewStatus, d.StatusChanged = StatusSuspect, true
			d.Reason, d.ReasonSeq = ReasonGapOpened, in.Seq
		}
	case !in.Qualified:
		d.NewStreak = 0
		if st.Status == StatusHealthy {
			d.NewStatus, d.StatusChanged = StatusSuspect, true
			d.Reason, d.ReasonSeq = ReasonQualityBelowContract, in.Seq
		}
	default:
		d.NewStreak = st.Streak + 1
		if st.Status != StatusHealthy && d.NewStreak >= healthyStreak {
			d.NewStatus, d.StatusChanged, d.Recovered = StatusHealthy, true, true
			d.Reason, d.ReasonSeq = ReasonHealthyStreakRestored, in.Seq
		}
	}
	return d
}
