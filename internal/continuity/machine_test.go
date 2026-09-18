package continuity

import "testing"

const streak = 3

func evalSeq(t *testing.T, st SourceState, seq int64, qualified, lateByTime bool) (SourceState, Decision) {
	t.Helper()
	d := Evaluate(st, Input{Seq: seq, Qualified: qualified, LateByTime: lateByTime}, streak)
	next := st
	if d.Advance {
		next.Watermark = d.NewWatermark
	}
	if seq > next.MaxSeen {
		next.MaxSeen = seq
	}
	next.Status = d.NewStatus
	next.Streak = d.NewStreak
	return next, d
}

func TestFirstContactInitializesWatermark(t *testing.T) {
	st, d := evalSeq(t, SourceState{Status: StatusSuspect}, 41, true, false)
	if d.Classification != ClassProjection || !d.Advance || d.NewWatermark != 41 {
		t.Fatalf("首次接触应初始化水位到 41: %+v", d)
	}
	if st.Status != StatusHealthy || st.Streak != 1 || d.OpenGap {
		t.Fatalf("首个合格样本应直接 healthy 且不追溯缺口: %+v -> %+v", d, st)
	}
}

func TestFirstContactUnqualified(t *testing.T) {
	st, d := evalSeq(t, SourceState{Status: StatusSuspect}, 1, false, false)
	if st.Status != StatusSuspect || st.Streak != 0 || d.Reason != ReasonInitialUnqualified {
		t.Fatalf("首个不合格样本应保持 suspect: %+v -> %+v", d, st)
	}
}

func TestGapOpensAndStreakResets(t *testing.T) {
	st := SourceState{Status: StatusHealthy, Watermark: 3, MaxSeen: 3, Streak: 3}
	st, d := evalSeq(t, st, 5, true, false)
	if !d.OpenGap || d.OpenGapFrom != 4 || d.OpenGapTo != 4 {
		t.Fatalf("应开出缺口 [4,4]: %+v", d)
	}
	if st.Status != StatusSuspect || st.Streak != 0 || d.Reason != ReasonGapOpened || d.ReasonSeq != 5 {
		t.Fatalf("缺口应使 healthy 降级 suspect 且清零 streak: %+v -> %+v", d, st)
	}
}

func TestBackfillDoesNotTouchProjection(t *testing.T) {
	st := SourceState{Status: StatusSuspect, Watermark: 6, MaxSeen: 6, Streak: 1}
	next, d := evalSeq(t, st, 4, true, false)
	if d.Classification != ClassBackfill || !d.FillGap || d.FillSeq != 4 {
		t.Fatalf("序号低于水位应判为补缺: %+v", d)
	}
	if d.Advance || next.Watermark != 6 || next.Streak != 1 || next.Status != StatusSuspect {
		t.Fatalf("补缺不得回退投影: %+v -> %+v", d, next)
	}
}

func TestLateByTimeGoesToHistoryOnly(t *testing.T) {
	st := SourceState{Status: StatusSuspect, Watermark: 9, MaxSeen: 9, Streak: 2}
	next, d := evalSeq(t, st, 10, true, true)
	if d.Classification != ClassBackfill || d.Advance {
		t.Fatalf("超时迟到应只进历史: %+v", d)
	}
	if next.Watermark != 9 || next.Streak != 2 {
		t.Fatalf("超时迟到不得推进水位或 streak: %+v -> %+v", d, next)
	}
	if next.MaxSeen != 10 {
		t.Fatalf("MaxSeen 仍应记录已入账最大序号: %+v", next)
	}
}

func TestLateByTimeForwardSeqOpensGapButNotProjection(t *testing.T) {
	st := SourceState{Status: StatusHealthy, Watermark: 9, MaxSeen: 9, Streak: 5}
	next, d := evalSeq(t, st, 12, true, true)
	if d.Classification != ClassBackfill || !d.OpenGap || d.OpenGapFrom != 10 || d.OpenGapTo != 11 {
		t.Fatalf("超时且序号超前应只登记缺口: %+v", d)
	}
	if next.Status != StatusHealthy || next.Watermark != 9 || next.Streak != 5 {
		t.Fatalf("投影不得变化: %+v -> %+v", d, next)
	}
}

// 验收主链路:缺口 → 补传 → 连续合格恢复,恢复样本必须准确。
func TestRecoveryAfterBackfill(t *testing.T) {
	st := SourceState{Status: StatusSuspect}
	var d Decision
	// 1,2,3 正常:首个样本即 healthy。
	for seq := int64(1); seq <= 3; seq++ {
		st, d = evalSeq(t, st, seq, true, false)
	}
	if st.Status != StatusHealthy {
		t.Fatalf("前 3 个样本后应 healthy: %+v", st)
	}
	// 跳过 4,来 5:开缺口,降级 suspect。
	st, d = evalSeq(t, st, 5, true, false)
	if st.Status != StatusSuspect || d.Reason != ReasonGapOpened {
		t.Fatalf("缺口后应 suspect: %+v -> %+v", d, st)
	}
	// 6:streak 1。
	st, _ = evalSeq(t, st, 6, true, false)
	// 补传 4:只进历史,streak 不动。
	st, d = evalSeq(t, st, 4, true, false)
	if d.Classification != ClassBackfill || st.Streak != 1 || st.Watermark != 6 {
		t.Fatalf("补传不得影响投影: %+v -> %+v", d, st)
	}
	// 7:streak 2,仍 suspect。
	st, d = evalSeq(t, st, 7, true, false)
	if st.Status != StatusSuspect || st.Streak != 2 {
		t.Fatalf("streak 2/3 应仍 suspect: %+v -> %+v", d, st)
	}
	// 8:streak 3,在样本 8 真正恢复。
	st, d = evalSeq(t, st, 8, true, false)
	if st.Status != StatusHealthy || !d.Recovered || d.ReasonSeq != 8 || d.Reason != ReasonHealthyStreakRestored {
		t.Fatalf("应在样本 8 恢复: %+v -> %+v", d, st)
	}
}

func TestUnqualifiedBreaksRecoveryStreak(t *testing.T) {
	st := SourceState{Status: StatusSuspect, Watermark: 6, MaxSeen: 6, Streak: 2}
	st, d := evalSeq(t, st, 7, false, false)
	if st.Streak != 0 || st.Status != StatusSuspect {
		t.Fatalf("不合格样本应清零 streak: %+v -> %+v", d, st)
	}
	// 重新累计 3 个才恢复。
	st, _ = evalSeq(t, st, 8, true, false)
	st, _ = evalSeq(t, st, 9, true, false)
	st, d = evalSeq(t, st, 10, true, false)
	if !d.Recovered || d.ReasonSeq != 10 {
		t.Fatalf("应在样本 10 恢复: %+v -> %+v", d, st)
	}
}

func TestOfflineAlsoNeedsFullStreak(t *testing.T) {
	st := SourceState{Status: StatusOffline, Watermark: 20, MaxSeen: 20, Streak: 0}
	st, d := evalSeq(t, st, 21, true, false)
	if st.Status != StatusOffline || st.Streak != 1 {
		t.Fatalf("offline 单个合格样本不得恢复: %+v -> %+v", d, st)
	}
	st, _ = evalSeq(t, st, 22, true, false)
	st, d = evalSeq(t, st, 23, true, false)
	if st.Status != StatusHealthy || !d.Recovered || d.ReasonSeq != 23 {
		t.Fatalf("offline 应连续 3 个合格样本后在 23 恢复: %+v -> %+v", d, st)
	}
}

func TestGapWhileSuspectKeepsFirstCause(t *testing.T) {
	st := SourceState{Status: StatusSuspect, Watermark: 5, MaxSeen: 5, Streak: 1}
	st, d := evalSeq(t, st, 9, true, false)
	if !d.OpenGap || d.OpenGapFrom != 6 || d.OpenGapTo != 8 {
		t.Fatalf("应开出缺口 [6,8]: %+v", d)
	}
	if d.StatusChanged || st.Status != StatusSuspect || st.Streak != 0 {
		t.Fatalf("已 suspect 再开缺口应保持原状态依据,仅清零 streak: %+v -> %+v", d, st)
	}
}
