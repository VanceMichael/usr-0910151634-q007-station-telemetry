package domain

import (
	"testing"
	"time"
)

func mkEvent(id string, seq int64, at time.Time) Event {
	return Event{EventID: id, StationID: "st", SourceID: "link-a", SourceSeq: seq, ObservedAt: at, ReceivedAt: at}
}

// 连续 3 个合格样本才 healthy，且恢复锚点必须是第 3 个样本。
func TestRecoveryRequiresContractStreak(t *testing.T) {
	base := time.Unix(1000, 0).UTC()
	var p Projection
	for i := int64(1); i <= 3; i++ {
		e := mkEvent("evt-"+string(rune('0'+i)), i, base.Add(time.Duration(i)*10*time.Second))
		p = Decide(p, e, true, "", 3)
		if i < 3 {
			if p.State != StateSuspect {
				t.Fatalf("第 %d 个样本不应恢复，状态=%s", i, p.State)
			}
			if p.Recovered != nil {
				t.Fatalf("第 %d 个样本不应锚定恢复", i)
			}
		}
	}
	if p.State != StateHealthy {
		t.Fatalf("第 3 个合格样本后应为 healthy，实际=%s", p.State)
	}
	if p.Recovered == nil || p.Recovered.EventID != "evt-3" {
		t.Fatalf("恢复必须锚定第 3 个样本 evt-3，实际=%+v", p.Recovered)
	}
	if p.RecoveredStreak != 3 {
		t.Fatalf("恢复计数应为 3，实际=%d", p.RecoveredStreak)
	}
}

// 质量不合格清零计数；此前的正常样本不得被当作恢复依据。
func TestBadQualityResetsStreak(t *testing.T) {
	base := time.Unix(2000, 0).UTC()
	p := Decide(Projection{}, mkEvent("g1", 1, base), true, "", 3)
	p = Decide(p, mkEvent("g2", 2, base.Add(10*time.Second)), true, "", 3)
	if p.Streak != 2 {
		t.Fatalf("前置计数错误: %d", p.Streak)
	}
	p = Decide(p, mkEvent("bad", 3, base.Add(20*time.Second)), false, "", 3)
	if p.State != StateSuspect || p.Streak != 0 || p.StreakStart != nil {
		t.Fatalf("不合格样本应清零: %+v", p)
	}
	if p.Recovered != nil {
		t.Fatal("不合格样本应作废旧恢复锚点")
	}
	// 再来两个合格样本仍不能恢复。
	p = Decide(p, mkEvent("g3", 4, base.Add(30*time.Second)), true, "", 3)
	p = Decide(p, mkEvent("g4", 5, base.Add(40*time.Second)), true, "", 3)
	if p.State != StateSuspect || p.Recovered != nil {
		t.Fatalf("中断后未满 3 连不得恢复: %+v", p)
	}
	p = Decide(p, mkEvent("g5", 6, base.Add(50*time.Second)), true, "", 3)
	if p.State != StateHealthy || p.Recovered.EventID != "g5" {
		t.Fatalf("真正恢复应锚定 g5: %+v", p.Recovered)
	}
}

// 观测间隔超过契约窗口（freshness 中断），旧连续段作废。
func TestFreshnessBreakRestartsStreak(t *testing.T) {
	base := time.Unix(3000, 0).UTC()
	p := Decide(Projection{}, mkEvent("h1", 1, base), true, "", 3)
	p = Decide(p, mkEvent("h2", 2, base.Add(10*time.Second)), true, "", 3)
	if p.Streak != 2 {
		t.Fatalf("前置计数错误: %d", p.Streak)
	}
	// 间隔 100 秒（>90 秒 offline 窗口）。
	p = Decide(p, mkEvent("after-gap", 3, base.Add(110*time.Second)), true, ReasonFreshnessTimeout, 3)
	if p.Streak != 1 || p.StreakStart.EventID != "after-gap" {
		t.Fatalf("断链后计数必须从 after-gap 重新开始: streak=%d start=%+v", p.Streak, p.StreakStart)
	}
}

// 来源序号跳号同样打断连续合格段。
func TestLinkGapBreakRestartsStreak(t *testing.T) {
	base := time.Unix(3500, 0).UTC()
	p := Decide(Projection{}, mkEvent("a1", 1, base), true, "", 3)
	p = Decide(p, mkEvent("a2", 2, base.Add(10*time.Second)), true, "", 3)
	p = Decide(p, mkEvent("a4", 4, base.Add(20*time.Second)), true, ReasonLinkGap, 3)
	if p.Streak != 1 || p.StreakStart.EventID != "a4" || p.StateReason != ReasonLinkGap {
		t.Fatalf("跳号后应从 a4 重新计数且原因为 link_gap: %+v", p)
	}
	p = Decide(p, mkEvent("a5", 5, base.Add(30*time.Second)), true, "", 3)
	p = Decide(p, mkEvent("a6", 6, base.Add(40*time.Second)), true, "", 3)
	if p.State != StateHealthy || p.Recovered.EventID != "a6" {
		t.Fatalf("跳号后的新 3 连应在 a6 恢复: %+v", p.Recovered)
	}
}

// healthy 之后继续收到合格样本不应改写恢复锚点（锚点是“真正恢复”的那一帧）。
func TestRecoveredAnchorStableWhileHealthy(t *testing.T) {
	base := time.Unix(4000, 0).UTC()
	p := Decide(Projection{}, mkEvent("r1", 1, base), true, "", 3)
	p = Decide(p, mkEvent("r2", 2, base.Add(10*time.Second)), true, "", 3)
	p = Decide(p, mkEvent("r3", 3, base.Add(20*time.Second)), true, "", 3)
	p = Decide(p, mkEvent("r4", 4, base.Add(30*time.Second)), true, "", 3)
	if p.Recovered == nil || p.Recovered.EventID != "r3" {
		t.Fatalf("恢复锚点应稳定在 r3: %+v", p.Recovered)
	}
}
