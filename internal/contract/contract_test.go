package contract

import (
	"testing"
	"time"
)

func testPolicy() Policy {
	return Policy{
		Version:                "station-recovery-1",
		OfflineAfterSeconds:    90,
		HealthyStreak:          3,
		AllowedLatenessSeconds: 300,
		Quality:                Quality{MinimumSatellites: 4, MaximumPDOP: 6.0},
	}
}

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }

func TestQualified(t *testing.T) {
	p := testPolicy()
	cases := []struct {
		name string
		m    Metrics
		want bool
	}{
		{"达标", Metrics{Satellites: intPtr(8), PDOP: floatPtr(1.7)}, true},
		{"边界达标", Metrics{Satellites: intPtr(4), PDOP: floatPtr(6.0)}, true},
		{"卫星不足", Metrics{Satellites: intPtr(3), PDOP: floatPtr(1.0)}, false},
		{"PDOP超限", Metrics{Satellites: intPtr(9), PDOP: floatPtr(6.1)}, false},
		{"缺卫星指标", Metrics{PDOP: floatPtr(1.0)}, false},
		{"缺PDOP指标", Metrics{Satellites: intPtr(9)}, false},
		{"空指标", Metrics{}, false},
	}
	for _, c := range cases {
		if got := p.Qualified(c.m); got != c.want {
			t.Errorf("%s: Qualified=%v, 期望 %v", c.name, got, c.want)
		}
	}
}

func TestLateByTime(t *testing.T) {
	p := testPolicy()
	base := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	if p.LateByTime(base, base.Add(299*time.Second)) {
		t.Error("299 秒延迟不应判为迟到")
	}
	if !p.LateByTime(base, base.Add(301*time.Second)) {
		t.Error("301 秒延迟应判为迟到")
	}
	if p.LateByTime(base, base.Add(-time.Minute)) {
		t.Error("时钟轻微回拨不应判为迟到")
	}
}

func TestLoadValidates(t *testing.T) {
	if _, err := Load("不存在.json"); err == nil {
		t.Error("缺失文件应报错")
	}
}
