// Package contract 加载并校验恢复策略(contracts/recovery-policy.json),
// 判定样本是否合格、是否因超时而迟到。策略属于部署配置,不随请求改变。
package contract

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Quality 是合格样本的阈值:卫星数下限与 PDOP 上限。
type Quality struct {
	MinimumSatellites int     `json:"minimum_satellites"`
	MaximumPDOP       float64 `json:"maximum_pdop"`
}

// Policy 对应 contracts/recovery-policy.json。
type Policy struct {
	Version                string  `json:"version"`
	OfflineAfterSeconds    int     `json:"offline_after_seconds"`
	HealthyStreak          int     `json:"healthy_streak"`
	AllowedLatenessSeconds int     `json:"allowed_lateness_seconds"`
	Quality                Quality `json:"quality"`
}

// Load 从 path 读取策略并做合法性校验。
func Load(path string) (Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Policy{}, fmt.Errorf("读取策略文件: %w", err)
	}
	var p Policy
	if err := json.Unmarshal(raw, &p); err != nil {
		return Policy{}, fmt.Errorf("解析策略文件 %s: %w", path, err)
	}
	if p.Version == "" {
		return Policy{}, fmt.Errorf("策略缺少 version")
	}
	if p.HealthyStreak < 1 {
		return Policy{}, fmt.Errorf("healthy_streak 必须 >= 1, 当前 %d", p.HealthyStreak)
	}
	if p.OfflineAfterSeconds < 1 {
		return Policy{}, fmt.Errorf("offline_after_seconds 必须 >= 1, 当前 %d", p.OfflineAfterSeconds)
	}
	if p.AllowedLatenessSeconds < 0 {
		return Policy{}, fmt.Errorf("allowed_lateness_seconds 不能为负, 当前 %d", p.AllowedLatenessSeconds)
	}
	if p.Quality.MinimumSatellites < 0 {
		return Policy{}, fmt.Errorf("quality.minimum_satellites 不能为负")
	}
	if p.Quality.MaximumPDOP <= 0 {
		return Policy{}, fmt.Errorf("quality.maximum_pdop 必须 > 0")
	}
	return p, nil
}

// OfflineAfter 是静默多久后把来源标记为 offline。
func (p Policy) OfflineAfter() time.Duration {
	return time.Duration(p.OfflineAfterSeconds) * time.Second
}

// AllowedLateness 是观测时刻到接收时刻之间允许的最大延迟。
func (p Policy) AllowedLateness() time.Duration {
	return time.Duration(p.AllowedLatenessSeconds) * time.Second
}

// Metrics 是观测携带的质量指标,未知字段被忽略。
type Metrics struct {
	Satellites *int     `json:"satellites"`
	PDOP       *float64 `json:"pdop"`
}

// Qualified 按策略判定样本质量;缺少必要指标一律视为不合格。
func (p Policy) Qualified(m Metrics) bool {
	if m.Satellites == nil || m.PDOP == nil {
		return false
	}
	return *m.Satellites >= p.Quality.MinimumSatellites && *m.PDOP <= p.Quality.MaximumPDOP
}

// LateByTime 判定样本是否超过允许延迟才送达;轻微的时钟回拨按 0 处理。
func (p Policy) LateByTime(observedAt, receivedAt time.Time) bool {
	lag := receivedAt.Sub(observedAt)
	if lag < 0 {
		lag = 0
	}
	return lag > p.AllowedLateness()
}
