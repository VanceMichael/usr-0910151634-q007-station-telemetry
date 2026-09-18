// Package contracts 加载站点恢复策略契约（contracts/recovery-policy.json）。
package contracts

import _ "embed"
import "encoding/json"

//go:embed recovery-policy.json
var policyJSON []byte

// Policy 描述站点何时判为离线、何时可以恢复 healthy 以及质量合格门槛。
type Policy struct {
	Version             string  `json:"version"`
	OfflineAfterSeconds int64   `json:"offline_after_seconds"`
	HealthyStreak       int     `json:"healthy_streak"`
	AllowedLatenessSecs int64   `json:"allowed_lateness_seconds"`
	Quality             Quality `json:"quality"`
}

type Quality struct {
	MinimumSatellites int     `json:"minimum_satellites"`
	MaximumPDOP       float64 `json:"maximum_pdop"`
}

// Load 解析内嵌契约。契约随二进制发布，避免运行环境与契约版本漂移。
func Load() (Policy, error) {
	var p Policy
	if err := json.Unmarshal(policyJSON, &p); err != nil {
		return Policy{}, err
	}
	if p.OfflineAfterSeconds <= 0 || p.HealthyStreak <= 0 || p.AllowedLatenessSecs < 0 {
		return Policy{}, ErrInvalidContract
	}
	return p, nil
}
