// 集成测试:需要真实 PostgreSQL(设置 TEST_DATABASE_URL 时运行,
// 否则跳过)。镜像 scripts/acceptance.sh 的完整验收链路。
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vancemichael/station-telemetry-service/internal/api"
	"github.com/vancemichael/station-telemetry-service/internal/contract"
	"github.com/vancemichael/station-telemetry-service/internal/store"
)

const (
	testStation = "station-demo"
	testSource  = "link-a"
)

func newTestEnv(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL 未设置,跳过 PostgreSQL 集成测试")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("连接测试库: %v", err)
	}
	t.Cleanup(pool.Close)

	// 每个测试用例从干净的 schema 开始,直接执行仓库里的迁移文件。
	down, err := os.ReadFile("../../migrations/000001_initial.down.sql")
	if err != nil {
		t.Fatalf("读取 down 迁移: %v", err)
	}
	up, err := os.ReadFile("../../migrations/000001_initial.up.sql")
	if err != nil {
		t.Fatalf("读取 up 迁移: %v", err)
	}
	if _, err := pool.Exec(ctx, string(down)); err != nil {
		t.Fatalf("回滚迁移: %v", err)
	}
	if _, err := pool.Exec(ctx, string(up)); err != nil {
		t.Fatalf("应用迁移: %v", err)
	}

	policy, err := contract.Load("../../contracts/recovery-policy.json")
	if err != nil {
		t.Fatalf("加载合约: %v", err)
	}

	st := store.New(pool)
	srv := httptest.NewServer(api.NewServer(st, policy).Handler())
	t.Cleanup(srv.Close)
	return srv, st
}

func postObs(t *testing.T, base, eventID string, seq int64, observedAgo time.Duration, sats int, pdop float64) map[string]any {
	t.Helper()
	now := time.Now().UTC()
	body := map[string]any{
		"event_id":    eventID,
		"station_id":  testStation,
		"source_id":   testSource,
		"source_seq":  seq,
		"observed_at": now.Add(-observedAgo).Format(time.RFC3339),
		"received_at": now.Format(time.RFC3339),
		"metrics":     map[string]any{"satellites": sats, "pdop": pdop},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("序列化请求: %v", err)
	}
	resp, err := http.Post(base+"/v1/observations", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST /v1/observations: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("解析响应: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("入账 %s 返回 %d: %v", eventID, resp.StatusCode, out)
	}
	return out
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("解析响应: %v", err)
	}
	return out
}

func wantEq(t *testing.T, what string, got, want any) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%s: 得到 %v, 期望 %v", what, got, want)
	}
}

func handoffSource(t *testing.T, base string) map[string]any {
	t.Helper()
	h := getJSON(t, base+"/v1/stations/"+testStation+"/handoff")
	sources, ok := h["sources"].([]any)
	if !ok || len(sources) == 0 {
		t.Fatalf("交班视图缺少 sources: %v", h)
	}
	return sources[0].(map[string]any)
}

// TestAcceptanceFlow 镜像验收脚本:缺口 → 补传 → 恢复 → 幂等/争用 → 续读 → offline 恢复。
func TestAcceptanceFlow(t *testing.T) {
	srv, st := newTestEnv(t)
	base := srv.URL

	// 正常观测 1..3 → healthy。
	r := postObs(t, base, "evt-1", 1, 30*time.Second, 8, 1.5)
	wantEq(t, "seq1 分类", r["classification"], "projection")
	wantEq(t, "seq1 状态", r["status"], "healthy")
	postObs(t, base, "evt-2", 2, 25*time.Second, 8, 1.5)
	postObs(t, base, "evt-3", 3, 20*time.Second, 8, 1.5)

	// 人为缺口:跳过 4,发送 5。
	r = postObs(t, base, "evt-5", 5, 15*time.Second, 8, 1.5)
	wantEq(t, "seq5 状态", r["status"], "suspect")
	gap := r["gap_opened"].(map[string]any)
	wantEq(t, "缺口起点", gap["from"], 4)
	wantEq(t, "缺口终点", gap["to"], 4)

	src := handoffSource(t, base)
	wantEq(t, "状态依据", src["status_reason"], "gap_opened")
	wantEq(t, "依据样本", src["status_seq"], 5)
	wantEq(t, "连续水位", src["contiguous_watermark"], 3)
	gaps := src["pending_gaps"].([]any)
	wantEq(t, "待补齐区间数", len(gaps), 1)
	wantEq(t, "待补齐起点", gaps[0].(map[string]any)["from"], 4)

	// 6 到达,恢复进度 1/3。
	r = postObs(t, base, "evt-6", 6, 10*time.Second, 8, 1.5)
	wantEq(t, "seq6 streak", r["streak"], 1)

	// 跨链路补传 4:只进历史,投影不回退。
	r = postObs(t, base, "evt-4", 4, 240*time.Second, 8, 1.5)
	wantEq(t, "补传分类", r["classification"], "backfill")
	wantEq(t, "补传缺口", r["gap_filled"], 4)
	src = handoffSource(t, base)
	wantEq(t, "补传后状态", src["status"], "suspect")
	wantEq(t, "补传后水位", src["watermark"], 6)
	wantEq(t, "补传后 streak", src["streak"], 1)
	wantEq(t, "补传后待补齐", len(src["pending_gaps"].([]any)), 0)
	wantEq(t, "补传后连续水位", src["contiguous_watermark"], 6)
	wantEq(t, "最近补传样本", src["last_backfill"].(map[string]any)["source_seq"], 4)

	// 7、8 到达:在样本 8 真正恢复。
	r = postObs(t, base, "evt-7", 7, 8*time.Second, 8, 1.5)
	wantEq(t, "seq7 streak", r["streak"], 2)
	wantEq(t, "seq7 状态", r["status"], "suspect")
	r = postObs(t, base, "evt-8", 8, 6*time.Second, 8, 1.5)
	wantEq(t, "seq8 streak", r["streak"], 3)
	wantEq(t, "seq8 状态", r["status"], "healthy")
	wantEq(t, "seq8 恢复标记", r["recovered"], true)
	src = handoffSource(t, base)
	wantEq(t, "恢复依据", src["status_reason"], "healthy_streak_restored")
	wantEq(t, "恢复样本", src["last_recovery"].(map[string]any)["source_seq"], 8)

	// 超时迟到样本(观测于 10 分钟前)只进历史。
	r = postObs(t, base, "evt-9", 9, 600*time.Second, 8, 1.5)
	wantEq(t, "超时分类", r["classification"], "backfill")
	wantEq(t, "超时标记", r["late_by_time"], true)
	wantEq(t, "超时后水位", handoffSource(t, base)["watermark"], 8)

	// event_id 重送不重复入账。
	r = postObs(t, base, "evt-8", 8, 6*time.Second, 8, 1.5)
	wantEq(t, "重送标记", r["duplicate"], true)
	wantEq(t, "重送分类", r["classification"], "duplicate_event")

	// 两个接收端争用序号 10:只保留一条。
	var wg sync.WaitGroup
	results := make([]map[string]any, 2)
	for i, eventID := range []string{"evt-10a", "evt-10b"} {
		wg.Add(1)
		go func(i int, eventID string) {
			defer wg.Done()
			results[i] = postObs(t, base, eventID, 10, 4*time.Second, 8, 1.5)
		}(i, eventID)
	}
	wg.Wait()
	var recorded, seqDup int
	for _, res := range results {
		if res["recorded"] == true {
			recorded++
		}
		if res["classification"] == "duplicate_seq" {
			seqDup++
		}
	}
	wantEq(t, "争用入账数", recorded, 1)
	wantEq(t, "争用去重数", seqDup, 1)

	// 事件流中序号 10 只有一条;断点续读恰好覆盖全部 10 条事件。
	stream := getJSON(t, base+"/v1/events?station_id="+testStation+"&limit=100")
	var seq10 int
	for _, e := range stream["events"].([]any) {
		if e.(map[string]any)["source_seq"] == float64(10) {
			seq10++
		}
	}
	wantEq(t, "序号 10 事件数", seq10, 1)

	seen := map[float64]bool{}
	cursor := 0
	total := 0
	for {
		page := getJSON(t, fmt.Sprintf("%s/v1/events?station_id=%s&after=%d&limit=3", base, testStation, cursor))
		events := page["events"].([]any)
		if len(events) == 0 {
			break
		}
		for _, e := range events {
			id := e.(map[string]any)["stream_id"].(float64)
			if seen[id] {
				t.Fatalf("续读出现重复事件: %v", id)
			}
			seen[id] = true
			total++
		}
		cursor = int(page["next_cursor"].(float64))
		if page["has_more"] != true {
			break
		}
	}
	wantEq(t, "事件总数", total, 10)

	// 重启前最终状态:水位 10,恢复样本 8。
	src = handoffSource(t, base)
	wantEq(t, "最终水位", src["watermark"], 10)
	wantEq(t, "最终连续水位", src["contiguous_watermark"], 10)
	wantEq(t, "最终恢复样本", src["last_recovery"].(map[string]any)["source_seq"], 8)

	// 静默清扫 → offline;恢复仍需连续 3 个合格样本。
	swept, err := st.SweepOffline(context.Background(), 0)
	if err != nil {
		t.Fatalf("offline 清扫: %v", err)
	}
	wantEq(t, "清扫数量", swept, 1)
	src = handoffSource(t, base)
	wantEq(t, "清扫后状态", src["status"], "offline")
	wantEq(t, "清扫后依据", src["status_reason"], "silence_timeout")
	wantEq(t, "清扫后 streak", src["streak"], 0)

	r = postObs(t, base, "evt-11", 11, 3*time.Second, 8, 1.5)
	wantEq(t, "offline 恢复中 1", r["status"], "offline")
	r = postObs(t, base, "evt-12", 12, 2*time.Second, 8, 1.5)
	wantEq(t, "offline 恢复中 2", r["status"], "offline")
	r = postObs(t, base, "evt-13", 13, time.Second, 8, 1.5)
	wantEq(t, "offline 恢复完成", r["status"], "healthy")
	wantEq(t, "offline 恢复标记", r["recovered"], true)
	wantEq(t, "offline 恢复样本",
		handoffSource(t, base)["last_recovery"].(map[string]any)["source_seq"], 13)
}

// TestQualityGate 不合格样本清零恢复进度。
func TestQualityGate(t *testing.T) {
	srv, _ := newTestEnv(t)
	base := srv.URL

	postObs(t, base, "q-1", 1, 30*time.Second, 8, 1.5)
	r := postObs(t, base, "q-2", 2, 25*time.Second, 2, 9.9) // 卫星不足且 PDOP 超限
	wantEq(t, "不合格后状态", r["status"], "suspect")
	wantEq(t, "不合格判定", r["qualified"], false)
	postObs(t, base, "q-3", 3, 20*time.Second, 8, 1.5)
	r = postObs(t, base, "q-4", 4, 15*time.Second, 8, 1.5)
	wantEq(t, "streak 2/3 仍 suspect", r["status"], "suspect")
	r = postObs(t, base, "q-5", 5, 10*time.Second, 8, 1.5)
	wantEq(t, "streak 3/3 恢复", r["status"], "healthy")
}

// TestUnknownStation 交班查询对未知站点返回 404。
func TestUnknownStation(t *testing.T) {
	srv, _ := newTestEnv(t)
	resp, err := http.Get(srv.URL + "/v1/stations/不存在/handoff")
	if err != nil {
		t.Fatalf("GET handoff: %v", err)
	}
	defer resp.Body.Close()
	wantEq(t, "未知站点状态码", resp.StatusCode, 404)
}

// TestValidation 非法请求被拒绝。
func TestValidation(t *testing.T) {
	srv, _ := newTestEnv(t)
	resp, err := http.Post(srv.URL+"/v1/observations", "application/json",
		bytes.NewReader([]byte(`{"station_id":"s","source_id":"x","source_seq":1,"observed_at":"2026-09-18T00:00:00Z"}`)))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	wantEq(t, "缺 event_id 状态码", resp.StatusCode, 400)
}
