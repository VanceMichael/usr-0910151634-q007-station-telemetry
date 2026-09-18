#!/usr/bin/env bash
# 验收脚本:人为制造序号缺口 → 跨链路补传 → 连续合格样本恢复 →
# 重启整套环境 → 交班结果延续原水位并指出真正恢复发生的样本。
# 依赖:docker compose、curl、jq、GNU date。
set -euo pipefail

COMPOSE=${COMPOSE:-"docker compose"}
BASE=${BASE:-"http://localhost:8080"}
STATION=${STATION:-"station-demo"}
SOURCE=${SOURCE:-"link-a"}

say()  { printf '\n== %s ==\n' "$*"; }
fail() { echo "断言失败: $*" >&2; exit 1; }

now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
iso_ago() { date -u -d "@$(( $(date -u +%s) - $1 ))" +%Y-%m-%dT%H:%M:%SZ; }

post_obs() { # event_id seq observed_at received_at satellites pdop
  curl -sS -X POST "$BASE/v1/observations" \
    -H 'Content-Type: application/json' \
    -d "{\"event_id\":\"$1\",\"station_id\":\"$STATION\",\"source_id\":\"$SOURCE\",\"source_seq\":$2,\"observed_at\":\"$3\",\"received_at\":\"$4\",\"metrics\":{\"satellites\":$5,\"pdop\":$6}}"
}

handoff() { curl -sS "$BASE/v1/stations/$STATION/handoff"; }

assert_jq() { # json filter expected — 从 JSON 中取值并与期望比较
  local got
  got=$(jq -r "$2" <<<"$1")
  [ "$got" = "$3" ] || fail "jq '$2' => '$got', 期望 '$3'"
}

wait_health() {
  for _ in $(seq 1 90); do
    if curl -sf "$BASE/health" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  fail "服务在 90 秒内未就绪"
}

say "1. 启动整套环境(应用、迁移、数据库探活、持久卷)"
$COMPOSE up -d --build
wait_health

say "2. 正常观测 seq 1..3,站点应为 healthy"
r=$(post_obs evt-acc-1 1 "$(iso_ago 30)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.classification' projection
assert_jq "$r" '.status' healthy
post_obs evt-acc-2 2 "$(iso_ago 25)" "$(now_iso)" 8 1.5 >/dev/null
post_obs evt-acc-3 3 "$(iso_ago 20)" "$(now_iso)" 8 1.5 >/dev/null
assert_jq "$(handoff)" '.status' healthy

say "3. 人为留下序号缺口:跳过 4,直接发送 5"
r=$(post_obs evt-acc-5 5 "$(iso_ago 15)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.classification' projection
assert_jq "$r" '.status' suspect
assert_jq "$r" '.gap_opened.from' 4
assert_jq "$r" '.gap_opened.to' 4
h=$(handoff)
assert_jq "$h" '.status' suspect
assert_jq "$h" '.sources[0].status_reason' gap_opened
assert_jq "$h" '.sources[0].status_seq' 5
assert_jq "$h" '.sources[0].pending_gaps[0].from' 4
assert_jq "$h" '.sources[0].pending_gaps[0].to' 4
assert_jq "$h" '.sources[0].contiguous_watermark' 3

say "4. 继续发送 6,恢复进度 1/3"
r=$(post_obs evt-acc-6 6 "$(iso_ago 10)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.streak' 1
assert_jq "$r" '.status' suspect

say "5. 跨链路补传缺口 4:只进历史,投影不回退"
r=$(post_obs evt-acc-4 4 "$(iso_ago 240)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.classification' backfill
assert_jq "$r" '.gap_filled' 4
h=$(handoff)
assert_jq "$h" '.status' suspect
assert_jq "$h" '.sources[0].watermark' 6
assert_jq "$h" '.sources[0].streak' 1
assert_jq "$h" '.sources[0].pending_gaps | length' 0
assert_jq "$h" '.sources[0].contiguous_watermark' 6
assert_jq "$h" '.sources[0].last_backfill.source_seq' 4

say "6. 新鲜合格样本 7、8 到达,真正恢复发生在样本 8"
r=$(post_obs evt-acc-7 7 "$(iso_ago 8)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.streak' 2
assert_jq "$r" '.status' suspect
r=$(post_obs evt-acc-8 8 "$(iso_ago 6)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.streak' 3
assert_jq "$r" '.status' healthy
assert_jq "$r" '.recovered' true
h=$(handoff)
assert_jq "$h" '.status' healthy
assert_jq "$h" '.sources[0].status_reason' healthy_streak_restored
assert_jq "$h" '.sources[0].last_recovery.source_seq' 8

say "7. 超时迟到样本(seq 9,观测于 10 分钟前)只进历史,水位不动"
r=$(post_obs evt-acc-9 9 "$(iso_ago 600)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.classification' backfill
assert_jq "$r" '.late_by_time' true
assert_jq "$(handoff)" '.sources[0].watermark' 8

say "8. event_id 重送不重复入账"
r=$(post_obs evt-acc-8 8 "$(iso_ago 6)" "$(now_iso)" 8 1.5)
assert_jq "$r" '.duplicate' true
assert_jq "$r" '.classification' duplicate_event

say "9. 两个接收端争用同一序号 10,只保留一条"
tmpdir=$(mktemp -d)
post_obs evt-acc-10a 10 "$(iso_ago 4)" "$(now_iso)" 8 1.5 >"$tmpdir/a.json" &
post_obs evt-acc-10b 10 "$(iso_ago 4)" "$(now_iso)" 8 1.5 >"$tmpdir/b.json" &
wait
recorded=$(jq -s '[.[] | select(.recorded == true)] | length' "$tmpdir/a.json" "$tmpdir/b.json")
dups=$(jq -s '[.[] | select(.classification == "duplicate_seq")] | length' "$tmpdir/a.json" "$tmpdir/b.json")
[ "$recorded" = "1" ] || fail "序号 10 应恰好一条入账,实际 $recorded"
[ "$dups" = "1" ] || fail "序号 10 应恰好一条被判争用重复,实际 $dups"
seq10=$(curl -sS "$BASE/v1/events?station_id=$STATION&source_id=$SOURCE&limit=100" \
  | jq '[.events[] | select(.source_seq == 10)] | length')
[ "$seq10" = "1" ] || fail "事件流中序号 10 应只有一条,实际 $seq10"

say "10. 重启整套环境(持久卷保留全部状态)"
$COMPOSE restart
wait_health

say "11. 交班结果:延续原水位,真正恢复仍指向样本 8,补传不被误认"
h=$(handoff)
assert_jq "$h" '.status' healthy
assert_jq "$h" '.sources[0].watermark' 10
assert_jq "$h" '.sources[0].contiguous_watermark' 10
assert_jq "$h" '.sources[0].status_reason' healthy_streak_restored
assert_jq "$h" '.sources[0].status_seq' 8
assert_jq "$h" '.sources[0].last_recovery.source_seq' 8
assert_jq "$h" '.sources[0].pending_gaps | length' 0
assert_jq "$h" '.sources[0].last_backfill.source_seq' 9
recovery_seq=$(jq -r '.sources[0].last_recovery.source_seq' <<<"$h")
[ "$recovery_seq" != "4" ] || fail "补传的样本 4 被误认作恢复点"

say "12. 事件流可断点续读(每页 3 条)"
cursor=0
total=0
seen=$(mktemp)
while :; do
  page=$(curl -sS "$BASE/v1/events?station_id=$STATION&after=$cursor&limit=3")
  n=$(jq '.count' <<<"$page")
  [ "$n" = "0" ] && break
  jq -r '.events[].stream_id' <<<"$page" >>"$seen"
  cursor=$(jq '.next_cursor' <<<"$page")
  total=$((total + n))
  [ "$(jq -r '.has_more' <<<"$page")" = "true" ] || break
done
[ "$total" = "10" ] || fail "事件流应恰好 10 条(重发与争用失败不入账),实际 $total"
[ "$(sort -u "$seen" | wc -l | tr -d ' ')" = "$total" ] || fail "续读过程中出现重复事件"

say "验收通过"
echo "最终交班视图:"
echo "$h" | jq .
echo
echo "环境仍在运行,清理请执行: $COMPOSE down -v"
