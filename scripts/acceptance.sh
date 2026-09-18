#!/usr/bin/env bash
# 验收演练（纯后端，无需人工干预）：
#
#   1) 人为留下一个序号缺口：seq 1..3 建立 healthy 后，seq 5 跳号到达；
#   2) 交班接口必须指出：状态依据（link_gap，旧正常数据不算恢复）与待补区间 [4,4]，
#      连续水位停在 3；
#   3) seq 4 跨链路晚到补传：入账、回填缺口、水位推进到 5，但当前投影不回退
#      （applied_to_projection=false，observed_at 早于当前表头），
#      几分钟前的正常数据不被当作刚恢复；
#   4) 补齐后重启整套环境，交班结果延续原水位；随后 seq 6/7 到达，
#      真正恢复发生在第 3 个连续合格样本 seq 7；
#   5) event_id 重送不重复入账；两个接收端并发争用同一序号只保留一条；
#   6) 事件流以 after 游标断点续读，不重不漏；
#   7) 超时静默翻 offline，重新取得完整 3 连才回 healthy。
#
# 用法：
#   scripts/acceptance.sh                              # 默认 http://127.0.0.1:8080
#   BASE_URL=http://host:8080 scripts/acceptance.sh
#   RESTART_CMD='docker compose restart app postgres' scripts/acceptance.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
RESTART_CMD="${RESTART_CMD:-docker compose restart app postgres}"
RUN_ID="$(date +%s)"
STATION="station-accept-$RUN_ID"
RACE_STATION="station-race-$RUN_ID"
OLD_STATION="station-old-$RUN_ID"
SRC="link-a"
ST="$STATION"

# 观测时间线以当前墙钟为基准（间隔 10s），保证演练过程中不触发 90s 离线翻转。
T0=$(( $(date -u +%s) - 50 ))
iso() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }
NOW_ISO() { date -u +%Y-%m-%dT%H:%M:%SZ; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31m验收失败: %s\033[0m\n' "$*" >&2; exit 1; }

# post_event <station> <event_id> <seq> <observed_at> <received_at> <satellites> <pdop>
post_event() {
  local station="$1" eid="$2" seq="$3" obs="$4" recv="$5" sats="$6" pdop="$7"
  local body
  body=$(STATION="$station" EID="$eid" SEQ="$seq" OBS="$obs" RECV="$recv" SATS="$sats" PDOP="$pdop" \
    python3 - <<'PY'
import json, os
print(json.dumps({
    "event_id": os.environ["EID"],
    "station_id": os.environ["STATION"],
    "source_id": "link-a",
    "source_seq": int(os.environ["SEQ"]),
    "observed_at": os.environ["OBS"],
    "received_at": os.environ["RECV"],
    "metrics": {"satellites": int(os.environ["SATS"]), "pdop": float(os.environ["PDOP"])},
}))
PY
  )
  curl -sS -X POST "$BASE_URL/v1/events" -H 'Content-Type: application/json' -d "$body"
}

handoff() { curl -sS "$BASE_URL/v1/stations/$1/handoff"; }

# assert_json <json-file> <python-expr（变量 j 为文档）> <message>
assert_json() {
  local file="$1" expr="$2" msg="$3"
  python3 - "$file" "$expr" "$msg" <<'PY'
import json, sys
j = json.load(open(sys.argv[1]))
ok = eval(sys.argv[2], {"__builtins__": {}}, {"j": j})
if not ok:
    print(json.dumps(j, ensure_ascii=False, indent=2), file=sys.stderr)
    sys.exit(sys.argv[3])
print("ok:", sys.argv[3])
PY
}

wait_healthy() {
  for _ in $(seq 1 60); do
    curl -fsS "$BASE_URL/health" >/dev/null 2>&1 && return 0
    sleep 2
  done
  return 1
}

say "0) 等待应用就绪"
wait_healthy || fail "服务未就绪: $BASE_URL"

say "1) seq 1,2,3 连续合格 -> 第 3 个样本建立 healthy"
post_event "$STATION" "$RUN_ID-e-1" 1 "$(iso $((T0)))"    "$(iso $((T0+2)))"  8 1.6 >"$tmp/p1.json"
post_event "$STATION" "$RUN_ID-e-2" 2 "$(iso $((T0+10)))" "$(iso $((T0+12)))" 8 1.6 >"$tmp/p2.json"
post_event "$STATION" "$RUN_ID-e-3" 3 "$(iso $((T0+20)))" "$(iso $((T0+22)))" 8 1.6 >"$tmp/p3.json"
assert_json "$tmp/p3.json" "j['status']=='accepted' and j['projection']['state']=='healthy' and j['projection']['recovered_at_sample']['event_id'].endswith('e-3')" "seq3 达成 healthy 且锚定 e-3"

say "2) 人为留下缺口：seq 5 跳号到达（缺 seq 4）"
post_event "$STATION" "$RUN_ID-e-5" 5 "$(iso $((T0+40)))" "$(iso $((T0+42)))" 8 1.6 >"$tmp/p5.json"
assert_json "$tmp/p5.json" "j['status']=='accepted' and j['projection']['state']=='suspect' and j['projection']['state_reason']=='link_gap' and j['watermark']==3" "跳号后 suspect/link_gap 且水位停在 3"

say "3) 交班：状态依据 + 待补序号区间 [4,4]"
handoff "$STATION" >"$tmp/h1.json"
assert_json "$tmp/h1.json" "j['projection']['state']=='suspect'" "交班状态 suspect"
assert_json "$tmp/h1.json" "j['sources'][0]['missing_ranges']==[{'start':4,'end':4}] and j['sources'][0]['contiguous_seq']==3" "交班指出待补区间 4..4、水位 3"
assert_json "$tmp/h1.json" "'跳号' in j['projection']['state_basis']" "交班给出跳号状态依据"

say "4) seq 4 跨链路晚到补传（observed_at 停留在 e-5 之前，received_at 为现在）"
post_event "$STATION" "$RUN_ID-e-4-late" 4 "$(iso $((T0+30)))" "$(NOW_ISO)" 8 1.6 >"$tmp/p4.json"
assert_json "$tmp/p4.json" "j['status']=='accepted' and j['applied_to_projection'] is False and j['watermark']==5" "晚到补传入账、不回退/推进投影、水位到 5"
handoff "$STATION" >"$tmp/h2.json"
assert_json "$tmp/h2.json" "j['sources'][0]['missing_ranges']==[] and j['sources'][0]['contiguous_seq']==5" "缺口已清零、水位延续到 5"
assert_json "$tmp/h2.json" "j['projection']['last_sample']['source_seq']==5 and j['projection']['healthy_streak']==1" "当前投影仍以 seq5 为表头，未被历史样本回退"

say "5) 重启整套环境（$RESTART_CMD）"
bash -c "$RESTART_CMD"
wait_healthy || fail "重启后服务未恢复"
handoff "$STATION" >"$tmp/h3.json"
assert_json "$tmp/h3.json" "j['sources'][0]['contiguous_seq']==5 and j['sources'][0]['missing_ranges']==[]" "重启后水位仍为 5、缺口集合仍为空（持久卷生效）"
assert_json "$tmp/h3.json" "j['projection']['state']=='suspect' and j['projection']['healthy_streak']==1" "重启后投影与计数延续，未被误判为刚恢复"

say "6) 重启后 seq 6,7 到达：真正恢复发生在 seq 7（e-5 之后第 3 个连续合格样本）"
R=$(( $(date -u +%s) ))
post_event "$STATION" "$RUN_ID-e-6" 6 "$(iso $((R)))"    "$(iso $((R+2)))"  8 1.6 >"$tmp/p6.json"
post_event "$STATION" "$RUN_ID-e-7" 7 "$(iso $((R+10)))" "$(iso $((R+12)))" 8 1.6 >"$tmp/p7.json"
assert_json "$tmp/p6.json" "j['projection']['healthy_streak']==2 and j['projection']['state']=='suspect'" "seq6 计数 2/3 仍 suspect"
assert_json "$tmp/p7.json" "j['projection']['state']=='healthy' and j['projection']['recovered_at_sample']['event_id'].endswith('e-7') and j['projection']['recovered_at_sample']['source_seq']==7 and j['watermark']==7" "恢复锚定第 3 个样本 e-7(seq7)、水位 7"
handoff "$STATION" >"$tmp/h4.json"
assert_json "$tmp/h4.json" "j['projection']['recovered_at_sample']['event_id'].endswith('e-7') and '真正恢复' in j['projection']['state_basis']" "交班依据明确指出真正恢复样本 e-7"

say "7) event_id 重送不重复入账"
post_event "$STATION" "$RUN_ID-e-3" 3 "$(iso $((T0+20)))" "$(iso $((T0+22)))" 8 1.6 >"$tmp/dup.json"
assert_json "$tmp/dup.json" "j['status']=='duplicate_event'" "重送返回 duplicate_event"

say "8) 两个接收端并发争用 seq 100，只保留一条（独立站点，不污染主演练）"
for id in r1 r2 r3 r4 r5 r6 r7 r8; do
  body=$(STATION="$RACE_STATION" EID="$RUN_ID-race-$id" python3 - <<'PY'
import json, os
print(json.dumps({"event_id": os.environ["EID"], "station_id": os.environ["STATION"], "source_id": "link-a",
 "source_seq": 100, "observed_at": "2026-09-18T07:00:00Z", "received_at": "2026-09-18T07:00:02Z",
 "metrics": {"satellites": 8, "pdop": 1.6}}))
PY
)
  curl -sS -X POST "$BASE_URL/v1/events" -H 'Content-Type: application/json' -d "$body" >"$tmp/race-$id.json" &
done
wait
accepted=$(grep -l '"status":"accepted"' "$tmp"/race-*.json | wc -l | tr -d ' ')
conflicts=$(grep -l '"status":"conflict_seq"' "$tmp"/race-*.json | wc -l | tr -d ' ')
[ "$accepted" = "1" ] && [ "$conflicts" = "7" ] || fail "序号竞争结果异常 accepted=$accepted conflicts=$conflicts"
echo "ok: 8 个并发请求中 1 条 accepted、7 条 conflict_seq，且回执指出获胜事件"
# 每个冲突回执都必须指出获胜事件，且获胜者正是唯一 accepted 的那个 event_id。
winner_id=$(grep -l '"status":"accepted"' "$tmp"/race-*.json | head -1 | sed "s#.*race-\([^.]*\)\.json#$RUN_ID-race-\1#")
for f in "$tmp"/race-*.json; do
  grep -q '"status":"conflict_seq"' "$f" || continue
  grep -q "\"winner_event_id\":\"$winner_id\"" "$f" || fail "冲突回执 $f 未正确指出获胜事件 $winner_id"
done
echo "ok: 7 条冲突回执均指向获胜事件 $winner_id"

say "9) 事件流断点续读（after 游标，不重不漏）"
python3 - "$BASE_URL" <<'PY'
import json, sys, urllib.request
base = sys.argv[1]
cursor, seen, pages = 0, [], 0
while True:
    with urllib.request.urlopen(f"{base}/v1/events?after={cursor}&limit=3") as r:
        page = json.load(r)
    pages += 1
    evs = page["events"]
    if not evs:
        break
    ids = [e["id"] for e in evs]
    if ids != sorted(ids) or len(ids) != len(set(ids)):
        sys.exit("事件流页内乱序或重复")
    seen.extend(ids)
    cursor = page["next_cursor"]
    if not page["has_more"]:
        break
    if pages > 100:
        sys.exit("分页未收敛")
if seen != sorted(seen) or len(seen) != len(set(seen)):
    sys.exit("断点续读整体乱序或重复")
print(f"ok: 断点续读 {len(seen)} 个事件、{pages} 页，游标推进到 {cursor}，不重不漏")
PY

say "10) 离线规则：静默超 90s 交班为 offline；重新 3 连才回 healthy"
HOUR_AGO=$(iso $(( $(date -u +%s) - 3600 )))
post_event "$OLD_STATION" "$RUN_ID-old-1" 1 "$HOUR_AGO" "$HOUR_AGO" 8 1.6 >/dev/null
handoff "$OLD_STATION" >"$tmp/old1.json"
assert_json "$tmp/old1.json" "j['projection']['state']=='offline' and j['projection']['offline_since'] is not None" "超时静默交班为 offline"
N=$(( $(date -u +%s) ))
post_event "$OLD_STATION" "$RUN_ID-old-2" 2 "$(iso $((N)))"    "$(iso $((N+2)))"  8 1.6 >/dev/null
post_event "$OLD_STATION" "$RUN_ID-old-3" 3 "$(iso $((N+10)))" "$(iso $((N+12)))" 8 1.6 >"$tmp/old3.json"
post_event "$OLD_STATION" "$RUN_ID-old-4" 4 "$(iso $((N+20)))" "$(iso $((N+22)))" 8 1.6 >"$tmp/old4.json"
assert_json "$tmp/old3.json" "j['projection']['state']=='suspect' and j['projection']['healthy_streak']==2" "offline 后 2 连仍 suspect"
assert_json "$tmp/old4.json" "j['projection']['state']=='healthy' and j['projection']['recovered_at_sample']['event_id'].endswith('old-4')" "offline 后第 3 连 old-4 才恢复"

printf '\n\033[1;32m全部验收通过：留缺口 -> 交班指出依据与待补区间 -> 晚到补传不回退投影 -> 重启水位延续 -> 真正恢复锚定 e-7(seq7)\033[0m\n'
