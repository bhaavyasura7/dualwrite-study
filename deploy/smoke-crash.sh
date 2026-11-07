#!/bin/bash
# Milestone 4 acceptance. Sequential requests leave nothing else in flight, so the
# damage at each crash point is exactly predictable. A run under load then checks
# that hooked and collateral damage are separated.
set -uo pipefail

work=$(mktemp -d)
fail=0
api=http://localhost:8080
check() { if [ "$2" == "$3" ]; then echo "ok    $1"; else echo "FAIL  $1: want '$2', got '$3'"; fail=1; fi; }
psql_() { docker compose exec -T postgres-primary psql -U app -d shop -Atc "$1"; }
kafka() { docker compose exec -T broker-1 /opt/kafka/bin/"$@"; }
field() { python3 -c "import json,sys;print(json.load(open(sys.argv[1]))[sys.argv[2]])" "$1" "$2"; }
service() { docker compose --profile app up -d --force-recreate --wait orderservice >/dev/null 2>&1; }
save_logs() { docker compose logs --no-log-prefix orderservice >> "$work/$1.log" 2>/dev/null; }

go build -o "$work/loadgen" ./cmd/loadgen && go build -o "$work/checker" ./cmd/checker || exit 1
docker compose --profile app build orderservice >/dev/null 2>&1

begin() { # name, crash point, sync send
  export CYCLE_ID="M4_$1_$(date +%s)" PATTERN=P1 CRASH_AT="$2" CRASH_AFTER_N=50 KAFKA_SYNC_SEND="$3"
  psql_ "TRUNCATE orders, orders_history, outbox" >/dev/null
  kafka kafka-topics.sh --bootstrap-server localhost:29091 --create --topic "orders.$CYCLE_ID" \
    --partitions 3 --config message.timestamp.type=LogAppendTime >/dev/null
  service
}
verify_crash() { # name
  check "$1: died by SIGKILL" 137 "$(docker inspect -f '{{.State.ExitCode}}' dualwrite-orderservice-1)"
  save_logs "$1"
  check "$1: marker logged once" 1 "$(grep -c CRASH_POINT_HIT "$work/$1.log")"
  check "$1: no graceful shutdown" 0 "$(grep -c 'shutting down' "$work/$1.log")"
}
recover() { # name; the recovery rule: recreate with the crash point cleared
  export CRASH_AT=
  service
  for _ in $(seq 10); do curl -s -o /dev/null --max-time 5 -X POST "$api/orders" -d '{"amount": 1}'; done
  sleep 0.5
  docker compose stop orderservice >/dev/null 2>&1
  save_logs "$1"
  "$work/checker" -cycle "$CYCLE_ID" -logs "$work/$1.log" -damage "$work/$1.damage.json" > "$work/$1.json" || { echo "FAIL  checker errored"; exit 1; }
}
sequential() { # name, point, sync, committed, lost, hooked_lost, lost_after_send
  begin "$1" "$2" "$3"
  sent=0
  until ! curl -s -o /dev/null --fail --max-time 5 -X POST "$api/orders" -d '{"amount": 1}' || [ $sent -ge 60 ]; do
    sent=$((sent + 1)); sleep 0.03
  done
  sleep 1
  verify_crash "$1"
  recover "$1"
  check "$1: committed versions" "$4" "$(field "$work/$1.json" events_total)"
  check "$1: lost" "$5" "$(field "$work/$1.json" lost)"
  check "$1: hooked_lost" "$6" "$(field "$work/$1.json" hooked_lost)"
  check "$1: collateral_lost" 0 "$(field "$work/$1.json" collateral_lost)"
  check "$1: lost_after_send" "$7" "$(field "$work/$1.json" lost_after_send)"
  check "$1: ghost" 0 "$(field "$work/$1.json" ghost)"
  check "$1: duplicates" 0 "$(field "$work/$1.json" duplicates)"
}

echo "--- exact cases: crash on the 50th sequential order, then 10 orders after recovery"
#          name         point                    sync  committed lost hooked after_send
sequential a1           after_db_commit          false 60        1    1      0
sequential a2           before_db_commit         false 59        0    0      0
sequential a8_async     after_send_before_flush  false 60        1    1      1
sequential a8_sync      after_send_before_flush  true  60        0    0      0

echo "--- A-1 under load: 100 orders/s, three runs"
for run in 1 2 3; do
  name="load$run"
  begin "$name" after_db_commit false
  LOAD_RATE_PER_S=100 LOAD_DURATION_S=12 LOAD_REQUEST_TIMEOUT_MS=2000 LOAD_SEED=$run "$work/loadgen" > "$work/$name.load.json" &
  load=$!
  sleep 5
  verify_crash "$name"
  wait $load
  recover "$name"
  check "$name: hooked event lost" 1 "$(field "$work/$name.json" hooked_lost)"
  check "$name: no ghosts or duplicates" "0 0" "$(field "$work/$name.json" ghost) $(field "$work/$name.json" duplicates)"
  # Independent check: nothing the broker acknowledged may be reported lost.
  python3 - "$work/$name.log" "$work/$name.damage.json" <<'PY'
import json,sys
sent,acked={},set()
for line in open(sys.argv[1]):
    try: e=json.loads(line)
    except ValueError: continue
    if e.get("msg")=="SENT_CALLED": sent[e["event_id"]]=(e["order_id"],e["seq"])
    if e.get("msg")=="SEND_ACKED": acked.add(e["event_id"])
acked_pairs={sent[i] for i in acked if i in sent}
lost={(p["order"],p["seq"]) for p in json.load(open(sys.argv[2]))["lost"] or []}
wrong=lost&acked_pairs
print(("FAIL " if wrong else "ok   "),f" lost={len(lost)} none acknowledged" if not wrong else f" {len(wrong)} acknowledged events reported lost")
sys.exit(1 if wrong else 0)
PY
  [ $? -eq 0 ] || fail=1
  echo "info  $name: lost=$(field "$work/$name.json" lost) collateral=$(field "$work/$name.json" collateral_lost) lost_after_send=$(field "$work/$name.json" lost_after_send)"
done

echo "evidence in $work"
exit $fail
