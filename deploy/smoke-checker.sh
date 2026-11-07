#!/bin/bash
# Milestone 3 acceptance. A checker that reports zero proves little unless it is
# also shown finding real damage, so this runs one clean cycle and one with a
# Kafka outage, and compares the second against an independent count.
set -uo pipefail

work=$(mktemp -d)
fail=0
check() { if [ "$2" == "$3" ]; then echo "ok    $1"; else echo "FAIL  $1: want '$2', got '$3'"; fail=1; fi; }
psql_() { docker compose exec -T postgres-primary psql -U app -d shop -Atc "$1"; }
kafka() { docker compose exec -T broker-1 /opt/kafka/bin/"$@"; }
proxy() { curl -fsS -X POST "localhost:8474/proxies/$1" -d "{\"enabled\":$2}" >/dev/null; }
field() { python3 -c "import json,sys;print(json.load(open(sys.argv[1]))[sys.argv[2]])" "$1" "$2"; }

go test ./internal/checker/... -run Golden -count=1 | tail -1
go build -o "$work/loadgen" ./cmd/loadgen && go build -o "$work/checker" ./cmd/checker || exit 1

start_cycle() {
  export CYCLE_ID="$1" PATTERN=P1
  psql_ "TRUNCATE orders, orders_history, outbox" >/dev/null
  kafka kafka-topics.sh --bootstrap-server localhost:29091 --create --topic "orders.$CYCLE_ID" \
    --partitions 3 --config message.timestamp.type=LogAppendTime >/dev/null
  docker compose --profile app up -d --build --force-recreate --wait orderservice 2>&1 | tail -1
}
finish_cycle() {
  docker compose stop orderservice >/dev/null 2>&1
  docker compose logs --no-log-prefix orderservice > "$work/$1.log" 2>/dev/null
  "$work/checker" -cycle "$CYCLE_ID" -logs "$work/$1.log" -damage "$work/$1.damage.json" > "$work/$1.json" || { echo "FAIL  checker errored"; exit 1; }
  cat "$work/$1.json"
}

echo "--- no-fault cycle: 100 orders/s for 30 s"
start_cycle "M3_clean_$(date +%s)"
LOAD_RATE_PER_S=100 LOAD_DURATION_S=30 "$work/loadgen" | tee "$work/clean.load.json"
finish_cycle clean
check "no failed requests" 0 "$(field "$work/clean.load.json" failed)"
for metric in lost ghost duplicates out_of_order lost_after_send send_failed; do
  check "clean: $metric" 0 "$(field "$work/clean.json" "$metric")"
done
check "checker saw every order" "$(field "$work/clean.load.json" orders)" "$(field "$work/clean.json" orders_total)"
check "checker saw every version" "$(psql_ 'SELECT count(*) FROM orders_history')" "$(field "$work/clean.json" events_total)"

echo "--- damaged cycle: Kafka cut for 8 s during load"
# Idempotence off: with it on, the same cut sometimes loses nothing (finding F24).
export KAFKA_IDEMPOTENT=false
start_cycle "M3_outage_$(date +%s)"
LOAD_RATE_PER_S=100 LOAD_DURATION_S=25 "$work/loadgen" > "$work/outage.load.json" &
load=$!
sleep 8; proxy kafka-app-b1 false
sleep 8; proxy kafka-app-b1 true
wait $load
sleep 2
finish_cycle outage
lost=$(field "$work/outage.json" lost)
failed=$(field "$work/outage.json" send_failed)
[ "$lost" -gt 0 ] && echo "ok    outage produced lost events ($lost)" || { echo "FAIL  no lost events"; fail=1; }
check "every loss has a SEND_FAILED marker" "$failed" "$lost"
check "lost events were all sent to the producer" "$lost" "$(field "$work/outage.json" lost_after_send)"
check "no ghosts" 0 "$(field "$work/outage.json" ghost)"
check "damage file lists each loss" "$lost" "$(python3 -c "import json,sys;print(len(json.load(open(sys.argv[1]))['lost']))" "$work/outage.damage.json")"

unset KAFKA_IDEMPOTENT

echo "--- guard: a topic without broker timestamps is refused"
guard="M3_guard_$(date +%s)"
kafka kafka-topics.sh --bootstrap-server localhost:29091 --create --topic "orders.$guard" --partitions 1 >/dev/null
echo '{}' | kafka kafka-console-producer.sh --bootstrap-server localhost:29091 --topic "orders.$guard" 2>/dev/null
if "$work/checker" -cycle "$guard" 2>"$work/guard.err" >/dev/null; then echo "FAIL  checker accepted CreateTime topic"; fail=1
else grep -q LogAppendTime "$work/guard.err" && echo "ok    CreateTime topic refused" || { echo "FAIL  wrong error: $(cat "$work/guard.err")"; fail=1; }; fi

echo "evidence in $work"
exit $fail
