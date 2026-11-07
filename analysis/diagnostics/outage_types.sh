#!/bin/bash
# Does "the delivery timeout does not fire during an outage" depend on how the
# outage is made? Runs P2 under each outage type and setting. Usage: outage_types.sh <out dir> <loadgen> <checker>
set -uo pipefail
out=$1 loadgen=$2 checker=$3
api=localhost:8474/proxies/kafka-app-b1
mkdir -p "$out"
inject() { case $1 in
  cut)       curl -fsS -X POST $api -d '{"enabled":false}' >/dev/null ;;
  blackhole) for s in upstream downstream; do curl -fsS -X POST $api/toxics -d "{\"name\":\"bh_$s\",\"type\":\"timeout\",\"stream\":\"$s\",\"attributes\":{\"timeout\":0}}" >/dev/null; done ;;
  reset)     curl -fsS -X POST $api/toxics -d '{"name":"rp","type":"reset_peer","stream":"upstream","toxicity":1,"attributes":{"timeout":0}}' >/dev/null ;;
  broker)    docker compose stop broker-1 >/dev/null 2>&1 ;;
esac; }
restore() { case $1 in
  cut)       curl -fsS -X POST $api -d '{"enabled":true}' >/dev/null ;;
  blackhole) for s in upstream downstream; do curl -fsS -X DELETE $api/toxics/bh_$s >/dev/null; done ;;
  reset)     curl -fsS -X DELETE $api/toxics/rp >/dev/null ;;
  broker)    docker compose start broker-1 >/dev/null 2>&1 ;;
esac; }
run() { # label, outage, idempotent, record retries
  export CYCLE_ID="OT_$1_$(date +%s)" PATTERN=P2 CRASH_AT= KAFKA_SYNC_SEND=false KAFKA_TRACE_ATTEMPTS=false KAFKA_IDEMPOTENT=$3 KAFKA_RECORD_RETRIES=$4
  docker compose exec -T postgres-primary psql -U app -d shop -Atc "TRUNCATE orders, orders_history, outbox" >/dev/null
  docker compose exec -T broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29091 --create --topic "orders.$CYCLE_ID" --partitions 3 --config message.timestamp.type=LogAppendTime >/dev/null
  docker compose --profile app up -d --force-recreate --wait orderservice >/dev/null 2>&1
  LOAD_RATE_PER_S=100 LOAD_DURATION_S=30 LOAD_SEED=7 "$loadgen" > "$out/$1.load.json" &
  load=$!
  sleep 8
  t0=$(python3 -c 'import time;print(time.time())'); inject "$2"
  sleep 8
  t1=$(python3 -c 'import time;print(time.time())'); restore "$2"
  echo "$t0 $t1" > "$out/$1.fault_times"
  wait $load; sleep 15
  docker compose stop orderservice >/dev/null 2>&1
  docker compose logs --no-log-prefix orderservice > "$out/$1.log" 2>/dev/null
  "$checker" -cycle "$CYCLE_ID" -logs "$out/$1.log" > "$out/$1.json"
  python3 - "$out/$1" "$1" "$t0" "$t1" <<'PY'
import json,sys,datetime as dt
base,label,t0,t1=sys.argv[1],sys.argv[2],float(sys.argv[3]),float(sys.argv[4])
fails,retries,exhausted=[],0,0
for l in open(base+".log"):
    try:e=json.loads(l)
    except ValueError: continue
    if not isinstance(e,dict): continue
    m=e.get("msg")
    if m=="SEND_FAILED": fails.append(dt.datetime.fromisoformat(e["time"][:26]+"+00:00").timestamp())
    retries+=m=="RETRY_SCHEDULED"; exhausted+=m=="RETRY_EXHAUSTED"
r=json.load(open(base+".json"))
first=f"{min(fails)-t0:.1f}" if fails else "-"
during=sum(1 for f in fails if f<t1)
print(f"{label:22} first_fail_s={first:>5} restored_s={t1-t0:.1f} failed_during={during:5} failed_total={len(fails):5} retries={retries:5} exhausted={exhausted:4} lost={r['lost']:5} dup={r['duplicates']:4} stale={r['stale_redelivery']:3} window_max_s={r['window_max_ms']/1000:.2f}")
PY
}
run cut_on       cut       true  0
run blackhole_on blackhole true  0
run reset_on     reset     true  0
run broker_on    broker    true  0
run cut_off      cut       false 0
run blackhole_off blackhole false 0
run broker_off   broker    false 0
run cut_on_r3    cut       true  3
run cut_off_r3   cut       false 3
