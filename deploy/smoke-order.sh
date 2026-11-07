#!/bin/bash
# Milestone 2 acceptance: P1 end to end, comparing Postgres with Kafka exactly.
set -uo pipefail

export CYCLE_ID="M2_$(date +%s)" PATTERN=P1
topic="orders.$CYCLE_ID"
api=http://localhost:8080
fail=0
check() { if [ "$2" == "$3" ]; then echo "ok    $1"; else echo "FAIL  $1: want '$2', got '$3'"; fail=1; fi; }
psql_() { docker compose exec -T postgres-primary psql -U app -d shop -Atc "$1"; }
kafka() { docker compose exec -T broker-1 /opt/kafka/bin/"$@"; }
code() { curl -s -o /dev/null -w '%{http_code}' --max-time "${2:-10}" -X POST "$api$1" "${@:3}"; }
create() { curl -s --max-time 10 -X POST "$api/orders" -d '{"amount": 499.50}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["order_id"])'; }
proxy() { # pg proxies live on their own Toxiproxy instance
  local api=8474; case $1 in pg-*) api=8475 ;; esac
  curl -fsS -X POST "localhost:$api/proxies/$1" -d "{\"enabled\":$2}" >/dev/null
}
logs() { docker compose logs --no-log-prefix orderservice 2>/dev/null; }
marker() { logs | grep -c "\"msg\":\"$1\""; }

psql_ "TRUNCATE orders, orders_history, outbox" >/dev/null
kafka kafka-topics.sh --bootstrap-server localhost:29091 --create --topic "$topic" --partitions 3 >/dev/null
docker compose --profile app up -d --build --force-recreate --wait orderservice 2>&1 | tail -1

# 30 orders: 18 paid, 6 cancelled, 6 left PLACED, so 54 committed versions.
ids=()
for i in $(seq 30); do ids+=("$(create)"); done
for i in $(seq 0 17); do check_code=$(code "/orders/${ids[$i]}/pay"); [ "$check_code" == 204 ] || { echo "FAIL pay: $check_code"; fail=1; }; done
for i in $(seq 18 23); do check_code=$(code "/orders/${ids[$i]}/cancel"); [ "$check_code" == 204 ] || { echo "FAIL cancel: $check_code"; fail=1; }; done

check "illegal transition refused" 409 "$(code "/orders/${ids[0]}/cancel")"
check "unknown order refused" 409 "$(code "/orders/$(uuidgen)/pay")"
check "bad amount refused" 400 "$(code /orders 10 -d '{"amount": 0}')"
check "orders in postgres" 30 "$(psql_ 'SELECT count(*) FROM orders')"
check "versions in history" 54 "$(psql_ 'SELECT count(*) FROM orders_history')"

# Graceful stop right after a write: the record must still be delivered (SPEC 6.5).
last=$(create)
docker compose stop orderservice 2>&1 | tail -1
check "clean exit on SIGTERM" 0 "$(docker inspect -f '{{.State.ExitCode}}' dualwrite-orderservice-1)"
check "shutdown completed" 1 "$(logs | grep -c 'shutdown complete')"

expected=$(psql_ "SELECT order_id || ':' || version FROM orders_history ORDER BY 1")
records=$(kafka kafka-console-consumer.sh --bootstrap-server localhost:29091 --topic "$topic" \
  --from-beginning --timeout-ms 8000 --property print.key=true --property print.partition=true 2>/dev/null)
echo "$records" > "${TMPDIR:-/tmp}/m2-records.txt"
actual=$(python3 -c '
import json,sys,collections
parts=collections.defaultdict(set); out=[]
for line in open(sys.argv[1]):
    line=line.strip()
    if not line.startswith("Partition:"): continue
    part,key,value=line.split("\t",2)
    ev=json.loads(value)
    assert key==ev["order_id"], "key is not order_id"
    assert ev["cycle_id"]==sys.argv[2] and ev["pattern"]=="P1"
    assert len(ev)==9
    parts[key].add(part); out.append(ev["order_id"]+":"+str(ev["seq"]))
assert all(len(p)==1 for p in parts.values()), "order split across partitions"
print("\n".join(sorted(out)))' "${TMPDIR:-/tmp}/m2-records.txt" "$CYCLE_ID")
check "kafka matches postgres exactly" "$(echo "$expected" | wc -l | tr -d ' ') $(echo "$expected" | shasum)" "$(echo "$actual" | wc -l | tr -d ' ') $(echo "$actual" | shasum)"

check "SENT_CALLED per event" 55 "$(marker SENT_CALLED)"
check "SEND_ACKED per event" 55 "$(marker SEND_ACKED)"
check "no SEND_FAILED" 0 "$(marker SEND_FAILED)"
check "fields on every log line" 0 "$(logs | grep '^{' | grep -v "\"component\":\"orderservice\",\"pattern\":\"P1\",\"cycle_id\":\"$CYCLE_ID\"" | wc -l | tr -d ' ')"

# Traffic really uses the proxies: cutting each one must break the matching path.
docker compose --profile app up -d --wait orderservice 2>&1 | tail -1
proxy pg-app false
check "postgres path goes through pg-app" 500 "$(code /orders 10 -d '{"amount": 1}')"
proxy pg-app true

proxy kafka-app-b1 false
start=$(date +%s)
check "commit succeeds with kafka cut (dual write)" 201 "$(code /orders 10 -d '{"amount": 1}')"
until [ "$(marker SEND_FAILED)" -ge 1 ] || [ $(($(date +%s) - start)) -gt 20 ]; do sleep 0.2; done
took=$(($(date +%s) - start))
proxy kafka-app-b1 true
check "kafka failure reaches the app" 1 "$(marker SEND_FAILED)"
echo "info  SEND_FAILED after ${took}s (delivery timeout is 5s)"

docker compose stop orderservice >/dev/null 2>&1
exit $fail
