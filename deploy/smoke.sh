#!/bin/bash
# Milestone 1 acceptance: each check exercises the path the study depends on,
# not just "the container is up".
set -uo pipefail

fail=0
check() { # name, expected, actual
  if [ "$2" == "$3" ]; then echo "ok    $1"; else echo "FAIL  $1: want '$2', got '$3'"; fail=1; fi
}
primary() { docker compose exec -T postgres-primary psql -U app -d shop -Atc "$1"; }
replica() { docker compose exec -T postgres-replica psql -U app -d shop -Atc "$1"; }
kafka() { docker compose exec -T broker-1 /opt/kafka/bin/"$@"; }

check "wal_level is logical" logical "$(primary 'SHOW wal_level')"
check "idle_in_transaction timeout" 10s "$(primary 'SHOW idle_in_transaction_session_timeout')"
check "commit timestamps on primary" on "$(primary 'SHOW track_commit_timestamp')"
check "commit timestamps on replica" on "$(replica 'SHOW track_commit_timestamp')"
check "replica in recovery" t "$(replica 'SELECT pg_is_in_recovery()')"
check "replica streaming" streaming "$(primary 'SELECT state FROM pg_stat_replication')"

# A write on the primary must reach the replica, and the history trigger must fire.
id=$(uuidgen | tr 'A-Z' 'a-z')
primary "INSERT INTO orders (order_id, status, version, amount) VALUES ('$id', 'PLACED', 1, 1)" >/dev/null
sleep 1
check "row replicated" 1 "$(replica "SELECT count(*) FROM orders WHERE order_id = '$id'")"
check "history trigger fired" 1 "$(primary "SELECT count(*) FROM orders_history WHERE order_id = '$id'")"
check "commit time readable on replica" 1 "$(replica "SELECT count(pg_xact_commit_timestamp(xmin)) FROM orders_history WHERE order_id = '$id'")"
primary "DELETE FROM orders_history WHERE order_id = '$id'; DELETE FROM orders WHERE order_id = '$id'" >/dev/null

# Postgres through both proxy groups, from inside the network.
for port in 15432 15433; do
  check "postgres via toxiproxy:$port" 1 "$(docker compose exec -T -e PGPASSWORD=app postgres-primary psql -h toxiproxy-pg -p $port -U app -d shop -Atc 'SELECT 1')"
done

# Each client class must be told to stay on its own proxy after metadata discovery.
versions() { docker compose exec -T kafka-connect bash -c "timeout 15 bin/kafka-broker-api-versions.sh --bootstrap-server toxiproxy:$1 2>/dev/null | head -1"; }
for port in 19091 19191; do
  meta=$(versions $port)
  case "$meta" in
    toxiproxy:$port*) echo "ok    kafka keeps client on proxy $port" ;;
    *) echo "FAIL  advertised listener for $port: $meta"; fail=1 ;;
  esac
done

# Cutting one group must cut only that class of client: no bypass, no cross-talk.
curl -fsS -X POST localhost:8474/proxies/kafka-app-b1 -d '{"enabled":false}' >/dev/null
case "$(versions 19091)" in
  toxiproxy:19091*) echo "FAIL  app client reached kafka with its proxy disabled"; fail=1 ;;
  *) echo "ok    app clients cut off when kafka-app disabled" ;;
esac
case "$(versions 19191)" in
  toxiproxy:19191*) echo "ok    connect clients unaffected by kafka-app fault" ;;
  *) echo "FAIL  connect client affected by kafka-app fault"; fail=1 ;;
esac
curl -fsS -X POST localhost:8474/proxies/kafka-app-b1 -d '{"enabled":true}' >/dev/null

# Host listener round trip, as the runner and checker will use it.
topic="smoke.$id"
kafka kafka-topics.sh --bootstrap-server localhost:29091 --create --topic "$topic" --partitions 3 >/dev/null
echo hello | kafka kafka-console-producer.sh --bootstrap-server localhost:29091 --topic "$topic" 2>/dev/null
got=$(kafka kafka-console-consumer.sh --bootstrap-server localhost:29091 --topic "$topic" --from-beginning --max-messages 1 --timeout-ms 15000 2>/dev/null)
check "kafka round trip" hello "$got"
kafka kafka-topics.sh --bootstrap-server localhost:29091 --delete --topic "$topic" >/dev/null

plugins=$(curl -fsS localhost:8083/connector-plugins)
case "$plugins" in
  *io.debezium.connector.postgresql.PostgresConnector*) echo "ok    debezium postgres plugin loaded" ;;
  *) echo "FAIL  debezium plugin missing"; fail=1 ;;
esac

[ "$(docker compose ps --format '{{.Service}}' | wc -l | tr -d ' ')" == 6 ] && echo "ok    6 services running" || { echo "FAIL  service count"; fail=1; }
echo; for s in $(docker compose images --format json | python3 -c "import sys,json;[print(i['Repository']+':'+i['Tag']) for i in json.load(sys.stdin)]" | sort -u); do
  echo "arch $(docker image inspect "$s" --format '{{.Architecture}}')  $s"
done
exit $fail
