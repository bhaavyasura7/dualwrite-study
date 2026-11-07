#!/bin/bash
# Bootstraps the streaming replica with pg_basebackup on first start (SPEC 16).
set -euo pipefail

PGDATA=/var/lib/postgresql/data

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  chown -R postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
  until pg_isready -h postgres-primary -U replicator; do sleep 1; done
  # Through the pg-repl proxy, so replication lag can be injected (D-1).
  until pg_isready -h toxiproxy-pg -p 15434 -U replicator; do sleep 1; done
  gosu postgres pg_basebackup -h toxiproxy-pg -p 15434 -U replicator -D "$PGDATA" \
    -R -X stream -S replica_slot
fi

exec gosu postgres postgres -c config_file=/etc/postgresql/postgresql.conf
