#!/usr/bin/env bash
# Один прогон: ./run.sh <scylla|cassandra|postgres|redis|mongodb|tarantool|rabbitmq|rocketmq|kafka> [KEY=VAL ...]
# Пример: ./run.sh postgres WRITE_BATCH=100 PG_CONSUME_MODE=delete DURATION=5m
# Поднимает чистую БД, гоняет бенч, параллельно снимает docker stats + размер data dir,
# сохраняет results/<run>.json, results/<run>.stats.csv, results/<run>.docker.json, затем удаляет БД с томами.
set -euo pipefail
cd "$(dirname "$0")"

backend=${1:?usage: ./run.sh <scylla|cassandra|postgres|redis|mongodb|tarantool|rabbitmq|rocketmq|kafka> [KEY=VAL ...]}
shift
for kv in "$@"; do export "$kv"; done

case $backend in
  scylla)    container=tb-scylla;    datadir=/var/lib/scylla ;;
  cassandra) container=tb-cassandra; datadir=/var/lib/cassandra ;;
  postgres) container=tb-postgres; datadir=/var/lib/postgresql/data ;;
  redis)    container=tb-redis;    datadir=/data ;;
  mongodb)   container=tb-mongodb;   datadir=/data/db ;;
  tarantool) container=tb-tarantool; datadir=/var/lib/tarantool ;;
  rabbitmq)  container=tb-rabbitmq;  datadir=/var/lib/rabbitmq ;;
  rocketmq)  container=tb-rocketmq;  datadir=/home/rocketmq/store ;;
  kafka)     container=tb-kafka;     datadir=/var/lib/kafka/data ;;
  *) echo "unknown backend $backend"; exit 1 ;;
esac

export RUN_ID=${RUN_ID:-$backend-$(date +%Y%m%d-%H%M%S)}
mkdir -p results
dc() { docker compose --profile "$backend" "$@"; }

dc down -v --remove-orphans >/dev/null 2>&1 || true
dc build -q "bench-$backend"
dc up -d --wait --wait-timeout 300 "$backend"

stats=results/$RUN_ID.stats.csv
echo "ts,cpu_pct,mem_mb,disk_mb" > "$stats"
(
  while docker inspect "$container" >/dev/null 2>&1; do
    line=$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' "$container" 2>/dev/null) || break
    cpu=${line%%,*}; cpu=${cpu%\%}
    mem=$(echo "${line#*,}" | awk '{v=$1; u=v; gsub(/[0-9.]/,"",u); gsub(/[A-Za-z]/,"",v);
      if (u=="GiB") v*=1024; else if (u=="KiB") v/=1024; else if (u=="B") v/=1048576; printf "%.0f", v}')
    disk=$(docker exec "$container" du -sm "$datadir" 2>/dev/null | awk '{print $1}')
    echo "$(date +%s),$cpu,$mem,${disk:-0}" >> "$stats"
    sleep 5
  done
) &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

dc run --rm "bench-$backend"

kill $sampler 2>/dev/null || true
wait $sampler 2>/dev/null || true

if [ "$backend" = scylla ] || [ "$backend" = cassandra ]; then
  docker exec "$container" nodetool tablestats bench.tasks 2>/dev/null \
    | grep -E 'Space used \(live\)|Number of partitions|SSTable count|tombstone' > "results/$RUN_ID.tablestats.txt" || true
fi

awk -F, 'NR>1 && $2!="" {n++; s+=$2; if($2>cm)cm=$2; if($3>mm)mm=$3; if($4>dm)dm=$4}
  END {printf "{\"cpu_avg\": %.1f, \"cpu_max\": %.1f, \"mem_max_mb\": %d, \"disk_max_mb\": %d}\n", (n?s/n:0), cm, mm, dm}' \
  "$stats" > "results/$RUN_ID.docker.json"
echo "DB resources ($container, CPU% = 100 на ядро): $(cat "results/$RUN_ID.docker.json")"

[ "${KEEP:-0}" = 1 ] || dc down -v >/dev/null 2>&1
