#!/usr/bin/env bash
# Быстрый замер потолков всех движков без лимита RPS на одинаковых ресурсах (DB_CPUS=4, RAM как в матрице).
# На каждую БД три прогона (~7 мин на БД, ~1 час на все):
#   write  — 1 минута записи одиночными задачами, 256 писателей;
#   batch  — 1 минута записи пачками по 100 (COPY у PG, UNLOGGED BATCH у CQL, insertMany, pipeline, ...), 64 писателя;
#   read   — prefill DRAIN_N задач пачками по 500 (это ещё и потолок батчевой записи) и вычитка на максимальной скорости.
# Результаты: results/peak-<ts>-<db>-{write,batch,read}.json
# Большие payload: PAYLOAD_BYTES=5120 PAYLOAD_BYTES_MAX=10240 MAX_WRITES=1000000 DRAIN_N=1000000 ./peak.sh
# (MAX_WRITES ограничивает объём записи за прогон — иначе минута записи не влезает в диск Docker VM;
#  для Redis отдельные REDIS_MAX_WRITES / REDIS_DRAIN_N, у него всё в maxmemory 5 GB)
cd "$(dirname "$0")"
D=${PEAK_DURATION:-1m}
ts=${PEAK_TS:-$(date +%m%d-%H%M)}
for db in ${DBS:-scylla cassandra postgres redis mongodb tarantool rabbitmq rocketmq}; do
  extra=()
  drain_n=${DRAIN_N:-5000000}
  case $db in
    # с 1.5 GB RabbitMQ уходит в memory alarm, см. run-all.sh
    rabbitmq) extra=(DISK_DB_MEM=${RABBIT_MEM:-4g} RABBIT_CONF=rabbitmq-4g.conf) ;;
    redis)
      [ -n "${REDIS_MAX_WRITES:-}" ] && extra=(MAX_WRITES=$REDIS_MAX_WRITES)
      drain_n=${REDIS_DRAIN_N:-$drain_n} ;;
  esac
  ./run.sh $db RUN_ID=peak-$ts-$db-write DURATION=$D ROLE=producer PREFILL=0 WRITE_RPS=0 WRITE_BATCH=1 WRITERS=256 \
    PG_POOL=256 REDIS_POOL=256 MONGO_POOL=256 PREFILL_ETA=0s REPORT_INTERVAL=5s "${extra[@]}" || echo "RUN FAILED: $db write"
  ./run.sh $db RUN_ID=peak-$ts-$db-batch DURATION=$D ROLE=producer PREFILL=0 WRITE_RPS=0 WRITE_BATCH=100 WRITERS=64 \
    PG_WRITE_MODE=copy PREFILL_ETA=0s REPORT_INTERVAL=5s "${extra[@]}" || echo "RUN FAILED: $db batch"
  ./run.sh $db RUN_ID=peak-$ts-$db-read DURATION=$D ROLE=drain SPEED=100000 PREFILL=$drain_n PREFILL_ETA=0s \
    REPORT_INTERVAL=2s "${extra[@]}" || echo "RUN FAILED: $db read"
done
