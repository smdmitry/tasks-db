#!/usr/bin/env bash
# Полная матрица: ScyllaDB, Cassandra, PostgreSQL, Redis, MongoDB, Tarantool (vinyl), RabbitMQ, RocketMQ, Kafka.
# Затем сводная таблица results/REPORT.md. Подмножество: DBS="mongodb rabbitmq" ./run-all.sh
#
# Каждый прогон — 5 минут измерений (DURATION=5m; плюс старт БД и prefill), по одному разу.
# Три вида прогонов:
#   steady — 5k задач/с одиночными или батчами, prefill на суточную глубину, консюмер идёт за часами.
#            SPEED=30 (1 реальная минута = 30 виртуальных) -> глубина ~10M задач, это в 2-3 раза больше
#            лимита RAM дисковых БД (DISK_DB_MEM=1536m), т.е. данные не живут целиком в кэше;
#   maxw   — запись одиночными задачами без лимита (256 писателей), консюмер выключен: потолок записи;
#   drain  — залить 10M задач и вычитать на максимальной скорости: потолок чтения / время разбора бэклога.
set -uo pipefail
cd "$(dirname "$0")"
RUN_DURATION=${RUN_DURATION:-5m}
STEADY_SPEED=${STEADY_SPEED:-30}
DRAIN_N=${DRAIN_N:-10000000}

# падение одного прогона не останавливает матрицу
run()    { ./run.sh "$@" || echo "RUN FAILED: $*"; }
steady() { run "$1" DURATION=$RUN_DURATION SPEED=$STEADY_SPEED "${@:2}"; }
maxw()   { run "$1" DURATION=$RUN_DURATION ROLE=producer PREFILL=0 WRITE_RPS=0 WRITE_BATCH=1 WRITERS=256 PG_POOL=256 REDIS_POOL=256 MONGO_POOL=256 "${@:2}"; }
# drain останавливается, как только всё вычитано; 5 минут — верхняя граница
drain()  { run "$1" DURATION=$RUN_DURATION ROLE=drain SPEED=100000 PREFILL=$DRAIN_N REPORT_INTERVAL=2s "${@:2}"; }

for db in ${DBS:-scylla cassandra postgres redis mongodb tarantool rabbitmq rocketmq kafka}; do
  case $db in
  # ScyllaDB и Cassandra (один CQL-код): одиночные инсерты vs батчи по партиции; чтение с TTL vs partition delete
  scylla|cassandra)
    steady $db WRITE_BATCH=1   SCYLLA_CONSUME_MODE=ttl
    steady $db WRITE_BATCH=100 SCYLLA_CONSUME_MODE=ttl
    steady $db WRITE_BATCH=1   SCYLLA_CONSUME_MODE=delete
    maxw   $db
    drain  $db ;;
  # PostgreSQL: одиночные INSERT vs COPY vs multi-insert; чтение keyset + DROP партиции vs DELETE .. SKIP LOCKED
  postgres)
    steady postgres WRITE_BATCH=1   PG_CONSUME_MODE=drop
    steady postgres WRITE_BATCH=100 PG_WRITE_MODE=copy  PG_CONSUME_MODE=drop
    steady postgres WRITE_BATCH=100 PG_WRITE_MODE=multi PG_CONSUME_MODE=drop
    steady postgres WRITE_BATCH=1   PG_CONSUME_MODE=delete
    maxw   postgres
    drain  postgres PG_CONSUME_MODE=drop
    drain  postgres PG_CONSUME_MODE=delete ;;
  # Redis (RAM не урезана — для него данные в памяти и есть хранилище): RPUSH по одному vs pipeline
  redis)
    steady redis WRITE_BATCH=1
    steady redis WRITE_BATCH=100
    maxw   redis
    drain  redis ;;
  # MongoDB: коллекция на час + drop vs одна коллекция + deleteMany по бакету
  mongodb)
    steady mongodb WRITE_BATCH=1   MONGO_CONSUME_MODE=drop
    steady mongodb WRITE_BATCH=100 MONGO_CONSUME_MODE=drop
    steady mongodb WRITE_BATCH=1   MONGO_CONSUME_MODE=delete
    maxw   mongodb
    drain  mongodb MONGO_CONSUME_MODE=drop
    drain  mongodb MONGO_CONSUME_MODE=delete ;;
  # Tarantool vinyl: спейс на час + drop vs один спейс + take (чтение с удалением строк)
  tarantool)
    steady tarantool WRITE_BATCH=1   TARANTOOL_CONSUME_MODE=drop
    steady tarantool WRITE_BATCH=100 TARANTOOL_CONSUME_MODE=drop
    steady tarantool WRITE_BATCH=1   TARANTOOL_CONSUME_MODE=delete
    maxw   tarantool
    drain  tarantool TARANTOOL_CONSUME_MODE=drop
    drain  tarantool TARANTOOL_CONSUME_MODE=delete ;;
  # RabbitMQ: очередь на бакет, publish с confirm по одному vs пачкой по 100
  # С DISK_DB_MEM=1536m (порог 1 GB) брокер на ~5M сообщений уходит в memory alarm и блокирует публикацию
  # (prefill падает до ~4k/s), поэтому RabbitMQ гоняется с 4 GB RAM (RABBIT_MEM) и порогом 3 GB.
  rabbitmq)
    rmem="DISK_DB_MEM=${RABBIT_MEM:-4g} RABBIT_CONF=${RABBIT_CONF:-rabbitmq-4g.conf}"
    steady rabbitmq WRITE_BATCH=1   $rmem
    steady rabbitmq WRITE_BATCH=100 $rmem
    maxw   rabbitmq $rmem
    drain  rabbitmq $rmem ;;
  # RocketMQ: таймер-сообщения, брокер сам доставляет в срок. PREFILL_ETA — запас реального времени на prefill
  rocketmq)
    steady rocketmq WRITE_BATCH=1 PREFILL_ETA=${ROCKET_PREFILL_ETA:-5m}
    maxw   rocketmq PREFILL_ETA=0s
    drain  rocketmq PREFILL_ETA=0s ;;
  # Kafka: кольцо топиков-слотов по часу или 5 минут, прочитанный слот чистится DeleteRecords
  kafka)
    steady kafka WRITE_BATCH=1   KAFKA_SLOT=1h
    steady kafka WRITE_BATCH=1   KAFKA_SLOT=5m
    steady kafka WRITE_BATCH=100 KAFKA_SLOT=5m
    maxw   kafka KAFKA_SLOT=1h
    drain  kafka KAFKA_SLOT=1h
    drain  kafka KAFKA_SLOT=5m ;;
  esac
done

docker compose --profile report build -q report && docker compose --profile report run --rm report | grep "^| " | tee results/REPORT.md
