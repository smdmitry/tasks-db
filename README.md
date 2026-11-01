# Бенчмарк хранилищ отложенных задач

Стенд к ADR notifications/2026-10-29-replacement-tasks. Сравниваются 9 вариантов хранения и доставки отложенных задач:
- ScyllaDB, Cassandra, PostgreSQL, Redis, MongoDB, Tarantool (vinyl);
- брокеры RabbitMQ, RocketMQ и Kafka.

**Итоги:**
- [`queue-db-selection.md`](queue-db-selection.md) — сводка: что тестировали, какую нагрузку держит каждый движок, сколько потребляет и что выбрать для каких сценариев.
- [`results/SUMMARY.md`](results/SUMMARY.md) — все таблицы и разборы.
- [`results/REPORT.md`](results/REPORT.md) — сырая сводка по каждому прогону.

## Модель

- **Планирование.** Задача планируется на случайный **будущий** час из окна `[HOUR_FROM, HOUR_TO)` = 08:00–22:00 (TZ `Europe/Moscow`), в пределах горизонта 24 ч. Внутри часа — случайная секунда.
- **Где хранится.** Задача пишется в **часовую партицию** (`hour` = unix-час) и **минутный бакет** (`minute` 0..59). Внутри бакета есть ещё `shard = user_id % SHARDS` (16): так бакет не превращается в одну горячую партицию, а консюмер может читать параллельно.
- **Консюмер** идёт по минутам строго по порядку. Бакет минуты M вычитывается, когда прошло M+1 минут (+5 с grace), целиком, страницами по `READ_PAGE_LIMIT` (1000). Скорость можно ограничить через `READ_QPS`. Прочитанное «отправляется в Kafka», то есть дропается.
  - Брокеры читают по-своему: RocketMQ доставляет таймер-сообщения сам, Kafka читает слот кольца целиком (см. схемы ниже).
- **Payload** по умолчанию 300 Б случайных байт, как `SendDto` в ADR. `PAYLOAD_BYTES_MAX` включает случайный размер, например 5–10 KB.
- **Виртуальные часы.** При `SPEED=60` одна реальная минута — один виртуальный час. Старт по умолчанию в 07:30, поэтому за прогон видно и утро без чтения, и дневной режим.
- **Prefill.** Перед стартом очередь заливается так, будто продюсер уже сутки писал с `WRITE_RPS`; иначе до стационара пришлось бы ждать 24 виртуальных часа.
  - Глубина = `WRITE_RPS × 86400 / SPEED` сгенерированных задач, из них ~70% остаются в будущих часах. В матрице (`SPEED=30`) это ~10.2M задач.
  - Prefill пишется пачками по 500 без лимита, это заодно замер потолка пакетной записи.

В стационаре чтение днём ≈ `WRITE_RPS × 24/14`, то есть ≈8.6k/s при записи 5k/s: всё, что пишется за сутки, читается за 14 часов окна.

## Схемы

| Бэкенд | Запись | Чтение | Очистка |
|---|---|---|---|
| **ScyllaDB / Cassandra** (`scylla`, `cassandra`) | `PRIMARY KEY ((hour, minute, shard), id)`, TWCS 1h, TTL 48h. При `WRITE_BATCH>1` — UNLOGGED BATCH на партицию | постраничный `SELECT` по партиции | `ttl` — ничего; `delete` — tombstone партиции после вычитки бакета |
| **PostgreSQL** (`postgres`) | `PARTITION BY RANGE (hour)`, партиция на час, индекс `(minute, shard, id)`. Партиции создаются наперёд. При `WRITE_BATCH>1` — `copy` (COPY) или `multi` (INSERT … unnest) | `drop`: keyset `id > $last ORDER BY id LIMIT n`; `delete`: `DELETE … SKIP LOCKED … RETURNING` | `DETACH CONCURRENTLY` + `DROP` партиции после вычитки часа |
| **Redis** (`redis`) | `RPUSH t:{hour:minute:shard}` + `EXPIRE NX 48h`; пачка — pipeline | `LPOP key 1000` (с удалением) | пустой список удаляется сам |
| **MongoDB** (`mongodb`) | `drop`: коллекция на час `h<hour>` с индексом `{m, s, _id}`; `delete`: одна коллекция с индексом `{h, m, s, _id}`. w:1 без j | keyset по `_id` | `drop` коллекции часа или `deleteMany` по бакету |
| **Tarantool vinyl** (`tarantool`) | Lua-процедуры из `tarantool/init.lua`. `drop`: спейс на час `t<hour>`, PK `(hour, minute, shard, id)`; `delete`: один спейс. `wal_mode=write` | `drop`: keyset; `delete`: `take` — чтение и удаление страницы в одной транзакции | `drop` спейса часа |
| **RabbitMQ** (`rabbitmq`) | durable classic-очередь на бакет `q.<hour>.<minute>.<shard>` (`RABBIT_SHARDS`=4), persistent + publisher confirms | `basic.consume`, prefetch = страница, ack multiple | очередь удаляется после вычитки |
| **RocketMQ 5** (`rocketmq`) | таймер-сообщения (`TIMER_DELIVER_MS`, точность 1 с) в топик `tasks` на 16 очередей; бакетов нет | push consumer: брокер сам доставляет в момент исполнения | сам брокер |
| **Kafka** (`kafka`) | кольцо топиков `ring-NNN` по `KAFKA_SLOT` (1h → 26 топиков, 5m → 290), горизонт + 2 слота, по 4 партиции; запись в топик слота времени исполнения, acks=all | закончившийся слот вычитывается целиком до high watermark | `DeleteRecords` до high watermark |

## Запуск

```bash
./run.sh scylla                                  # один прогон с дефолтами
./run.sh cassandra SCYLLA_CONSUME_MODE=delete
./run.sh postgres WRITE_BATCH=100 PG_WRITE_MODE=copy DURATION=5m
./run.sh kafka KAFKA_SLOT=5m DURATION=5m SPEED=30
./run.sh redis ROLE=drain SPEED=100000 PREFILL=10000000   # потолок чтения

./run-all.sh                     # вся матрица по 5 минут (~5–6 ч), затем results/REPORT.md
DBS="tarantool kafka" ./run-all.sh   # подмножество
./peak.sh                        # потолки без лимита RPS, по 1 минуте на вид нагрузки (~1 ч)
PAYLOAD_BYTES=5120 PAYLOAD_BYTES_MAX=10240 MAX_WRITES=1000000 DRAIN_N=1000000 \
  REDIS_MAX_WRITES=500000 REDIS_DRAIN_N=500000 ./peak.sh   # то же на задачах 5–10 KB
docker compose --profile report run --rm report  # сводная таблица по results/*.json
```

Что делает `run.sh` на каждом прогоне:
- поднимает чистую БД (`down -v`);
- раз в 5 с снимает `docker stats` и `du` data-директории;
- после прогона удаляет БД вместе с томами. `KEEP=1` — оставить.

**Какие прогоны в `run-all.sh`:**
- **steady** — 5k/s с суточной очередью, консюмер идёт за часами;
- **maxw** — запись без лимита, 256 писателей;
- **drain** — залить 10M задач и вычитать на максимальной скорости.

**Какие прогоны в `peak.sh`:** три прогона по минуте — write (по одной), batch (по 100) и read (drain).

Результаты в `results/`:
- `<run>.json` — сводка плюс метрики по интервалам: rows/s, p50/p99, backlog, lag, размер хранилища по данным БД;
- `<run>.stats.csv`, `<run>.docker.json` — CPU, память и диск контейнера БД (CPU% = 100 на ядро);
- `<run>.tablestats.txt` — `nodetool tablestats` для Scylla и Cassandra;
- `*.log` — логи серий прогонов;
- `smoke/`, `invalid-*/`, `aborted-*/` — отладочные и забракованные прогоны.

Роли (`ROLE`):
- `both` — по умолчанию;
- `producer`;
- `consumer`;
- `drain` — prefill и вычитка на максимальной скорости; останавливается, когда всё прочитано.

## Основные параметры

| Переменная | Default | |
|---|---|---|
| `DURATION` | 10m | длительность прогона после prefill (в скриптах — 5m / 1m) |
| `SPEED` | 60 | ускорение виртуального времени (в матрице 30) |
| `VIRTUAL_START` | сегодня 07:30 | формат `2026-10-30T07:30` |
| `WRITE_RPS` | 5000 | 0 — без лимита |
| `WRITE_BATCH` | 1 | задач на один вызов записи |
| `WRITERS` | 64 | параллельных писателей |
| `PAYLOAD_BYTES` / `PAYLOAD_BYTES_MAX` | 300 / 0 | при `MAX > BYTES` размер случайный в диапазоне |
| `PAYLOAD_RANDOM` | true | несжимаемый payload |
| `MAX_WRITES` | 0 | остановить прогон после N записанных задач (лимит объёма на диске) |
| `SHARDS` | 16 | шардов внутри минутного бакета |
| `CONSUMER_WORKERS` | 4 | воркер w читает шарды `s % W == w` (у Kafka — слоты параллельно) |
| `READ_PAGE_LIMIT` / `READ_QPS` | 1000 / 0 | лимит одной выборки / выборок в секунду |
| `PREFILL` / `PREFILL_BATCH` | auto / 500 | задач для prefill (0 — без) / размер пачки |
| `PREFILL_ETA` | 3m | только RocketMQ: через сколько после старта начинают идти часы (таймеры брокера в реальном времени) |
| `DB_CPUS` | 4 | одинаково для всех БД |
| `DISK_DB_MEM` | 1536m | лимит RAM всех БД кроме Redis (cgroup, вместе с page cache); RabbitMQ в скриптах — 4g |
| `SCYLLA_MEM` / `CASSANDRA_HEAP` | 1G / 768M | память seastar / heap JVM |
| `SCYLLA_CONSUME_MODE` | ttl | `ttl` / `delete` |
| `PG_WRITE_MODE` / `PG_CONSUME_MODE` | copy / drop | `copy` / `multi`; `drop` / `delete` |
| `PG_SHARED_BUFFERS` / `PG_SYNC_COMMIT` / `PG_POOL` | 384MB / on / 64 | |
| `REDIS_MEM` / `REDIS_MAXMEM` / `REDIS_AOF` | 6g / 5gb / yes | Redis RAM не урезается: память и есть хранилище; `appendfsync everysec` |
| `MONGO_CONSUME_MODE` / `MONGO_CACHE_GB` / `MONGO_POOL` | drop / 0.5 / 64 | `drop` / `delete` |
| `TARANTOOL_CONSUME_MODE` / `VINYL_MEMORY` / `VINYL_CACHE` | drop / 256 MB / 128 MB | `drop` / `delete` |
| `RABBIT_SHARDS` / `RABBIT_CONF` / `RABBIT_CONNS` | 4 / rabbitmq.conf / 8 | `rabbitmq-4g.conf` — порог памяти 3 GB |
| `ROCKET_QUEUES` / `ROCKETMQ_HEAP` | 16 / 512m | |
| `KAFKA_SLOT` / `KAFKA_PARTITIONS` / `KAFKA_SEGMENT_BYTES` / `KAFKA_HEAP` | 1h / 4 / 32 MB / 768m | длина слота кольца (`1h`, `5m`, ...) |

## Память

Все БД, кроме Redis и RabbitMQ, ограничены `DISK_DB_MEM=1536m`. Лимит cgroup учитывает и page cache, поэтому кэш данных у каждой БД ограничен:

| БД | Что занимает память в пределах лимита |
|---|---|
| Scylla | собственный row cache внутри `--memory 1G` (O_DIRECT, мимо page cache) |
| Cassandra | heap 768M + page cache |
| PostgreSQL | `shared_buffers` 384MB + page cache |
| MongoDB | WiredTiger cache 0.5 GB |
| Tarantool | vinyl 256 + 128 MB |
| RocketMQ | heap 512m |
| Kafka | heap 768m, остальное — page cache |

RabbitMQ с 1.5 GB на суточной очереди упирается в memory alarm, поэтому в скриптах ему дают 4 GB (порог 3 GB).

В матрице стационарные прогоны идут с `SPEED=30`: глубина ~10M задач, на диске это в 2–3 раза больше лимита RAM.

## Ограничения стенда

- **Одна нода каждой БД, без реплик.** RF=1 у Scylla, Cassandra и Kafka; у PG нет синхронной реплики; у Redis, Tarantool, RabbitMQ и RocketMQ реплик нет. Сравнивается стоимость одной ноды, а не HA-топология из ADR (1 мастер и 2 реплики в разных ДЦ).
- **Режимы сброса на диск разные:**
  - PG — fsync на коммит;
  - RabbitMQ — confirm после записи;
  - Kafka — acks=all на одной ноде, без fsync;
  - Scylla, Cassandra, Tarantool, MongoDB (w:1 без j) и RocketMQ (ASYNC_FLUSH) — асинхронно.
- **Docker Desktop на macOS.** Диск идёт через VM, `fsync` медленный и шумный; сильнее всего это бьёт по PG с одиночными коммитами. Абсолютные цифры на железе будут другими, смотреть стоит на соотношения. На той же VM работали посторонние контейнеры.
- **Scylla** запущена с `--overprovisioned --developer-mode`; её CPU% в docker stats включает поллинг reactor'а.
- **Глубина очереди** ~10M задач, а не 430M из ADR. Память и диск на задачу масштабируются линейно; чтобы данные не помещались в кэш, урезана RAM, а не увеличен объём.
- **Каждый вариант прогнан один раз**, разброс между прогонами неизвестен.
