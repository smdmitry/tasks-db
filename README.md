# Бенчмарк отложенных задач: ScyllaDB vs Cassandra vs PostgreSQL vs Redis

Стенд к ADR notifications/2026-10-29-replacement-tasks
## Модель

- Задача планируется на случайный **будущий** час из окна `[HOUR_FROM, HOUR_TO)` = 08:00–22:00 (TZ `Europe/Moscow`) в пределах горизонта 24 ч; внутри часа — случайная секунда.
- Пишется в **часовую партицию** (`hour` = unix-час) и **минутный бакет** (`minute` 0..59). Внутри бакета ещё `shard = user_id % SHARDS` (16), чтобы бакет не был одной горячей партицией и консюмер мог читать параллельно.
- Консюмер идёт по минутам строго по порядку: бакет минуты M вычитывается, когда прошло M+1 минут (+5 с grace), **целиком, но страницами** по `READ_PAGE_LIMIT` (1000) с опциональным `READ_QPS`. Прочитанное «отправляется в Kafka» — то есть дропается.
- Payload 300 Б (как `SendDto` в ADR).
- **Виртуальные часы**: `SPEED=60` — 1 реальная минута = 1 виртуальный час. Старт по умолчанию в 07:30, так что за прогон видно и утро без чтения, и дневной режим.
- **Prefill**: перед стартом очередь заливается так, будто продюсер уже сутки писал с `WRITE_RPS` (иначе до стационара ждать 24 виртуальных часа). Глубина очереди = `WRITE_RPS × 86400 / SPEED` сгенерированных (≈70% из них остаются в будущих часах): при 5k/s и SPEED=60 это ~5.1M задач. Prefill пишется батчами по 500 без лимита — это заодно замер потолка батчевой записи.

В стационаре чтение днём ≈ `WRITE_RPS × 24/14` (≈8.6k/s при 5k/s записи): всё, что пишется за сутки, читается за 14 часов окна.

## Схемы

| | Запись | Чтение | Очистка |
|---|---|---|---|
| **ScyllaDB** | `PRIMARY KEY ((hour, minute, shard), id)`, TWCS 1h, TTL 48h. `WRITE_BATCH>1` — UNLOGGED BATCH на партицию | постраничный `SELECT` по партиции (paging state) | `ttl` — ничего не делаем; `delete` — partition tombstone после вычитки бакета |
| **Cassandra** | тот же CQL-код и схема, что у Scylla (бэкенд `cassandra`) | то же | то же |
| **PostgreSQL** | `PARTITION BY RANGE (hour)`, партиция на час, индекс `(minute, shard, id)`; партиции создаются наперёд через `CREATE + ATTACH`. `WRITE_BATCH>1`: `copy` (COPY) или `multi` (INSERT … unnest) | `drop`: keyset `id > $last ORDER BY id LIMIT n`; `delete`: `DELETE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT n) RETURNING` | `DETACH PARTITION CONCURRENTLY` + `DROP` после вычитки часа (в обоих режимах) |
| **Redis** | `RPUSH t:{hour:minute:shard}` + `EXPIRE NX 48h`, батч = pipeline | `LPOP key 1000` (деструктивно) | пустой список удаляется сам |

## Запуск

```bash
./run.sh scylla                                  # один прогон с дефолтами
./run.sh cassandra SCYLLA_CONSUME_MODE=delete
./run.sh postgres WRITE_BATCH=100 PG_WRITE_MODE=copy PG_CONSUME_MODE=delete DURATION=5m
./run.sh redis ROLE=drain SPEED=100000 PREFILL=10000000   # потолок чтения
./run-all.sh                                     # вся матрица: scylla -> cassandra -> postgres -> redis, затем results/REPORT.md
docker compose --profile report run --rm report  # сводная таблица по results/*.json
```

`run.sh` каждый раз поднимает чистую БД (`down -v`), параллельно раз в 5 с снимает `docker stats` и `du` data-директории БД, после прогона удаляет БД (`KEEP=1` — оставить).

Результаты в `results/`:
- `<run>.json` — сводка + поинтервальные метрики (rows/s, p50/p99, backlog, lag, размер хранилища со стороны БД);
- `<run>.stats.csv`, `<run>.docker.json` — CPU / память / диск контейнера БД (CPU% = 100 на ядро);
- `<run>.tablestats.txt` — `nodetool tablestats` для Scylla/Cassandra.

Роли (`ROLE`): `both` (по умолчанию), `producer`, `consumer`, `drain` (prefill + вычитка на максимальной скорости, останавливается, когда всё прочитано).

## Основные параметры

| Переменная | Default | |
|---|---|---|
| `DURATION` | 10m | длительность прогона (после prefill) |
| `SPEED` | 60 | ускорение виртуального времени |
| `VIRTUAL_START` | сегодня 07:30 | формат `2026-10-30T07:30` |
| `WRITE_RPS` | 5000 | 0 — без лимита |
| `WRITE_BATCH` | 1 | задач на один вызов записи |
| `WRITERS` | 64 | параллельных писателей |
| `SHARDS` | 16 | шардов внутри минутного бакета |
| `CONSUMER_WORKERS` | 4 | воркер w читает шарды `s % W == w` |
| `READ_PAGE_LIMIT` | 1000 | лимит одной выборки |
| `READ_QPS` | 0 | лимит выборок/с |
| `PREFILL` | auto | число задач для prefill, 0 — без |
| `DB_CPUS` | 4 | одинаково для всех БД |
| `DISK_DB_MEM` | 1536m | лимит RAM Scylla / Cassandra / PG (cgroup, включая page cache) |
| `SCYLLA_MEM` | 1G | память seastar (внутри лимита контейнера) |
| `CASSANDRA_HEAP` | 768M | heap JVM |
| `PG_SHARED_BUFFERS` | 384MB | |
| `REDIS_MEM` | 6g | Redis RAM не урезается: для него память и есть хранилище |
| `SCYLLA_CONSUME_MODE` | ttl | `ttl` / `delete` |
| `PG_WRITE_MODE` | copy | `copy` / `multi` (при `WRITE_BATCH>1`) |
| `PG_CONSUME_MODE` | drop | `drop` / `delete` |
| `PG_SYNC_COMMIT` | on | |
| `REDIS_AOF` | yes | `appendfsync everysec` |

## Память

Scylla, Cassandra и PostgreSQL ограничены `DISK_DB_MEM=1536m`. Лимит cgroup учитывает и page cache, поэтому кэш данных у каждой БД ограничен: у Scylla это её собственный row cache внутри `--memory 1G` (она пишет и читает через O_DIRECT, мимо page cache), у Cassandra — page cache сверх heap 768M, у PG — `shared_buffers` 384MB плюс page cache. В `run-all.sh` стационарные прогоны идут с `SPEED=30` — глубина ~10M задач, на диске это в 2–3 раза больше лимита RAM.

## Ограничения стенда

- Одна нода каждой БД, без реплик (RF=1 у Scylla/Cassandra, без синхронных реплик у PG, без реплики у Redis) — сравнивается стоимость одной ноды, а не HA-топология из ADR.
- Docker Desktop на macOS: диск через VM, `fsync` медленный и шумный — сильнее всего это бьёт по PG с одиночными коммитами. Абсолютные цифры на железе будут другие, смотреть стоит на соотношения.
- Scylla запущена с `--overprovisioned --developer-mode`, её CPU% в docker stats включает поллинг reactor'а.
- Глубина очереди ~10M задач, а не 430M из ADR. Память/диск на задачу масштабируются линейно; чтобы данные не помещались в кэш, урезана RAM, а не увеличен объём.
