#!/usr/bin/env bash
# Cassandra, те же 5 прогонов, что в run-all.sh, но PAYLOAD_RANDOM=true
cd "$(dirname "$0")/.."
export PAYLOAD_RANDOM=true
run()    { ./run.sh "$@" || echo "RUN FAILED: $*"; }
run cassandra DURATION=5m SPEED=30 WRITE_BATCH=1   SCYLLA_CONSUME_MODE=ttl
run cassandra DURATION=5m SPEED=30 WRITE_BATCH=100 SCYLLA_CONSUME_MODE=ttl
run cassandra DURATION=5m SPEED=30 WRITE_BATCH=1   SCYLLA_CONSUME_MODE=delete
run cassandra DURATION=5m ROLE=producer PREFILL=0 WRITE_RPS=0 WRITE_BATCH=1 WRITERS=256
run cassandra DURATION=5m ROLE=drain SPEED=100000 PREFILL=10000000 REPORT_INTERVAL=2s
