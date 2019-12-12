#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

TEST_DIR=/tmp/backup_restore_test

PD_ADDR="127.0.0.1:2379"
TIDB_IP="127.0.0.1"
TIDB_PORT="4000"
TIDB_ADDR="127.0.0.1:4000"
TIDB_STATUS_ADDR="127.0.0.1:10080"
# actaul tikv_addr are TIKV_ADDR${i}
TIKV_ADDR="127.0.0.1:2016"
TIKV_COUNT=4

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true

    find "$TEST_DIR" -maxdepth 1 -not -path "$TEST_DIR" -not -name "*.log" | xargs rm -r || true
}

start_services() {
    stop_services

    mkdir -p "$TEST_DIR"
    rm -f "$TEST_DIR"/*.log

    echo "Starting PD..."
    bin/pd-server \
        --client-urls "http://$PD_ADDR" \
        --log-file "$TEST_DIR/pd.log" \
        --data-dir "$TEST_DIR/pd" &
    # wait until PD is online...
    while ! curl -o /dev/null -sf "http://$PD_ADDR/pd/api/v1/version"; do
        sleep 1
    done

    echo "Starting TiKV..."
    for i in $(seq $TIKV_COUNT); do
        bin/tikv-server \
            --pd "$PD_ADDR" \
            -A "$TIKV_ADDR$i" \
            --log-file "$TEST_DIR/tikv${i}.log" \
            -C "tests/config/tikv.toml" \
            -s "$TEST_DIR/tikv${i}" &
    done
    sleep 1

    echo "Starting TiDB..."
    bin/tidb-server \
        -P 4000 \
        --status 10080 \
        --store tikv \
        --path "$PD_ADDR" \
        --config "tests/config/tidb.toml" \
        --log-file "$TEST_DIR/tidb.log" &

    echo "Verifying TiDB is started..."
    i=0
    while ! curl -o /dev/null -sf "http://$TIDB_IP:10080/status"; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo 'Failed to start TiDB'
            exit 1
        fi
        sleep 3
    done

    i=0
    while ! curl "http://$PD_ADDR/pd/api/v1/cluster/status" -sf | grep -q "\"is_initialized\": true"; do
        i=$((i+1))
        if [ "$i" -gt 10 ]; then
            echo 'Failed to bootstrap cluster'
            exit 1
        fi
        sleep 3
    done
}

trap stop_services EXIT
start_services

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
    shift
fi

run_one()
{
    echo "*===== Running test $1... =====*"
    TEST_DIR="$TEST_DIR" \
    PD_ADDR="$PD_ADDR" \
    TIDB_IP="$TIDB_IP" \
    TIDB_PORT="$TIDB_PORT" \
    TIDB_ADDR="$TIDB_ADDR" \
    TIDB_STATUS_ADDR="$TIDB_STATUS_ADDR" \
    TIKV_ADDR="$TIKV_ADDR" \
    PATH="tests/_utils:bin:$PATH" \
    TEST_NAME="$(basename "$(dirname "$1")")" \
    sh "$1"
}

if [ $# -eq 0 ]
then
    for script in tests/*/run.sh; do
        run_one "$script"
    done
else
    run_one "tests/$1/run.sh"
fi
