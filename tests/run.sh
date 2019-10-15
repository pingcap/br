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
IMPORTER_ADDR="127.0.0.1:8808"
TIDB_IP="127.0.0.1"
TIDB_PORT="4000"
TIDB_ADDR="127.0.0.1:4000"
# actaul tikv_addr are TIKV_ADDR${i} 
TIKV_ADDR="127.0.0.1:2016"
TIKV_COUNT=2

stop_services() {
    killall -9 tikv-server || true
    killall -9 pd-server || true
    killall -9 tidb-server || true
    killall -9 tikv-importer || true

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
        --store tikv \
        --path "$PD_ADDR" \
        --config "tests/config/tidb.toml" \
        --log-file "$TEST_DIR/tidb.log" &

    echo "Starting Importer..."
    bin/tikv-importer \
        -A "$IMPORTER_ADDR" \
        --log-file "$TEST_DIR/importer.log" \
        --import-dir "$TEST_DIR/importer" &

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
}

trap stop_services EXIT
start_services

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

for script in tests/*/run.sh; do
    echo "*===== Running test $script... =====*"
    TEST_DIR="$TEST_DIR" \
    PD_ADDR="$PD_ADDR" \
    IMPORTER_ADDR="$IMPORTER_ADDR" \
    TIDB_IP="$TIDB_IP" \
    TIDB_PORT="$TIDB_PORT" \
    TIDB_ADDR="$TIDB_ADDR" \
    TIKV_ADDR="$TIKV_ADDR" \
    PATH="tests/_utils:bin:$PATH" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    sh "$script"
    rm backupmeta
done
