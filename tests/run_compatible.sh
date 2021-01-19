#!/bin/bash
#
# Copyright 2020 PingCAP, Inc.
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

# This test is used to test compatible for BR restore.
# It will download backup data from internal file server.
# And make sure these backup data can restore through newly BR tools to newly cluster.

set -eu
cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/_utils/run_services

TEST_DIR=/tmp/backup_restore_compatible_test
mkdir -p "$TEST_DIR"
rm -f "$TEST_DIR"/*.log &> /dev/null

trap stop_services EXIT
start_services

if [ "${1-}" = '--debug' ]; then
    echo 'You may now debug from another terminal. Press [ENTER] to continue.'
    read line
fi

for script in $(find tests/ -name "br_compatible_*/run.sh"); do
    echo "*===== Running test $script... =====*"
    TEST_DIR="$TEST_DIR" \
    PD_ADDR="$PD_ADDR" \
    TIDB_IP="$TIDB_IP" \
    TIDB_PORT="$TIDB_PORT" \
    TIDB_ADDR="$TIDB_ADDR" \
    TIDB_STATUS_ADDR="$TIDB_STATUS_ADDR" \
    TIKV_ADDR="$TIKV_ADDR" \
    PATH="tests/_utils:bin:$PATH" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    BR_LOG_TO_TERM=1 \
    bash "$script"
done
