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

source ${BASH_SOURCE[0]%/*}/../compatibility/prepare_backup.sh
echo "start test on $TAGS"

EXPECTED_KVS=1000
PD_ADDR="pd0:2379"
GCS_HOST="gcs"
GCS_PORT="20818"
TEST_DIR=/tmp/backup_restore_compatibility_test
TEST_DIR_NO_TMP=backup_restore_compatibility_test
#FIXME: When dir changed in
#       https://github.com/pingcap/br/blob/master/compatibility/backup_cluster.yaml#L13
#       please also change $DOCKER_DIR
DOCKER_DIR=/tmp/br/docker/backup_logs #${TAG}
mkdir -p "$TEST_DIR"
rm -f "$TEST_DIR"/*.log &> /dev/null

for script in tests/docker_compatible_*/${1}.sh; do
    echo "*===== Running test $script... =====*"
    TEST_DIR="$TEST_DIR" \
    DOCKER_DIR="$DOCKER_DIR" \
    PD_ADDR="$PD_ADDR" \
    GCS_HOST="$GCS_HOST" \
    GCS_PORT="$GCS_PORT" \
    TAGS="$TAGS" \
    EXPECTED_KVS="$EXPECTED_KVS" \
    PATH="tests/_utils:bin:$PATH" \
    TEST_NAME="$(basename "$(dirname "$script")")" \
    BR_LOG_TO_TERM=1 \
    bash "$script"
done

# When $1 is prepare, only backup $TAG
# When $2 is run, restore all $TAGS
if [[ ${1} == "prepare" ]]; 
then
    echo "finish preparing for $TAG"
    touch $TEST_DIR/${TAG}_prepare_finish
else
    for TAG_ in ${TAGS}; do
        rm $DOCKER_DIR/${TAG_}/$TEST_DIR_NO_TMP/${TAG_}_prepare_finish
    done
fi
