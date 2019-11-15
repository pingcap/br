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
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3
TABLE_COUNT=3

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    for j in $(seq $TABLE_COUNT); do
        go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i} -p table=$TABLE${j}
    done
done

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB${1}.$TABLE${1};" | awk '/COUNT/{print $2}')

# backup full
echo "backup start..."
br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --ratelimit 5 --concurrency 4

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

# restore full
echo "restore start..."
br restore full --connect "root@tcp($TIDB_ADDR)/" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB${1}.$TABLE${1};" | awk '/COUNT/{print $2}')

fail=false
if [ "${row_count_ori}" != "${row_count_new}" ];then
    fail=true
    echo "TEST: [$TEST_NAME] fail on database $DB${1} $TABLE${1}"
fi
echo "database $DB${1} $TABLE${1} [original] row count: ${row_count_ori}, [after br] row count: ${row_count_new}"

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
else
    echo "TEST: [$TEST_NAME] successed!"
fi
