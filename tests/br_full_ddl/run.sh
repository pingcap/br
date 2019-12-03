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
DDL_COUNT=10

run_sql "CREATE DATABASE $DB;"
go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

for i in $(seq $DDL_COUNT); do
    run_sql "USE $DB; ALTER TABLE $TABLE ADD INDEX (FIELD$i);"
done

run_sql "USE $DB; ALTER TABLE $TABLE DROP INDEX FIELD1;"

# backup full
echo "backup start..."
br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --ratelimit 5 --concurrency 4 --fastchecksum true

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB;"
done

# restore full
echo "restore start..."
br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

fail=false
if [ "${row_count_ori}" != "${row_count_new}" ];then
    fail=true
    echo "TEST: [$TEST_NAME] fail on database $DB${i}"
fi
echo "database $DB$ [original] row count: ${row_count_ori}, [after br] row count: ${row_count_new}"

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
else
    echo "TEST: [$TEST_NAME] successed!"
fi