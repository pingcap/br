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

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

echo "backup with tikv unknown error start..." 
export GO_FAILPOINTS="github.com/pingcap/br/pkg/backup/tikv-rw-error=return(\"tikv read or write error\")"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB-tikverr" || echo "br log test done!"
export GO_FAILPOINTS=""


for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done