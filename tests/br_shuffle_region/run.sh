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

run_sql "CREATE DATABASE $DB;"

go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

# add shuffle region scheduler
echo "-u $PD_ADDR -d sched add shuffle-region-scheduler" | pd-ctl
echo "-u $PD_ADDR -d sched show" | pd-ctl

# backup with shuffle region
br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB/backupdata" --db $DB -t $TABLE --ratelimit 100 --concurrency 4

run_sql "DELETE FROM $DB.$TABLE;"

# restore with shuffle region
br restore table --db $DB --table $TABLE --connect "root@tcp($TIDB_ADDR)/" --importer $IMPORTER_ADDR --meta backupmeta --status $TIDB_IP:10080 --pd $PD_ADDR

# remove shuffle region scheduler
echo "-u $PD_ADDR -d sched remove shuffle-region-scheduler" | pd-ctl
echo "-u $PD_ADDR -d sched show" | pd-ctl

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB.$TABLE;" | awk '/COUNT/{print $2}')

echo "[original] row count: $row_count_ori, [after br] row count: $row_count_new"

if [ "$row_count_ori" -eq "$row_count_new" ];then
    echo "TEST: [$TEST_NAME] successed!"
else
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi