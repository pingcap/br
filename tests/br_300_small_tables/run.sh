#!/bin/sh
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

set -eu
DB="$TEST_NAME"
TABLES_COUNT=300

PROGRESS_FILE="$TEST_DIR/progress_file"
BACKUP_LOG="$TEST_DIR/backup.log"
RESTORE_LOG="$TEST_DIR/restore.log"
rm -rf $PROGRESS_FILE

run_sql "create schema $DB;"

# generate 300 tables with 1 row content.
i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "create table $DB.sbtest$i(id int primary key, k int not null, c char(120) not null, pad char(60) not null);"
    run_sql "insert into $DB.sbtest$i values ($i, $i, '$i', '$i');"
    i=$(($i+1))
done

# backup db
echo "backup start..."
unset BR_LOG_TO_TERM
rm -f $BACKUP_LOG
export GO_FAILPOINTS="github.com/pingcap/br/pkg/task/progress-call-back=return(\"$PROGRESS_FILE\")"
run_br backup db --db "$DB" --log-file $BACKUP_LOG -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --use-backupmeta-v2 
backup_size=`tail -n 2 ${BACKUP_LOG} | grep "backup data size" | grep -oP '\[\K[^\]]+' | grep "backup data size" | awk -F '=' '{print $2}' | grep -oP '\d*\.\d+'`
echo ${backup_size}
export GO_FAILPOINTS=""

if [[ "$(wc -l <$PROGRESS_FILE)" == "1" ]] && [[ $(grep -c "range" $PROGRESS_FILE) == "1" ]];
then
  echo "use the correct progress unit"
else
  echo "use the wrong progress unit, expect range"
  cat $PROGRESS_FILE
  exit 1
fi

rm -rf $PROGRESS_FILE

# truncate every table
# (FIXME: drop instead of truncate. if we drop then create-table will still be executed and wastes time executing DDLs)
i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "truncate $DB.sbtest$i;"
    i=$(($i+1))
done

rm -rf $RESTORE_LOG
echo "restore 1/300 of the table start..."
run_br restore table --db $DB  --table "sbtest100" --log-file $RESTORE_LOG -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --no-schema
restore_size=`tail -n 2 ${RESTORE_LOG} | grep "restore data size" | grep -oP '\[\K[^\]]+' | grep "restore data size" | awk -F '=' '{print $2}' | grep -oP '\d*\.\d+'`
echo ${restore_size}

diff=`echo "${backup_size}-${restore_size}*${TABLES_COUNT}" | bc`
echo ${diff}

threshold="3"

if [ `echo "${diff}<${threshold}" | bc` -eq 1 ]; then 
    echo "statistics match" 
else 
    echo "statistics unmatch"
    exit 1 
fi

# restore db
# (FIXME: shouldn't need --no-schema to be fast, currently the alter-auto-id DDL slows things down)
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --no-schema

run_sql "DROP DATABASE $DB;"
