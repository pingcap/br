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

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(10) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

for i in `seq 1 100`
do
run_sql "INSERT INTO $DB.usertable1 VALUES (\"a$i\", \"bbbbbbbbbb\");"
done

# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --ratelimit 5 --concurrency 4

# Test validate backupmeta
run_br validate backupmeta -s "local://$TEST_DIR/$DB"
run_br validate backupmeta -s "local://$TEST_DIR/$DB" --offset 100

# Test validate checksum
run_br validate checksum -s "local://$TEST_DIR/$DB"

# Test validate checksum
for sst in $TEST_DIR/$DB/*.sst; do
    echo "corrupted!" >> $sst
    echo "$sst corrupted!"
    break
done

corrupted=0
run_br validate checksum -s "local://$TEST_DIR/$DB" || corrupted=1
if [ "$corrupted" -ne "1" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

# backup full with ratelimit = 1 to make sure this backup task won't finish quickly
echo "backup start to test lock file"
BACKGROUND_LOG=$TEST_DIR/bg.log
rm -f $BACKGROUND_LOG
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/lock" --ratelimit 1 --ratelimit-unit 1 --concurrency 4 > $BACKGROUND_LOG 2>&1 &
# record last backup pid
_pid=$!

# let's test the dynamic pprof at the same time :D
# use its port firstly :evil:
nc -l -p 6060 &
nc_pid=$!

# give the former backup some time to write down lock file (and initialize signal listener).
sleep 1
start_pprof=$(cat $BACKGROUND_LOG | grep 'dynamic pprof started, you can enable pprof by' | grep -oP 'kill -s 10 [0-9]+' | head -n1)
echo "executing $start_pprof"
# this will fail
$start_pprof
sleep 1
# and this should start pprof at :6061
$start_pprof

# give the former backup some time to write down lock file (and start pprof server).
sleep 2
curl "http://localhost:6061/debug/pprof/trace?seconds=5" 2>&1 > /dev/null
kill -9 $nc_pid || true

backup_fail=0
echo "another backup start expect to fail due to last backup add a lockfile"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/lock" --concurrency 4 || backup_fail=1
if [ "$backup_fail" -ne "1" ];then
    echo "TEST: [$TEST_NAME] test backup lock file failed!"
    exit 1
fi

if ps -p $_pid > /dev/null
then
   echo "$_pid is running"
   # kill last backup progress
   kill -9 $_pid
else
   echo "TEST: [$TEST_NAME] test backup lock file failed! the last backup finished"
   exit 1
fi

run_sql "DROP DATABASE $DB;"

# Test version
run_br --version
run_br -V
