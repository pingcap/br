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

# Test debug decode
run_br -s "local://$TEST_DIR/$DB" debug decode --field "Schemas"
run_br -s "local://$TEST_DIR/$DB" debug decode --field "EndVersion"
# Ensure compatibility
run_br -s "local://$TEST_DIR/$DB" validate decode --field "end-version"

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
PPROF_PORT=6080
GO_FAILPOINTS="github.com/pingcap/br/pkg/utils/determined-pprof-port=return($PPROF_PORT)" \
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/lock" --remove-schedulers --ratelimit 1 --ratelimit-unit 1 --concurrency 4 2>&1 >$TEST_DIR/background-br.log &
# record last backup pid
_pid=$!

# give the former backup some time to write down lock file (and initialize signal listener).
sleep 1
# pkill -10 -P $_pid
echo "starting pprof..."

# give the former backup some time to write down lock file (and start pprof server).
sleep 1
# curl "http://localhost:$PPROF_PORT/debug/pprof/trace?seconds=1" 2>&1 > /dev/null
echo "pprof started..."

curl -s http://$PD_ADDR/pd/api/v1/config/schedule | grep '"disable": true'
curl -s http://$PD_ADDR/pd/api/v1/config/schedule | jq '."enable-location-replacement"' | grep "false"
backup_fail=0
echo "another backup start expect to fail due to last backup add a lockfile"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB/lock" --concurrency 4 || backup_fail=1
if [ "$backup_fail" -ne "1" ];then
    echo "TEST: [$TEST_NAME] test backup lock file failed!"
    exit 1
fi

if ps -p $_pid
then
   echo "$_pid is running"
   # kill last backup progress (Don't send SIGKILL, or we might stuck PD in no scheduler state.)
   pkill -P $_pid
else
   echo "TEST: [$TEST_NAME] test backup lock file failed! the last backup finished"
   exit 1
fi

# make sure we won't stuck in non-scheduler state, even we send a SIGTERM to it.
# give enough time to BR so it can gracefully stop.
sleep 5
if curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '[."schedulers-v2"][0][0]' | grep -q '"disable": false'
  # FIXME if we receive a SIGTERM, the deferred task won't be executed, hence we cannot restore the pd config :(.
  # || curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '."enable-location-replacement"' | grep "false"
then
  echo "TEST: [$TEST_NAME] failed because scheduler has not been removed, or location replacement is disabled."
  echo "current config:"
  curl http://$PD_ADDR/pd/api/v1/config/schedule
  echo "log of background br":
  cat "$TEST_DIR/background-br.log"
  exit 1
fi

pd_settings=6
# we need reset pd scheduler/config to default
# until pd has the solution to temporary set these scheduler/configs.
run_br validate reset-pd-config-as-default

# max-merge-region-size set to default 20
curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '."max-merge-region-size"' | grep "20" || ((pd_settings--))

# max-merge-region-keys set to default 200000
curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '."max-merge-region-keys"' | grep "200000" || ((pd_settings--))
# balance-region scheduler enabled
curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '."schedulers-v2"[] | {disable: .disable, type: ."type" | select (.=="balance-region")}' | grep '"disable": false' || ((pd_settings--))
# balance-leader scheduler enabled
curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '."schedulers-v2"[] | {disable: .disable, type: ."type" | select (.=="balance-leader")}' | grep '"disable": false' || ((pd_settings--))
# hot region scheduler enabled
curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '."schedulers-v2"[] | {disable: .disable, type: ."type" | select (.=="hot-region")}' | grep '"disable": false' || ((pd_settings--))
# location replacement enabled
curl http://$PD_ADDR/pd/api/v1/config/schedule | jq '."enable-location-replacement"' | grep "true" || ((pd_settings--))

if [ "$pd_settings" -ne "6" ];then
    echo "TEST: [$TEST_NAME] test validate reset pd config failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"

# Test version
run_br --version
run_br -V
