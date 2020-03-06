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

BACKUP_DIR="raw_backup"

# generate raw kv randomly in range[start-key, end-key) in 10s
bin/rawkv --pd $PD_ADDR --mode rand-gen --start-key 31 --end-key 3130303030303030 --duration 10

# output checksum
bin/rawkv --pd $PD_ADDR --mode checksum --start-key 31 --end-key 3130303030303030 > /$TEST_DIR/checksum.out

checksum_ori=$(cat /$TEST_DIR/checksum.out | grep result | awk '{print $3}')

# backup rawkv
echo "backup start..."
run_br --pd $PD_ADDR backup raw -s "local://$TEST_DIR/$BACKUP_DIR" --start 31 --end 3130303030303030 --format hex --concurrency 4

# delete data in range[start-key, end-key)
bin/rawkv --pd $PD_ADDR --mode delete --start-key 31 --end-key 3130303030303030

# TODO: Finish check after restore ready
# restore rawkv
# echo "restore start..."
# run_br --pd $PD_ADDR restore raw -s "local://$TEST_DIR/$BACKUP_DIR" --start 31 --end 3130303030303030 --format hex --concurrency 4

# output checksum after restore
# bin/rawkv --pd $PD_ADDR --mode checksum --start-key 31 --end-key 3130303030303030 > /$TEST_DIR/checksum.out

checksum_new=$(cat /$TEST_DIR/checksum.out | grep result | awk '{print $3}')

if [ "$checksum_ori" == "$checksum_new" ];then
    echo "TEST: [$TEST_NAME] successed!"
else
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi


