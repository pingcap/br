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

# backup empty.
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/empty" --ratelimit 5 --concurrency 4
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster!"
    exit 1
fi

# restore empty.
echo "restore start..."
run_br restore full -s "local://$TEST_DIR/empty" --pd $PD_ADDR --ratelimit 1024
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on restore empty cluster!"
    exit 1
fi

echo "TEST: [$TEST_NAME] successed!"
