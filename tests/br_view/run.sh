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

run_sql "create schema $DB;"
run_sql "create view $DB.test_backup_view as select 133;"

echo "backup start..."
run_br backup db --db "$DB" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

run_sql "drop schema $DB;"

echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

restored=$(run_sql "select * from $DB.test_backup_view;" | awk '/133/{print $2}')
[ $restored = '133' ]

run_sql "drop schema $DB;"
