#!/bin/bash
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

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/run_services

DB="$TEST_NAME"

# prepare database
echo "Restart cluster with alter-primary-key = true"
start_services "$cur"

run_sql "drop schema if exists $DB;"
run_sql "create schema $DB;"

run_sql "create table $DB.a (a int primary key, b int unique);"
run_sql "insert into $DB.a values (42, 42);"

# backup
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/$DB"

# restore
run_sql "drop schema $DB;"
run_br --pd $PD_ADDR restore db --db "$DB" -s "local://$TEST_DIR/$DB"

run_sql "drop schema $DB;"
echo "Restart service with alter-primary-key = false"
start_services
