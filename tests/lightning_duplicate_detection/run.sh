#!/bin/sh
#
# Copyright 2021 PingCAP, Inc.
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

set -eux

check_cluster_version 4 0 0 'local backend' || exit 0

#run_lightning --backend local --enable-checkpoint=1 --log-file "$TEST_DIR/lightning-duplicate-detection.log" --config "tests/$TEST_NAME/config.toml"
