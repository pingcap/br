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

set -eux

# update tags
git fetch --tags

# get latest 3 version clusters
TAGS=$(git for-each-ref --sort=creatordate  refs/tags | awk -F '/' '{print $3}' | tail -n2)
echo "recent version of cluster is $TAGS"

i=0
for tag in $TAGS; do
	i=$(( i + 3 ))
	export TAG=$tag
	export PORT_SUFFIX=$i
	# generate backup data in /tmp/br/docker/backup_data/$TAG/, we can do restore after all data backuped later.
	echo "build $TAG cluster"
	docker-compose -f compatible/backup_cluster.yaml build
	docker-compose -f compatible/backup_cluster.yaml up -d
	# wait for cluster ready
	sleep 5
	# prepare SQL data
	docker-compose -f compatible/backup_cluster.yaml exec -T control /go/bin/go-ycsb load mysql -P /prepare_data/workload -p mysql.host=tidb -p mysql.port=4000 -p mysql.user=root -p mysql.db=test
	# prepare SQL data
	docker-compose -f compatible/backup_cluster.yaml exec -T control make compatible_test_prepare
	docker-compose -f compatible/backup_cluster.yaml down
	echo "destroy $TAG cluster"
done
