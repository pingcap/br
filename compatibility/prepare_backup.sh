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

TAGS="v5.0.0"
getLatestTags() {
  release_5_branch_regex="^release-5\.[0-9].*$"
  release_4_branch_regex="^release-4\.[0-9].*$"
  TOTAL_TAGS=$(git for-each-ref --sort=creatordate  refs/tags | awk -F '/' '{print $3}')
  # latest tags
  TAGS=$(echo $TOTAL_TAGS | tr ' ' '\n' | tail -n3)
  if git rev-parse --abbrev-ref HEAD | egrep -q $release_5_branch_regex
  then
    # If we are in release-5.0 branch, try to use latest 3 version of 5.x and last 4.x version
    TAGS=$(echo $TOTAL_TAGS | tr ' ' '\n' | fgrep "v4." | tail -n1 && echo $TOTAL_TAGS | tr ' ' '\n' | fgrep "v5." | tail -n3)
  elif git rev-parse --abbrev-ref HEAD | egrep -q $release_4_branch_regex
  then
    # If we are in release-4.0 branch, try to use latest 3 version of 4.x
    TAGS=$(echo $TOTAL_TAGS | tr ' ' '\n' | fgrep "v4." | tail -n3)
  fi
}

getLatestTags
echo "recent version of cluster is $TAGS"

i=0

runBackup() {
  # generate backup data in /tmp/br/docker/backup_data/$TAG/, we can do restore after all data backuped later.
  echo "build $1 cluster"
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f compatibility/backup_cluster.yaml build
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f compatibility/backup_cluster.yaml up -d
  trap "TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f compatibility/backup_cluster.yaml down" EXIT
  # wait for cluster ready
  sleep 20
  # prepare SQL data
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f compatibility/backup_cluster.yaml exec -T control /go/bin/go-ycsb load mysql -P /prepare_data/workload -p mysql.host=tidb -p mysql.port=4000 -p mysql.user=root -p mysql.db=test
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f compatibility/backup_cluster.yaml exec -T control make compatibility_test_prepare
  touch /tmp/br/docker/backup_data/$1/prepare_finish
  echo "finish preparing for $1"
}

for tag in $TAGS; do
   i=$(( i + 1 ))
   runBackup $tag $i &
done

wait

echo "prepare backup data successfully"
