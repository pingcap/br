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

# Download tools for running the integration test

set -eu

BIN="$(dirname "$0")/../bin"

if [ "$(uname -s)" != Linux ]; then
    echo 'Can only automatically download binaries on Linux.'
    exit 1
fi

MISSING_TIDB_COMPONENTS=
for COMPONENT in tidb-server pd-server tikv-server pd-ctl; do
    if [ ! -e "$BIN/$COMPONENT" ]; then
        MISSING_TIDB_COMPONENTS="$MISSING_TIDB_COMPONENTS tidb-latest-linux-amd64/bin/$COMPONENT"
    fi
done

if [ -n "$MISSING_TIDB_COMPONENTS" ]; then
    echo "Downloading latest TiDB bundle..."
    # TODO: the url is going to change from 'latest' to 'nightly' someday.
    curl -L -f -o "$BIN/tidb.tar.gz" "https://download.pingcap.org/tidb-latest-linux-amd64.tar.gz"
    tar -x -f "$BIN/tidb.tar.gz" -C "$BIN/" $MISSING_TIDB_COMPONENTS
    rm "$BIN/tidb.tar.gz"
    mv "$BIN"/tidb-latest-linux-amd64/bin/* "$BIN/"
    rmdir "$BIN/tidb-latest-linux-amd64/bin"
    rmdir "$BIN/tidb-latest-linux-amd64"
fi

if [ ! -e "$BIN/go-ycsb" ]; then
    # TODO: replace this once there's a public downloadable release.
    echo 'go-ycsb is missing. Please build manually following https://github.com/pingcap/go-ycsb#getting-started'
    exit 1
fi

if [ ! -e "$BIN/minio" ]; then
    echo "Downloading minio..."
    curl -L -f -o "$BIN/minio" "https://dl.min.io/server/minio/release/linux-amd64/minio"
    chmod a+x "$BIN/minio"
fi

echo "All binaries are now available."
