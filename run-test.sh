#! /bin/sh

apt update && apt install default-mysql-client jq --yes

cd /brie
TEST_NAME=br_other make integration_test