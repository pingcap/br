#!/usr/bin/env bash

set -eo pipefail

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        --cleanup-docker)
            CLEANUP_DOCKER=1
            shift
            ;;
        --cleanup-data)
            CLEANUP_DATA=1
            shift
            ;;
        --help)
            HELP=1
            shift
            ;;
        *)
            HELP=1
            break
            ;;
    esac
done

if [ "$HELP" ]; then
    echo "Usage: $0 [OPTIONS]"
    echo "OPTIONS:"
    echo "  --help           Display this message"
    echo "  --cleanup-docker Clean up Docker images and containers"
    echo "  --cleanup-data   Clean up persistent data"
    exit 0
fi

docker_repo=br_tests
host_tmp=/tmp/br_tests
host_bash_history=$host_tmp/bash_history

if [ "$CLEANUP_DOCKER" ]; then
    docker rm $(docker container ps --all --filter="ancestor=$docker_repo" -q)
    docker rmi $(docker images $docker_repo -q)
    exit 0
fi

# Persist tests data and bash history
mkdir -p $host_tmp
touch $host_bash_history || true

if [ "$CLEANUP_DATA" ]; then
    rm -rf $host_tmp || { echo try "sudo rm -rf $host_tmp"? ; exit 1; }
    exit 0
fi

docker build -t $docker_repo tests

exec docker run -it \
    -v `pwd`:/br \
    -v $host_tmp:/tmp \
    -v $host_bash_history:/root/.bash_history \
    $docker_repo
