#! /bin/bash

set -eux

. run_services

wait_file_exist() {
    until [ -e "$1" ]; do
        sleep 1
    done
}

single_point_fault() {
    type=$1
    victim=$(shuf -i 1-3 -n 1)
    echo "Will make failure($type) to store#$victim."
    case $type in
        outage)
            wait_file_exist "$hint_backup_start"
            kv_outage -d 20 -i $victim;;
        outage-after-request)
            wait_file_exist "$hint_get_backup_client"
            kv_outage -d 30 -i $victim;;
        twice-outage)
            wait_file_exist "$hint_backup_start"
            local_victim=$(shuf -i 1-3 -n 1)
            kv_outage -d 20 -i $local_victim
            wait_file_exist "$hint_finegrained"
            local_victim=$(shuf -i 1-3 -n 1)
            kv_outage -d 20 -i $local_victim;;
        shutdown)
            wait_file_exist "$hint_backup_start"
            kv_outage --kill -i $victim;;
        scale-out)
            wait_file_exist "$hint_backup_start"
            kv_outage --kill -i $victim
            kv_outage --scale-out -i 4;;
        random)
            # Maybe we need to skip this case for less CI failure...
            after=$(shuf -i 0-8 -n 1)
            echo "injecting after random seconds (${after}s)."
            sleep "$after"
            kv_outage -d $(shuf -i 1-30 -n 1) -i $victim
    esac
}

load() {
    run_sql "create database if not exists $TEST_NAME"
    go-ycsb load mysql -P tests/"$TEST_NAME"/workload -p mysql.host="$TIDB_IP" -p mysql.port="$TIDB_PORT" -p mysql.user=root -p mysql.db="$TEST_NAME"
    run_sql 'use '$TEST_NAME'; show tables'
}

check() {
    run_sql 'drop database if exists '$TEST_NAME';'
    run_br restore full -s local://"$backup_dir" 
    count=$(run_sql 'select count(*) from '$TEST_NAME'.usertable;' | tail -n 1 | awk '{print $2}')
    [ "$count" -eq 20000 ]
}

load

hint_finegrained=$TEST_DIR/hint_finegrained
hint_backup_start=$TEST_DIR/hint_backup_start
hint_get_backup_client=$TEST_DIR/hint_get_backup_client
export GO_FAILPOINTS="github.com/pingcap/br/pkg/backup/hint-backup-start=1*return(\"$hint_backup_start\");\
github.com/pingcap/br/pkg/backup/hint-fine-grained-backup=1*return(\"$hint_finegrained\");\
github.com/pingcap/br/pkg/conn/hint-get-backup-client=1*return(\"$hint_get_backup_client\")"

for failure in outage outage-after-request twice-outage shutdown scale-out random; do
    rm -f "$hint_finegrained" "$hint_backup_start" "$hint_get_backup_client"
    backup_dir=${TEST_DIR:?}/"backup{test:${TEST_NAME}|with:${failure}}"
    run_br backup full -s local://"$backup_dir" --ratelimit 128 --ratelimit-unit 1024 &
    backup_pid=$!
    single_point_fault $failure
    wait $backup_pid
    case $failure in
    scale-out | shutdown ) stop_services
        start_services ;;
    *) ;;
    esac
    check
done

