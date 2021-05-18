#! /bin/bash

set -eux

backup_dir=$TEST_DIR/$TEST_NAME

test_data="('TiDB'),('TiKV'),('TiFlash'),('TiSpark'),('TiCDC'),('TiPB'),('Rust'),('C++'),('Go'),('Haskell'),('Scala')"

modify_systables() {
    run_sql "CREATE USER 'Alyssa P. Hacker'@'%' IDENTIFIED BY 'password';"
    run_sql "UPDATE mysql.tidb SET VARIABLE_VALUE = '1h' WHERE VARIABLE_NAME = 'tikv_gc_life_time';"

    run_sql "CREATE TABLE mysql.foo(pk int primary key auto_increment, field varchar(255));"
    run_sql "CREATE TABLE mysql.bar(pk int primary key auto_increment, field varchar(255));"

    run_sql "INSERT INTO mysql.foo(field) VALUES $test_data"
    run_sql "INSERT INTO mysql.bar(field) VALUES $test_data"

    go-ycsb load mysql -P tests/"$TEST_NAME"/workload \
        -p mysql.host="$TIDB_IP" \
        -p mysql.port="$TIDB_PORT" \
        -p mysql.user=root \
        -p mysql.db=mysql

    run_sql "ANALYZE TABLE mysql.usertable;"
}

rollback_modify() {
    run_sql "DROP TABLE mysql.foo;"
    run_sql "DROP TABLE mysql.bar;"
    run_sql "UPDATE mysql.tidb SET VARIABLE_VALUE = '10m' WHERE VARIABLE_NAME = 'tikv_gc_life_time';"
    run_sql "DROP USER 'Alyssa P. Hacker';"
    run_sql "DROP TABLE mysql.usertable;"
}

check() {
    run_sql "SELECT count(*) from mysql.foo;" | grep 11
    run_sql "SELECT count(*) from mysql.usertable;" | grep 1000
    run_sql "SHOW TABLES IN mysql;" | awk '/bar/{exit 1}'
    # we cannot let user overwrite `mysql.tidb` through br in any time.
    run_sql "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'tikv_gc_life_time'" | grep -v "1h"

    # TODO remove this after supporting auto flush.
    run_sql "FLUSH PRIVILEGES;"
    run_sql "SELECT CURRENT_USER();" -u'Alyssa P. Hacker' -p'password' | grep 'Alyssa P. Hacker'
    run_sql "SHOW DATABASES" | grep -v '__TiDB_BR_Temporary_'
    # TODO check stats after supportting.
}

modify_systables
run_br backup full -s "local://$backup_dir"
rollback_modify
run_br restore full -f '*.*' -f '!mysql.bar' -s "local://$backup_dir"
check
