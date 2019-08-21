#!/bin/zsh

echo ">>>>>>>>>>>>>>>> source table"
mysql -u root -h 172.16.5.36 -P 42002 -p -e "use sbtest; select * from sbtest1;"

echo ">>>>>>>>>>>>>>>> destination table"
mysql -u root -h 172.16.5.36 -P 42002 -p -e "use sbtest1; select * from sbtest1;"
