#!/bin/zsh

echo ">>>>>>>>>>>>>>>>>>>>>>> cleanup"
ssh 172.16.5.36 "rm /data3/tmh/1*"
mysql -u root -h 172.16.5.36 -P 42002 -p -e "use sbtest1; drop table sbtest1;"

echo ">>>>>>>>>>>>>>>>>>>>>>> backup file"
./br backup full -u 172.16.5.36:42010 -s "local:///data3/tmh"

echo ">>>>>>>>>>>>>>>>>>>>>>> restore file"
make build && ./br restore -r "root@tcp(172.16.5.36:42002)/sbtest" -d "root@tcp(172.16.5.36:42002)/sbtest1" -i 172.16.5.36:8287 -m backupmeta -t sbtest1 -u 172.16.5.36:42010 -c 1

echo ">>>>>>>>>>>>>>>>>>>>>>> show restored table"
mysql -u root -h 172.16.5.36 -P 42002 -p -e "use sbtest1; select * from sbtest1;"
