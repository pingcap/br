# test latest 3 version clusters
TAGS=$(git tag | sort -r | head -n3)
echo $TAGS

i=0
for tag in $TAGS; do 
	i=$(( i + 1 ))
	export TAG=$tag
	export PORT_SUFFIX=$i
	echo "build $TAG cluster"
	docker-compose -f compatible/backup_cluster.yaml build
	docker-compose -f compatible/backup_cluster.yaml up -d
	## wait for cluster ready
	sleep 5
	docker-compose -f compatible/backup_cluster.yaml exec -T control /go/bin/go-ycsb load mysql -P /prepare_data/workload -p mysql.host=tidb -p mysql.port=4000 -p mysql.user=root -p mysql.db=test
	docker-compose -f compatible/backup_cluster.yaml exec -T control make compatible_test
	docker-compose -f compatible/backup_cluster.yaml down
	echo "destroy $TAG cluster"
done 
