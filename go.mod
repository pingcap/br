module github.com/pingcap/br

go 1.13

require (
	github.com/cheggaaa/pb/v3 v3.0.1
	github.com/coreos/etcd v3.3.17+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/failpoint v0.0.0-20190708053854-e7b1061e6e81 // indirect
	github.com/pingcap/kvproto v0.0.0-20191025022903-62abb760d9b1
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pingcap/parser v0.0.0-20190917040145-a90dba59f50d
	github.com/pingcap/pd v1.1.0-beta.0.20191031081404-d0c4df68eb38
	github.com/pingcap/tidb v0.0.0-20191022034824-9b4d7891def5
	github.com/pingcap/tidb-tools v3.0.6-0.20191102064951-fd78257c02b6+incompatible
	github.com/prometheus/client_golang v0.9.1
	github.com/prometheus/common v0.4.1
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20181202132449-6a9ea43bcacd // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550 // indirect
	golang.org/x/net v0.0.0-20191011234655-491137f69257 // indirect
	golang.org/x/sys v0.0.0-20190910064555-bbd175535a8b // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	golang.org/x/tools v0.0.0-20191012152004-8de300cfc20a // indirect
	google.golang.org/genproto v0.0.0-20190905072037-92dd089d5514 // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190930215403-16217165b5de
