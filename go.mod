module github.com/pingcap/br

go 1.12

require (
	github.com/5kbpers/tidb-tools v3.0.0-beta.1.0.20191018085309-1d625db17643+incompatible
	github.com/cheggaaa/pb/v3 v3.0.1
	github.com/coreos/etcd v3.3.17+incompatible // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/btree v1.0.0
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/kvproto v0.0.0-20191017041915-199062ebc51c
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pingcap/parser v0.0.0-20190923031704-33636bc5e5d6
	github.com/pingcap/pd v1.1.0-beta.0.20190923032047-5c648dc365e0
	github.com/pingcap/tidb v0.0.0-20191010035248-d82a94fe5ea7
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/common v0.6.0
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20181202132449-6a9ea43bcacd // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	github.com/twinj/uuid v1.0.0
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550 // indirect
	golang.org/x/net v0.0.0-20191011234655-491137f69257 // indirect
	golang.org/x/tools v0.0.0-20191012152004-8de300cfc20a // indirect
	google.golang.org/grpc v1.24.0
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190930215403-16217165b5de
