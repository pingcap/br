module github.com/pingcap/br

go 1.13

require (
	github.com/cheggaaa/pb/v3 v3.0.1
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/kvproto v0.0.0-20191113105027-4f292e1801d8
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pingcap/parser v0.0.0-20191121045207-8b5639e42f59
	github.com/pingcap/pd v1.1.0-beta.0.20191115131715-6b7dc037010e
	github.com/pingcap/tidb v1.1.0-beta.0.20191127051612-b8000b1b9d2d
	github.com/pingcap/tidb-tools v3.0.6-0.20191126140929-617bc410fa59+incompatible
	github.com/pingcap/tipb v0.0.0-20191126033718-169898888b24
	github.com/prometheus/client_golang v1.0.0
	github.com/sirupsen/logrus v1.2.0
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20191011191535-87dc89f01550 // indirect
	golang.org/x/net v0.0.0-20191011234655-491137f69257 // indirect
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	golang.org/x/tools v0.0.0-20191012152004-8de300cfc20a // indirect
	google.golang.org/genproto v0.0.0-20190905072037-92dd089d5514 // indirect
	google.golang.org/grpc v1.24.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20190930215403-16217165b5de

replace github.com/google/pprof => github.com/lonng/pprof v0.0.0-20191012154247-04dfd648ce8d
