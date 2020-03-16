PROTOC ?= $(shell which protoc)
PROTOS := $(shell find $(shell pwd) -type f -name '*.proto' -print)
CWD := $(shell pwd)
PACKAGES := go list ./...
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's/github.com\/pingcap\/br\/*//'
GOCHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'

BR_PKG := github.com/pingcap/br

LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

ifeq ("$(WITH_RACE)", "1")
	RACEFLAG = -race
endif

all: check test build

build:
	GO111MODULE=on go build -ldflags '$(LDFLAGS)' ${RACEFLAG} -o bin/br

build_for_integration_test:
	GO111MODULE=on go test -c -cover -covermode=count \
		-coverpkg=$(BR_PKG)/... \
		-o bin/br.test
	# build key locker
	GO111MODULE=on go build ${RACEFLAG} -o bin/locker tests/br_key_locked/*.go
	# build gc
	GO111MODULE=on go build ${RACEFLAG} -o bin/gc tests/br_z_gc_safepoint/*.go
	# build rawkv client
	GO111MODULE=on go build ${RACEFLAG} -o bin/rawkv tests/br_rawkv/*.go

test:
	GO111MODULE=on go test ${RACEFLAG} -tags leak ./...

testcover:
	GO111MODULE=on retool do overalls \
		-project=$(BR_PKG) \
		-covermode=count \
		-ignore='.git,vendor,tests,_tools' \
		-debug \
		-- -coverpkg=./...

integration_test: build build_for_integration_test
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/pd-ctl
	@which bin/go-ycsb
	@which bin/minio
	@which bin/br
	tests/run.sh

tools:
	@echo "install tools..."
	@GO111MODULE=off go get github.com/twitchtv/retool
	@GO111MODULE=off retool sync

check-all: static lint tidy
	@echo "checking"

check: tools check-all

static: export GO111MODULE=on
static:
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	retool do goimports -w -d -format-only -local $(BR_PKG) $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)
	retool do govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)

	CGO_ENABLED=0 retool do golangci-lint run --enable-all --deadline 120s \
		--disable gochecknoglobals \
		--disable gochecknoinits \
		--disable interfacer \
		--disable goimports \
		--disable gofmt \
		--disable wsl \
		--disable funlen \
		--disable whitespace \
		--disable gocognit \
		--disable godox \
		--disable gomnd \
		$$($(PACKAGE_DIRECTORIES))

lint:
	@echo "linting"
	CGO_ENABLED=0 retool do revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --quiet go.mod go.sum
