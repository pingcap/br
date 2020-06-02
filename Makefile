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

build_for_integration_test: failpoint-enable
	(GO111MODULE=on go test -c -cover -covermode=count \
		-coverpkg=$(BR_PKG)/... \
		-ldflags '$(LDFLAGS)'\
		-o bin/br.test && \
	GO111MODULE=on go build ${RACEFLAG} -o bin/locker tests/br_key_locked/*.go && \
	GO111MODULE=on go build ${RACEFLAG} -o bin/gc tests/br_z_gc_safepoint/*.go && \
	GO111MODULE=on go build ${RACEFLAG} -o bin/rawkv tests/br_rawkv/*.go) || (make failpoint-disable && exit 1)
	@make failpoint-disable

test: failpoint-enable
	GO111MODULE=on go test ${RACEFLAG} -tags leak ./... || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

testcover: tools failpoint-enable
	GO111MODULE=on tools/bin/overalls \
		-project=$(BR_PKG) \
		-covermode=count \
		-ignore='.git,vendor,tests,_tools' \
		-debug \
		-- -coverpkg=./... || ( make failpoint-disable && exit 1 )

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
	@cd tools && make

check-all: static lint tidy
	@echo "checking"

check: tools check-all

static: export GO111MODULE=on
static: tools
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	tools/bin/goimports -w -d -format-only -local $(BR_PKG) $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)
	tools/bin/govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)

	@# why some lints are disabled?
	@#   gochecknoglobals - disabled because we do use quite a lot of globals
	@#          goimports - executed above already
	@#              gofmt - ditto
	@#                wsl - too pedantic about the formatting
	@#             funlen - PENDING REFACTORING
	@#           gocognit - PENDING REFACTORING
	@#              godox - TODO
	@#              gomnd - too many magic numbers, and too pedantic (even 2*x got flagged...)
	@#        testpackage - several test packages still rely on private functions
	@#             nestif - PENDING REFACTORING
	@#           goerr113 - it mistaken pingcap/errors with standard errors
	CGO_ENABLED=0 tools/bin/golangci-lint run --enable-all --deadline 120s \
		--disable gochecknoglobals \
		--disable goimports \
		--disable gofmt \
		--disable wsl \
		--disable funlen \
		--disable gocognit \
		--disable godox \
		--disable gomnd \
		--disable testpackage \
		--disable nestif \
		--disable goerr113 \
		$$($(PACKAGE_DIRECTORIES))

lint: tools
	@echo "linting"
	CGO_ENABLED=0 tools/bin/revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --quiet go.mod go.sum

failpoint-enable: tools
	tools/bin/failpoint-ctl enable

failpoint-disable: tools
	tools/bin/failpoint-ctl disable

.PHONY: tools
