PROTOC ?= $(shell which protoc)
PROTOS := $(shell find $(shell pwd) -type f -name '*.proto' -print)
CWD := $(shell pwd)
PACKAGES := go list ./...
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's/github.com\/pingcap\/br\/*//'
CHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'

BR_PKG := github.com/pingcap/br

LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRBuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(BR_PKG)/pkg/utils.BRGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

GOBUILD := CGO_ENABLED=0 GO111MODULE=on go build -trimpath -ldflags '$(LDFLAGS)'
GOTEST  := CGO_ENABLED=1 GO111MODULE=on go test -ldflags '$(LDFLAGS)'
PREPARE_MOD := cp go.mod1 go.mod && cp go.sum1 go.sum
FINISH_MOD := cp go.mod go.mod1 && cp go.sum go.sum1

ifeq ("$(WITH_RACE)", "1")
	RACEFLAG = -race
endif

all: build check test

prepare:
	$(PREPARE_MOD)

build:
	$(PREPARE_MOD)
	$(GOBUILD) $(RACEFLAG) -o bin/br

build_for_integration_test:
	$(PREPARE_MOD)
	@make failpoint-enable
	($(GOTEST) -c -cover -covermode=count \
		-coverpkg=$(BR_PKG)/... \
		-o bin/br.test && \
	$(GOBUILD) $(RACEFLAG) -o bin/locker tests/br_key_locked/*.go && \
	$(GOBUILD) $(RACEFLAG) -o bin/gc tests/br_z_gc_safepoint/*.go && \
	$(GOBUILD) $(RACEFLAG) -o bin/rawkv tests/br_rawkv/*.go) || (make failpoint-disable && exit 1)
	@make failpoint-disable

test:
	$(PREPARE_MOD)
	@make failpoint-enable
	$(GOTEST) $(RACEFLAG) -tags leak ./... || ( make failpoint-disable && exit 1 )
	@make failpoint-disable

testcover: tools
	$(PREPARE_MOD)
	@make failpoint-enable
	GO111MODULE=on tools/bin/overalls \
		-project=$(BR_PKG) \
		-covermode=count \
		-ignore='.git,vendor,tests,_tools' \
		-debug \
		-- -coverpkg=./... || ( make failpoint-disable && exit 1 )

integration_test: bins build build_for_integration_test
	tests/run.sh

bins:
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/pd-ctl
	@which bin/go-ycsb
	@which bin/minio
	@which bin/br
	@which bin/tiflash
	@which bin/libtiflash_proxy.so
	@which bin/cdc
	@which bin/fake-gcs-server
	if [ ! -d bin/flash_cluster_manager ]; then echo "flash_cluster_manager not exist"; exit 1; fi

tools:
	@echo "install tools..."
	@cd tools && make

check:
	@# Tidy first to avoid go.mod being affected by static and lint
	@make tidy
	@# Build tools for targets errdoc, static and lint
	@make tools errdoc static lint

static: export GO111MODULE=on
static: prepare tools
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	tools/bin/gofumports -w -d -format-only -local $(BR_PKG) $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(CHECKER)
	tools/bin/govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(CHECKER)

	@# why some lints are disabled?
	@#   gochecknoglobals - disabled because we do use quite a lot of globals
	@#          goimports - executed above already, gofumports
	@#              gofmt - ditto
	@#                gci - ditto
	@#                wsl - too pedantic about the formatting
	@#             funlen - PENDING REFACTORING
	@#           gocognit - PENDING REFACTORING
	@#              godox - TODO
	@#              gomnd - too many magic numbers, and too pedantic (even 2*x got flagged...)
	@#        testpackage - several test packages still rely on private functions
	@#             nestif - PENDING REFACTORING
	@#           goerr113 - it mistaken pingcap/errors with standard errors
	@#                lll - pingcap/errors may need to write a long line
	@#       paralleltest - no need to run test parallel
	@#           nlreturn - no need to ensure a new line before continue or return
	@#   exhaustivestruct - Protobuf structs have hidden fields, like "XXX_NoUnkeyedLiteral"
	@#         exhaustive - no need to check exhaustiveness of enum switch statements
	@#              gosec - too many false positive
	CGO_ENABLED=0 tools/bin/golangci-lint run --enable-all --deadline 120s \
		--disable gochecknoglobals \
		--disable goimports \
		--disable gofmt \
		--disable gci \
		--disable wsl \
		--disable funlen \
		--disable gocognit \
		--disable godox \
		--disable gomnd \
		--disable testpackage \
		--disable nestif \
		--disable goerr113 \
		--disable lll \
		--disable paralleltest \
		--disable nlreturn \
		--disable exhaustivestruct \
		--disable exhaustive \
		--disable godot \
		--disable gosec \
		$$($(PACKAGE_DIRECTORIES))
	# pingcap/errors APIs are mixed with multiple patterns 'pkg/errors',
	# 'juju/errors' and 'pingcap/parser'. To avoid confusion and mistake,
	# we only allow a subset of APIs, that's "Normalize|Annotate|Trace|Cause".
	@# TODO: allow more APIs when we need to support "workaound".
	grep -Rn --exclude="*_test.go" -E "(\t| )errors\.[A-Z]" cmd pkg | \
		grep -vE "Normalize|Annotate|Trace|Cause|RedactLogEnabled" 2>&1 | $(CHECKER)

lint: prepare tools
	@echo "linting"
	CGO_ENABLED=0 tools/bin/revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	$(PREPARE_MOD)
	GO111MODULE=on go mod tidy
	$(FINISH_MOD)
	cd tests && GO111MODULE=on go mod tidy
	git diff --exit-code go.mod1 go.sum1 tools/go.mod tools/go.sum

errdoc: tools
	@echo "generator errors.toml"
	./tools/check-errdoc.sh

failpoint-enable: tools
	tools/bin/failpoint-ctl enable

failpoint-disable: tools
	tools/bin/failpoint-ctl disable

.PHONY: tools
