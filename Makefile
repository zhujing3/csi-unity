NAME:=csi-unity

.PHONY: all
all: go-build

ifneq (on,$(GO111MODULE))
export GO111MODULE := on
endif

.PHONY: go-vendor
go-vendor:
	go mod vendor

.PHONY: go-build
go-build: clean
	git config core.hooksPath hooks
	rm -f core/core_generated.go
	cd core && go generate
	go build .

# Integration tests using Godog. Populate env.sh with the hardware parameters
integration-test:
	( cd test/integration-test; sh run.sh )

# Unit tests using Godog. Populate env.sh with the hardware parameters
# unit-test:
# 	( cd test/unit-test; sh run.sh )

unit-test:
	( cd service;  go test -v -coverprofile=coverage.out -timeout 30m -coverpkg=./ *unit_test.go )

coverage:
	cd service; go tool cover -html=coverage.out -o coverage.html

# #
# Docker-related tasks
#
# Generates the docker container (but does not push)
podman-build: go-build
	sh build.sh

podman-push: go-build
	sh build.sh -p

#
# Docker-related tasks
#
# Generates the docker container (but does not push)
docker-build: go-build
	cd core && go generate
	go run core/semver/semver.go -f mk >semver.mk
	make -f docker.mk docker-build

docker-push:
	make -f docker.mk docker-push

version:
	go generate
	go run core/semver/semver.go -f mk >semver.mk
	sh build.sh -h

.PHONY: clean
clean:
	rm -f core/core_generated.go
	go clean

#
# Tests-related tasks
.PHONY: integ-test
integ-test: go-build
	go test -v ./test/...

check:
	sh scripts/check.sh
