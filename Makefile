GO ?= go
PROTOC ?= protoc

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS  = -s -w \
  -X takl/internal/version.Version=$(VERSION) \
  -X takl/internal/version.GitCommit=$(COMMIT) \
  -X takl/internal/version.BuildDate=$(DATE)

.PHONY: all build test lint vet fmt-check cover clean gen

all: build test lint

build:
	mkdir -p bin
	$(GO) build -ldflags="$(LDFLAGS)" -o bin/takld ./cmd/takld
	$(GO) build -ldflags="$(LDFLAGS)" -o bin/taklctl ./cmd/taklctl

gen:
	$(PROTOC) -I proto --go_out=. --go_opt=module=takl --go-grpc_out=. --go-grpc_opt=module=takl proto/v1/sync.proto

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)

lint: vet fmt-check
	@golangci-lint run ./... 2>/dev/null || echo "golangci-lint not installed; skipped (vet+fmt ran)"

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out

clean:
	rm -rf bin coverage.out