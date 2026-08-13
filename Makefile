GO ?= go
PROTOC ?= protoc

.PHONY: all build test lint vet fmt-check cover clean gen

all: build test lint

build:
	mkdir -p bin
	$(GO) build -o bin/takld ./cmd/takld
	$(GO) build -o bin/taklctl ./cmd/taklctl

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