.PHONY: all build test lint fmt vulncheck

GOLANGCI_LINT = go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0

all: fmt lint test build

build:
	go build ./...

test:
	go test -v -race ./...

lint:
	$(GOLANGCI_LINT) run ./...

fmt:
	$(GOLANGCI_LINT) fmt ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
