.PHONY: all build test lint fmt vet

all: build

build:
	go build ./...

test:
	go test -race ./...

lint:
	golangci-lint run

fmt:
	gofmt -s -w .

vet:
	go vet ./...

