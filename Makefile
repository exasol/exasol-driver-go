all: build

install-deps:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.45.2

lint-fix:
	golangci-lint run --print-issued-lines=false --fix ./...

lint:
	golangci-lint run --print-issued-lines=false ./...

test:
	go test -v -coverprofile=coverage.out ./...

testshort:
	go test -v -short -coverprofile=coverage.out ./...


coverage: test
	go tool cover -html=coverage.out -o cover.html