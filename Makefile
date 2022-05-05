all: build

install_deps:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.45.2

lint:
	golangci-lint run --print-issued-lines=false --fix ./...

test:
	go test -v -coverprofile=coverage.out ./...

testshort:
	go test -v -short -coverprofile=coverage.out ./...


coverage: test
	go tool cover -html=coverage.out -o cover.html