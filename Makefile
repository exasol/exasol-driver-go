all: build

install-deps:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v2.12.2

lint-fix:
	golangci-lint run --fix ./...

lint:
	golangci-lint run ./...

test:
	go test -count 1 -v -p 1 -coverpkg=.,./pkg/...,./internal/...  -coverprofile=coverage.out ./...

testshort:
	go test -count 1 -v -short -coverpkg=.,./pkg/...,./internal/... -coverprofile=coverage.out ./...

coverage: test
	go tool cover -html=coverage.out -o coverage.html
