all: build

install-deps:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.59.1

lint-fix:
	golangci-lint run --print-issued-lines=false --fix ./...

lint:
	golangci-lint run --print-issued-lines=false ./...

test:
	go test -count 1 -v -p 1 -coverprofile=coverage.out ./...

testshort:
	go test -count 1 -v -short -coverprofile=coverage.out ./...

coverage: test
	go tool cover -html=coverage.out -o coverage.html
