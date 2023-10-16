# Makefile

MODULE		:= $(shell go list -m)
MODULE_URL	:= $(shell go list -m | tr [:upper:] [:lower:])
TARGET		:= $(shell basename $(MODULE))

build:
	echo $(MODULE)
	go build -o bin/$(TARGET) main.go

publish:
	#GOPROXY=proxy.golang.org go list -m "$(MODULE)@$(shell git tag | tail -n 1)"
	curl "https://sum.golang.org/lookup/$(MODULE_URL)@$(shell git tag | tail -n 1)"
	GOARCH=amd64 GOOS=linux   go build -o bin/$(TARGET)-linux   main.go
	GOARCH=amd64 GOOS=windows go build -o bin/$(TARGET)-windows main.go
	GOARCH=arm64 GOOS=darwin  go build -o bin/$(TARGET)-darwin  main.go

run:
	go run ./...

test:
	go test ./...

testv:
	go test -v ./...

test-integration:
	go test -tags=integration ./...

vet:
	go vet ./...

lint:
	$(shell go env GOPATH)/bin/golangci-lint run ./...

update-deps:
	go get -u ./...
	go mod tidy

update-go:
	go mod edit -go=$(shell go version | awk '{print $$3}' | sed -e 's/go//g')
	go mod tidy

clean:
	go clean
	rm -rf bin

.PHONY: run test testv test-integration vet lint clean update-deps update-go
