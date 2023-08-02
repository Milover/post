# Makefile

MODULE		:= $(shell go list -m)
TARGET		:= $(shell basename $(MODULE))

build:
	go build -o bin/$(TARGET) main.go

publish:
	GOARCH=amd64 GOOS=linux   go build -o bin/$(TARGET)-linux   main.go
	GOARCH=amd64 GOOS=windows go build -o bin/$(TARGET)-windows main.go
	GOARCH=arm64 GOOS=darwin  go build -o bin/$(TARGET)-darwin  main.go

run:
	go run ./...

test:
	go test ./...

test-integration:
	go test -tags=integration ./...

vet:
	go vet ./...

lint:
	$(shell go env GOPATH)/bin/golangci-lint run ./...

clean:
	go clean
	rm -rf bin
