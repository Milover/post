# Makefile

TARGET		:= fp

build:
	go build -o $(TARGET) main.go

run:
	./$(TARGET)

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
	rm -f $(TARGET)
