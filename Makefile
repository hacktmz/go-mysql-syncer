all: build

build:
	go build -o ./binlog_producer ./main
clean:
	@rm -rf bin
test:
	go test ./go/... -race
