all: build

build:
	go build -o ./GoProducer ./producer/producer_main.go
	go build -o ./GoConsumer ./consumer/consumer_main.go
clean:
	@rm -rf bin
test:
	go test ./go/... -race
