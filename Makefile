.PHONY: echo broadcast ids grow kafka

all: echo broadcast ids grow

echo:
	go build -o echo cmd/echo.go

ids:
	go build -o ids cmd/unique_id.go

broadcast:
	go build -o broadcast cmd/broadcast.go

grow:
	go build -o grow cmd/grow.go

kafka:
	go build -o kafka cmd/kafka.go

clean:
	rm echo broadcast grow ids kafka