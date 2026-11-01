.PHONY: build test integration dist image

build:
	go build -trimpath -o bin/cnld-agent ./cmd/cnld-agent
	go build -trimpath -o bin/cnld-watch ./cmd/cnld-watch

test:
	go test -race ./...
	go vet ./...

integration:
	CNLD_INTEGRATION=1 go test -race -v -count=1 -timeout 3m ./tests/integration

dist:
	./scripts/build.sh

image:
	docker build -t cnld:local .
