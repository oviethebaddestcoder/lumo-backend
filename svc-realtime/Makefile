.PHONY: deps build test lint run docker
IMAGE ?= ghcr.io/oviethebaddestcoder/svc-realtime
TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

deps:     ## resolve modules and write go.sum (run once after cloning)
	go mod tidy

build:
	CGO_ENABLED=0 go build -trimpath -o bin/realtime ./cmd/realtime

test:     ## needs Postgres 16 + Redis 7 running locally (see lumo-infra docker-compose)
	LUMO_TEST_PG=postgres://postgres:postgres@localhost:5432/postgres LUMO_TEST_REDIS=redis://localhost:6379/9 go test -race -count=1 ./...

lint:
	go vet ./...
	golangci-lint run ./...

run: build
	set -a; . ./.env; set +a; ./bin/realtime serve

docker:
	docker build -f Dockerfile -t $(IMAGE):$(TAG) ..
