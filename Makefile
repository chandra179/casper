.PHONY: vendor build test test-all test-unit test-integration test-e2e docker-up docker-down run-api run-scheduler run-worker clean

vendor:
	go mod tidy && go mod vendor

build:
	go build -o bin/api ./cmd/api/
	go build -o bin/scheduler ./cmd/scheduler/
	go build -o bin/worker ./cmd/worker/

test: test-unit test-integration test-e2e

test-unit:
	go test -v -count=1 ./modules/metrics/ -timeout 30s
	go test -v -count=1 ./modules/scheduler/ -timeout 30s
	go test -v -count=1 ./modules/worker/ -timeout 30s

test-integration:
	go test -v -count=1 ./modules/task/ -timeout 120s
	go test -v -count=1 ./modules/broker/ -timeout 120s
	go test -v -count=1 ./modules/api/ -timeout 60s
	go test -v -count=1 ./modules/scheduler/ -timeout 120s

test-e2e:
	go test -v -count=1 ./integration/ -timeout 120s

docker-up:
	docker compose up -d --wait

docker-down:
	docker compose down -v

run-api:
	go run ./cmd/api/ config/config.yaml

run-scheduler:
	go run ./cmd/scheduler/ config/config.yaml

run-worker:
	go run ./cmd/worker/ config/config.yaml

clean:
	rm -rf bin/
