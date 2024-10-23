.PHONY: default

default: init

init:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.55.2
	go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@v0.15.0

clean:
	rm -rf ./build

linter:
	fieldalignment -fix ./...
	golangci-lint run -c .golangci.yml --timeout=5m -v --fix

lint:
	golangci-lint run -c .golangci.yml --timeout=5m -v

test:
	go test ./... -bench . -benchmem

compose:
	docker compose up --wait --build --force-recreate --remove-orphans

tidy:
	go mod tidy
	cd example/default-mapper && go mod tidy && cd ../..
	cd example/simple && go mod tidy && cd ../..
	cd example/simple-logger && go mod tidy && cd ../..
	cd example/struct-config && go mod tidy && cd ../..
	cd example/grafana && go mod tidy && cd ../..
	cd test/integration && go mod tidy && cd ../..