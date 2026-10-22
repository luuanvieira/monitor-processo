TEST_DATABASE_URL ?= postgres://monitor:monitor@localhost:5432/monitor?sslmode=disable

.PHONY: rodar rodar-rapido testes integracao lint docker parar

## Histórico em memória, tempo real.
rodar:
	go run ./cmd/monitor

## 30 segundos de planta por segundo real: bom para ver a degradação da bomba.
rodar-rapido:
	SIM_VELOCIDADE=30 go run ./cmd/monitor

testes:
	go test -race ./...

## Testes contra um PostgreSQL de verdade (sobe só o banco do compose).
integracao:
	docker compose up -d --wait postgres
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -tags integracao -count=1 ./internal/armazenamento/postgres/

lint:
	test -z "$$(gofmt -l .)" || (gofmt -l . && false)
	go vet ./...

## Aplicação + PostgreSQL. Dashboard em http://localhost:8080
docker:
	docker compose up --build

parar:
	docker compose down
