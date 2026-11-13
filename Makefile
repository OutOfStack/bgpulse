.PHONY: build run test lint vuln up down setup test-integration

build:
	go build -o bin/ ./cmd/bgpulse

# make run ARGS="watch -host rrc00"
run:
	go run ./cmd/bgpulse $(ARGS)

test:
	go test -race ./...

# Keep in sync with golangci-lint-action version in .github/workflows/ci.yml.
LINT_VERSION := v2.15.0
LINT_PKG := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(LINT_VERSION)

lint:
	@golangci-lint version 2>/dev/null | grep -q '$(LINT_VERSION:v%=%)' || { echo "Installing golangci-lint $(LINT_VERSION)..."; go install $(LINT_PKG); }
	golangci-lint run

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

# Dev dependencies (NATS, Scylla). Docker if installed, otherwise Podman.
COMPOSE ?= $(if $(shell docker --version 2>/dev/null),docker compose,podman compose)
COMPOSE_FILE := deploy/compose.yaml

up:
	$(COMPOSE) -f $(COMPOSE_FILE) up -d --wait

# Keeps volumes. Wipe them with: $(COMPOSE) -f deploy/compose.yaml down -v
down:
	$(COMPOSE) -f $(COMPOSE_FILE) down

# make setup ARGS="-check"
setup:
	go run ./cmd/bgpulse setup $(ARGS)

test-integration:
	go test -race -count=1 -tags=integration ./tests/integration/...
