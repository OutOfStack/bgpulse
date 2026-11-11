.PHONY: build run test lint

build:
	go build -o bin/ ./cmd/bgpulse

# make run ARGS="-host rrc00"
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
