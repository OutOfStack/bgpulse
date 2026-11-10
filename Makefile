LINT_VERSION := v2.15.0
LINT_PKG := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(LINT_VERSION)

.PHONY: lint
lint:
	@golangci-lint version 2>/dev/null | grep -q '$(LINT_VERSION:v%=%)' || { echo "Installing golangci-lint $(LINT_VERSION)..."; go install $(LINT_PKG); }
	golangci-lint run
