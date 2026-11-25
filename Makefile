# Disable all the default make stuff
MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

GO_VERSION := $(shell awk '$$1 == "go" { print $$2; exit }' go.mod)
GO := env GOTOOLCHAIN=go$(GO_VERSION) go
YQ_VERSION ?= v4.47.2
YQ := $(GO) run github.com/mikefarah/yq/v4@$(YQ_VERSION)

## Display help menu
.PHONY: help
help:
	@echo Documented Make targets:
	@perl -e 'undef $$/; while (<>) { while ($$_ =~ /## (.*?)(?:\n# .*)*\n.PHONY:\s+(\S+).*/mg) { printf "\033[36m%-30s\033[0m %s\n", $$2, $$1 } }' $(MAKEFILE_LIST) | sort

.PHONY: .ALWAYS
.ALWAYS:

## Install dependencies
.PHONY: install
install:
	go mod download

## Generate mocks
.PHONY: generate
generate:
	$(YQ) 'del(.components.securitySchemes, (.. | select(has("security")).security) )' openapi/spec.yaml | $(YQ) --prettyPrint -o=json > openapi/spec.json
	$(GO) generate -v ./...
	$(GO) tool oapi-codegen --config=shared/genclient/oapi-codegen.cfg.yaml openapi/spec.yaml
	$(GO) tool oapi-codegen --config=shared/genevents/oapi-codegen.cfg.yaml openapi/events.yaml
	$(GO) tool oapi-codegen --config=shared/observationapi/oapi-codegen.cfg.yaml openapi/observation.yaml

# ------------------------------------------------------------------------------
# NOTE: CI and local tooling rely on these target names.
# ------------------------------------------------------------------------------

## Build the containers in docker compose
.PHONY: build
build:
	$(MAKE) -C integration-tests build

## Teardown any docker compose containers
.PHONY: clean
clean:
	$(MAKE) -C integration-tests clean

## Run integration tests, starting docker compose containers if necessary
.PHONY: test-integration
test-integration: build
	$(MAKE) -C integration-tests prepare-tests
	$(MAKE) -C integration-tests test

## Run golang tests
.PHONY: test-unit
test-unit:
	go tool gotestsum --format testname -- -coverprofile=cover.out ./cmd/server ./internal/... ./scripts/gosecurity
	GOWORK=off go test -mod=readonly -count=1 github.com/stellwerk-labs/golib/htelemetry -run '^TestStartOTel_DefaultConfig$$'

## Run the complete API storage/authority suite against explicit isolated PostgreSQL, NATS and Vault fixtures
.PHONY: test-pipeline-storage
test-pipeline-storage:
	@test -n "$$MODEL_TEST_DB_CONNECTION_STRING" || { echo "MODEL_TEST_DB_CONNECTION_STRING must name an isolated test database" >&2; exit 1; }
	@test -n "$$OUTCOME_ACK_TEST_NATS_URL" || { echo "OUTCOME_ACK_TEST_NATS_URL must name an isolated test NATS" >&2; exit 1; }
	@test -n "$$COST_POLICY_TEST_VAULT_ADDR" || { echo "COST_POLICY_TEST_VAULT_ADDR must name an isolated test Vault" >&2; exit 1; }
	@test -n "$$COST_POLICY_TEST_VAULT_TOKEN" || { echo "COST_POLICY_TEST_VAULT_TOKEN is required" >&2; exit 1; }
	go test -race -tags=integration ./internal/api ./internal/worker/handlers/envdestroyhandler -count=1 -timeout=10m

## Lint the code
.PHONY: lint
lint:
	golangci-lint run

## Print out the commands for connecting to the running integration tests database
.PHONY:
test-integration-logs:
	$(MAKE) -C integration-tests test-integration-logs

.PHONY: connect-to-integ-db
connect-to-integ-db:
	$(MAKE) -C integration-tests connect-to-integ-db
