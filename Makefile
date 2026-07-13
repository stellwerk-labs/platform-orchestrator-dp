# Disable all the default make stuff
MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

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
	yq 'del(.components.securitySchemes, (.. | select(has("security")).security) )' openapi/spec.yaml | yq --prettyPrint -o=json > openapi/spec.json
	go generate -v ./...
	go tool oapi-codegen --config=shared/genclient/oapi-codegen.cfg.yaml openapi/spec.yaml
	go tool oapi-codegen --config=shared/genevents/oapi-codegen.cfg.yaml openapi/events.yaml

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
	go tool gotestsum --format testname -- -coverprofile=cover.out ./internal/...

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
