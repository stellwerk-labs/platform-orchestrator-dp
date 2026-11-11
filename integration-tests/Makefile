KUBECONFIG := runner-cluster/kubeconfig.yaml
KUBECONFIG_INTERNAL := runner-cluster/kubeconfig_internal.yaml
KUBECTL := kubectl --kubeconfig $(KUBECONFIG)
KUBECTL_INTERNAL := kubectl --kubeconfig $(KUBECONFIG_INTERNAL)
NAME ?= runner
CLUSTER_NAME := kind-$(NAME)
KIND_NODE_IMAGE ?= kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed
KIND_CREATE_ARGS ?=
COMPOSE_PROJECT_NAME ?= integration-tests
export COMPOSE_PROJECT_NAME
PUBLIC_PORT ?= 8080
DATABASE_PORT ?= 5432
CP_PORT ?= 8081
DP_PORT ?= 8082
IAM_PORT ?= 8083
NATS_PORT ?= 4222
NATS_HEALTH_PORT ?= 8222
VAULT_PORT ?= 8200
export NATS_PORT NATS_HEALTH_PORT VAULT_PORT
DEFAULT_K8S_RES := platform-orchestrator-runner
RUNNER_CONFIGURATION := runner_config.json
# Historical material tests require CP 3.3.0's authenticated execution catalogue.
# CI overrides these compatible public defaults with its frozen Core image set.
CP_IMAGE ?= ghcr.io/stellwerk-labs/platform-orchestrator-cp@sha256:53f0ab233e5c8f495fa2f23f859625f22c3523050c46df642df01525798d867c
IAM_IMAGE ?= ghcr.io/stellwerk-labs/platform-orchestrator-iam@sha256:b9667a64ac9e896960ef04fea43cd2d0b3e0bac2dfbebe86ab2ddfbce162f87c
RUNNER_IMAGE ?= ghcr.io/stellwerk-labs/platform-orchestrator-runner@sha256:944f7698905fc8be4fb3ab29cd7d4ba54a56aef377deef1b6ddaf7d0c2b534f2
# Docker tags and kind imports cannot use a registry digest as a tag. Keep the
# immutable source in RUNNER_IMAGE and use a digest-derived alias only locally.
RUNNER_TEST_IMAGE = $(if $(findstring @sha256:,$(RUNNER_IMAGE)),stellwerk-runner-integration:sha256-$(lastword $(subst :, ,$(RUNNER_IMAGE))),$(RUNNER_IMAGE))
RUNNER_IMAGE_PULL ?= true
TEST_PARALLEL ?= 2
TEST_TIMEOUT ?= 10m

.PHONY: compose.yaml
compose.yaml: BUILD_ARGS := {"context":"../"}
compose.yaml: ../score.yaml Makefile compose-mixins.yaml score.provisioners.yaml score.cp.yaml score.iam.yaml
	score-compose check-version '>=0.16.0'
	score-compose init --no-sample --project $(COMPOSE_PROJECT_NAME)
	cp score.provisioners.yaml .score-compose/
	cp score.cp.yaml score.iam.yaml .score-compose/
	yq -i '(.[] | select(.uri == "template://env").outputs) |= sub("ghcr.io/stellwerk-labs/platform-orchestrator-runner:v3.1.0@sha256:6a18787b33aa1c4173982ac5876a62b1cde49307fe1f7a4e86b52f1519134d43"; "$(RUNNER_TEST_IMAGE)")' .score-compose/score.provisioners.yaml
	yq -i '.containers.platform-orchestrator-cp.image = "$(CP_IMAGE)"' .score-compose/score.cp.yaml
	yq -i '.containers.platform-orchestrator-iam.image = "$(IAM_IMAGE)"' .score-compose/score.iam.yaml
	score-compose --quiet generate ../score.yaml --build 'platform-orchestrator-dp=$(BUILD_ARGS)'
	score-compose --quiet generate .score-compose/score.iam.yaml
	score-compose --quiet generate .score-compose/score.cp.yaml
	score-compose --quiet generate --publish $(DATABASE_PORT):postgres.default#platform-orchestrator-dp.postgres.host:5432 --publish $(CP_PORT):platform-orchestrator-cp:8080 --publish $(DP_PORT):platform-orchestrator-dp:8080 --publish $(IAM_PORT):platform-orchestrator-iam:8080
	yq eval-all '. as $$item ireduce ({}; . * $$item)' --inplace compose.yaml compose-mixins.yaml
	yq -i '.services.runner-gateway.image = "$(RUNNER_TEST_IMAGE)"' compose.yaml
	yq -i '(.services[].ports[]? | select(.published == "8080").published) = "$(PUBLIC_PORT)"' compose.yaml

.PHONY: build
build: compose.yaml
	docker compose build --pull

TEST_ENV = RUNNER_TEST_KUBE_CONTEXT="$(CLUSTER_NAME)" PUBLIC_PORT="$(PUBLIC_PORT)" DATABASE_PORT="$(DATABASE_PORT)" CP_PORT="$(CP_PORT)" DP_PORT="$(DP_PORT)" IAM_PORT="$(IAM_PORT)"
RUN_TESTS = $(TEST_ENV) bash -c 'set -euo pipefail; source ./local-test-env.sh; exec "$$@"' --

.PHONY: test
test: compose.yaml
	$(MAKE) test-environment
	docker compose up --no-build -d platform-orchestrator-cp-platform-orchestrator-cp platform-orchestrator-dp-platform-orchestrator-dp runner-gateway --wait
	timeout 60s sh -c 'until curl --fail --silent --output /dev/null http://127.0.0.1:$(CP_PORT)/health && curl --fail --silent --output /dev/null http://127.0.0.1:$(DP_PORT)/health; do sleep 1; done'
	docker network connect $(COMPOSE_PROJECT_NAME)_default $(NAME)-control-plane || true
	$(MAKE) test-model-integration
	$(MAKE) test-outcome-ack-integration
	@$(RUN_TESTS) go tool gotestsum --format testname ./ -test.timeout $(TEST_TIMEOUT) -test.parallel=$(TEST_PARALLEL) -test.v $(TESTARGS)

# Run last: this phase enables real admission authorization instead of the
# legacy internal-system test actor. The normal clean target removes its proxy.
.PHONY: test-historical-removal
test-historical-removal:
	@test -f compose.yaml
	$(MAKE) test-environment
	python3 bootstrap-admission.py --iam-port $(IAM_PORT)
	@$(RUN_TESTS) go tool gotestsum --format testname -- -tags=admissionintegration -count=1 ./ -test.timeout $(TEST_TIMEOUT) -test.run '^TestHistoricalResourceRemoval$$' -test.v

# Requires the matching new CP image; retirement is activated only on this
# fully upgraded disposable fixture, after the normal real-admission phase.
.PHONY: test-provider-retirement
test-provider-retirement:
	@test -f compose.yaml
	$(MAKE) test-environment
	python3 bootstrap-admission.py --iam-port $(IAM_PORT) --provider-retirement
	@$(RUN_TESTS) go tool gotestsum --format testname -- -tags=admissionintegration -count=1 ./ -test.timeout $(TEST_TIMEOUT) -test.run '^TestProviderRetirementLifecycle$$' -test.v

# Exercise the actual Score-to-process boundary before starting the test suite.
.PHONY: test-environment
test-environment:
	@$(RUN_TESTS) python3 check-test-env.py

.PHONY: test-model-integration
test-model-integration:
	@MODEL_TEST_DB_CONNECTION_STRING="$(shell score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' -f 'host=localhost port=$(DATABASE_PORT) dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable')" go test -tags=integration ../internal/model -count=1 -timeout=60s

.PHONY: test-outcome-ack-integration
test-outcome-ack-integration:
	@MODEL_TEST_DB_CONNECTION_STRING="$(shell score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' -f 'host=localhost port=$(DATABASE_PORT) dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable')" OUTCOME_ACK_TEST_NATS_URL="nats://localhost:$(NATS_PORT)" go test -tags=integration ../internal/api -run '^TestOutcomeAcknowledgement' -count=1 -timeout=60s

.PHONY: clean
clean:
	if [ -f compose.yaml ]; then docker compose down -v --remove-orphans; fi
	rm -rfv .score-compose
	rm -f compose.yaml
	kind delete cluster --name $(NAME) --kubeconfig $(KUBECONFIG)

.PHONY: test-integration-logs
test-integration-logs:
	@if [ -f .score-compose/admission/compose.json ]; then docker compose -f compose.yaml -f .score-compose/admission/compose.json logs; else docker compose logs; fi

.PHONY: connect-to-integ-db
connect-to-integ-db:
	@score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' \
		--format 'Command: docker run -it --network integration-tests_default --rm postgres:16-alpine@sha256:721873c34ceb9f8d8fc265984940dc982404c105f19ad51be9fdc5970a6080ea psql -h {{ .host }} -U {{ .username }} --dbname {{ .name }}'
	@score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' --format 'Password: {{ .password }}'

.PHONY: prepare-tests
prepare-tests: create-cluster write-cluster-config configure-cluster

## create a test cluster
.PHONY: create-cluster
create-cluster:
	docker version
	kind version
	kind create cluster --name "$(NAME)" --image "$(KIND_NODE_IMAGE)" $(KIND_CREATE_ARGS) --wait 5m --kubeconfig "$(KUBECONFIG)"


## create kubeconfig to access runner cluster
.PHONY: write-cluster-config
write-cluster-config:
	kind export kubeconfig --name $(NAME) --kubeconfig $(KUBECONFIG)
	kind export kubeconfig --name $(NAME) --internal --kubeconfig $(KUBECONFIG_INTERNAL)

.PHONY: configure-cluster
configure-cluster:
	$(KUBECTL) delete namespace $(DEFAULT_K8S_RES) --ignore-not-found
	$(KUBECTL) create namespace $(DEFAULT_K8S_RES)
	$(KUBECTL) apply -f runner-cluster/config -R
	@printf '{"cluster_data":{"certificate_authority_data": "%s", "server": "%s"},"auth":{"client_certificate_data": "%s", "client_key_data": "%s"}}\n' "$$($(KUBECTL_INTERNAL) config view -o jsonpath='{.clusters[?(@.name == "$(CLUSTER_NAME)")].cluster.certificate-authority-data}' --raw )" "$$($(KUBECTL_INTERNAL) config view -o jsonpath='{.clusters[?(@.name == "$(CLUSTER_NAME)")].cluster.server}' --raw )" "$$( $(KUBECTL_INTERNAL) config view -o jsonpath='{.users[?(@.name == "$(CLUSTER_NAME)")].user.client-certificate-data}' --raw )" "$$( $(KUBECTL_INTERNAL) config view -o jsonpath='{.users[?(@.name == "$(CLUSTER_NAME)")].user.client-key-data}' --raw )" > $(RUNNER_CONFIGURATION)
	if [ "$(RUNNER_IMAGE_PULL)" = "true" ]; then \
		printf 'FROM %s\n' "$(RUNNER_IMAGE)" | docker buildx build --pull --load --provenance=false --platform "linux/$$(docker version --format '{{.Server.Arch}}')" --tag "$(RUNNER_TEST_IMAGE)" -; \
	fi
	kind load docker-image --name $(NAME) "$(RUNNER_TEST_IMAGE)"
