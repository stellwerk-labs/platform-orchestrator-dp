KUBECONFIG := runner-cluster/kubeconfig.yaml
KUBECONFIG_INTERNAL := runner-cluster/kubeconfig_internal.yaml
KUBECTL := kubectl --kubeconfig $(KUBECONFIG)
KUBECTL_INTERNAL := kubectl --kubeconfig $(KUBECONFIG_INTERNAL)
NAME ?= runner
CLUSTER_NAME := kind-$(NAME)
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
CP_IMAGE ?= ghcr.io/stellwerk-labs/platform-orchestrator-cp:v2.0.0
IAM_IMAGE ?= ghcr.io/stellwerk-labs/platform-orchestrator-iam:v2.2.0
RUNNER_IMAGE ?= ghcr.io/stellwerk-labs/platform-orchestrator-runner:v3.0.0
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
	yq -i '(.[] | select(.uri == "template://env").outputs) |= sub("ghcr.io/stellwerk-labs/platform-orchestrator-runner:v3.0.0"; "$(RUNNER_IMAGE)")' .score-compose/score.provisioners.yaml
	yq -i '.containers.platform-orchestrator-cp.image = "$(CP_IMAGE)"' .score-compose/score.cp.yaml
	yq -i '.containers.platform-orchestrator-iam.image = "$(IAM_IMAGE)"' .score-compose/score.iam.yaml
	score-compose --quiet generate ../score.yaml --build 'platform-orchestrator-dp=$(BUILD_ARGS)'
	score-compose --quiet generate .score-compose/score.iam.yaml
	score-compose --quiet generate .score-compose/score.cp.yaml
	score-compose --quiet generate --publish $(DATABASE_PORT):postgres.default#platform-orchestrator-dp.postgres.host:5432 --publish $(CP_PORT):platform-orchestrator-cp:8080 --publish $(DP_PORT):platform-orchestrator-dp:8080 --publish $(IAM_PORT):platform-orchestrator-iam:8080
	yq eval-all '. as $$item ireduce ({}; . * $$item)' --inplace compose.yaml compose-mixins.yaml
	yq -i '.services.runner-gateway.image = "$(RUNNER_IMAGE)"' compose.yaml
	yq -i '(.services[].ports[]? | select(.published == "8080").published) = "$(PUBLIC_PORT)"' compose.yaml

.PHONY: build
build: compose.yaml
	docker compose build --pull

.PHONY: test
test: compose.yaml
	docker compose up --no-build -d platform-orchestrator-cp-platform-orchestrator-cp platform-orchestrator-dp-platform-orchestrator-dp runner-gateway --wait
	docker network connect $(COMPOSE_PROJECT_NAME)_default $(NAME)-control-plane || true
	$(MAKE) test-model-integration
	@SERVER_URL="http://$(shell score-compose resources get-outputs dns.default\#api-dns | jq -r .host):$(PUBLIC_PORT)" OIDC_SERVER_URL="http://$(shell score-compose resources get-outputs dns.default\#platform-orchestrator-dp.dns-oidc | jq -r .host):$(PUBLIC_PORT)" INTERNAL_CP_URL=http://localhost:$(CP_PORT) INTERNAL_DP_URL=http://localhost:$(DP_PORT) INTERNAL_IAM_URL=http://localhost:$(IAM_PORT) DB_CONNECTION_STRING="$(shell score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' -f 'host=localhost port=$(DATABASE_PORT) dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable')" CP_DB_CONNECTION_STRING="$(shell score-compose resources get-outputs 'postgres.default#platform-orchestrator-cp.postgres' -f 'host=localhost port=$(DATABASE_PORT) dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable')" NATS_URL=nats://localhost:$(NATS_PORT) RUNNER_TOKEN_SALT="vnXd0Ses8L1X0TGGcp+mf/V8dQs+L4/fWvijORwb7lI=" TEST_USER_IDENTITY_RECIPIENT=age10hmkvsqfxr5ha05ay6pqj9zdtal7pw7x2r3t4n00rzucc3en3u9q3cycqm go tool gotestsum --format testname ./ -test.timeout $(TEST_TIMEOUT) -test.parallel=$(TEST_PARALLEL) -test.v $(TESTARGS)

.PHONY: test-model-integration
test-model-integration:
	@MODEL_TEST_DB_CONNECTION_STRING="$(shell score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' -f 'host=localhost port=$(DATABASE_PORT) dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable')" go test -tags=integration ../internal/model -count=1 -timeout=60s

.PHONY: clean
clean:
	if [ -f compose.yaml ]; then docker compose down -v --remove-orphans; fi
	rm -rfv .score-compose
	rm -f compose.yaml
	kind delete cluster --name $(NAME) --kubeconfig $(KUBECONFIG)

.PHONY: test-integration-logs
test-integration-logs:
	docker compose logs

.PHONY: connect-to-integ-db
connect-to-integ-db:
	@score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' \
		--format 'Command: docker run -it --network integration-tests_default --rm postgres:16-alpine psql -h {{ .host }} -U {{ .username }} --dbname {{ .name }}'
	@score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' --format 'Password: {{ .password }}'

.PHONY: prepare-tests
prepare-tests: create-cluster write-cluster-config configure-cluster

## create a test cluster
.PHONY: create-cluster
create-cluster:
	docker version
	kind version
	kind create cluster --name $(NAME) --wait 5m --kubeconfig $(KUBECONFIG)


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
		printf 'FROM %s\n' "$(RUNNER_IMAGE)" | docker buildx build --pull --load --provenance=false --platform "linux/$$(docker version --format '{{.Server.Arch}}')" --tag "$(RUNNER_IMAGE)" -; \
	fi
	kind load docker-image --name $(NAME) "$(RUNNER_IMAGE)"
