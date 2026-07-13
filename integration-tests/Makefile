KUBECONFIG := runner-cluster/kubeconfig.yaml
KUBECONFIG_INTERNAL := runner-cluster/kubeconfig_internal.yaml
KUBECTL := kubectl --kubeconfig $(KUBECONFIG)
KUBECTL_INTERNAL := kubectl --kubeconfig $(KUBECONFIG_INTERNAL)
CLUSTER_NAME := kind-runner
NAME := runner
DEFAULT_K8S_RES := platform-orchestrator-runner
RUNNER_CONFIGURATION := runner_config.json
RUNNER_IMAGE := ghcr.io/stellwerk-labs/platform-orchestrator-runner:v1.0.0

compose.yaml: BUILD_ARGS := {"context":"../"}
compose.yaml: ../score.yaml Makefile compose-mixins.yaml score.provisioners.yaml
	score-compose check-version '>=0.16.0'
	score-compose init --no-sample
	cp score.provisioners.yaml .score-compose/
	yq -i '(.[] | select(.uri == "template://env").outputs) |= sub("ghcr.io/stellwerk-labs/platform-orchestrator-runner:v1.0.0"; "$(RUNNER_IMAGE)")' .score-compose/score.provisioners.yaml
	score-compose generate ../score.yaml --build 'platform-orchestrator-dp=$(BUILD_ARGS)'
	score-compose generate ./score.iam.yaml
	score-compose generate ./score.cp.yaml
	score-compose generate --publish 5432:postgres.default#platform-orchestrator-dp.postgres.host:5432 --publish 8081:platform-orchestrator-cp:8080 --publish 8082:platform-orchestrator-dp:8080 --publish 8083:platform-orchestrator-iam:8080
	yq eval-all '. as $$item ireduce ({}; . * $$item)' --inplace compose.yaml compose-mixins.yaml

.PHONY: build
build: compose.yaml
	docker compose build --pull

.PHONY: test
test: compose.yaml
	docker compose up --no-build -d platform-orchestrator-cp-platform-orchestrator-cp platform-orchestrator-dp-platform-orchestrator-dp  --wait
	docker network connect integration-tests_default runner-control-plane || true
	SERVER_URL="http://$(shell score-compose resources get-outputs dns.default\#api-dns | jq -r .host):8080" OIDC_SERVER_URL="http://$(shell score-compose resources get-outputs dns.default\#platform-orchestrator-dp.dns-oidc | jq -r .host):8080" INTERNAL_CP_URL=http://localhost:8081 INTERNAL_DP_URL=http://localhost:8082 INTERNAL_IAM_URL=http://localhost:8083 DB_CONNECTION_STRING="$(shell score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' -f 'host=localhost port=5432 dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable')" AMQP_CONNECTION_STRING=$(shell score-compose resources get-outputs 'amqp.default#rabbitmq' -f 'amqp://{{ .username }}:{{ .password }}@localhost:5672/{{ .vhost }}') RUNNER_TOKEN_SALT="vnXd0Ses8L1X0TGGcp+mf/V8dQs+L4/fWvijORwb7lI=" TEST_USER_IDENTITY_RECIPIENT=age10hmkvsqfxr5ha05ay6pqj9zdtal7pw7x2r3t4n00rzucc3en3u9q3cycqm go tool gotestsum --format testname ./ -test.timeout 10m -test.parallel=2 -test.v $(TESTARGS)

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
	docker pull $(RUNNER_IMAGE)
	kind load docker-image --name ${NAME} $(RUNNER_IMAGE)
