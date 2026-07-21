.PHONY: up down bootstrap build test run infra-up infra-down infra-verify check-env

up:            ## start local Redpanda (broker + schema registry)
	docker-compose up -d

down:
	docker-compose down

bootstrap:     ## create topics + register schemas
	./scripts/bootstrap-local-topics.sh

build:
	mvn clean verify

test:
	mvn test

run:
	 mvn exec:java -Dexec.mainClass="com.healthcare.rxvigilance.AdherenceJob" \
              -Dexec.args="--kafka.brokers=localhost:9092 \
                           --schema.registry.url=http://localhost:8081 \
                           --checkpoint.dir=file:///tmp/rx-vigilance-checkpoints"
push:
	git push origin HEAD

fetch:
	git fetch origin


# terraform run time related
TF_RUNTIME := infra/terraform/runtime

check-env:
	@test -n "$$TF_VAR_redpanda_flink_password" || \
    	{ echo "ERROR: Kafka password not in env — run: source ~/.redpanda-cloud.env"; exit 1; }
	@test -n "$$GHCR_READ_TOKEN" || \
            { echo "ERROR: GHCR read token not in env — run: source ~/.redpanda-cloud.env"; exit 1; }

infra-up: check-env
	terraform -chdir=$(TF_RUNTIME) apply -auto-approve
	$$(terraform -chdir=$(TF_RUNTIME) output -raw kubeconfig_command)
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/flink/flink-serviceaccount.yaml
	kubectl create secret generic kafka-credentials \
		--namespace rx-vigilance \
        --from-literal=sasl-username=rx-vigilance-flink \
        --from-literal=sasl-password="$$TF_VAR_redpanda_flink_password" \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl create secret docker-registry ghcr-pull \
		--namespace rx-vigilance \
		--docker-server=ghcr.io \
		--docker-username=sourabhragari \
		--docker-password="$$GHCR_READ_TOKEN" \
		--docker-email=agarisra@gmail.com \
		--dry-run=client -o yaml | kubectl apply -f -
	$(MAKE) infra-verify

infra-down:
	terraform -chdir=$(TF_RUNTIME) destroy -auto-approve

infra-verify:
	kubectl wait --for=condition=Ready pod --all -n cert-manager --timeout=300s
	kubectl wait --for=condition=Ready pod --all -n flink-system --timeout=300s
	kubectl wait --for=condition=Ready pod --all -n monitoring --timeout=600s
	@kubectl get sa flink -n rx-vigilance \
		-o jsonpath='{.metadata.annotations.iam\.gke\.io/gcp-service-account}'; echo
	@kubectl describe secret kafka-credentials -n rx-vigilance | grep sasl-
	@echo "✔ runtime stack healthy"

