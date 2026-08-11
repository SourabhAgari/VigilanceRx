.PHONY: up down bootstrap build test run infra-up infra-down infra-verify

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
# ESO app version. MUST match the appVersion of the chart version pinned in
# infra/terraform/runtime/helm.tf (helm_release.external_secrets). Chart 2.9.0
# has appVersion v2.9.0 — these are separate numbers that happen to match.
# Verify with: helm show chart external-secrets --repo https://charts.external-secrets.io --version <chart>
ESO_VERSION := v2.9.0
ESO_CRDS := https://raw.githubusercontent.com/external-secrets/external-secrets/$(ESO_VERSION)/deploy/crds/bundle.yaml

infra-up:
	terraform -chdir=$(TF_RUNTIME) apply -auto-approve \
		-target=google_container_cluster.vigilance-rx \
		-target=google_container_node_pool.primary

	$$(terraform -chdir=$(TF_RUNTIME) output -raw kubeconfig_command)

	kubectl apply --server-side -f $(ESO_CRDS)

	terraform -chdir=$(TF_RUNTIME) apply -auto-approve

	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/argocd/appproject.yaml
	kubectl apply -f k8s/argocd/application.yaml

	$(MAKE) infra-verify

infra-down:
	terraform -chdir=$(TF_RUNTIME) destroy -auto-approve

infra-verify:
	kubectl wait --for=condition=Ready pod --all -n cert-manager --timeout=300s
	kubectl wait --for=condition=Ready pod --all -n flink-system --timeout=300s
	kubectl wait --for=condition=Ready pod --all -n monitoring --timeout=600s
	kubectl wait --for=condition=Ready pod --all -n argocd --timeout=300s

	@kubectl get sa flink -n rx-vigilance \
		-o jsonpath='{.metadata.annotations.iam.gke.io/gcp-service-account}' 2>/dev/null \
		&& echo \
		|| echo "flink SA not present — created by Argo CD from #114 (D54)"

	@kubectl get crd externalsecrets.external-secrets.io >/dev/null

	kubectl wait \
		--for=condition=Ready \
		externalsecret/kafka-credentials \
		-n rx-vigilance \
		--timeout=300s

	@kubectl describe secret kafka-credentials -n rx-vigilance | grep sasl-

	@echo "✔ runtime stack healthy"

