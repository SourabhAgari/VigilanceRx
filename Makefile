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

infra-up:
	terraform -chdir=$(TF_RUNTIME) apply -auto-approve
	$$(terraform -chdir=$(TF_RUNTIME) output -raw kubeconfig_command)
	kubectl apply -f k8s/namespace.yaml
	# flink-serviceaccount.yaml and flink-rbac.yaml are deliberately NOT applied
    # here: k8s/flink/ is owned by Argo CD from #114 (D54). Two things with
    # authority over the same resources is what GitOps exists to remove.
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
                    -o jsonpath='{.metadata.annotations.iam\.gke\.io/gcp-service-account}' 2>/dev/null \
                    && echo \
                    || echo "flink SA not present — created by Argo CD from #114 (D54)"
	@kubectl describe secret kafka-credentials -n rx-vigilance | grep sasl-
	@echo "✔ runtime stack healthy"

