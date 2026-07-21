Kubernetes objects for the RxVigilance workload namespace. Applied with                                                                                                       
`kubectl` (Phase 10 moves the applying into the deploy pipeline). The                                                                                                         
platform itself (GKE cluster, Helm releases) lives in `infra/terraform/`                                                                                                      
and is NOT managed from this directory.

| File | What |                                                                                                                                                               
  |---|---|                                                                                                                                                                     
| `namespace.yaml` | `rx-vigilance` namespace — all workload objects live here |                                                                                              
| `flink/flink-serviceaccount.yaml` | KSA `flink` with the Workload Identity annotation → GSA `rx-vigilance-sa` (GCS checkpoint access, no key files) |                       
| `flink/flink-deployment.yaml` | FlinkDeployment CR (arrives in Phase 2) |
| `flink/flink-rbac.yaml` | Role + RoleBinding — `flink` KSA's Kubernetes API permissions (separate from Workload Identity) |                                                                                                   

Apply order (namespace first — everything else is namespaced):

      kubectl apply -f k8s/namespace.yaml                                                                                                                                       
      kubectl apply -f k8s/flink/flink-serviceaccount.yaml 

## Kafka credentials Secret (manual — never committed)

The Flink job authenticates to Redpanda Cloud with SASL/SCRAM as the                                                                                                          
`rx-vigilance-flink` user. Those credentials exist in exactly two places:                                                                                                     
Redpanda Cloud, and `~/.redpanda-cloud.env` on the operator's machine                                                                                                         
(outside this repo). They are never written to git, Terraform state, or                                                                                                       
any manifest — this Secret is created imperatively:

      source ~/.redpanda-cloud.env                                                                                                                                              
      kubectl create secret generic kafka-credentials \                                                                                                                         
        --namespace rx-vigilance \                                                                                                                                              
        --from-literal=sasl-username=rx-vigilance-flink \                                                                                                                       
        --from-literal=sasl-password=<REDPANDA_PASSWORD> 

(In practice the password comes from `$TF_VAR_redpanda_flink_password`                                                                                                        
in the env file — shown as a placeholder here on purpose.)

Name contract (frozen; consumed by `flink-deployment.yaml` in Phase 2):                                                                                                       
Secret `kafka-credentials`, keys `sasl-username`, `sasl-password`.

Because the runtime cluster is disposable (D8), this Secret dies with                                                                                                         
every `terraform destroy` and must be recreated on every fresh cluster —                                                                                                      
`make infra-up` (#23) automates exactly that from the same env vars.

Verify without printing values:

      kubectl describe secret kafka-credentials -n rx-vigilance                                                                                                                 

## Notes

- A Secret's values are base64-encoded, not encrypted — protection is                                                                                                         
  RBAC + namespace scoping + etcd encryption at rest, not the format.
- The Workload Identity pair `[rx-vigilance/flink]` and the Secret/key                                                                                                        
  names above are name-frozen against IAM bindings and Phase 2 manifests;                                                                                                     
  renaming any of them is a coordinated change, not a cleanup.  
## FlinkDeployment gotchas (found during #40/#41)

- **Manual image builds must target the cluster's architecture explicitly.**
  Apple Silicon (arm64) Macs default `docker build` to arm64; GKE's
  e2-standard-4 nodes are amd64. Always: `docker build --platform linux/amd64 ...`.
  Phase 10's CI builds won't hit this (GitHub Actions runners are amd64).
- **The runtime image's JDK must match the compiled bytecode.** `flink:1.18`
  defaults to JDK 11; this project compiles to Java 17 (`maven.compiler.release=17`).
  Use `flink:1.18-java17` — mismatch fails at startup with
  `UnsupportedClassVersionError`, not at build time.
- **Flink's native Kubernetes mode needs its own K8s RBAC**, separate from
  Workload Identity. WI only grants *GCP* API access (GCS); the JobManager
  also calls the *Kubernetes* API directly to create/watch TaskManager pods
  and read its own Deployment (for owner references) — see
  `k8s/flink/flink-rbac.yaml` (Role: pods/configmaps/services/endpoints +
  apps/deployments, bound to the `flink` ServiceAccount).
- **Don't share one `podTemplate` across `jobManager`/`taskManager`** — the
  operator's merge path for a shared top-level template corrupted the
  TaskManager pod's `kind` field (`"pod"` lowercase → rejected by the API
  server) even though the source YAML was correct. Fix: define
  `podTemplate` separately under `jobManager:` and `taskManager:`; use a
  YAML anchor (`&podTemplate` / `*podTemplate`) to avoid duplicating the
  identical content.
- **Kafka SASL and Schema Registry auth are two separate configs.** SASL
  properties on `KafkaSource` only authenticate the Kafka *broker*
  connection. The Confluent Avro deserializer talks to the registry over
  plain HTTP and needs its *own* basic-auth config
  (`basic.auth.credentials.source=USER_INFO` +
  `schema.registry.basic.auth.user.info=<user>:<pass>`) — omitting it fails
  late and confusingly (`Could not find schema with id N` →
  `Unauthorized`), well after the Kafka connection has already succeeded.
- **No custom TLS truststore needed.** Redpanda Cloud's certificate chains
  to a public CA; the JVM's default `cacerts` validates it with zero extra
  config on either the Kafka or registry connection.
- **Testing against the deployed job needs a separate producer identity.**
  The job's own `rx-vigilance-flink` user is correctly READ-only on its
  source topics (#20) — it can't produce test events into `rx-fill-events`
  itself, nor should it be able to. Use `rx-vigilance-test-producer`
  (Terraform, D15) for manual/test event injection instead of widening the
  job's own ACLs.
