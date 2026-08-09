Kubernetes objects for the RxVigilance workload namespace. Applied with                                                                                                       
`kubectl` (Phase 10 moves the applying into the deploy pipeline). The                                                                                                         
platform itself (GKE cluster, Helm releases) lives in `infra/terraform/`                                                                                                      
and is NOT managed from this directory.

| File | What |                                                                                                                                                               
  |---|---|                                                                                                                                                                     
| `namespace.yaml` | `rx-vigilance` namespace — all workload objects live here |                                                                                              
| `flink/flink-serviceaccount.yaml` | KSA `flink` with the Workload Identity annotation → GSA `rx-vigilance-sa` (GCS checkpoint access, no key files) |                       
| `flink/flink-deployment.yaml` | FlinkDeployment CR — synced by Argo CD from #114, not applied by hand |
| `flink/flink-rbac.yaml` | Role + RoleBinding — `flink` KSA's Kubernetes API permissions (separate from Workload Identity) |
| `flink/kustomization.yaml` | Kustomize entrypoint for `k8s/flink/` — lets CI set the image tag with `kustomize edit set image` (#103) |
| `flink/secret-store.yaml` | `SecretStore` — points External Secrets Operator at Google Secret Manager; authenticates with the operator's own Workload Identity, no key file |
| `flink/external-secrets.yaml` | `ExternalSecret` for `kafka-credentials` and `ghcr-pull` — references only, never values, so it is safe in git |

Bringing up a fresh cluster:

      make infra-up

No credentials are needed on your machine to bring up a cluster. Both Secrets
are fetched from Google Secret Manager by External Secrets Operator (#118).
`~/.redpanda-cloud.env` is still needed for the platform Terraform stack and
for `CloudEventPublisher`, but not for this.

That runs Terraform, applies the namespace, and creates the Argo CD AppProject
and Application. It does NOT apply anything under `k8s/flink/` — Argo CD owns
that directory from #114 (D54), and two things with authority over the same
resources is exactly what GitOps exists to remove.

The namespace stays outside Argo CD on purpose: the AppProject sets
`clusterResourceWhitelist: []`, so Argo CD cannot create cluster-scoped
resources. It operates inside the namespace, so it must not be able to create
the namespace.

## Secrets — produced by External Secrets Operator (#118, D56)

Neither Secret is created by hand any more. External Secrets Operator reads
the values from Google Secret Manager and writes the Kubernetes Secrets.

| Kubernetes Secret | Keys | Source |
|---|---|---|
| `kafka-credentials` | `sasl-username`, `sasl-password` | username is a literal in the manifest; password from Secret Manager `rx-vigilance-redpanda-flink-password` |
| `ghcr-pull` | `.dockerconfigjson` | built by the manifest template from Secret Manager `rx-vigilance-ghcr-token` |

**Why there is no credential to protect.** The operator pod proves its identity
to Google using Workload Identity and receives a token that expires within the
hour. No key file, password or token is stored on any machine, in git, or in
Terraform state. That was the point: every other option (Vault, Sealed Secrets,
SOPS) needs a root key kept somewhere.

Only two values are actually secret. The usernames — `rx-vigilance-flink` and
the GHCR account — are not, and stay as literals in the manifest.

**Name contract (frozen; consumed by `flink-deployment.yaml` since Phase 2):**
Secret `kafka-credentials`, keys `sasl-username`, `sasl-password`. Only the
mechanism that produces it changed.

### Rotating a value

Add a new version in Secret Manager, then either wait for the refresh interval
(1 hour) or force it:

      printf '%s' "<new value>" \
        | gcloud secrets versions add rx-vigilance-redpanda-flink-password \
            --project vigilancerx-502702 --data-file=-

      kubectl annotate externalsecret kafka-credentials -n rx-vigilance \
        force-sync=$(date +%s) --overwrite

Use `printf`, not `echo` — `echo` appends a newline, it becomes part of the
stored password, and authentication then fails in a way that is hard to
diagnose.

Rotating the Redpanda password also means bumping `password_wo_version` in
`redpanda.tf`, since Redpanda holds the other copy. The GHCR token is simply
regenerated on GitHub; a PAT is never shown twice, but making a new one costs
nothing.

### Verifying

      kubectl get secretstore -n rx-vigilance      # STATUS Valid, READY True
      kubectl get externalsecret -n rx-vigilance   # STATUS SecretSynced
      kubectl get secret -n rx-vigilance           # both Secrets present

`SecretStore` reporting `Valid` is the real proof that Workload Identity works —
nothing else exercises the impersonation.

### On a brand-new cluster

Order is: Terraform installs the operator, then Argo CD syncs the store and the
ExternalSecrets, then the Secrets appear, then the Flink pods can start. If Argo
applies the FlinkDeployment first you will briefly see `ImagePullBackOff` or
`CreateContainerConfigError`. Both retry and clear once the Secrets arrive.
Alarming on a first run; not a fault.

## Notes

- A Secret's values are base64-encoded, not encrypted — protection is                                                                                                         
  RBAC + namespace scoping + etcd encryption at rest, not the format.
- The Workload Identity pair `[rx-vigilance/flink]` and the Secret/key                                                                                                        
  names above are name-frozen against IAM bindings and Phase 2 manifests;                                                                                                     
  renaming any of them is a coordinated change, not a cleanup.
- The same applies to `[external-secrets/external-secrets]`, the operator's
  own pair. The IAM binding in `platform/secrets.tf` names it literally, which
  is why `serviceAccount.name` is pinned in the chart values rather than left
  to the chart's generated name.

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
