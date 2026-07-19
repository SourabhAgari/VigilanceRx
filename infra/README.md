# `infra/terraform` — Two Stacks, Split by Lifecycle (D8)

The infrastructure is intentionally split into two Terraform stacks with different lifecycles:

| Stack       | Contents                                                                                                                                                                                             | Lifecycle                                                                                                                                                                                                                                                                     |
|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `platform/` | Redpanda Serverless cluster, 7 Kafka topics, service user + ACLs, Schema Registry subjects, GCS checkpoint bucket, Google Service Account (GSA), Workload Identity IAM binding, billing budget alert | **Persistent.** Cheap (or nearly free) while idle. **Never destroy.** Kafka topics, Schema Registry subjects, consumer offsets, IAM resources, and Flink checkpoints must outlive every runtime cluster. Protected with `allow_deletion = false` and `force_destroy = false`. |
| `runtime/`  | GKE cluster (`vigilance-rx-gke`, single-node `e2-standard-4` Spot VM) and Helm releases (`cert-manager`, Flink Kubernetes Operator, `kube-prometheus-stack`)                                         | **Disposable.** Incurs compute costs only while it exists. Destroy when idle to avoid unnecessary GKE charges.                                                                                                                                                                |

## Terraform State

Terraform uses a shared GCS backend.

Bucket:

```text
<project>-tf-state
```

State prefixes:

```text
platform/
runtime/
```

The bucket is created once using:

```bash
scripts/bootstrap-tf-state.sh
```

Terraform state:

- is **never committed to Git**
- is **never edited manually**
- is shared through the remote backend

---

# Prerequisites (Per Shell Session)

Before running Terraform, load the required environment variables:

```bash
source ~/.redpanda-cloud.env
```

This file lives **outside the repository** and contains:

| Variable                         | Purpose                                                                                                          |
|----------------------------------|------------------------------------------------------------------------------------------------------------------|
| `REDPANDA_CLIENT_ID`             | Redpanda Cloud organization service account ID used by the **platform** Terraform stack                          |
| `REDPANDA_CLIENT_SECRET`         | Secret for the Redpanda Cloud organization service account                                                       |
| `TF_VAR_redpanda_flink_password` | SASL password used to create the Kafka service user and later populate the Kubernetes `kafka-credentials` Secret |

Only placeholder values are committed to Git. See `k8s/README.md`.

Authenticate to Google Cloud using Application Default Credentials (ADC):

```bash
gcloud auth login
gcloud auth application-default login
```

---

# Idle-Cost Workflow (D8)

The runtime stack is intentionally disposable.

If you're stepping away for more than about an hour, destroy only the runtime infrastructure:

```bash
make infra-down
```

This destroys:

- the GKE cluster
- all Helm releases
- every Kubernetes object inside the cluster

Because the cluster no longer exists, compute billing stops.

When you're ready to continue working:

```bash
make infra-up
```

This recreates:

- the GKE cluster
- kubeconfig
- all Helm releases
- Kubernetes manifests
- the `kafka-credentials` Secret

and then runs the standard verification steps.

## What Survives `infra-down`

Everything managed by the **platform** stack remains intact:

- Red panda Serverless cluster
- Kafka topics
- Schema Registry subjects
- consumer offsets
- GCS checkpoint bucket
- IAM resources
- Workload Identity configuration
- billing budget

## What Is Destroyed

Everything inside the Kubernetes cluster is deleted, including:

- namespaces
- deployments
- services
- Flink jobs
- ConfigMaps
- the `kafka-credentials` Secret

Since the Secret is deleted with the cluster, `infra-up` recreates it from the environment variables every time.

> **Rule of thumb:** If you're not actively working for more than about one hour, run `make infra-down`.

---

# Sharp Edges (Known and Expected)

These behaviors are expected. Do not spend time debugging them as application bugs.

## 1. First Apply in a Brand-New GCP Project

The platform stack creates a Workload Identity IAM binding using:

```text
google_service_account_iam_member
```

However, the Workload Identity pool:

```text
<project>.svc.id.goog
```

does not exist until the **first GKE cluster** is created with:

```hcl
workload_identity_config {
  workload_pool = "${var.project_id}.svc.id.goog"
}
```

Therefore, in a brand-new project:

1. Create the runtime cluster first.
2. Apply the platform stack afterward.

Otherwise, Terraform fails with an error similar to:

```text
Identity Pool does not exist
```

This happens only once. After any GKE cluster has existed, the Workload Identity pool persists permanently, so normal `infra-down` / `infra-up` cycles never encounter this issue again.

---

## 2. Helm Provider on an Empty Runtime State

The Helm provider derives its Kubernetes connection from the GKE cluster resource.

With an empty Terraform state, those values are unknown during the planning phase, causing Terraform to fail while planning Helm releases.

The workaround is:

```bash
terraform -chdir=runtime apply \
  -target=google_container_cluster.vigilance-rx
```

Then run a normal apply:

```bash
terraform -chdir=runtime apply
```

This behavior is documented in **CLAUDE.md §10**.

It is expected. Do **not** attempt to refactor the Terraform configuration to eliminate it.

---

## 3. Billing Budget API Quota

The Google provider includes:

```hcl
provider "google" {
  user_project_override = true
  billing_project       = "my-billing-project"
}
```

These settings are required when creating `google_billing_budget` resources while authenticated using user Application Default Credentials (ADC).

Removing either setting will cause Terraform applies to fail.

---

# Verification

Run:

```bash
make infra-verify
```

The verification performs the following checks:

- waits for all pods in:
    - `cert-manager`
    - `flink-system`
    - `monitoring`
- verifies the Workload Identity annotation
- verifies that the `kafka-credentials` Secret contains the expected keys

Behavioral end-to-end verification (producing, consuming, checkpoint creation, and checkpoint recovery) is performed later by the **Phase 2 SmokeJob**.