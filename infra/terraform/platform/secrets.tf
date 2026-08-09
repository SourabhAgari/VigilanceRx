# Secret Manager holds the two values that are genuinely secret. The usernames
# that go with them are not secret and stay in the manifests as literals.
#
# NOTE: there are deliberately no google_secret_manager_secret_version resources
# here. A version carries the actual value, and Terraform writes every managed
# value into its state file in plain text (§9). Terraform creates the empty
# container; the value is added out of band with gcloud (see infra README).

resource "google_project_service" "secret_manager" {
  project = var.project_id
  service = "secretmanager.googleapis.com"

  # Do not turn the API back off on destroy: it is a project-level setting and
  # other things may come to depend on it.
  disable_on_destroy = false
}

resource "google_secret_manager_secret" "redpanda_flink_password" {
  project             = var.project_id
  secret_id           = "rx-vigilance-redpanda-flink-password"
  deletion_protection = true

  # auto = Google picks the regions. The alternative is naming them explicitly,
  # which matters for data residency rules we do not have.
  replication {
    auto {}
  }

  depends_on = [google_project_service.secret_manager]
}

resource "google_secret_manager_secret" "ghcr_read_token" {
  project             = var.project_id
  secret_id           = "rx-vigilance-ghcr-token"
  deletion_protection = true

  replication {
    auto {}
  }

  depends_on = [google_project_service.secret_manager]
}

# The identity External Secrets Operator runs as. It has no key file - nothing
# to download, nothing to leak. The binding at the bottom is what lets the
# operator's Kubernetes account borrow this identity.
resource "google_service_account" "external_secrets" {
  project      = var.project_id
  account_id   = "rx-vigilance-eso"
  display_name = "External Secrets Operator (workload identity)"
}

# Access granted per secret, not project-wide: the operator can read these two
# and nothing else that may be added to the project later.
resource "google_secret_manager_secret_iam_member" "eso_redpanda_password" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.redpanda_flink_password.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.external_secrets.email}"
}

resource "google_secret_manager_secret_iam_member" "eso_ghcr_token" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.ghcr_read_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.external_secrets.email}"
}

# Workload Identity: the Kubernetes account external-secrets/external-secrets
# may impersonate the Google account above. The name in brackets is
# [namespace/serviceaccount] and must match the chart exactly - step 3 pins
# serviceAccount.name so this cannot drift.
resource "google_service_account_iam_member" "external_secrets_workload_identity" {
  service_account_id = google_service_account.external_secrets.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[external-secrets/external-secrets]"
}
