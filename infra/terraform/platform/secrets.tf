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
  project   = var.project_id
  secret_id = "rx-vigilance-redpanda-flink-password"
  deletion_protection = true

  # auto = Google picks the regions. The alternative is naming them explicitly,
  # which matters for data residency rules we do not have.
  replication {
    auto {}
  }

  depends_on = [google_project_service.secret_manager]
}

resource "google_secret_manager_secret" "ghcr_read_token" {
  project   = var.project_id
  secret_id = "rx-vigilance-ghcr-token"
  deletion_protection = true

  replication {
    auto {}
  }

  depends_on = [google_project_service.secret_manager]
}
