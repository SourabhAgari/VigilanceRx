# Grafana reads Google Cloud Logging so log panels can exist at all (#181).
# Nothing here grants write: a dashboard needs to read logs and nothing more.
#
# Third instance of the Workload Identity pattern in this stack, after
# google_service_account.flink (gcs.tf) and .external_secrets (secrets.tf).
# No key file is created, so there is nothing to download and nothing to leak.
resource "google_service_account" "grafana" {
  project      = var.project_id
  account_id   = "rx-vigilance-grafana"
  display_name = "Grafana Cloud Logging reader (workload identity)"
}

# Project-scoped, unlike the ESO grants next door which are per-secret.
# Cloud Logging has no per-namespace scoping without log views and a bucket
# split — that is deliberately a separate piece of work (D79), so this is
# read-everything-in-the-project for now, and read is all it is.
resource "google_project_iam_member" "grafana_logging_viewer" {
  project = var.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.grafana.email}"
}

# The Kubernetes account monitoring/kube-prometheus-stack-grafana may
# impersonate the Google account above. The string in brackets is
# <namespace>/<service account name> and must match exactly — a typo produces
# a pod that authenticates as nobody, with no error at apply time.
resource "google_service_account_iam_member" "grafana_workload_identity" {
  service_account_id = google_service_account.grafana.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[monitoring/kube-prometheus-stack-grafana]"
}