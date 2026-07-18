resource "google_storage_bucket" "checkpoints" {
  name = "${var.project_id}-rx-vigilance-ckpt"
  project = var.project_id
  location     = var.region

  uniform_bucket_level_access = true

  # force_destroy stays false (default): a non-empty checkpoint bucket
  # must refuse deletion — checkpoints outlive the runtime stack (D8).
}

resource "google_service_account" "flink" {
  project = var.project_id
  account_id = "rx-vigilance-sa"
  display_name = "RxVigilance Flink chekpoint writer (workload identity)"
}

resource "google_storage_bucket_iam_member" "ckpt_writer" {
  bucket = google_storage_bucket.checkpoints.name
  member = "serviceAccount:${google_service_account.flink.email}"
  role   = "roles/storage.objectAdmin"
}

resource "google_service_account_iam_member" "flink_workload_identity" {
  // member represents the kubernetes service account
  member             = "serviceAccount:${var.project_id}.svc.id.goog[rxvigilance/flink]"
  role               = "roles/iam.workloadIdentityUser" // some one is allowed to impersonate this GSA
  service_account_id = google_service_account.flink.name
}
