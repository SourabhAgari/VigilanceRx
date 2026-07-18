output "checkpoint_bucket" {
  description = "GCS bucket for flink checkpoints/savepoints"
  value = google_storage_bucket.checkpoints.name
}

output "checkpoint_uri" {
  description = "checkpoint base uri as consumed by the flink job (spec : state and checkpointing)"
  value = "gs://${google_storage_bucket.checkpoints.name}/tx-vigilance-ckpt"
}

output "flink_gsa_email" {
  description = "GSA email - goes into KSA iam"
  value = google_service_account.flink.email
}