output "checkpoint_bucket" {
  description = "GCS bucket for flink checkpoints/savepoints"
  value = google_storage_bucket.checkpoints.name
}

output "checkpoint_uri" {
  description = "checkpoint base uri as consumed by the flink job (spec : state and checkpointing)"
  value = "gs://${google_storage_bucket.checkpoints.name}/rx-vigilance-ckpt"
}

output "flink_gsa_email" {
  description = "GSA email - goes into KSA iam"
  value = google_service_account.flink.email
}

output "redpanda_cluster_id" {
  description = "Serverless cluster ID (rpk cloud profile, later ACL/schema work)"
  value       = redpanda_serverless_cluster.main.id
}

output "kafka_brokers" {
  description = "Bootstrap servers for kafka.brokers (Phase 2 config, #22 Secret)"
  value       = join(",", redpanda_serverless_cluster.main.kafka_api.seed_brokers)
}

output "schema_registry_url" {
  description = "Schema registry URL for schema.registry.url (Phase 2 config)"
  value       = redpanda_serverless_cluster.main.schema_registry.url
}

output "kafka_sasl_username" {
  description = "SASL username for the Flink job — password stays in env/K8s Secret only, never in outputs or state"
  value       = redpanda_user.flink.name
}