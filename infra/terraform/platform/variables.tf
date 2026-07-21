variable "project_id" {
  description = "GCP project that owns all RxVigilance resources"
  type        = string
  default     = "vigilancerx-502702"
}

variable "region" {
  description = "GCP region for regional resources (GCS buckets)"
  type        = string
  default     = "us-central1"
}

variable "billing_account_id" {
  description = "Billing account the D7 budget alert attaches to (set in terraform.tfvars, never committed)"
  type        = string
}

variable "redpanda_serverless_region" {
  description = "Serverless region(AWS backed; GCP serverless is beta gated). Confirm the exact string in console's create-cluster dropdown"
  type = string
  default = "us-east-1"
}

variable "redpanda_flink_password" {
  description = "SASL password for the Flink Kafka user (env: TF_VAR_redpanda_flink_password; write-only, never in state)"
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "redpanda_test_producer_password" {
  description = "SASL password for the test-only producer identity (manual smoke-test event injection, never used by the deployed job)"
  type        = string
  sensitive   = true
  ephemeral   = true
}
