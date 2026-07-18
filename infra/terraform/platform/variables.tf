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
