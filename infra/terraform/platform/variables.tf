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
