variable "project_id" {
  description = "GCP project that owns all RxVigilance resources"
  type        = string
  default     = "vigilancerx-502702"
}

variable "region" {
  description = "GCP region for regional resources"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the zonal GKE cluster (D7: single zone for cost)"
  type        = string
  default     = "us-central1-a"
}