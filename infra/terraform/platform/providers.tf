terraform {
  required_version = ">= 1.9"

  backend "gcs" {
    bucket = "vigilancerx-502702-tf-state"
    prefix = "runtime"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.0"
    }
  }
}