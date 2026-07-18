terraform {
  required_version = ">= 1.9"

  backend "gcs" {
    bucket = "vigilancerx-502702-tf-state"
    prefix = "platform"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    redpanda = {
      source  = "redpanda-data/redpanda"
      version = "~> 1.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  user_project_override = true
  billing_project = var.project_id
}

provider "redpanda" {}