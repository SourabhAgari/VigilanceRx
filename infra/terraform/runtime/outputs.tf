output "cluster_name" {
  description = "GKE Cluster name"
  value = google_container_cluster.vigilance-rx.name
}

output "cluster_zone" {
  description = "zone of the zonal cluster"
  value = google_container_cluster.vigilance-rx.location
}

output "kubeconfig_command" {
  description = "Run after infra-up to point kubectl at the cluster (#23 wrapper uses this)"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.vigilance-rx.name} --zone ${google_container_cluster.vigilance-rx.location} --project ${var.project_id}"
}