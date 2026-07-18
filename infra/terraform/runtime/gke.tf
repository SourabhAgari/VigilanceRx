resource "google_container_cluster" "vigilance-rx" {
  name     = "vigilance-rx-gke"
  project  = var.project_id
  location = var.zone # zonal, not regional: one control plane, one zone (D7 cost)

  # GKE requires a default pool at creation, drop it immediately and
  # manage the real pool as a separate resource below
  remove_default_node_pool = true
  initial_node_count       = 1

  # This stacks whole purpose is destroy when idle; the providers
  # default of true would make terraform destroy fail
  deletion_protection = false

  # Enable workload identity for this cluster
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
}

resource "google_container_node_pool" "primary" {
  name       = "rx-vigilance-pool"
  project    = var.project_id
  cluster    = google_container_cluster.vigilance-rx.name
  location   = var.zone
  node_count = 1 # D7: single node; ~13.3 GB Allocatable holds the full stack

  node_config {
    machine_type = "e2-standard-4"
    spot         = true # D7: ~70% cheaper; preemption OK on self-healing demo
    disk_size_gb = 50   # default is 100 GB; 50 is plenty and halves disk cost


    # Broad scope is fine: actual permissions come from Workload Identity
    # IAM, not scopes. Narrow scopes would only add a second thing to debug.
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA" # per-pod WI tokens instead of node SA
    }

  }
}
