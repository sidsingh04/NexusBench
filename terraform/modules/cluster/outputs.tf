# modules/cluster/outputs.tf
#
# These outputs are the contract between the cluster module and its callers.
# The root main.tf and the node-pools and registry modules consume them.
# Do not remove or rename outputs without updating every consumer.

output "cluster_name" {
  description = "Name of the created GKE cluster."
  value       = google_container_cluster.nexusbench.name
}

output "endpoint" {
  description = "Internal IP address of the GKE API server."
  value       = google_container_cluster.nexusbench.endpoint
  sensitive   = true
}

output "ca_certificate" {
  description = "Base64-encoded cluster CA certificate."
  value       = google_container_cluster.nexusbench.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

output "node_service_account_email" {
  description = <<-EOT
    Email of the GCP service account used by GKE nodes.
    The registry module grants this SA image-pull access to Artifact Registry.
  EOT
  value       = google_service_account.gke_nodes.email
}

output "workload_identity_pool" {
  description = <<-EOT
    Workload Identity Pool resource name.
    Format: projects/<number>/locations/global/workloadIdentityPools/<pool-id>
    Used by GitHub Actions OIDC federation in Stage 4.4.
  EOT
  value       = google_iam_workload_identity_pool.github.name
}

output "workload_identity_provider" {
  description = "Workload Identity Pool Provider resource name. Used in GitHub Actions workflow."
  value       = google_iam_workload_identity_pool_provider.github_oidc.name
}

output "network_id" {
  description = "Self-link of the VPC network. Passed to node-pools module."
  value       = google_compute_network.nexusbench.id
}

output "subnetwork_id" {
  description = "Self-link of the node subnet. Passed to node-pools module."
  value       = google_compute_subnetwork.nodes.id
}
