# outputs.tf — values exported from the root module
#
# These outputs serve two consumers:
#   1. Human operators: readable after `terraform apply` to bootstrap kubectl.
#   2. CI/CD: captured by `terraform output -raw <name>` in deploy.yml to
#      authenticate kubectl and push images without any hard-coded values.
#
# Sensitive outputs are marked sensitive = true so Terraform redacts them in
# plan/apply output. They are still accessible via `terraform output -raw`.

output "cluster_name" {
  description = "Name of the GKE cluster. Used in kubectl / gcloud commands."
  value       = module.cluster.cluster_name
}

output "cluster_endpoint" {
  description = <<-EOT
    Private IP address of the GKE API server.
    Reachable only from the authorised master CIDR blocks defined in variables.
    Use with: kubectl --server=https://<endpoint>
  EOT
  value       = module.cluster.endpoint
  sensitive   = true
}

output "cluster_ca_certificate" {
  description = <<-EOT
    Base64-encoded certificate authority data for the GKE cluster.
    Used by kubectl to verify the API server's TLS certificate.
  EOT
  value       = module.cluster.ca_certificate
  sensitive   = true
}

output "kubeconfig_command" {
  description = <<-EOT
    gcloud command to fetch and merge credentials for this cluster into your
    local kubeconfig. Run this after `terraform apply` to configure kubectl.
  EOT
  value       = <<-CMD
    gcloud container clusters get-credentials ${module.cluster.cluster_name} \
      --region ${var.region} \
      --project ${var.project_id}
  CMD
}

output "registry_url" {
  description = <<-EOT
    Base URL of the Artifact Registry Docker repository.
    Use as the image prefix in K8s manifests and docker push commands.
    Example: docker push <registry_url>/control-plane:latest
  EOT
  value       = module.registry.repository_url
}

output "worker_pool_id" {
  description = <<-EOT
    Resource ID of the benchmark worker node pool.
    Surfaced here so Stage 4.3 KEDA configuration can reference it in
    nodeSelector / toleration labels without looking up the pool separately.
  EOT
  value       = module.node_pools.worker_pool_id
}

output "workload_identity_pool" {
  description = <<-EOT
    Workload Identity Pool identifier.
    Used by GitHub Actions OIDC configuration in Stage 4.4 to federate
    GitHub's OIDC token to a GCP service account without a JSON key.
    Format: projects/<number>/locations/global/workloadIdentityPools/<pool>
  EOT
  value       = module.cluster.workload_identity_pool
}
