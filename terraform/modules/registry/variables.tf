# modules/registry/variables.tf

variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region (used for any regional resources in this module)."
  type        = string
}

variable "registry_location" {
  description = "Artifact Registry multi-region location: us, europe, or asia."
  type        = string
  default     = "us"
}

variable "registry_name" {
  description = "Name of the Artifact Registry Docker repository."
  type        = string
  default     = "nexusbench"
}

variable "node_service_account_email" {
  description = <<-EOT
    Email of the GKE node service account created by the cluster module.
    Granted roles/artifactregistry.reader so nodes can pull images without
    imagePullSecrets.
  EOT
  type        = string
}

variable "project_number" {
  description = <<-EOT
    Numeric GCP project number (not the project ID string).
    Used to construct the Workload Identity pool principal set.
    Obtain with: gcloud projects describe <project_id> --format='value(projectNumber)'
  EOT
  type        = string
}

variable "workload_identity_pool" {
  description = <<-EOT
    Full resource name of the Workload Identity Pool created by the cluster module.
    Format: projects/<number>/locations/global/workloadIdentityPools/<pool-id>
    Used to scope the GitHub Actions → CI service account binding.
  EOT
  type        = string
}

variable "github_repository" {
  description = <<-EOT
    GitHub repository in 'owner/repo' format.
    Only workflows running in this repository can impersonate the CI push
    service account. Prevents fork-based credential abuse.
    Example: "acme-corp/nexusbench"
  EOT
  type        = string
}

variable "labels" {
  description = "Labels applied to registry resources."
  type        = map(string)
  default     = {}
}
