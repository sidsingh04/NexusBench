# variables.tf — all root-level input variables
#
# Design rules:
#   1. Every variable has a description and a type constraint.
#   2. Sensitive variables have no default — they must be supplied explicitly
#      via a .tfvars file or TF_VAR_ environment variable. This forces the
#      operator to make a conscious decision and prevents accidental use of
#      production credentials in dev.
#   3. Variables that differ between dev and prod have defaults tuned for
#      the smallest viable dev environment — cheap and fast to spin up.
#   4. No cloud credentials appear here. Authentication is handled outside
#      Terraform via Application Default Credentials (ADC) or Workload Identity.

# ── Project & Location ────────────────────────────────────────────────────────

variable "project_id" {
  description = <<-EOT
    GCP project ID that owns all resources created by this workspace.
    Must be supplied — no default to prevent accidental cross-project deploys.
    Obtain with: gcloud config get-value project
  EOT
  type        = string

  validation {
    condition     = length(var.project_id) > 0
    error_message = "project_id must not be empty."
  }
}

variable "region" {
  description = <<-EOT
    GCP region for the GKE cluster and Artifact Registry repository.
    All zonal resources (node pools) are placed in zones within this region.
    Prefer regions close to your contestant base to minimise submission latency.
    Example values: us-central1, europe-west4, asia-northeast1.
  EOT
  type        = string
  default     = "us-central1"

  validation {
    condition     = can(regex("^[a-z]+-[a-z]+[0-9]+$", var.region))
    error_message = "region must be a valid GCP region (e.g. us-central1)."
  }
}

variable "zones" {
  description = <<-EOT
    Ordered list of GCP zones within var.region where node pool VMs are placed.
    GKE distributes nodes across these zones for availability.
    Dev: one zone is fine. Prod: at least two for node-pool HA.
    Example: ["us-central1-a", "us-central1-b"]
  EOT
  type        = list(string)
  default     = ["us-central1-a"]

  validation {
    condition     = length(var.zones) >= 1
    error_message = "At least one zone must be specified."
  }
}

# ── Cluster ───────────────────────────────────────────────────────────────────

variable "cluster_name" {
  description = "Name of the GKE cluster. Used as a prefix for node pool names and DNS labels."
  type        = string
  default     = "nexusbench"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,38}[a-z0-9]$", var.cluster_name))
    error_message = "cluster_name must be lowercase alphanumeric and hyphens, 2-40 chars."
  }
}

variable "kubernetes_version" {
  description = <<-EOT
    GKE release channel or static K8s version for the control plane.
    Use "STABLE" (GKE release channel) in production for automatic patch upgrades.
    Pin an explicit version (e.g. "1.29.4-gke.1043001") only when you need to
    reproduce a specific environment.
  EOT
  type        = string
  default     = "STABLE"
}

# ── Control-plane node pool ───────────────────────────────────────────────────

variable "control_plane_machine_type" {
  description = <<-EOT
    GCE machine type for the control-plane node pool.
    Hosts: NexusBench control plane, metrics-consumer, Prometheus, Grafana,
    Redpanda, TimescaleDB.
    Dev minimum: e2-standard-2 (2 vCPU, 8 GB).
    Prod recommendation: e2-standard-4 (4 vCPU, 16 GB).
  EOT
  type        = string
  default     = "e2-standard-2"
}

variable "control_plane_node_count" {
  description = <<-EOT
    Fixed number of nodes in the control-plane pool.
    This pool does NOT autoscale — it hosts stateful services (TimescaleDB,
    Redpanda) that should not be interrupted by node churn.
    Dev: 1. Prod: 2 (simple HA; upgrade surge policy handles rolling updates).
  EOT
  type        = number
  default     = 1

  validation {
    condition     = var.control_plane_node_count >= 1
    error_message = "control_plane_node_count must be at least 1."
  }
}

# ── Worker node pool ──────────────────────────────────────────────────────────

variable "worker_node_machine_type" {
  description = <<-EOT
    GCE machine type for the benchmark worker node pool.
    Workers run the bot fleet and sandbox containers. CPU-bound.
    Dev minimum: e2-standard-2 (2 vCPU, 8 GB) — allows 1 concurrent benchmark.
    Prod recommendation: c2-standard-8 (8 vCPU, 32 GB) — allows 4 concurrent.
    Must support spot/preemptible; N1/E2/C2 families all do.
  EOT
  type        = string
  default     = "e2-standard-2"
}

variable "worker_node_min" {
  description = <<-EOT
    Minimum number of nodes in the worker pool during low traffic.
    Set to 0 to allow the pool to fully scale down when no jobs are queued
    (saves cost). KEDA will scale up from 0 when the first job arrives.
    Set to 1 if cold-start latency of node provisioning (~60s) is unacceptable.
  EOT
  type        = number
  default     = 0

  validation {
    condition     = var.worker_node_min >= 0
    error_message = "worker_node_min must be >= 0."
  }
}

variable "worker_node_max" {
  description = <<-EOT
    Maximum number of nodes in the worker pool.
    Each node can host one worker pod (resource limits: 2 vCPU, 1 Gi).
    Size this to the maximum concurrent benchmarks you want to support.
    Dev: 3 (enough for smoke testing autoscaling).
    Prod: 10 (handles simultaneous submissions from all hackathon teams).
  EOT
  type        = number
  default     = 3

  validation {
    condition     = var.worker_node_max >= 1
    error_message = "worker_node_max must be at least 1."
  }
}

# ── Registry ──────────────────────────────────────────────────────────────────

variable "registry_location" {
  description = <<-EOT
    Artifact Registry multi-region location.
    Use a location close to var.region to minimise image pull latency.
    Valid values: us, europe, asia.
  EOT
  type        = string
  default     = "us"

  validation {
    condition     = contains(["us", "europe", "asia"], var.registry_location)
    error_message = "registry_location must be one of: us, europe, asia."
  }
}

variable "registry_name" {
  description = "Name of the Artifact Registry Docker repository."
  type        = string
  default     = "nexusbench"
}

# ── Networking ────────────────────────────────────────────────────────────────

variable "master_ipv4_cidr" {
  description = <<-EOT
    RFC 1918 /28 CIDR block for the GKE control plane internal network.
    Must not overlap with any subnet in your VPC.
    This CIDR is private to GCP and is never exposed externally.
    Default is safe for new projects that have not customised their VPC.
  EOT
  type        = string
  default     = "172.16.0.0/28"
}

variable "pods_ipv4_cidr" {
  description = <<-EOT
    Secondary IP range CIDR for pod IPs (VPC-native alias IP mode).
    /16 gives up to 65 536 pod IPs — sufficient for 10 nodes × 110 pods.
    Must not overlap with node or service CIDRs.
  EOT
  type        = string
  default     = "10.1.0.0/16"
}

variable "services_ipv4_cidr" {
  description = <<-EOT
    Secondary IP range CIDR for Kubernetes Service cluster IPs.
    /20 gives 4 096 service IPs — far more than NexusBench needs.
  EOT
  type        = string
  default     = "10.2.0.0/20"
}

# ── Authorised master networks ─────────────────────────────────────────────────

variable "master_authorized_cidr_blocks" {
  description = <<-EOT
    List of CIDR blocks allowed to reach the private GKE API server.
    Add your CI runner's IP and your team's office/VPN CIDR here.
    The GKE API server is private (no public endpoint) — only these CIDRs
    can run kubectl commands against the cluster.
    Example: ["203.0.113.10/32", "10.100.0.0/16"]
  EOT
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default = [
    # Placeholder: replace with real CIDRs before applying.
    # Left intentionally broad for first-run convenience — tighten for prod.
    {
      cidr_block   = "0.0.0.0/0"
      display_name = "all (replace before prod)"
    }
  ]
}

# ── GitHub / CI ──────────────────────────────────────────────────────────────

variable "github_repository" {
  description = <<-EOT
    GitHub repository in 'owner/repo' format.
    Used to scope Workload Identity Federation so only workflows in this
    repository can impersonate the CI push service account.
    Example: "acme-corp/nexusbench"
    Supply via -var or TF_VAR_github_repository (never hard-code here).
  EOT
  type        = string
}

# ── Labels ─────────────────────────────────────────────────────────────────────

variable "labels" {
  description = <<-EOT
    Key-value labels applied to all GCP resources.
    Used for cost attribution in billing reports.
    GCP label keys/values must be lowercase alphanumeric or hyphens, max 63 chars.
  EOT
  type        = map(string)
  default = {
    project     = "nexusbench"
    environment = "dev"
    managed-by  = "terraform"
  }
}
