# modules/node-pools/variables.tf

variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region."
  type        = string
}

variable "zones" {
  description = "GCP zones within the region where node VMs are placed."
  type        = list(string)
}

variable "cluster_name" {
  description = "Name of the GKE cluster these node pools attach to."
  type        = string
}

variable "node_service_account_email" {
  description = <<-EOT
    Email of the GCP service account used by GKE nodes.
    Created by the cluster module; passed in here so both pools share it.
  EOT
  type        = string
}

# ── Control-plane pool ────────────────────────────────────────────────────────

variable "control_plane_machine_type" {
  description = "GCE machine type for the control-plane node pool."
  type        = string
  default     = "e2-standard-2"
}

variable "control_plane_node_count" {
  description = "Fixed number of nodes in the control-plane pool (no autoscaling)."
  type        = number
  default     = 1
}

# ── Worker pool ───────────────────────────────────────────────────────────────

variable "worker_machine_type" {
  description = "GCE machine type for the benchmark worker node pool."
  type        = string
  default     = "e2-standard-2"
}

variable "worker_min_count" {
  description = "Minimum number of nodes in the worker pool (0 = scale-to-zero allowed)."
  type        = number
  default     = 0
}

variable "worker_max_count" {
  description = "Maximum number of nodes in the worker pool."
  type        = number
  default     = 3
}

# ── Shared ────────────────────────────────────────────────────────────────────

variable "labels" {
  description = "Labels applied to node pool resources."
  type        = map(string)
  default     = {}
}
