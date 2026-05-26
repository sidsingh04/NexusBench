# modules/cluster/variables.tf

variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region where the cluster is created."
  type        = string
}

variable "zones" {
  description = "List of GCP zones within the region for node placement."
  type        = list(string)
}

variable "cluster_name" {
  description = "Name of the GKE cluster."
  type        = string
}

variable "kubernetes_version" {
  description = "GKE release channel ('STABLE') or a pinned version string."
  type        = string
  default     = "STABLE"
}

variable "master_ipv4_cidr" {
  description = "RFC 1918 /28 CIDR for the GKE master network."
  type        = string
  default     = "172.16.0.0/28"
}

variable "pods_ipv4_cidr" {
  description = "Secondary IP range CIDR for pod IPs."
  type        = string
  default     = "10.1.0.0/16"
}

variable "services_ipv4_cidr" {
  description = "Secondary IP range CIDR for service cluster IPs."
  type        = string
  default     = "10.2.0.0/20"
}

variable "master_authorized_cidr_blocks" {
  description = "CIDRs allowed to reach the private GKE API server."
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
}

variable "labels" {
  description = "Labels applied to all resources in this module."
  type        = map(string)
  default     = {}
}
