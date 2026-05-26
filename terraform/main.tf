# main.tf — root module
#
# This file does exactly two things:
#   1. Configures the Google providers (data sources only — no resource blocks).
#   2. Calls the three child modules with computed arguments.
#
# No resource blocks live here. Every resource lives inside a module.
# This keeps the root plan readable: `terraform plan` shows module.* calls,
# not a wall of individual resources.
#
# Module call order (implicit dependency via outputs → inputs):
#   cluster → node_pools → registry
#   (registry needs the cluster's workload-identity service account)

# ── Provider configuration ────────────────────────────────────────────────────
# Authentication: Application Default Credentials (ADC).
# Locally: `gcloud auth application-default login`
# CI (GitHub Actions): Workload Identity Federation — no JSON key needed.
# Neither credential appears in this file.

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# The kubernetes provider is configured after the cluster exists.
# The cluster module outputs the endpoint and CA certificate needed here.
# Using depends_on would create a cycle; instead we reference module outputs
# directly — Terraform evaluates providers after the outputs are known.
provider "kubernetes" {
  host                   = "https://${module.cluster.endpoint}"
  cluster_ca_certificate = base64decode(module.cluster.ca_certificate)

  # Use gke-gcloud-auth-plugin for authentication.
  # The exec block invokes the plugin which returns a short-lived token —
  # no service-account JSON key is stored in state.
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "gke-gcloud-auth-plugin"
    args        = []
  }
}

# ── Data sources ──────────────────────────────────────────────────────────────

# Resolve the project number from the project ID.
# Project number (not ID) is required by Workload Identity resource names.
data "google_project" "this" {
  project_id = var.project_id
}

# ── Module: cluster ───────────────────────────────────────────────────────────
# Creates the GKE cluster control plane, VPC, subnets, and Workload Identity.
# Does NOT create node pools — those live in the node_pools module so they can
# be modified independently without triggering cluster recreation.

module "cluster" {
  source = "./modules/cluster"

  project_id         = var.project_id
  region             = var.region
  zones              = var.zones
  cluster_name       = var.cluster_name
  kubernetes_version = var.kubernetes_version

  master_ipv4_cidr              = var.master_ipv4_cidr
  pods_ipv4_cidr                = var.pods_ipv4_cidr
  services_ipv4_cidr            = var.services_ipv4_cidr
  master_authorized_cidr_blocks = var.master_authorized_cidr_blocks

  labels = var.labels
}

# ── Module: node_pools ────────────────────────────────────────────────────────
# Creates two node pools attached to the cluster above:
#   • control-plane pool — on-demand, hosts stateful services
#   • worker pool        — preemptible/spot, hosts benchmark worker pods

module "node_pools" {
  source = "./modules/node-pools"

  project_id   = var.project_id
  region       = var.region
  zones        = var.zones
  cluster_name = module.cluster.cluster_name # explicit dependency on cluster

  node_service_account_email = module.cluster.node_service_account_email

  # Control-plane pool
  control_plane_machine_type = var.control_plane_machine_type
  control_plane_node_count   = var.control_plane_node_count

  # Worker pool
  worker_machine_type = var.worker_node_machine_type
  worker_min_count    = var.worker_node_min
  worker_max_count    = var.worker_node_max

  labels = var.labels
}

# ── Module: registry ──────────────────────────────────────────────────────────
# Creates an Artifact Registry Docker repository and grants the node service
# accounts pull access via Workload Identity so pods never need image-pull
# secrets with long-lived credentials.

module "registry" {
  source = "./modules/registry"

  project_id        = var.project_id
  region            = var.region
  registry_location = var.registry_location
  registry_name     = var.registry_name

  # The node service account created by the cluster module is the identity
  # that GKE nodes use when pulling images. Granting it roles/artifactregistry.reader
  # here means every pod in the cluster can pull from this registry.
  node_service_account_email = module.cluster.node_service_account_email

  # Workload Identity pool is used to bind GitHub Actions' OIDC token to a
  # GCP service account for CI image pushes (Stage 4.4).
  project_number         = data.google_project.this.number
  workload_identity_pool = module.cluster.workload_identity_pool
  github_repository      = var.github_repository

  labels = var.labels
}
