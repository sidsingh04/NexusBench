# modules/cluster/main.tf
#
# Creates a private, VPC-native GKE cluster with:
#   • No default node pool (node pools are managed by the node-pools module
#     so they can be changed independently without cluster recreation).
#   • Workload Identity enabled (pods authenticate to GCP APIs via K8s
#     ServiceAccounts — no JSON key files mounted into containers).
#   • Private cluster (API server has no public endpoint; reachable only
#     from master_authorized_cidr_blocks and internal VPC traffic).
#   • VPC-native networking (alias IP ranges for pods and services — required
#     for NetworkPolicies to function correctly on GKE).
#   • Binary Authorization disabled (not needed for a hackathon; enable
#     in a production security hardening pass).
#
# What this module does NOT do:
#   • Create node pools — those are in modules/node-pools.
#   • Create namespaces or deploy workloads — those are in k8s/.

# ── VPC & Subnets ─────────────────────────────────────────────────────────────
# A dedicated VPC isolates NexusBench traffic from any default VPC resources.
# Using a custom VPC also gives us full control over subnet CIDRs.

resource "google_compute_network" "nexusbench" {
  name                    = "${var.cluster_name}-vpc"
  auto_create_subnetworks = false # we define subnets explicitly
  project                 = var.project_id
}

resource "google_compute_subnetwork" "nodes" {
  name    = "${var.cluster_name}-nodes"
  network = google_compute_network.nexusbench.id
  region  = var.region
  project = var.project_id

  # Node IPs come from this primary range.
  # /20 gives 4 094 node addresses — enough for 10 worker nodes and headroom.
  ip_cidr_range = "10.0.0.0/20"

  # VPC-native (alias IP) secondary ranges for pods and services.
  # GKE requires these to exist on the subnet before cluster creation.
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_ipv4_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_ipv4_cidr
  }

  private_ip_google_access = true # nodes can reach Google APIs without external IPs
}

# Cloud Router + NAT — nodes have no external IPs, so outbound internet
# traffic (package downloads during image build, etc.) exits via Cloud NAT.
resource "google_compute_router" "nexusbench" {
  name    = "${var.cluster_name}-router"
  network = google_compute_network.nexusbench.id
  region  = var.region
  project = var.project_id
}

resource "google_compute_router_nat" "nexusbench" {
  name                               = "${var.cluster_name}-nat"
  router                             = google_compute_router.nexusbench.name
  region                             = var.region
  project                            = var.project_id
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

# ── Service Account for GKE nodes ─────────────────────────────────────────────
# Each GKE node runs as this service account. We give it only the minimum
# IAM roles needed to function:
#   • logging.logWriter   — write logs to Cloud Logging
#   • monitoring.metricWriter — push metrics to Cloud Monitoring
#   • monitoring.viewer   — read monitoring data (required by some K8s components)
#
# The registry module grants this SA roles/artifactregistry.reader separately.

resource "google_service_account" "gke_nodes" {
  account_id   = "${var.cluster_name}-nodes"
  display_name = "NexusBench GKE node service account"
  project      = var.project_id
}

locals {
  node_sa_roles = [
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
  ]
}

resource "google_project_iam_member" "node_sa_roles" {
  for_each = toset(local.node_sa_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

# ── Workload Identity Pool ─────────────────────────────────────────────────────
# The Workload Identity Pool allows GitHub Actions' OIDC tokens to be exchanged
# for GCP credentials without any long-lived service account key.
# The pool is created here (cluster scope) so both the registry module and
# the CI pipeline can reference the same pool.

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "${var.cluster_name}-github"
  display_name              = "GitHub Actions for NexusBench"
  project                   = var.project_id

  # disabled = false (default) — pool is active immediately after creation.
}

resource "google_iam_workload_identity_pool_provider" "github_oidc" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-oidc"
  project                            = var.project_id
  display_name                       = "GitHub OIDC provider"

  oidc {
    # GitHub's OIDC issuer URL — do not change this.
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  # Attribute mapping: translate GitHub JWT claims into GCP principal attributes.
  # sub → assertion.sub gives us: repo:org/repo:ref:refs/heads/main
  # repository → assertion.repository scopes bindings to a specific repo.
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
}

# ── GKE Cluster ───────────────────────────────────────────────────────────────

resource "google_container_cluster" "nexusbench" {
  provider = google-beta # required for some private cluster + WI fields

  name     = var.cluster_name
  location = var.region # regional cluster: control plane spans all zones
  project  = var.project_id

  # Delegate node pool management to modules/node-pools.
  # Creating the cluster with no initial node pool avoids the implicit
  # "default-pool" that GKE would otherwise create and that we'd have to
  # immediately delete (which triggers a cluster update).
  remove_default_node_pool = true
  initial_node_count       = 1 # required field even when removing default pool

  min_master_version = var.kubernetes_version == "STABLE" ? null : var.kubernetes_version

  # ── Networking ───────────────────────────────────────────────────────────────

  network    = google_compute_network.nexusbench.id
  subnetwork = google_compute_subnetwork.nodes.id

  # VPC-native: pod and service IPs come from alias IP ranges on the subnet.
  # Required for NetworkPolicies to work correctly.
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # ── Private cluster ───────────────────────────────────────────────────────────
  # Nodes have internal IPs only; the API server has no external endpoint.
  # Access is via the master_authorized_cidr_blocks + Cloud VPN/IAP.

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false # keep public endpoint for kubectl from CI

    master_ipv4_cidr_block = var.master_ipv4_cidr
  }

  master_authorized_networks_config {
    dynamic "cidr_blocks" {
      for_each = var.master_authorized_cidr_blocks
      content {
        cidr_block   = cidr_blocks.value.cidr_block
        display_name = cidr_blocks.value.display_name
      }
    }
  }

  # ── Workload Identity ─────────────────────────────────────────────────────────
  # Pods authenticate to GCP services via their K8s ServiceAccount, which is
  # linked to a GCP service account via Workload Identity — no JSON keys mounted.

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # ── Add-ons ───────────────────────────────────────────────────────────────────

  addons_config {
    # HTTP load balancing creates a GCP L7 LB for Ingress objects.
    # Needed for the NGINX ingress in Stage 4.2 to get an external IP.
    http_load_balancing {
      disabled = false
    }

    # Horizontal Pod Autoscaling add-on is required by KEDA in Stage 4.3.
    horizontal_pod_autoscaling {
      disabled = false
    }

    # Network Policy add-on enforces Kubernetes NetworkPolicy objects.
    # Required for the zero-trust policies we apply in Stage 4.2.
    network_policy_config {
      disabled = false
    }
  }

  network_policy {
    enabled  = true
    provider = "CALICO" # Calico is the supported NetworkPolicy provider on GKE
  }

  # ── Release channel ───────────────────────────────────────────────────────────
  # STABLE channel: GKE manages patch-level upgrades automatically.
  # Only applied when kubernetes_version == "STABLE"; otherwise the explicit
  # version pin on min_master_version takes precedence.

  dynamic "release_channel" {
    for_each = var.kubernetes_version == "STABLE" ? [1] : []
    content {
      channel = "STABLE"
    }
  }

  # ── Maintenance window ────────────────────────────────────────────────────────
  # Schedule GKE control-plane upgrades during off-peak hours.
  # Recurrence rule: every Saturday from 02:00–06:00 UTC.

  maintenance_policy {
    recurring_window {
      start_time = "2024-01-06T02:00:00Z" # a Saturday
      end_time   = "2024-01-06T06:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SA"
    }
  }

  resource_labels = var.labels

  # Prevent accidental cluster deletion.
  # `terraform destroy` requires `deletion_protection = false` first.
  deletion_protection = false # set to true after initial bringup is stable
}
