# modules/node-pools/main.tf
#
# Two node pools, both attached to the cluster managed by modules/cluster:
#
#   1. control-plane pool — on-demand VMs, fixed count, NO taint.
#      Hosts: NexusBench server, metrics-consumer, Redpanda, TimescaleDB,
#             Prometheus, Grafana. These are stateful or singleton services
#             that must not land on spot nodes (which can be evicted mid-run).
#
#   2. worker pool — spot/preemptible VMs, cluster autoscaler enabled.
#      Hosts: benchmark worker pods ONLY (enforced by taint + toleration).
#      Taint: role=benchmark-worker:NoSchedule
#      This means a pod must explicitly tolerate the taint to be scheduled
#      here — accidental co-location with control-plane services is impossible.
#
# Why separate node pools instead of one pool with mixed workloads?
#   • Spot evictions only kill worker nodes, never the control plane.
#   • Worker nodes can be a different, more CPU-dense machine type than the
#     control plane nodes (optimised for bot fleet goroutines).
#   • The cluster autoscaler can scale the worker pool to 0 between contests
#     without disrupting the always-on control plane.

# ── Shared node configuration ─────────────────────────────────────────────────

locals {
  # OAuth scopes granted to every node's service account.
  # These are the minimum required scopes for a GKE node to function:
  #   • logging.write  — ship container logs to Cloud Logging
  #   • monitoring     — export metrics to Cloud Monitoring
  #   • devstorage.read_only — pull images from GCS-backed registries
  # Artifact Registry pull access is granted via IAM (see registry module),
  # not via OAuth scopes, so storage.read_only is not strictly needed for AR.
  default_oauth_scopes = [
    "https://www.googleapis.com/auth/logging.write",
    "https://www.googleapis.com/auth/monitoring",
    "https://www.googleapis.com/auth/devstorage.read_only",
  ]

  # Shared labels applied to every node in both pools.
  # GKE propagates these as node labels and resource labels on the underlying
  # Compute Engine instances. Used for cost attribution in billing.
  base_labels = merge(var.labels, {
    cluster = var.cluster_name
  })
}

# ── Pool 1: Control-plane node pool ──────────────────────────────────────────
# On-demand nodes. No autoscaling. Fixed count.
# ALL non-worker workloads (including Redpanda and TimescaleDB) schedule here
# unless explicitly configured with nodeSelector otherwise.

resource "google_container_node_pool" "control_plane" {
  name     = "${var.cluster_name}-control-plane"
  location = var.region
  cluster  = var.cluster_name
  project  = var.project_id

  # Fixed node count — no autoscaler on this pool.
  node_count = var.control_plane_node_count

  # Place nodes in the same zones as the cluster.
  node_locations = var.zones

  # ── Upgrade strategy ──────────────────────────────────────────────────────
  # Surge upgrade: GKE brings up one extra node before draining an old one.
  # This means zero capacity is lost during a GKE version upgrade.
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
    strategy        = "SURGE"
  }

  node_config {
    machine_type = var.control_plane_machine_type

    # On-demand (not preemptible/spot) — control plane must not be evicted.
    preemptible = false
    spot        = false

    service_account = var.node_service_account_email
    oauth_scopes    = local.default_oauth_scopes

    # 50 GB SSD boot disk — enough for OS, container images, and log buffers.
    disk_size_gb = 50
    disk_type    = "pd-ssd"

    # Enable Workload Identity on this pool so pods can use K8s ServiceAccounts
    # to authenticate to GCP APIs without JSON keys.
    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    # Shielded VMs: protect against rootkit / bootkit attacks on the node.
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    # Node labels — visible in `kubectl get nodes --show-labels`.
    # The `role` label lets node affinity rules in K8s manifests target
    # this pool without depending on the pool name.
    labels = merge(local.base_labels, {
      role = "control-plane"
    })

    # No taints on the control-plane pool — all pods without a toleration
    # schedule here by default.

    metadata = {
      # Disable the legacy metadata API endpoints that expose service account
      # tokens without Workload Identity protection.
      "disable-legacy-endpoints" = "true"
    }

    tags = ["nexusbench", "control-plane"]
  }

  # Auto-repair: GKE automatically replaces unhealthy nodes.
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  lifecycle {
    # Prevent Terraform from destroying this pool during a machine type change.
    # Instead: add a new pool, drain the old one, then remove it manually.
    create_before_destroy = true

    # Ignore changes to node_count made by the autoscaler between Terraform runs.
    ignore_changes = [node_count]
  }

  # This pool must wait for the cluster to finish creating before it can attach.
  depends_on = [
    # Expressed via cluster_name, which is only known after cluster creation.
    # Terraform resolves this dependency implicitly through the reference.
  ]
}

# ── Pool 2: Benchmark worker node pool ───────────────────────────────────────
# Spot/preemptible nodes. Cluster Autoscaler enabled (min → max).
# Only benchmark worker pods schedule here (enforced via taint + toleration).

resource "google_container_node_pool" "workers" {
  name     = "${var.cluster_name}-workers"
  location = var.region
  cluster  = var.cluster_name
  project  = var.project_id

  node_locations = var.zones

  # Cluster Autoscaler: scales node count between min and max based on
  # unschedulable pod demand. When the queue drains, KEDA scales worker
  # pods to 0; the CA then removes the now-empty nodes (down to min_node_count).
  autoscaling {
    min_node_count = var.worker_min_count
    max_node_count = var.worker_max_count

    # BALANCED location policy: spread nodes evenly across var.zones.
    location_policy = "BALANCED"
  }

  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
    strategy        = "SURGE"
  }

  node_config {
    machine_type = var.worker_machine_type

    # Spot VMs: ~60-91% cheaper than on-demand. Trade-off: can be evicted
    # with 30s notice. Acceptable because:
    #   1. Each benchmark job is designed to be idempotent (worker restarts
    #      re-deliver the job from Redpanda at-least-once guarantee).
    #   2. PodDisruptionBudget in Stage 4.2 limits simultaneous evictions.
    spot        = true
    preemptible = false # spot supersedes preemptible; keep preemptible=false

    service_account = var.node_service_account_email
    oauth_scopes    = local.default_oauth_scopes

    # 100 GB SSD — workers extract contestant archives and run sandbox images.
    # Large disk prevents OOM-evictions when multiple sandbox layers are cached.
    disk_size_gb = 100
    disk_type    = "pd-ssd"

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    labels = merge(local.base_labels, {
      role = "benchmark-worker"
    })

    # KEY SECURITY CONTROL: taint prevents any non-worker pod from landing here.
    # A pod must have a matching toleration (see k8s/worker/deployment.yaml)
    # to be scheduled on this node pool. Every other pod — including Redpanda,
    # TimescaleDB, and the control plane — is repelled automatically.
    taint {
      key    = "role"
      value  = "benchmark-worker"
      effect = "NO_SCHEDULE"
    }

    metadata = {
      "disable-legacy-endpoints" = "true"
    }

    tags = ["nexusbench", "benchmark-worker"]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  lifecycle {
    create_before_destroy = true
    ignore_changes        = [node_count]
  }
}
