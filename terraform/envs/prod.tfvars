# terraform/envs/prod.tfvars
#
# Production environment — sized for hackathon contest day traffic.
# All teams submit simultaneously; the worker pool scales to absorb the burst.
#
# Usage (CI/CD — see .github/workflows/deploy.yml):
#   terraform apply -var-file=envs/prod.tfvars \
#                   -var="project_id=$TF_VAR_project_id" \
#                   -auto-approve
#
# Manual apply (requires cloud credentials and VPN/IAP to reach the API server):
#   export TF_VAR_project_id=my-gcp-project
#   terraform apply -var-file=envs/prod.tfvars

# ── Project & region ──────────────────────────────────────────────────────────
region = "us-central1"
zones  = ["us-central1-a", "us-central1-b", "us-central1-c"]

# ── Cluster ───────────────────────────────────────────────────────────────────
cluster_name       = "nexusbench-prod"
kubernetes_version = "STABLE"

# ── Control-plane node pool ───────────────────────────────────────────────────
# e2-standard-4: 4 vCPU, 16 GB — handles Redpanda + TimescaleDB + control plane
# comfortably under contest-day write load.
# 2 nodes: one per zone for upgrade surge (one node updated at a time).
control_plane_machine_type = "e2-standard-4"
control_plane_node_count   = 2

# ── Worker node pool ──────────────────────────────────────────────────────────
# c2-standard-8: 8 vCPU, 32 GB — each worker runs up to 4 sandboxes + bot fleet.
# min=1: keep one warm node to avoid cold-start node provisioning latency when
#         the first submission arrives.
# max=10: handles 10 simultaneous benchmark runs (plenty for the hackathon scale).
worker_node_machine_type = "c2-standard-8"
worker_node_min          = 1
worker_node_max          = 10

# ── Registry ──────────────────────────────────────────────────────────────────
registry_location = "us"
registry_name     = "nexusbench-prod"

# ── Networking ────────────────────────────────────────────────────────────────
master_ipv4_cidr   = "172.16.0.0/28"
pods_ipv4_cidr     = "10.1.0.0/16"
services_ipv4_cidr = "10.2.0.0/20"

# Restrict API server access to CI runner IPs and team VPN/office CIDRs.
# Replace these placeholder CIDRs with real values before applying to prod.
master_authorized_cidr_blocks = [
  {
    cidr_block   = "35.235.240.0/20" # Google Cloud IAP tunnel IPs
    display_name = "GCP IAP"
  },
  {
    # Placeholder: replace with your GitHub Actions runner IP range or
    # the static IP of a self-hosted runner / NAT gateway.
    cidr_block   = "0.0.0.0/0"
    display_name = "CI runners (replace with actual CIDR)"
  }
]

# ── Labels ────────────────────────────────────────────────────────────────────
labels = {
  project     = "nexusbench"
  environment = "prod"
  managed-by  = "terraform"
  hackathon   = "iicpc-2026"
}
