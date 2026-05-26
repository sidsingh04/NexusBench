# terraform/envs/dev.tfvars
#
# Development environment — cheap, fast to provision, safe to destroy.
#
# Usage:
#   terraform plan  -var-file=envs/dev.tfvars
#   terraform apply -var-file=envs/dev.tfvars
#
# Supply project_id at plan time to avoid committing it:
#   terraform apply -var-file=envs/dev.tfvars -var="project_id=my-gcp-project"
#
# Or export as an env var (Terraform picks it up automatically):
#   export TF_VAR_project_id=my-gcp-project

# ── Project & region ──────────────────────────────────────────────────────────
# project_id is intentionally omitted — supply via -var or TF_VAR_project_id.
region = "us-central1"
zones  = ["us-central1-a"]

# ── Cluster ───────────────────────────────────────────────────────────────────
cluster_name       = "nexusbench-dev"
kubernetes_version = "STABLE"

# ── Control-plane node pool ───────────────────────────────────────────────────
# e2-standard-2: 2 vCPU, 8 GB RAM — minimum viable for dev.
# Single node is fine; we are not testing HA here.
control_plane_machine_type = "e2-standard-2"
control_plane_node_count   = 1

# ── Worker node pool ──────────────────────────────────────────────────────────
# Scale to zero when idle (saves cost between test runs).
# Max 3 nodes — enough to smoke test autoscaling without a large bill.
worker_node_machine_type = "e2-standard-2"
worker_node_min          = 0
worker_node_max          = 3

# ── Registry ──────────────────────────────────────────────────────────────────
registry_location = "us"
registry_name     = "nexusbench-dev"

# ── Networking ────────────────────────────────────────────────────────────────
master_ipv4_cidr   = "172.16.0.0/28"
pods_ipv4_cidr     = "10.1.0.0/16"
services_ipv4_cidr = "10.2.0.0/20"

# Allow kubectl from anywhere in dev — tighten this to your team's CIDR for prod.
master_authorized_cidr_blocks = [
  {
    cidr_block   = "0.0.0.0/0"
    display_name = "all (dev only)"
  }
]

# ── Labels ────────────────────────────────────────────────────────────────────
labels = {
  project     = "nexusbench"
  environment = "dev"
  managed-by  = "terraform"
}
