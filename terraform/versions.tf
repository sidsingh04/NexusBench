# versions.tf — provider and Terraform version pins
#
# Every provider version is pinned to an exact minor release so that
# `terraform init` is fully reproducible across machines and CI runners.
# Bump these deliberately (test in dev, then promote to prod) rather than
# letting automatic upgrades change behaviour mid-hackathon.
#
# Provider choice: GCP (Google Cloud).
# Rationale: GKE is the most operationally straightforward managed K8s
# offering — Autopilot mode, Workload Identity, and Artifact Registry all
# integrate without glue code. EKS is equally valid; swap the google provider
# for hashicorp/aws and mirror the module interfaces.

terraform {
  required_version = ">= 1.7.0, < 2.0.0"

  required_providers {
    # Google Cloud provider — manages GKE, GCR/Artifact Registry, IAM, VPC.
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }

    # Google Beta — needed for a small number of GKE fields that have not
    # yet graduated out of beta (e.g. dns_config on private clusters).
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.30"
    }

    # Kubernetes provider — used by the registry module to create the
    # imagePullSecret in the nexusbench namespace after the cluster exists.
    # Version is intentionally kept in sync with the GKE cluster's K8s minor.
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }

  # Remote state keeps plan/apply operations consistent across teammates and
  # CI. The bucket is created manually once (bootstrap step) and is not
  # managed by this Terraform workspace to avoid the chicken-and-egg problem.
  #
  # To use local state during initial development, comment out this block and
  # run: terraform init -reconfigure
  # backend "gcs" {
  #   # Populated at init time via -backend-config or TF_VAR_ env vars.
  #   # Never hard-code bucket name or prefix here.
  #   # Example:
  #   #   terraform init \
  #   #     -backend-config="bucket=nexusbench-tfstate" \
  #   #     -backend-config="prefix=nexusbench/prod"
  # }
}
