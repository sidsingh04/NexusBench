# modules/registry/main.tf
#
# Creates an Artifact Registry Docker repository and configures two IAM bindings:
#
#   1. Node service account → roles/artifactregistry.reader
#      Every GKE node (both pools) can pull images from this registry.
#      No imagePullSecrets needed in K8s manifests — simpler and more secure.
#
#   2. CI service account → roles/artifactregistry.writer (via Workload Identity)
#      GitHub Actions exchanges its OIDC token for short-lived GCP credentials
#      to push new images on every merge to main. No JSON key stored in GitHub.
#
# Registry URL format:
#   <location>-docker.pkg.dev/<project_id>/<registry_name>
# Example:
#   us-docker.pkg.dev/my-project/nexusbench
#
# Images pushed by CI:
#   us-docker.pkg.dev/my-project/nexusbench/control-plane:$SHA
#   us-docker.pkg.dev/my-project/nexusbench/worker:$SHA
#   us-docker.pkg.dev/my-project/nexusbench/sandbox-go:$SHA
#   us-docker.pkg.dev/my-project/nexusbench/sandbox-rust:$SHA
#   us-docker.pkg.dev/my-project/nexusbench/sandbox-cpp:$SHA
#   us-docker.pkg.dev/my-project/nexusbench/sandbox-python:$SHA
#   us-docker.pkg.dev/my-project/nexusbench/sandbox-binary:$SHA

# ── Artifact Registry repository ─────────────────────────────────────────────

resource "google_artifact_registry_repository" "nexusbench" {
  location      = var.registry_location
  repository_id = var.registry_name
  format        = "DOCKER"
  project       = var.project_id
  description   = "NexusBench container images (control-plane, workers, sandboxes)"

  labels = var.labels

  # Cleanup policy: keep the 10 most recent tagged images per image name,
  # and delete untagged images (build cache layers) older than 7 days.
  # This prevents the registry from growing unboundedly during the hackathon.
  cleanup_policies {
    id     = "keep-10-tagged"
    action = "KEEP"

    condition {
      tag_state  = "TAGGED"
      newer_than = null
      older_than = null
    }

    most_recent_versions {
      keep_count = 10
    }
  }

  cleanup_policies {
    id     = "delete-old-untagged"
    action = "DELETE"

    condition {
      tag_state  = "UNTAGGED"
      older_than = "604800s" # 7 days in seconds
    }
  }
}

# ── IAM: node service accounts → read access ─────────────────────────────────
# Both node pools share the same node service account (created in the cluster
# module). Granting it reader access here means every pod on every node can
# pull images without any K8s imagePullSecret.

resource "google_artifact_registry_repository_iam_member" "node_pull" {
  location   = google_artifact_registry_repository.nexusbench.location
  repository = google_artifact_registry_repository.nexusbench.name
  project    = var.project_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${var.node_service_account_email}"
}

# ── CI service account for image push ────────────────────────────────────────
# A dedicated GCP service account for CI builds. It has ONLY write access to
# the registry — not cluster admin, not storage admin, not anything else.
# Principle of least privilege: a compromised CI token can push a bad image
# but cannot access cluster credentials or database data.

resource "google_service_account" "ci_push" {
  account_id   = "${var.registry_name}-ci-push"
  display_name = "NexusBench CI image push service account"
  project      = var.project_id
}

resource "google_artifact_registry_repository_iam_member" "ci_push" {
  location   = google_artifact_registry_repository.nexusbench.location
  repository = google_artifact_registry_repository.nexusbench.name
  project    = var.project_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.ci_push.email}"
}

# ── Workload Identity Federation binding for CI ────────────────────────────────
# Allows GitHub Actions workflows in the configured repository to impersonate
# the ci_push service account using an OIDC token — no JSON key needed.
#
# The principal format is:
#   principalSet://iam.googleapis.com/<pool_name>/attribute.repository/<repo>
#
# This binding is scoped to a specific GitHub repository (var.github_repository),
# preventing workflows in forked repos from acquiring push credentials.

resource "google_service_account_iam_member" "ci_wif_binding" {
  service_account_id = google_service_account.ci_push.name
  role               = "roles/iam.workloadIdentityUser"

  member = "principalSet://iam.googleapis.com/${var.workload_identity_pool}/attribute.repository/${var.github_repository}"
}
