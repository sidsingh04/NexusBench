# modules/registry/outputs.tf

output "repository_url" {
  description = <<-EOT
    Base URL of the Artifact Registry Docker repository.
    Use this as the image prefix in K8s manifests and docker push commands.
    Format: <location>-docker.pkg.dev/<project_id>/<registry_name>
    Example: docker push <repository_url>/control-plane:abc1234
  EOT
  value       = "${var.registry_location}-docker.pkg.dev/${var.project_id}/${var.registry_name}"
}

output "repository_id" {
  description = "Full resource ID of the Artifact Registry repository."
  value       = google_artifact_registry_repository.nexusbench.id
}

output "ci_push_service_account_email" {
  description = <<-EOT
    Email of the CI service account authorised to push images.
    Referenced in the GitHub Actions deploy.yml workflow to configure
    Workload Identity Federation authentication.
  EOT
  value       = google_service_account.ci_push.email
}
