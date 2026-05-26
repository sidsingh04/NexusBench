# modules/node-pools/outputs.tf

output "worker_pool_id" {
  description = <<-EOT
    Self-link of the worker node pool.
    Referenced by Stage 4.3 KEDA configuration and Stage 4.2 node affinity
    rules to ensure worker pods schedule exclusively on spot nodes.
    Format: projects/<project>/locations/<region>/clusters/<cluster>/nodePools/<pool>
  EOT
  value       = google_container_node_pool.workers.id
}

output "worker_pool_name" {
  description = "Short name of the worker node pool (used in nodeSelector labels)."
  value       = google_container_node_pool.workers.name
}

output "control_plane_pool_name" {
  description = "Short name of the control-plane node pool."
  value       = google_container_node_pool.control_plane.name
}

output "worker_node_label_role" {
  description = <<-EOT
    Value of the 'role' node label on worker nodes.
    K8s manifests use this in nodeSelector:
      nodeSelector:
        role: <worker_node_label_role>
  EOT
  value       = "benchmark-worker"
}

output "worker_node_taint_key" {
  description = "Taint key applied to worker nodes. Worker pod tolerations must match this."
  value       = "role"
}

output "worker_node_taint_value" {
  description = "Taint value applied to worker nodes."
  value       = "benchmark-worker"
}
