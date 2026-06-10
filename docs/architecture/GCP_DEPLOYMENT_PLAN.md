# NexusBench — GCP Cloud Deployment Implementation Plan

**Document status:** Ready for execution  
**Target environment:** GCP / GKE (us-central1)  
**Estimated total time:** 3–4 hours for a first-time operator  
**Safe to re-run:** Every stage is idempotent unless noted otherwise

---

## How to use this document

Each stage has:
- A **Gate** — the exact check to run before moving to the next stage
- **Rollback** instructions where a failure leaves the environment in a partial state
- Commands derived directly from the actual project files — no placeholders that require guessing

Work through stages in order. Do not skip ahead. Every stage has an explicit gate
that must pass before the next stage begins.

---

## Variable reference

Set these once at the start of your terminal session. Every command in this
document references them. Do not hard-code values into individual commands.

```bash
export PROJECT_ID="your-gcp-project-id"       # gcloud config get-value project
export REGION="us-central1"                    # must match terraform.tfvars
export CLUSTER_NAME="nexusbench"               # matches variables.tf default
export REGISTRY_LOCATION="us"                  # matches variables.tf default
export REGISTRY_NAME="nexusbench"              # matches variables.tf default
export GITHUB_REPO="your-org/nexusbench"       # owner/repo format, no https://
export ADMIN_KEY="$(openssl rand -hex 32)"     # strong random key for ADMIN_API_KEY
export POSTGRES_PASSWORD="$(openssl rand -hex 24)"
export TIMESCALE_PASSWORD="$(openssl rand -hex 24)"

# Derived — set after terraform apply in Stage 4
# REGISTRY_URL is set from terraform output in Stage 4
# SHA is set from git in Stage 5
```

---

## Stage 0 — Prerequisites

**What:** Verify every tool is installed at the required version before anything
touches GCP. A version mismatch here causes confusing errors three stages later.

```bash
# Terraform >= 1.7.0 (versions.tf required_version constraint)
terraform -version
# Expected: Terraform v1.7.x or higher

# gcloud SDK
gcloud --version
# Expected: Google Cloud SDK 460.0.0 or higher

# gke-gcloud-auth-plugin — required by main.tf's kubernetes provider exec block
# Without this, `terraform apply` fails when the kubernetes provider initialises
gke-gcloud-auth-plugin --version
# If missing:
gcloud components install gke-gcloud-auth-plugin

# kubectl
kubectl version --client
# Expected: v1.28.x or higher

# Docker — for building and pushing images in Stage 5
docker --version
# Expected: 24.x or higher

# git — for the SHA tag used in image tagging
git --version
```

**Gate:** All five tools respond without error. Proceed to Stage 1.

---

## Stage 1 — GCP project bootstrap

**What:** Enable the six GCP APIs that the Terraform modules touch. This is a
one-time operation per project. Running it again on an already-enabled API is
a no-op.

```bash
gcloud auth login
gcloud config set project $PROJECT_ID

gcloud services enable \
  container.googleapis.com \
  compute.googleapis.com \
  artifactregistry.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  sts.googleapis.com \
  cloudresourcemanager.googleapis.com

# Authenticate Terraform's Google providers via Application Default Credentials.
# main.tf uses ADC — no credential block in the provider config.
gcloud auth application-default login
```

**Gate:**
```bash
gcloud services list --enabled --filter="name:(container OR compute OR artifactregistry)"
# Must show all three services as ENABLED
```

---

## Stage 2 — Terraform variable file

**What:** Create `terraform/terraform.tfvars`. Two variables have no default
in `variables.tf` and Terraform will refuse to plan without them: `project_id`
and `github_repository`.

```bash
cat > terraform/terraform.tfvars << EOF
project_id        = "$PROJECT_ID"
github_repository = "$GITHUB_REPO"
region            = "$REGION"
zones             = ["${REGION}-a"]
cluster_name      = "$CLUSTER_NAME"

# Control-plane pool: on-demand, hosts Redpanda, TimescaleDB, Postgres,
# control-plane, consumer. e2-standard-2 is the stated minimum (2 vCPU, 8 GB).
control_plane_machine_type = "e2-standard-2"
control_plane_node_count   = 1

# Worker pool: spot VMs. Autoscaler manages count between min and max.
# One node comfortably hosts one concurrent benchmark.
# node-pools/main.tf: spot=true, taint role=benchmark-worker:NoSchedule
worker_node_machine_type = "e2-standard-2"
worker_node_min          = 0
worker_node_max          = 3

# Artifact Registry — modules/registry/main.tf
registry_location = "$REGISTRY_LOCATION"
registry_name     = "$REGISTRY_NAME"

# GKE private cluster API server access.
# The cluster module sets enable_private_endpoint=false (public endpoint kept
# for CI kubectl access) but restricts which IPs can reach it.
# Replace 0.0.0.0/0 with your team's actual IP range before production.
master_authorized_cidr_blocks = [
  {
    cidr_block   = "0.0.0.0/0"
    display_name = "all (replace before prod)"
  }
]

labels = {
  project     = "nexusbench"
  environment = "dev"
  managed-by  = "terraform"
}
EOF
```

**Gate:**
```bash
cd terraform
terraform init -backend=false -input=false
terraform validate
# Expected: Success! The configuration is valid.
cd ..
```

---

## Stage 3 — (Optional) Remote state backend

Skip this stage for a demo deployment. Use it for any environment that more
than one person will operate.

```bash
# Create the state bucket once — not managed by Terraform itself
gsutil mb -p $PROJECT_ID -l $REGION gs://${PROJECT_ID}-nexusbench-tfstate
gsutil versioning set on gs://${PROJECT_ID}-nexusbench-tfstate

# Uncomment the backend block in terraform/versions.tf:
#   backend "gcs" { }
# Then re-init with backend config:
cd terraform
terraform init \
  -backend-config="bucket=${PROJECT_ID}-nexusbench-tfstate" \
  -backend-config="prefix=nexusbench/dev" \
  -reconfigure
cd ..
```

**Gate (if using remote state):**
```bash
cd terraform && terraform state list && cd ..
# Expected: empty list (no resources yet), no authentication error
```

---

## Stage 4 — Terraform apply (cluster + registry)

**What:** Provisions the GKE cluster, two node pools, VPC, Cloud NAT,
Workload Identity pool + OIDC provider, Artifact Registry repository,
and all IAM bindings. This is the longest stage — GKE cluster creation
takes 8–12 minutes.

```bash
cd terraform

# Review the plan before applying.
# Expect ~30 resources across cluster, node_pools, and registry modules.
terraform plan -out=nexusbench.tfplan

# Verify the plan shows:
# + google_container_cluster.nexusbench           (cluster module)
# + google_container_node_pool.control_plane      (node_pools module)
# + google_container_node_pool.workers            (node_pools module)
# + google_artifact_registry_repository.nexusbench (registry module)
# + google_iam_workload_identity_pool.github      (cluster module)

terraform apply nexusbench.tfplan
```

When apply completes, capture all outputs immediately:

```bash
# Store these — they are needed in every subsequent stage
export REGISTRY_URL=$(terraform output -raw registry_url)
export WORKLOAD_IDENTITY_PROVIDER=$(terraform output -raw workload_identity_pool)

# Print the exact gcloud command to configure kubectl
terraform output kubeconfig_command

cd ..
```

**Gate:**
```bash
gcloud container clusters describe $CLUSTER_NAME \
  --region $REGION \
  --project $PROJECT_ID \
  --format="value(status)"
# Must print: RUNNING

gcloud artifacts repositories describe $REGISTRY_NAME \
  --location $REGISTRY_LOCATION \
  --project $PROJECT_ID \
  --format="value(name)"
# Must print the repository resource name without error
```

**Rollback:** `terraform destroy` removes everything created in this stage.
Run this if the cluster is in a broken state and you need a clean start.
GKE deletion takes ~5 minutes.

---

## Stage 5 — Configure kubectl

**What:** Fetch GKE credentials and verify the cluster is reachable. The
`kubeconfig_command` output from Stage 4 gives the exact command.

```bash
gcloud container clusters get-credentials $CLUSTER_NAME \
  --region $REGION \
  --project $PROJECT_ID

# Verify connectivity
kubectl cluster-info
kubectl get nodes
# Expected: 1 node in the control-plane pool (Ready)
# The worker pool starts at worker_node_min=0, so 0 worker nodes initially
```

**Gate:**
```bash
kubectl get nodes --show-labels | grep "role=control-plane"
# Must show at least one node with the role=control-plane label
# (set by node-pools/main.tf labels block)
```

---

## Stage 6 — Build and push all Docker images

**What:** Build the three application binaries and five sandbox language images.
The Artifact Registry repository was created in Stage 4. The `deploy.yml`
CI job does this automatically for every push to `main` — this stage is for
the first manual deployment only.

```bash
export SHA=$(git rev-parse --short HEAD)
REGISTRY_HOST="${REGISTRY_URL%%/*}"  # strips path, keeps host e.g. us-docker.pkg.dev

# Authenticate Docker to the registry
gcloud auth configure-docker $REGISTRY_HOST --quiet

# ── Image 1: control-plane (contains /app/server, /app/worker, /app/consumer)
# Dockerfile.server builds all three binaries from one image.
# deploy.yml uses this image for both the control-plane and worker Deployments.
docker build \
  -t $REGISTRY_URL/control-plane:$SHA \
  -t $REGISTRY_URL/control-plane:latest \
  -f Dockerfile.server \
  .
docker push $REGISTRY_URL/control-plane:$SHA
docker push $REGISTRY_URL/control-plane:latest

# ── Images 2–6: sandbox language images
# Each uses docker/sandbox/ as context with a language-specific Dockerfile.
# The entrypoint.sh is shared across all five (COPY entrypoint.sh in each Dockerfile).
for LANG in golang rust cpp python binary; do
  echo "Building sandbox-$LANG..."
  docker build \
    -t $REGISTRY_URL/sandbox-$LANG:$SHA \
    -t $REGISTRY_URL/sandbox-$LANG:latest \
    -f docker/sandbox/Dockerfile.$LANG \
    docker/sandbox/
  docker push $REGISTRY_URL/sandbox-$LANG:$SHA
  docker push $REGISTRY_URL/sandbox-$LANG:latest
done
```

**Gate:**
```bash
gcloud artifacts docker images list $REGISTRY_URL \
  --include-tags \
  --format="table(IMAGE, TAGS)"
# Must show 6 images: control-plane, sandbox-golang, sandbox-rust,
# sandbox-cpp, sandbox-python, sandbox-binary — each with :latest and :$SHA
```

---

## Stage 7 — Create the missing Kubernetes manifests

**What:** The analysis identified 9 directories under `k8s/` that are empty.
The two Deployment files that exist (`k8s/control-plane/deployment.yaml` and
`k8s/worker/deployment.yaml`) will crash immediately without these resources.
Create them in the order below.

Each file created in this stage must be committed to the repository so
`terraform validate` and `kubeconform` in CI can validate them.

### 7a — Namespace (already exists, apply first)

```bash
kubectl apply -f k8s/namespace.yaml
```

### 7b — Secrets

The control-plane `deployment.yaml` uses `envFrom: configMapRef: nexusbench-config`.
That ConfigMap references several values that must be secret. Create them as
a K8s Secret before the ConfigMap. These values come directly from the
`docker-compose.yml` environment blocks — use the same variable names.

```bash
kubectl create secret generic nexusbench-db-secrets \
  --namespace nexusbench \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=TIMESCALE_PASSWORD="$TIMESCALE_PASSWORD" \
  --from-literal=ADMIN_API_KEY="$ADMIN_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Save the file for the repository at `k8s/secrets/README.md` documenting that
this secret is created out-of-band (never committed to git). Add the
resource name to `.gitignore` if needed.

### 7c — RBAC (ServiceAccount)

`worker/deployment.yaml` declares `serviceAccountName: nexusbench-worker`.
Without this ServiceAccount the worker pods fail with
`serviceaccounts "nexusbench-worker" not found`.

Create `k8s/rbac/serviceaccount.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nexusbench-worker
  namespace: nexusbench
  labels:
    app.kubernetes.io/part-of: nexusbench
```

```bash
kubectl apply -f k8s/rbac/serviceaccount.yaml
```

### 7d — ConfigMap

Both Deployments use `envFrom: configMapRef: nexusbench-config`. The env var
names map directly from `internal/config/config.go`'s `getEnv` calls.

Create `k8s/configmaps/config.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nexusbench-config
  namespace: nexusbench
data:
  LISTEN_ADDR: ":8080"
  SUBMISSION_DIR: "/data/submissions"
  DISTRIBUTED_MODE: "true"
  REDPANDA_BROKERS: "redpanda-svc.nexusbench.svc.cluster.local:9092"
  POSTGRES_DSN: "postgres://nexusbench:$(POSTGRES_PASSWORD)@postgres-svc.nexusbench.svc.cluster.local:5432/nexusbench"
  TIMESCALE_DSN: "postgres://nexus:$(TIMESCALE_PASSWORD)@timescaledb-svc.nexusbench.svc.cluster.local:5432/nexusbench"
  ORCHESTRATOR_URL: "http://control-plane-svc.nexusbench.svc.cluster.local:8080"
  SANDBOX_IMAGE_GO:     "REGISTRY_URL/sandbox-golang:latest"
  SANDBOX_IMAGE_RUST:   "REGISTRY_URL/sandbox-rust:latest"
  SANDBOX_IMAGE_CPP:    "REGISTRY_URL/sandbox-cpp:latest"
  SANDBOX_IMAGE_PYTHON: "REGISTRY_URL/sandbox-python:latest"
  SANDBOX_IMAGE_BINARY: "REGISTRY_URL/sandbox-binary:latest"
  SANDBOX_CPU_QUOTA:    "100000"
  SANDBOX_MEMORY_BYTES: "536870912"
  SANDBOX_TIMEOUT:      "30m"
  SANDBOX_NETWORK_MODE: "bridge"
  SANDBOX_PORT_MIN:     "20000"
  SANDBOX_PORT_MAX:     "21000"
  SANDBOX_HOST:         "$(NODE_IP)"
  MAX_UPLOAD_BYTES:     "268435456"
  BOT_COUNT:              "100"
  BOT_TEST_DURATION:      "60s"
  BOT_RAMP_UP_DURATION:   "5s"
  BOT_ORDER_RATIO_LIMIT:  "0.60"
  BOT_ORDER_RATIO_MARKET: "0.30"
  BOT_ORDER_RATIO_CANCEL: "0.10"
  BOT_PER_REQUEST_TIMEOUT: "2s"
  JOB_TIMEOUT: "35m"
```

**Important:** Before applying, replace `REGISTRY_URL` with the actual value
from `$REGISTRY_URL` and remove the `$(POSTGRES_PASSWORD)` / `$(TIMESCALE_PASSWORD)`
references — those must be injected via the Secret using `secretKeyRef` in the
Deployment env block, not in the ConfigMap directly.

```bash
# Substitute REGISTRY_URL before applying
sed "s|REGISTRY_URL|$REGISTRY_URL|g" k8s/configmaps/config.yaml \
  | kubectl apply -f -
```

### 7e — Infrastructure Services (Redpanda, TimescaleDB, Postgres)

These three services must be running before any application pod starts. Create
StatefulSet + Service manifests for each, mirroring the docker-compose.yml
image versions and env vars exactly.

**Redpanda** (`k8s/redpanda/`): StatefulSet using
`redpandadata/redpanda:v24.1.13`, the same version as docker-compose. Single
broker in dev. ClusterIP Service exposing port 9092 as
`redpanda-svc.nexusbench.svc.cluster.local:9092` — the exact address in the
ConfigMap above.

**TimescaleDB** (`k8s/timescaledb/`): StatefulSet using
`timescale/timescaledb:2.15.2-pg16`, PersistentVolumeClaim (10 Gi SSD),
ClusterIP Service on port 5432 as `timescaledb-svc`.

**Postgres** (`k8s/postgres/`): StatefulSet using `postgres:16-alpine`,
PersistentVolumeClaim (5 Gi SSD), ClusterIP Service on port 5432 as
`postgres-svc`. Note: docker-compose maps this to host port 5433 to avoid
collision with TimescaleDB, but inside the cluster each has its own ClusterIP
so both use port 5432.

After creating all three manifest sets:

```bash
kubectl apply -f k8s/redpanda/
kubectl apply -f k8s/postgres/
kubectl apply -f k8s/timescaledb/

# Wait — application pods depend on these being ready
kubectl wait --for=condition=ready pod \
  -l app=redpanda -n nexusbench --timeout=120s
kubectl wait --for=condition=ready pod \
  -l app=postgres -n nexusbench --timeout=120s
kubectl wait --for=condition=ready pod \
  -l app=timescaledb -n nexusbench --timeout=120s
```

### 7f — PersistentVolumeClaim for submissions

`control-plane/deployment.yaml` mounts `claimName: submissions-data`.
Without this PVC the control-plane pod stays in `Pending`.

Create `k8s/control-plane/pvc.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: submissions-data
  namespace: nexusbench
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: standard-rwo   # GKE's default SSD StorageClass
  resources:
    requests:
      storage: 20Gi
```

```bash
kubectl apply -f k8s/control-plane/pvc.yaml
```

### 7g — Services for application Deployments

Neither Deployment has a Service manifest. Without them, inter-pod
communication via DNS is impossible.

Create `k8s/control-plane/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: control-plane-svc
  namespace: nexusbench
spec:
  selector:
    app.kubernetes.io/name: control-plane
  ports:
    - name: http
      port: 8080
      targetPort: http
  type: ClusterIP
```

Create `k8s/worker/service.yaml` — workers do not receive inbound traffic,
but a headless service is useful for DNS-based discovery:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: worker-svc
  namespace: nexusbench
spec:
  selector:
    app.kubernetes.io/name: worker
  clusterIP: None   # headless
  ports:
    - port: 8080
```

```bash
kubectl apply -f k8s/control-plane/service.yaml
kubectl apply -f k8s/worker/service.yaml
```

### 7h — Consumer Deployment

The metrics consumer is a third entrypoint in `Dockerfile.server` (`/app/consumer`).
docker-compose starts it as `metrics-consumer` with `entrypoint: ["/app/consumer"]`.

Create `k8s/consumer/deployment.yaml` mirroring the worker Deployment structure
but with `command: ["/app/consumer"]`, no Docker socket mount, no spot node
taint toleration (it runs on the control-plane pool), and only the
`REDPANDA_BROKERS`, `CONSUMER_GROUP_ID`, and `TIMESCALE_DSN` env vars from
the ConfigMap + Secret.

```bash
kubectl apply -f k8s/consumer/deployment.yaml
```

### 7i — KEDA ScaledObject

KEDA must be installed in the cluster before the ScaledObject resource is
recognised. Install KEDA first:

```bash
# Install KEDA via its official Helm chart
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda \
  --namespace keda \
  --create-namespace \
  --wait
```

Then create `k8s/keda/scaledobject.yaml`. The ScaledObject targets the worker
Deployment and scales based on the Redpanda consumer group lag on the
`nexusbench-jobs` topic:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: worker-scaler
  namespace: nexusbench
spec:
  scaleTargetRef:
    name: worker
  minReplicaCount: 0
  maxReplicaCount: 3          # must match worker_node_max in terraform.tfvars
  pollingInterval: 15
  cooldownPeriod: 60
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: redpanda-svc.nexusbench.svc.cluster.local:9092
        consumerGroup: nexusbench-worker
        topic: nexusbench-jobs
        lagThreshold: "1"     # scale up when 1+ message is unprocessed
        offsetResetPolicy: latest
```

```bash
kubectl apply -f k8s/keda/scaledobject.yaml

# Verify KEDA recognises the ScaledObject
kubectl get scaledobject -n nexusbench
# Expected: worker-scaler   READY=True
```

### 7j — NetworkPolicies

`worker/deployment.yaml` references `allow-worker-egress-redpanda.yaml` in its
security comment. The cluster module enables Calico. Without these policies all
pods can reach all other pods, which violates the stated security model.

Create `k8s/network-policies/default-deny.yaml` (deny all ingress/egress by
default, then open specific paths):

```yaml
# Default deny all ingress and egress in the nexusbench namespace.
# Subsequent NetworkPolicy objects selectively open required paths.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: nexusbench
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
```

Then create allow policies for each required path:
- `allow-control-plane-ingress.yaml` — allows ingress to port 8080 from anywhere
- `allow-worker-egress-redpanda.yaml` — workers → Redpanda:9092 only
- `allow-worker-egress-control-plane.yaml` — workers → control-plane:8080 (heartbeats)
- `allow-consumer-egress-redpanda.yaml` — consumer → Redpanda:9092
- `allow-consumer-egress-timescaledb.yaml` — consumer → TimescaleDB:5432
- `allow-control-plane-egress-postgres.yaml` — control-plane → Postgres:5432
- `allow-control-plane-egress-redpanda.yaml` — control-plane → Redpanda:9092
- `allow-dns.yaml` — all pods → kube-dns:53 (required for cluster DNS)

```bash
kubectl apply -f k8s/network-policies/
```

**Gate for all of Stage 7:**
```bash
kubectl get all -n nexusbench
# Must show: pods for redpanda, postgres, timescaledb, control-plane, consumer
# all in Running state (worker stays at 0 until a job is enqueued via KEDA)

kubectl get pvc -n nexusbench
# submissions-data must be Bound

kubectl get scaledobject -n nexusbench
# worker-scaler must show READY=True
```

---

## Stage 8 — Deploy the application

**What:** Apply the two Deployment manifests that already exist in the repo,
then update their image references to the SHA pushed in Stage 6.

```bash
export SHA=$(git rev-parse --short HEAD)

# Apply both Deployments
kubectl apply -f k8s/control-plane/deployment.yaml
kubectl apply -f k8s/worker/deployment.yaml

# Update the image to the SHA pushed in Stage 6.
# deploy.yml does this automatically via kubectl set image on every push to main.
kubectl set image deployment/control-plane \
  control-plane=$REGISTRY_URL/control-plane:$SHA \
  -n nexusbench

kubectl set image deployment/worker \
  worker=$REGISTRY_URL/control-plane:$SHA \
  -n nexusbench

# Watch the rollout — readinessProbe in control-plane/deployment.yaml
# hits /health before the pod is marked Ready
kubectl rollout status deployment/control-plane -n nexusbench --timeout=120s
# control-plane has 0 replicas until KEDA activates it (worker_node_min=0)
# The control-plane Deployment has replicas:1 in the manifest so it rolls out immediately
```

**Gate:**
```bash
# Control-plane pod must be Running and Ready
kubectl get pods -n nexusbench -l app.kubernetes.io/name=control-plane
# Expected: 1/1 Running

# Health check through port-forward
kubectl port-forward svc/control-plane-svc 8080:8080 -n nexusbench &
sleep 2
curl -sf http://localhost:8080/health && echo "HEALTHY"
# Expected: HEALTHY (or a JSON health response)
kill %1
```

---

## Stage 9 — Configure GitHub Actions

**What:** Set the six secrets that `deploy.yml` needs. The Terraform registry
module already created the Workload Identity Federation pool, provider, and
CI service account. This stage only adds the secrets to GitHub.

```bash
# Get the full WIF provider resource name
WIF_PROVIDER_NAME="projects/$(gcloud projects describe $PROJECT_ID \
  --format='value(projectNumber)')/locations/global/workloadIdentityPools/${CLUSTER_NAME}-github/providers/github-oidc"

CI_SA_EMAIL="${REGISTRY_NAME}-ci-push@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Add these secrets to GitHub → Settings → Environments → production:"
echo ""
echo "GCP_PROJECT_ID                = $PROJECT_ID"
echo "GCP_WORKLOAD_IDENTITY_PROVIDER= $WIF_PROVIDER_NAME"
echo "GCP_SERVICE_ACCOUNT           = $CI_SA_EMAIL"
echo "GKE_CLUSTER_NAME              = $CLUSTER_NAME"
echo "GKE_CLUSTER_REGION            = $REGION"
echo "REGISTRY                      = $REGISTRY_URL"
```

Add these six values under **GitHub → Repository Settings → Environments →
production → Environment secrets**. Do not add them as repository-level secrets
— `deploy.yml` scopes them to the `production` environment.

**Gate:**
```bash
# Push a trivial commit to main and watch the Actions tab.
# The CI workflow must pass all four jobs:
#   lint, unit-tests, tf-validate, k8s-validate
# The deploy workflow must pass:
#   build-and-push (6 images), build-control-plane, deploy (rolling update + smoke test)
git commit --allow-empty -m "chore: trigger deploy verification"
git push origin main
```

---

## Stage 10 — End-to-end verification

**What:** Run the Phase 7 smoke test against the live cluster to confirm the
complete pipeline works: submission → sandbox deploy → pre-flight gate → bot
fleet → leaderboard.

```bash
# Port-forward the control-plane service for the smoke test
kubectl port-forward svc/control-plane-svc 8080:8080 -n nexusbench &
PFPID=$!
sleep 2

export NEXUSBENCH_URL="http://localhost:8080"
export ADMIN_KEY="$ADMIN_KEY"  # the value set in Stage 0

# Run the full live smoke test
bash scripts/smoke_test_phase7.sh --live

kill $PFPID
```

Watch the worker autoscale. When a job lands in Redpanda, KEDA detects the lag
and scales the worker Deployment from 0 to 1. The Cluster Autoscaler then adds
a spot node to the worker pool to schedule the new pod.

```bash
# In a separate terminal, watch the worker pool scale up
kubectl get pods -n nexusbench -w &
kubectl get nodes -l role=benchmark-worker -w &

# Watch KEDA's HPA respond to the queue depth
kubectl get hpa -n nexusbench -w
```

**Gate:** The smoke test prints:
```
✅  Phase 7 pre-flight gate smoke test passed
```

Both A-series (broken engine) and B-series (correct engine) assertions must
pass. Specifically:
- `A1`: broken engine reaches `status=failed`
- `A2`: `dry_run_result` is non-null
- `B1`: correct engine reaches `status=completed`
- `B3`: `dry_run_result.all_passed=true`
- `B8`: correct engine appears on the leaderboard

---

## Stage 11 — Production hardening (before contest day)

These steps are not required to get the system running but must be completed
before exposing the platform to contestants.

### 11a — Tighten API server access

Replace the `0.0.0.0/0` CIDR in `terraform.tfvars` with your actual IP:

```bash
MY_IP=$(curl -sf https://checkip.amazonaws.com)
# Edit terraform/terraform.tfvars:
# master_authorized_cidr_blocks = [{ cidr_block = "$MY_IP/32", display_name = "ops" }]
cd terraform && terraform apply && cd ..
```

### 11b — Set deletion_protection on the cluster

In `terraform/modules/cluster/main.tf`, change:
```hcl
deletion_protection = true
```

Then apply:
```bash
cd terraform && terraform apply && cd ..
```

### 11c — Rotate the admin key

The `ADMIN_KEY` used in Stage 0 must be stored in a secrets manager, not in
a shell variable. Update the Kubernetes Secret:

```bash
NEW_KEY=$(openssl rand -hex 32)
kubectl create secret generic nexusbench-db-secrets \
  --namespace nexusbench \
  --from-literal=ADMIN_API_KEY="$NEW_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
# Restart the control-plane to pick up the new value
kubectl rollout restart deployment/control-plane -n nexusbench
```

### 11d — Enable GCS remote state (if not done in Stage 3)

Remote state is required if more than one team member will run `terraform apply`.
Follow Stage 3 now if it was skipped.

### 11e — Review NetworkPolicies

Audit the policies created in Stage 7j. The `default-deny-all` policy is in
place but the allow policies must be reviewed to ensure no unintended paths
are open. Specifically confirm workers cannot reach TimescaleDB or Postgres
directly — only the control-plane and consumer should have those paths.

---

## Rollback procedures

### Rollback a bad image deploy

The `deploy.yml` workflow uses `kubectl set image` + `rollout status`. If a
new image fails its readinessProbe, the rollout stalls (MaxUnavailable=0 means
the old pod is never killed). The old pod keeps serving traffic. No action
required beyond identifying the bad image and pushing a fix.

Manual rollback to the previous revision:
```bash
kubectl rollout undo deployment/control-plane -n nexusbench
kubectl rollout undo deployment/worker -n nexusbench
kubectl rollout status deployment/control-plane -n nexusbench
```

### Rollback a Terraform change

Terraform state tracks the previous configuration. To undo a specific change,
revert the `.tf` file and run `terraform apply`. Never run `terraform destroy`
unless you intend to tear down the entire cluster.

### Tear down everything

```bash
# Remove all K8s resources first (prevents VPC deletion from blocking on LB cleanup)
kubectl delete namespace nexusbench
kubectl delete namespace keda

# Then destroy infrastructure
cd terraform
terraform apply -var="cluster_name=$CLUSTER_NAME" # set deletion_protection=false first
terraform destroy
cd ..

# Delete the state bucket if you used remote state
gsutil rm -r gs://${PROJECT_ID}-nexusbench-tfstate
```

---

## Summary checklist

| Stage | Action | Time |
|-------|--------|------|
| 0 | Prerequisites verified | 5 min |
| 1 | GCP APIs enabled, ADC configured | 5 min |
| 2 | terraform.tfvars created and validated | 5 min |
| 3 | (Optional) Remote state bucket | 10 min |
| 4 | terraform apply — cluster + registry | 15 min |
| 5 | kubectl configured, nodes visible | 5 min |
| 6 | All 6 Docker images built and pushed | 15 min |
| 7 | Missing k8s manifests created and applied | 30 min |
| 8 | Application Deployments rolled out | 10 min |
| 9 | GitHub Actions secrets configured | 10 min |
| 10 | Smoke test passes end-to-end | 15 min |
| 11 | Production hardening | 20 min |
| **Total** | | **~2.5 hrs** |
