# gitea-autoscaler

A small Kubernetes controller that autoscales a Deployment of Gitea Actions
runners based on the job queue — no Prometheus, no KEDA, no CRDs.

## What it is

The controller runs inside the cluster and polls the Gitea **admin** Actions
API (`GET /api/v1/admin/actions/jobs?status=queued&status=in_progress`,
paginated) every `POLL_INTERVAL` seconds. From the result it computes

```
desired = clamp(queued + in_progress_on_live_runners, MIN_REPLICAS, MAX_REPLICAS)
```

and patches the runner Deployment's scale:

- **Scale up** — immediately patches the Deployment replicas.
- **Scale down** — only after a cooldown (`SCALE_DOWN_DELAY` seconds of the
  surplus persisting): it patches the `pod-deletion-cost` annotation high on
  busy runners (and resets idle ones), then shrinks the Deployment's replica
  count, so the ReplicaSet evicts idle pods first and a pod running a job is
  never a victim.
- **Stale in-progress jobs** — in-progress jobs whose runner no longer exists
  in Kubernetes are ignored, so vanished runners don't pin the scale up.
- **Stale-runner sweep** — deletes Gitea runner registrations that are
  offline, not busy, and have no matching pod in the cluster. The sweep is
  scoped to this controller's own fleet: only registrations whose name
  starts with `{RUNNER_DEPLOYMENT}-` are ever considered, so runners owned
  by others (e.g. persistent VM runners) are never touched.

## Provenance

Adapted from [github.com/bearlyplayz/gitea-autoscaler](https://github.com/bearlyplayz/gitea-autoscaler)
(Apache-2.0, see `LICENSE`). Our deltas:

1. **Sweep scoping** — the stale-registration sweep is restricted to runners
   whose names carry the autoscaled Deployment's pod-name prefix
   (`{RUNNER_DEPLOYMENT}-…`). Upstream swept every offline registration in
   the instance.
2. **Optional job-label filter** — when `JOB_LABEL` is set, only jobs
   carrying that `runs-on` label count toward scaling (see below). Upstream
   counted every queued/in-progress job.
3. **Scale-down via pod-deletion-cost** — surplus is removed by shrinking
   the Deployment while busy runners carry a high `pod-deletion-cost`
   annotation, instead of deleting idle pods by hand. Upstream deleted pods
   directly, which briefly makes the ReplicaSet spawn replacements.

## Configuration

All knobs are environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `GITEA_URL` | Yes | — | Base URL of the Gitea instance (e.g. `http://gitea-http.gitea.svc.cluster.local:3000`) |
| `GITEA_TOKEN` | One of | — | Site-admin API token (takes precedence over basic auth) |
| `GITEA_USERNAME` | One of | — | Basic-auth username (with `GITEA_PASSWORD`) |
| `GITEA_PASSWORD` | One of | — | Basic-auth password (required with `GITEA_USERNAME`) |
| `RUNNER_DEPLOYMENT` | No | `gitea-actions-runner` | Name of the Deployment to scale; also scopes the stale sweep to its pod-name prefix |
| `RUNNER_NAMESPACE` | No | `gitea` | Namespace of the runner Deployment |
| `MIN_REPLICAS` | No | `1` | Minimum replica count (floor) |
| `MAX_REPLICAS` | No | `3` | Maximum replica count (ceiling) |
| `POLL_INTERVAL` | No | `10` | Seconds between polling cycles |
| `SCALE_DOWN_DELAY` | No | `1000` | Seconds a scale-down must remain pending before it executes (cooldown) |
| `JOB_LABEL` | No | unset | Only count jobs whose `runs-on` labels contain this value |

Set `RUST_LOG=debug` for verbose logging.

### Job-label filter

Runner fleets usually serve only jobs that `runs-on:` one specific label
(e.g. `k8s`). When `JOB_LABEL` is set, jobs without that label are invisible
to the autoscaler: they contribute neither to the queued count nor to the
busy-runner set, so work served by other runner fleets never scales this
Deployment. Leave it unset to count every job (upstream behavior).

## RBAC for in-cluster use

The controller lists/deletes pods and reads/patches the Deployment (and its
`scale` subresource) in the runners' namespace:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gitea-autoscaler
  namespace: gitea
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gitea-autoscaler
  namespace: gitea
rules:
  - apiGroups: ["apps"]
    resources: ["deployments", "deployments/scale"]
    verbs: ["get", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: gitea-autoscaler
  namespace: gitea
subjects:
  - kind: ServiceAccount
    name: gitea-autoscaler
roleRef:
  kind: Role
  name: gitea-autoscaler
  apiGroup: rbac.authorization.k8s.io
```

Run the controller with that ServiceAccount, e.g.:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gitea-autoscaler
  namespace: gitea
spec:
  replicas: 1
  selector:
    matchLabels: { app: gitea-autoscaler }
  template:
    metadata:
      labels: { app: gitea-autoscaler }
    spec:
      serviceAccountName: gitea-autoscaler
      containers:
        - name: gitea-autoscaler
          image: ghcr.io/plumelo/gitea-autoscaler:main
          env:
            - name: GITEA_URL
              value: http://gitea-http.gitea.svc.cluster.local:3000
            - name: GITEA_TOKEN
              valueFrom:
                secretKeyRef: { name: gitea-autoscaler, key: token }
            - name: RUNNER_DEPLOYMENT
              value: gitea-actions-runner
            - name: RUNNER_NAMESPACE
              value: gitea
            - name: MAX_REPLICAS
              value: "12"
            - name: JOB_LABEL
              value: k8s
```

## Security

The Gitea credentials must belong to a **site-admin** account: the controller
reads `/api/v1/admin/actions/jobs` (the admin queue API) and administers
runner registrations for the stale sweep, which includes *deleting*
registrations. Use a dedicated PAT with the narrowest scope that works —
`write:admin` (no other categories) — and pass it via a Kubernetes `Secret`,
never inline. The image runs as a non-root user (`65534:65534`) and needs no
filesystem writes.

## Image

Prebuilt at `ghcr.io/plumelo/gitea-autoscaler`:

- `main` — every push to `main`
- `vX.Y.Z` — releases (published when a `v*` git tag is cut)