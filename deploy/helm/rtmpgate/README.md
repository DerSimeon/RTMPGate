# RTMPGate Helm chart

Deploys [RTMPGate](https://github.com/DerSimeon/RTMPGate) — a lightweight RTMP ingest router —
to Kubernetes.

## TL;DR

```bash
# From the published OCI registry (see "Publishing" below):
helm install rtmpgate oci://ghcr.io/dersimeon/charts/rtmpgate \
  --version 0.1.0 \
  --set secret.adminToken="$(openssl rand -hex 32)" \
  --set secret.redisUrl="redis://my-redis:6379"

# Or from a local checkout:
helm install rtmpgate ./deploy/helm/rtmpgate -f my-values.yaml
```

## Architecture notes

RTMPGate exposes **two** planes, deployed as two Services:

| Plane | Service                       | Default type | Purpose                                   |
|-------|-------------------------------|--------------|-------------------------------------------|
| RTMP  | `<release>-rtmpgate-rtmp`     | LoadBalancer | Ingest from publishers (OBS/ffmpeg).      |
| HTTP  | `<release>-rtmpgate`          | ClusterIP    | Control API (`/v1/*`) + `/metrics`.       |

- **RTMP is a raw TCP protocol — an Ingress cannot route it.** Expose the RTMP Service via
  `LoadBalancer` (recommended `externalTrafficPolicy: Local` to preserve the client IP) or
  `NodePort`. The HTTP plane should stay internal; reach it via `kubectl port-forward` or an
  internal Ingress you add yourself.
- Each publisher session is a long-lived TCP connection pinned to one pod. Rolling updates use
  `maxUnavailable: 0` and a `terminationGracePeriodSeconds` larger than
  `config.gracefulShutdownMs`, so the app drains in-flight publishers before exit (readiness
  flips to 503 first, then sessions drain up to the grace window).

## Probes

| Probe     | Path      | Notes                                                             |
|-----------|-----------|------------------------------------------------------------------|
| liveness  | `/livez`  | Process-only. Does **not** depend on Redis (avoids restart loops).|
| readiness | `/readyz` | 503 when storage is down or the pod is shutting down.            |
| startup   | `/livez`  | Covers slow first Redis connect.                                 |

## Redis / Valkey

`config.storage` defaults to `redis`. Provide the URL through `secret.redisUrl` (or an
`existingSecret` with a `redis-url` key). This chart does **not** deploy Redis — use managed
Memorystore/ElastiCache or a dedicated Valkey chart. For throwaway dev, set
`config.storage=memory` (routes are not persisted and not shared across replicas).

## Security

- Set `secret.adminToken` (or `secret.existingSecret`) to enable bearer auth on write
  endpoints. **Without it, route/session management is unauthenticated.**
- Enable `config.requireReadAuth=true` to also require the token on read endpoints.
- The pod runs as non-root with `readOnlyRootFilesystem`, dropped capabilities, and
  `seccompProfile: RuntimeDefault` by default.

## Key values

| Key                                   | Default                        | Description                                        |
|---------------------------------------|--------------------------------|----------------------------------------------------|
| `replicaCount`                        | `2`                            | Pod replicas (ignored when autoscaling.enabled).   |
| `image.repository` / `image.tag`      | `ghcr.io/dersimeon/rtmpgate` / appVersion | Container image.                       |
| `config.storage`                      | `redis`                        | `redis`, `valkey`, or `memory`.                    |
| `config.targetHostAllowlist`          | `""`                           | CSV of allowed upstream hosts (set in prod).       |
| `config.maxActiveSessions`            | `1000`                         | Global session cap.                                |
| `config.maxSessionsPerIp`             | `10`                           | Per-IP session cap.                                |
| `config.requireReadAuth`              | `false`                        | Require token on GET endpoints.                    |
| `config.gracefulShutdownMs`           | `15000`                        | Drain window on SIGTERM.                           |
| `secret.adminToken`                   | `""`                           | Bearer token for write (and optionally read) auth. |
| `secret.redisUrl`                     | `redis://valkey:6379`          | Redis/Valkey URL.                                  |
| `service.rtmp.type`                   | `LoadBalancer`                 | How RTMP ingest is exposed.                        |
| `resources`                           | 0.5–1 CPU / 0.5–3Gi            | Aligned with the upstream soak matrix.             |
| `autoscaling.enabled`                 | `false`                        | HPA (CPU/memory).                                  |
| `podDisruptionBudget.enabled`         | `true`                         | PDB (`minAvailable: 1`).                           |
| `metrics.serviceMonitor.enabled`      | `false`                        | Prometheus Operator ServiceMonitor.               |

See [`values.yaml`](values.yaml) for the complete list.

## Publishing

The chart is published as an OCI artifact to `oci://ghcr.io/dersimeon/charts` by
`.github/workflows/helm-publish.yml` on a `chart-v*` tag (or manual dispatch). Locally:

```bash
helm lint deploy/helm/rtmpgate
helm package deploy/helm/rtmpgate
helm push rtmpgate-0.1.0.tgz oci://ghcr.io/dersimeon/charts
```
