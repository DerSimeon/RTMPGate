# Deploying RTMPGate with Kustomize

RTMPGate ships a [Helm chart](../helm/rtmpgate) as its primary packaging. If your platform is
Kustomize-based, you have two supported options. Both keep the Helm chart as the single source
of truth for the manifests, so you don't re-implement Deployments/Services by hand.

> **Pin the chart version** in every example below for reproducible renders.

---

## Option 1 — Inflate the Helm chart from Kustomize (`helmCharts`)

Kustomize can render a Helm chart inline via the `helmCharts` field. This is the least
duplication: Kustomize pulls the chart, applies your `values`, then lets you layer patches.

`base/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

helmCharts:
  - name: rtmpgate
    repo: oci://ghcr.io/dersimeon/charts
    version: 0.1.0
    releaseName: rtmpgate
    namespace: rtmpgate
    valuesInline:
      replicaCount: 2
      config:
        storage: redis
        targetHostAllowlist: "rtmp-a.internal,rtmp-b.internal"
```

`overlays/prod/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: rtmpgate
resources:
  - ../../base

patches:
  # Bump replicas and tighten resources for production.
  - target:
      kind: Deployment
      name: rtmpgate
    patch: |-
      - op: replace
        path: /spec/replicas
        value: 4
      - op: replace
        path: /spec/template/spec/containers/0/resources/limits/cpu
        value: "2"

  # Point RTMPGate at the production Redis and require read auth.
  - target:
      kind: ConfigMap
      name: rtmpgate
    patch: |-
      - op: replace
        path: /data/RTMPGATE_REQUIRE_READ_AUTH
        value: "true"
```

Render and apply (Helm inflation requires the `--enable-helm` flag):

```bash
kubectl kustomize --enable-helm overlays/prod        # inspect
kubectl apply -k overlays/prod --enable-helm         # apply
```

> `--enable-helm` runs `helm template` under the hood, so Helm must be installed and, for an
> OCI `repo:`, you must be logged in: `helm registry login ghcr.io`.

### Managing the admin token

Do **not** put `secret.adminToken` in `valuesInline` (it would land in a rendered Secret in
git). Instead let the chart create an empty Secret, or set `secret.existingSecret`, and manage
the token separately — e.g. a Kustomize `secretGenerator` with an external file, or a Sealed
Secret / External Secret you add to the overlay:

```yaml
secretGenerator:
  - name: rtmpgate
    behavior: merge
    literals:
      - admin-token=REPLACE_ME   # source from a file/env, not committed
```

---

## Option 2 — Post-render an inflated chart (committed base)

If your GitOps flow forbids running Helm at apply time, inflate the chart once and commit the
output as a plain-manifest base, then patch it with overlays.

Generate the base:

```bash
helm template rtmpgate oci://ghcr.io/dersimeon/charts/rtmpgate \
  --version 0.1.0 \
  --namespace rtmpgate \
  --set config.storage=redis \
  > base/rtmpgate.yaml
```

`base/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - rtmpgate.yaml
```

`overlays/dev/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: rtmpgate-dev
resources:
  - ../../base

images:
  - name: ghcr.io/dersimeon/rtmpgate
    newTag: "1.0.0"

patches:
  - target:
      kind: ConfigMap
      name: rtmpgate
    patch: |-
      - op: replace
        path: /data/RTMPGATE_STORAGE
        value: "memory"
  - target:
      kind: Service
      name: rtmpgate-rtmp
    patch: |-
      - op: replace
        path: /spec/type
        value: NodePort
```

Apply (no `--enable-helm` needed here — the base is plain YAML):

```bash
kubectl apply -k overlays/dev
```

Re-run the `helm template` step whenever you bump the chart version to refresh the base.

---

## Which option?

| | Option 1 (`helmCharts`) | Option 2 (post-render) |
|---|---|---|
| Helm at apply time | Required (`--enable-helm`) | Not required |
| Drift from chart | None (rendered each apply) | Base can go stale |
| Best for | `kubectl -k`, Argo CD with Helm enabled | Strict plain-YAML GitOps |

For most users, **Option 1** is the least maintenance. Use **Option 2** when your delivery
pipeline cannot execute Helm.
