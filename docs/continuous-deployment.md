# Continuous deployment

`.github/workflows/deploy-main.yml` validates the repository, resolves an
immutable content key for each application component, and builds only content
that is not already present in GitHub Container Registry. Unchanged images are
retagged server-side with the new commit SHA without downloading layers or
rerunning Docker. New images target `linux/amd64`; every deployed revision
still uses a traceable commit-SHA tag. The complete application Helm release is
then applied after a push to `main`.

Processor dependencies are built once into a lockfile-keyed lite base.
Content-addressed bases separately package fetcher, quality, and KenLM
artifacts. Ordinary source changes reuse those bases, so they do not reinstall
Python or redownload multi-GB models. Each strict inference family has its own
image, Pod resources, Service, and HPA instead of one oversized curator image.

The workflow uses OpenConnect in Cisco AnyConnect-compatible mode. It does not
disable TLS certificate verification. If the VPN server uses a certificate
outside the runner's trust store, provide its OpenConnect certificate pin in
`CISCO_VPN_SERVERCERT`.

## GitHub configuration

Create a protected GitHub Environment named `production`. Add required
reviewers if deployments must be approved before they reach the cluster. Add
these Environment variables:

| Variable | Value or purpose |
| --- | --- |
| `CISCO_VPN_SERVER` | `drogon.dhbw-mannheim.de` |
| `CISCO_VPN_USERNAME` | VPN username. |
| `CISCO_VPN_AUTHGROUP` | Cisco connection profile, currently `Studierende` for this account. |
| `CISCO_VPN_SERVERCERT` | Leave empty initially. Add an OpenConnect pin only if certificate validation requires it. |
| `GHCR_PULL_USERNAME` | Optional registry username used by Kubernetes nodes. |

Add these Environment secrets:

| Secret | Purpose |
| --- | --- |
| `CISCO_VPN_PASSWORD` | VPN password. |
| `KUBECONFIG_B64` | Base64-encoded kubeconfig for the deployment identity. |
| `GHCR_PULL_TOKEN` | Optional token with `read:packages` for private GHCR images. |

The workflow uses the automatic `GITHUB_TOKEN` to publish images. If
`GHCR_PULL_TOKEN` is omitted, the images must be public in GHCR. For private
images, the workflow refreshes the namespace-local pull Secret on each deploy.

The cluster kubeconfig currently uses an IPv6 API address. After the VPN
tunnel is established, the workflow checks the runner's IPv6 routes and adds a
host route through the active tunnel only when the VPN did not install one.

Create `KUBECONFIG_B64` from a deployment-specific kubeconfig, not a personal
administrator kubeconfig, where the cluster supports a narrower identity. On
macOS, a one-line value can be produced with:

```sh
base64 < deploy-kubeconfig.yaml | tr -d '\n'
```

Do not commit VPN profiles, kubeconfig files, registry tokens, or private keys.

## Deployment and diagnostics

Provision platform, catalog, topics and credentials using the README first.
Normal application releases reuse immutable images and apply the declarative
Helm chart. Unchanged resources and model artifacts are not rebuilt.
Stateful progress remains on its retained PVC; deployment never resets source
offsets as a routine action.

The workflow updates the application tier. Terraform, edge networking,
Redpanda and catalog infrastructure have separate ownership.

Manual modes support deployment, a compact read-only pipeline check, detailed
diagnostics, UI audit, source/Foundry validation and storage inspection.
`capture-evidence` records matched resource, counter and object-size snapshots.
Validation modes that inject a controlled record are explicit operator tests,
not passive monitoring.

## Cisco compatibility note

This assumes the gateway accepts the Cisco AnyConnect-compatible SSL protocol
used by OpenConnect and does not require interactive MFA, posture checks, or a
proprietary client certificate. If the gateway rejects OpenConnect, install a
self-hosted GitHub runner on a VPN-connected VM and keep the same build and
Helm deployment stages there. That avoids putting VPN credentials in a
GitHub-hosted job.
