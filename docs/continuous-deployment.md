# Continuous deployment

`.github/workflows/deploy-main.yml` validates the repository, resolves an
immutable content key for each application component, and builds only content
that is not already present in GitHub Container Registry. Unchanged images are
retagged server-side with the new commit SHA without downloading layers or
rerunning Docker. New images target `linux/amd64`; every deployed revision
still uses a traceable commit-SHA tag. The complete application Helm release is
then applied after a push to `main`.

Processor dependencies are built once into a lockfile-keyed lite base.
Content-addressed bases separately package fetcher, quality, KenLM, and E5
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

## First deployment

The existing DHBW cluster still needs its documented one-time Helm migration
before this workflow can own the application release. The live release mixes
immutable selector schemes, and the curator StatefulSet has immutable fields
that differ from the chart. The workflow deliberately does not use
`helm upgrade --force`.

Complete the migration during an approved maintenance window:

1. Publish the application images or distribute identical image digests to all
   eligible nodes.
2. Recreate the affected stateless workloads with the clean chart selectors.
3. Plan the curator StatefulSet and checkpoint PVC migration separately.
4. Run a server-side dry-run and apply the chart.
5. Verify one controlled record through the pipeline and confirm all workloads
   are Ready.

After that migration, subsequent pushes to `main` use normal idempotent
Helmfile upgrades. An unchanged resource remains unchanged. A changed image
tag causes only the relevant workload to roll out.

The workflow updates the application release only. It does not reapply
Terraform, Ansible, Redpanda, Polaris, or the edge platform on every code
push. Those layers have separate ownership and maintenance procedures.

## Cisco compatibility note

This assumes the gateway accepts the Cisco AnyConnect-compatible SSL protocol
used by OpenConnect and does not require interactive MFA, posture checks, or a
proprietary client certificate. If the gateway rejects OpenConnect, install a
self-hosted GitHub runner on a VPN-connected VM and keep the same build and
Helm deployment stages there. That avoids putting VPN credentials in a
GitHub-hosted job.
