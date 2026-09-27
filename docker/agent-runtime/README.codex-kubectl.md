# Paperclip Codex + kubectl runtime proposal

This image adds the official Kubernetes `kubectl` v1.35.8 Linux amd64 binary
to Paperclip's stock Codex sandbox runtime. It keeps the upstream runtime
entrypoint, command, working directory, and uid/gid 1000. No credentials or
kubeconfig are included.

## Source and immutable base

Paperclip's `v2026.916.1` release source commit is `d554c47`. Its
`docker/agent-runtime/**` and `tools/agent-shim/**` trees are identical to
published image commit `38d8f371722b315d2fb3bbaa512518742e33ce2f`; the
source comparison was performed with:

```sh
git diff --quiet v2026.916.1 38d8f371722b315d2fb3bbaa512518742e33ce2f -- docker/agent-runtime tools/agent-shim
```

GHCR does not publish a `v2026.916.1` or `git-d554c47` tag for this image.
The equivalent published tag is
`ghcr.io/paperclipai/agent-runtime-codex:git-38d8f371722b315d2fb3bbaa512518742e33ce2f`.
Its immutable OCI index digest is
`sha256:3ec984b41bdcaa17690744f4e4123f3158e3f25b6c05a913e5945877b99cd8a4`.
The index contains a `linux/amd64` manifest and an `unknown/unknown` attestation
manifest. The Dockerfile pins the index by digest; use the specified amd64 build
platform so BuildKit selects the amd64 image.

The pinned image was published from Paperclip's stock Dockerfiles, whose Codex
layer installs `@openai/codex@latest`. That means the image's installed Codex
CLI version is the version present when that immutable image was built; the
v2026.916.1 source itself did not pin a Codex npm version.

## kubectl checksum

The Dockerfile downloads the binary from the official Kubernetes release host:

`https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl`

The expected SHA256 is
`874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf`, read
from the matching official `.sha256` endpoint. The image build verifies the
download with `sha256sum` before installing it as root-owned, executable
`/usr/local/bin/kubectl`.

## Build locally

From this directory:

```sh
docker buildx build \
  --platform linux/amd64 \
  --file Dockerfile.codex-kubectl \
  --tag paperclip-agent-runtime-codex-kubectl:v2026.916.1 \
  --load \
  .
```

No image build or runtime test was run while preparing this proposal.

## Read-only runtime and credentials

The v2026.916.1 Kubernetes provider runs containers as uid/gid 1000, drops all
capabilities, disallows privilege escalation, and sets
`readOnlyRootFilesystem: true`. Its pod spec mounts writable volumes at
`/workspace`, `/home/paperclip`, `/home/paperclip/.cache`, and `/tmp`. The added
binary lives under `/usr/local/bin` in the immutable image layer and only needs
to be read/executed, so it is compatible with that layout.

The v2026.916.1 provider disables automatic ServiceAccount token mounting. The
pilot's opt-in `agentApiAccess` flow uses a projected tenant ServiceAccount
token for opted-in runs instead of a static kubeconfig or credential Secret.
The image contains no token or kubeconfig; the projected token and tenant RBAC
remain runtime concerns.

## Paperclip image-reference wiring

In `v2026.916.1`, the Kubernetes plugin's fallback for `codex_local` is
`ghcr.io/paperclipai/agent-runtime-codex:v1`. The GHCR runtime package currently
publishes `git-*` image tags, not this `:v1` tag. After publishing the derived
image, set the pilot Kubernetes environment's `runtimeImage` to its final
repository and immutable digest. The provider's `adapters` registry can also
carry an image entry, but it replaces the built-in adapter defaults and must
include the Codex environment keys, egress domains, and probe command:

```json
[
  {
    "adapterType": "codex_local",
    "runtimeImage": "ghcr.io/ORG/agent-runtime-codex-kubectl@sha256:DIGEST",
    "envKeys": ["OPENAI_API_KEY"],
    "allowFqdns": ["api.openai.com"],
    "probeCommand": ["codex", "--version"]
  }
]
```

Replace `ORG` and `DIGEST` with the actual publication values. The
provider's `adapters` config can carry this entry; Paperclip also supports
instance-level `PAPERCLIP_ADAPTERS` / `PAPERCLIP_ADAPTERS_FILE` as another
source. A non-empty instance registry is authoritative for adapter
availability, so it must include every adapter intended to remain enabled.
`imageRegistry` only rewrites the registry/owner of the built-in image name; it
cannot change `agent-runtime-codex` to the new
`agent-runtime-codex-kubectl` repository. `imageAllowList` applies to per-run
`imageOverride` values; the configured default `runtimeImage` is resolved
directly. Private image registries can use the provider's `imagePullSecrets`.

## Primary sources

- [Paperclip v2026.916.1 source tag](https://github.com/paperclipai/paperclip/tree/v2026.916.1/docker/agent-runtime)
- [Paperclip Codex runtime package in GHCR](https://github.com/paperclipai/paperclip/pkgs/container/agent-runtime-codex)
- [Official Kubernetes kubectl v1.35.8 SHA256](https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl.sha256)
- [Kubernetes kubectl installation and checksum verification](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
