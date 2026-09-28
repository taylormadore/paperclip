# Paperclip Codex + kubectl runtime

This derived image pins the official Codex CLI to `@openai/codex@0.155.1` and
adds the official Kubernetes `kubectl` v1.35.8 Linux amd64 binary to Paperclip's
Codex runtime. It preserves the upstream entrypoint, command, workdir, and
uid/gid 1000. The image contains no credentials or kubeconfig.

## Codex version pin

The immutable upstream image below was built from Paperclip's stock Codex
Dockerfile, which installs `@openai/codex@latest`. Its installed CLI version is
therefore fixed at the time that image was published; the pinned base reports
`codex-cli 0.149.0`. The derived image installs the exact
`@openai/codex@0.155.1` version to align with the confirmed local CLI version
and keep the Codex CLI release fixed across rebuilds. The Docker build checks
that `codex --version` reports `codex-cli 0.155.1`.

The official [npm metadata for version 0.155.1](https://registry.npmjs.org/%40openai%2Fcodex/0.155.1)
declares Node.js `>=16` and optional aliases for six OS/CPU-specific packages.
For this `linux/amd64` image, the alias resolves to
`@openai/codex@0.155.1-linux-x64`; its [official npm metadata](https://registry.npmjs.org/%40openai%2Fcodex/0.155.1-linux-x64)
declares `os: linux` and `cpu: x64`. npm installs it as part of the
exact-version package command.

## Immutable base and kubectl checksum

Paperclip's `v2026.916.1` source commit is `d554c47`. Its
`docker/agent-runtime/**` and `tools/agent-shim/**` trees are identical to
published image commit `38d8f371722b315d2fb3bbaa512518742e33ce2f`; the source
comparison was performed with:

```sh
git diff --quiet v2026.916.1 38d8f371722b315d2fb3bbaa512518742e33ce2f -- docker/agent-runtime tools/agent-shim
```

GHCR does not publish a `v2026.916.1` or `git-d554c47` tag for this image. The
equivalent published tag is
`ghcr.io/paperclipai/agent-runtime-codex:git-38d8f371722b315d2fb3bbaa512518742e33ce2f`.
Its immutable OCI index digest is
`sha256:3ec984b41bdcaa17690744f4e4123f3158e3f25b6c05a913e5945877b99cd8a4`.
The index includes a `linux/amd64` manifest and an `unknown/unknown` attestation
manifest, so build with the amd64 platform.

The Dockerfile downloads kubectl from the official Kubernetes release host:

`https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl`

The expected SHA256 is
`874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf`, read from
the matching official `.sha256` endpoint. The image build verifies the
download before installing it as root-owned, executable `/usr/local/bin/kubectl`.

## Build and restricted local smoke

Build the local pilot image from this directory:

```sh
podman build \
  --platform linux/amd64 \
  --file Dockerfile.codex-kubectl \
  --tag paperclip-agent-runtime-codex-kubectl:pilot-2 \
  .
```

Smoke the CLI version, kubectl client, and non-root identity without network or
credentials:

```sh
podman run --rm \
  --network none \
  --read-only \
  --cap-drop all \
  --security-opt no-new-privileges \
  --user 1000:1000 \
  --entrypoint /bin/sh \
  paperclip-agent-runtime-codex-kubectl:pilot-2 \
  -ec 'test "$(id -u)" = 1000; test "$(id -g)" = 1000; codex --version; kubectl version --client --output=yaml'
```

## Runtime notes

The Kubernetes provider runs this image as uid/gid 1000, drops capabilities,
disallows privilege escalation, and sets `readOnlyRootFilesystem: true`. Its pod
spec mounts writable volumes at `/workspace`, `/home/paperclip`,
`/home/paperclip/.cache`, and `/tmp`. The added kubectl binary and pinned Codex
installation are read/executable from the image layer and fit that layout.

The provider disables automatic ServiceAccount token mounting. Its opt-in
`agentApiAccess` flow uses a projected tenant ServiceAccount token instead of a
static kubeconfig or credential Secret. Set the pilot's `runtimeImage` to the
final repository and immutable digest after image publication; this local
pilot tag is not a cluster image reference.

## Primary sources

- [Paperclip v2026.916.1 runtime source](https://github.com/paperclipai/paperclip/tree/v2026.916.1/docker/agent-runtime)
- [Paperclip Codex runtime package in GHCR](https://github.com/paperclipai/paperclip/pkgs/container/agent-runtime-codex)
- [Official npm metadata for `@openai/codex@0.155.1`](https://registry.npmjs.org/%40openai%2Fcodex/0.155.1)
- [Official npm metadata for the Linux x64 Codex package](https://registry.npmjs.org/%40openai%2Fcodex/0.155.1-linux-x64)
- [Official Kubernetes kubectl v1.35.8 SHA256](https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl.sha256)
- [Kubernetes kubectl installation and checksum verification](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
