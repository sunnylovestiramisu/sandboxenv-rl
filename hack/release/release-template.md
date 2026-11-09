# Agent Substrate Environments ($TAG)

We are pleased to announce the release of **Agent Substrate Environments (`sandboxenv-rl`) $TAG**!

<!--
Optional: Uncomment if this is an alpha, beta, or release candidate:
### ⚠️ Pre-Release Notice
This is a pre-release version of `sandboxenv-rl`. It is intended for testing and validation.
-->

<!--
Optional: Uncomment if there are breaking changes or manual migrations required:
### ⚠️ Breaking Changes / Action Required
- **[Component]**: Description of breaking change and migration steps (#PR).
-->

### Highlights

#### Core Runtime, CLI & Substrate
- Highlight 1 (#PR)

#### Python SDK & Fleet
- Highlight 1 (#PR)

### Full Changelog
$CHANGELOG

### Installation

#### Go CLI (`ate-env`)
Download the pre-built binary from the GitHub Release assets:
```bash
# Example for Linux amd64
curl -sSLO "https://github.com/ai-on-gke/sandboxenv-rl/releases/download/$TAG/ate-env_${TAG}_linux_amd64"
chmod +x "ate-env_${TAG}_linux_amd64"
sudo mv "ate-env_${TAG}_linux_amd64" /usr/local/bin/ate-env
```

#### Container Images
Container images are published to GitHub Container Registry (GHCR):
```bash
# Agent Substrate Environment API server
docker pull ghcr.io/ai-on-gke/sandboxenv-rl/ate-env-api:$TAG

# Guest agent init/exec daemon
docker pull ghcr.io/ai-on-gke/sandboxenv-rl/ate-env-guest:$TAG
```

Verify image signatures with Cosign:
```bash
cosign verify \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  --certificate-identity-regexp '^https://github.com/ai-on-gke/sandboxenv-rl/\.github/workflows/release\.yaml@refs/tags/' \
  ghcr.io/ai-on-gke/sandboxenv-rl/ate-env-api:$TAG
```

#### Python Client SDK
<!-- Include the PyPI snippet below ONLY when PyPI publishing is enabled (`vars.PYPI_PUBLISH_ENABLED=true`) and not skipped for this release: -->
Install the async Python client SDK from PyPI (`$VERSION` is the PEP 440 version, e.g. `0.1.0` for `v0.1.0` or `0.1.0rc1` for `v0.1.0-rc1`):
```bash
pip install "ate-env-rl==$VERSION"
```
<!-- When PyPI publishing is skipped or not yet configured (`vars.PYPI_PUBLISH_ENABLED` unset/false), replace the PyPI block above with this Git-tag install snippet instead:
Install the async Python client SDK from the release tag (PyPI package publishing is not enabled for this release):
```bash
pip install "git+https://github.com/ai-on-gke/sandboxenv-rl.git@$TAG#subdirectory=clients/python"
```
-->

### Server & Runtime Dependencies
$SERVER_DEPENDENCIES

### Contributors
$CONTRIBUTORS
