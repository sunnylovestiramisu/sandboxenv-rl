# Terminal-Bench 2.0 on ate-env

Run a Harbor dataset with each trial in an ate-env environment, through the
provider in [`integrations/harbor`](../../integrations/harbor/README.md). The
steps below use Terminal-Bench 2.0; any task directory or registry dataset
works the same way.

## 1. Prerequisites

- A cluster with ate-env deployed ([deployment guide](../../README.md#deployment-guide))
  and `ate-env-api` port-forwarded:

  ```bash
  kubectl -n ate-env port-forward svc/ate-env-api 7777:7777 &
  export SUBSTRATE_ENV_API=127.0.0.1:7777
  export SUBSTRATE_ENV_AUTH_TOKEN=$(kubectl -n ate-env get secret ate-env-api-token -o jsonpath='{.data.token}' | base64 -d)
  ```

- A registry the worker nodes can pull from, for the task images. With Artifact
  Registry, the node pool's service account needs `roles/artifactregistry.reader`
  on the repository, and your machine a login (`gcloud auth configure-docker
  <region>-docker.pkg.dev`):

  ```bash
  export SUBSTRATE_ENV_HARBOR_REGISTRY=us-central1-docker.pkg.dev/<project>/harbor
  ```

- Python 3.12 or newer, docker with buildx, and the two packages from this checkout:

  ```bash
  pip install -e clients/python -e integrations/harbor
  ```

- Egress from the actors. Most tasks and every installed agent download packages
  while they run; ate-env sets no egress policy, what an actor reaches follows the
  Substrate defaults. Check once with a throwaway environment:

  ```bash
  ate-env create hb-probe --rootfs-image ubuntu@sha256:<digest>
  ate-env hb-probe shell 'apt-get update >/dev/null && echo egress ok'
  ate-env delete hb-probe
  ```

## 2. Get the tasks

```bash
harbor datasets download terminal-bench@2.0 -o ./tb2   # ./tb2/terminal-bench/<task>/...
```

## 3. Images

Terminal-Bench 2.0 tasks already name a prebuilt image in `task.toml` (`docker_image`,
for example `alexgshaw/adaptive-rejection-sampler:20251031`). With the default
`HB_BUILD=none` the provider resolves that tag to a digest and the actors pull it from
Docker Hub: no build step, but the nodes need egress to Docker Hub and meet its pull
limits.

To serve the images from your own registry instead, mirror the dataset once. Harbor
rewrites `docker_image` in the task files only with `--override-config`, because the
tasks already have one; without the flag the push happens and the run keeps pulling
from Docker Hub:

```bash
harbor admin upload-images -t ./tb2/terminal-bench -r "$SUBSTRATE_ENV_HARBOR_REGISTRY" \
  --tag tb2 --push --update-config --override-config
```

The third option, `HB_BUILD=local` (or `cloud-build` for a build in your GCP project),
builds each task's `environment/Dockerfile` at its first trial. Builds are skipped when
the tag exists and happen once per task, but a run at `-n 50` then starts with up to 50
builds on your machine.

## 4. Register the resource classes

Substrate requires CPU and memory limits on every template, so each (cpus,
memory_mb) pair a task asks for needs a base template named for it. The script
prints one `ate-env manifest template` command per class the dataset uses; the
flags after `--` go on each, at least the guest image and the snapshots bucket
from your deployment:

```bash
integrations/harbor/hack/register-templates.sh ./tb2/terminal-bench -- \
  --guest-image <registry>/ate-env-guest@sha256:<digest> | sh
```

Without a class template the provider falls back to `default-template` with a
warning (the `auto` policy); `harbor run --cpus limit` makes that an error.

## 5. Run

```bash
examples/harbor/run-tb2.sh -p ./tb2/terminal-bench -a oracle -n 10
```

`run-tb2.sh` is `harbor run` with the provider selected and its settings read
from the environment (`HB_BUILD`, `HB_TEMPLATE`, `HB_ATESPACE`,
`SUBSTRATE_ENV_HARBOR_REGISTRY`); every flag is passed through. The `oracle`
agent runs each task's reference solution and needs no model: its rewards are
the dataset's ground truth, which makes it the right first run and the right
comparison with Docker. With a model:

```bash
export ANTHROPIC_API_KEY=...   # or the provider your model needs
examples/harbor/run-tb2.sh -p ./tb2/terminal-bench -a terminus-2 -m anthropic/claude-sonnet-5-5 -n 50
```

Keep the token in `SUBSTRATE_ENV_AUTH_TOKEN`, not in `--ek api_token=`: Harbor
records the environment kwargs in each trial's `result.json`.

## 6. Read the results

```bash
harbor view jobs/<job>
cat jobs/<job>/<trial>/result.json | jq .verifier_result.rewards
cat jobs/<job>/<trial>/ate_env.json          # environment id, atespace, template, image digest
```

`--no-delete` keeps each environment suspended after its trial; the log names
the `ate-env <id> shell` command to inspect it. Delete them when done:
`ate-env delete <id>`.

## 7. Compare with Docker

The same command with `-e docker` (Harbor's default) runs the tasks in local
containers:

```bash
harbor run -p ./tb2/terminal-bench -a oracle -n 10 -e docker
```

For `oracle` the rewards must match task by task. The numbers below are from
2026-11-03 on `rl-testing-tomer` (GKE, us-central2, Substrate v0.6.0, five
gVisor workers, `ate-env-api` through a port-forward): Harbor's `nop` agent
on the `echo` task (`integrations/harbor/tests/tasks/echo`, a prebuilt alpine
image), 10 and 50 trials at `-n 10` and `-n 50`, every environment deleted at
the end. The durations are Harbor's own, from each trial's `result.json`:
`environment_setup` is the create, the first guest answer and the
`environment/` upload; the verifier phase is the `tests/` upload, five
commands and the download of `/logs/verifier`.

| Measure | ate-env, plain create, `-n 10` | ate-env, plain create, `-n 50` | ate-env, warm pool | Docker |
|---|---|---|---|---|
| Environment setup, p50 / p95 | 2.6 s / 3.9 s | 2.2 s / 7.6 s | deferred (plan decision D-8) | not run: the measuring machine has no `docker compose` plugin |
| Verifier phase, p50 / p95 | 1.7 s / 1.7 s | 1.7 s / 1.7 s | | |
| Trial, p50 / p95 | 6.4 s / 7.7 s | 6.5 s / 11.5 s | | |
| Job wall time | 7.9 s | 11.9 s | | |
| Infrastructure errors | 0 of 10 | 0 of 50 | | |

`hello-world` itself was not the task: its verifier downloads packages, and
the actors on that cluster have no egress.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `InvalidArgument: image ... is not pinned by digest` | A tag reached the API: `docker` is not on PATH, so the tag could not be resolved. Install docker or pin the digest in `task.toml`. |
| `FailedPrecondition: base actor template "harbor-c..." not found` | Register the class template (step 4). Under the `auto` policy this is a warning and the run goes on with `default-template`. |
| The create succeeds, then `did not answer within 600s` | The nodes cannot pull the image from the registry, the image has no `sh`, or port 80 is taken inside it. Check the derived template's state and the worker pod logs. |
| `apt-get` or `curl` fail inside the task | No egress from the actors (prerequisites). |
| The verifier fails at its first command with a permission error | The image runs as a non-root user and has no `sudo`; see the provider README's limits. |
| A guest call hangs, then fails with `Unavailable` and `connection refused` | The router's allow-list is empty, thus the router serves no TLS listener. ate-setup does not check the list. Set it to ate-env-api's SPIFFE ID (root README, deployment guide). |
| A guest call fails at once with `FailedPrecondition` about the client certificate | The router rejected `ate-env-api`'s certificate: its SPIFFE ID is missing from the router's allow-list. |
