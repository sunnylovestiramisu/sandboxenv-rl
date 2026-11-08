---
name: swebench-loader-simulated-run
description: Runs one simulated RL measurement with swebench-loader on a cluster with Agent Substrate and ate-env (cold boot, cold or warm restore, suspend and wake, burst, density), after checking the cluster conditions that silently change what a run measures.
---

# Simulated runs with swebench-loader

Use this skill when asked to measure startup, restore, wake or density on Substrate without a
model: the Fast, ML Bench, high-cardinality and density rows of the RL benchmark plan. For
hooking up a real RL recipe or the `bench` harness and its bundles, use `benchmarking/SKILL.md`.
For which run shape measures what, read "Cold boot, cold and warm restore, suspend and wake" in
`README.md` in this directory; this skill does not repeat it. For a mixed two-shard R2E-Gym and
SWE-bench run with configurable concurrency, image count, and rollouts per image, use
[`./r2e-swebench/SKILL.md`](./r2e-swebench/SKILL.md).

## 1. Check the cluster before the first run

Each check below changed a result this week without any error in the loader. Run them all; each
is one command.

1. **Templates declare limits.** `kubectl-ate get actor-template BASE -a ATESPACE -o yaml` must
   show `resources.limits` with cpu and memory. A base template with `limits: {}` (and every
   template derived from it) is admitted without any resource check, up to the worker's actor cap
   of 1000; slot arithmetic is then meaningless. Newer servers refuse such templates; older ones
   keep the ones they have. Create a new base template with `ate-env manifest template --template
   NAME --guest-image IMAGE --cpu 2 --memory 4Gi` and run with `-base-template NAME`.
2. **Worker capacity is what you think.** The API log's GetWorker response, or `kubectl-ate get
   workers -o yaml`, shows each worker's `capacity`. With no limits on the WorkerPool template
   each worker registers the node's allocatable, so N workers on one node each claim the whole
   node. Set `spec.template.resources.limits` on the pool for a fixed slot count.
3. **Every template has a golden before a restore measurement.** Ask the API per image:
   `ate-env image-status <image>` prints `golden: ready|pending|failed` for the derived template
   (`--ensure` creates a missing template, which starts the bake, with no environment); poll
   until every image is `ready`. Without the API: `kubectl-ate get actor-templates -a ATESPACE -o json`,
   field `.actorTemplates[].status.goldenSnapshotStatus.goldenTag.name`; a template whose bake
   failed has `.errorMessage` instead and never gets a tag, so compare against the count without
   an error rather than the total. Goldens bake at 5 concurrent per ateapi replica, about two
   minutes each; thousands of new templates mean hours. An actor whose template has no golden
   boots from the image, silently. Wait for the counts to match, or scale `ate-api-server`
   replicas, or label the run a cold boot.
4. **atelet's image cache GC is ahead of kubelet's eviction.** `kubectl -n ate-system get ds
   atelet -o jsonpath='{.spec.template.spec.containers[0].args}'` should show
   `--image-cache-gc-period`, `--image-cache-high-percent` and `--image-cache-low-percent`. The
   defaults (5 min, 85, 80) start evicting at the same 85% where kubelet declares DiskPressure,
   and kubelet cannot free atelet's cache. For churn, 30s / 70 / 60. Budget node disk as images
   in flight per node times unpacked size (2 to 2.5 times compressed).
5. **One CPU family in the pool.** `gcloud compute instances list --format='value(name,cpuPlatform)'`
   over the worker nodes. A golden restores with memory only on the family it was baked on;
   elsewhere it relaxes to a rootfs restore, in effect a boot, with nothing in the events saying
   so. Label one family into the pool, or pin `--min-cpu-platform` on the node pools.
6. **Nodes can reach every registry a sandbox start needs.** On Substrate before v1.0.0-rc.1
   (commit 4f71f0ea), a gVisor sandbox pulls a pause image from `registry.k8s.io`; private nodes
   without Cloud NAT cannot reach it, every sandbox start fails with an i/o timeout in atelet's
   log, and the golden bake never completes. Editing the SandboxConfig's `pauseImage` does not
   take effect on a running build; add a NAT scoped to the cluster's subnet. From v1.0.0-rc.1 on,
   gVisor runs with `-no-root-container`, pulls no pause image, and the CRD has no `pauseImage`;
   the check is then only that nodes reach the registry the task images come from and the
   bucket the gVisor assets come from.
7. **The worker selector matches something.** `ate-env manifest template --workerpool` writes a
   `workerSelector`; on clusters whose workers carry no labels it matches nothing and placement
   fails with "no worker has room". Compare with a working template; drop the selector if it has
   none.
8. **The manifest has one reference per image.** `make-dataset.py --images-file` drops exact
   digest duplicates. References that resolve to the same platform image under different index
   digests still collide in the API ("exists but runs <other reference>"); such creates are
   refused and counted as failed, not run.

## 2. Run

From the directory where results should land, never from inside the repository (the events
file must not dirty the tree ko builds from). The scripts are addressed through the checkout,
and `run-in-cluster.sh` builds the loader image with ko, so the `report` binary is built
separately into the results directory:

```bash
export ENV_REPO=<path to the sandboxenv-rl checkout>
export KO_DOCKER_REPO=<registry the cluster pulls from>
"${ENV_REPO}/benchmarking/swebench/sample-node-cache.sh" before-RUN.txt
RESTORE_LOG=restores-RUN.jsonl AUTH_TOKEN_SECRET=ate-env-api-token "${ENV_REPO}/benchmarking/swebench/run-in-cluster.sh" RUN DATASET.jsonl <shape flags> -label arm=NAME
"${ENV_REPO}/benchmarking/swebench/sample-node-cache.sh" after-RUN.txt
go -C "${ENV_REPO}" run ./cmd/benchmarking/bench measure -k8s -atespace ATESPACE -run-id RUN -dest "${PWD}/bundle-RUN"
go -C "${ENV_REPO}" build -o "${PWD}/swebench-loader" ./cmd/benchmarking/swebench-loader
"${PWD}/swebench-loader" report RUN.jsonl -restore-log restores-RUN.jsonl -cache-before before-RUN.txt -cache-after after-RUN.txt -unresolved unresolved-RUN.txt
```

- `AUTH_TOKEN_SECRET` names the Secret with ate-env-api's caller token; without it every create
  fails with `Unauthenticated`. `arm` is the only label to pass: the loader records its own flags,
  and the `bench measure -k8s` line records the cluster, node pools, Kubernetes version,
  Substrate version and loader commit into `bundle-RUN/system_description.json`. Run it through
  `go -C "${ENV_REPO}"` with an absolute `-dest`: the loader commit comes from `git rev-parse HEAD`
  in the working directory. The boot disk type is the one thing neither records; put it in the
  arm name when it is the variable (`arm=pull-hyperdisk`).
- `RESTORE_LOG` harvests atelet's restore records every ten minutes during the run; kubelet
  rotates a busy atelet's log within the hour and one harvest at the end loses most of a long run.
- Do not chain the sampler with `&&`: it exits non-zero when any node scrape fails, and at
  hundreds of nodes one failed scrape is normal. Run the pieces one after another.
- A run in tmux survives a dropped connection; a run in a plain shell does not.
- One run id per arm, the same shape flags across arms, and `report -arm NAME=FILE` to compare;
  it refuses to pool runs whose shape labels differ.
- To assemble a complete `v1` benchmark bundle (`events/`, `cache_before.prom` / `cache_after.prom`,
  `atelet_restores.jsonl` joined into `report.json`, periodic in-flight `measurements_snapshots/`
  rolled up into `measurements.json` + `system_description.json`, `run_window.json`, and `report.md`)
  and optionally validate and upload it to GCS in one command, set `COLLECT_TELEMETRY=1`,
  `ARTIFACT_DIR="${PWD}/bundle-RUN"`, and `GCS_BENCHMARK_BUCKET=<bucket>` (plus optional
  `GCS_BENCHMARK_NICKNAME=fast-startup`, `PREFLIGHT_COVERAGE`, `FOOTING=warm|cold`, `ATE_ENDPOINT`,
  and `PROM_URL`) on `run-in-cluster.sh`. Never hardcode a bucket name in repository files.

Shape flags per measurement are in the README section named above. Two that are easy to get
wrong:

- Width above the slot count means admission queueing counted into `ttfi`; width equal to the
  slot count means none. Say which one the run is.
- Many cold restores per node at once measure the node's disk and CPU under contention, not the
  per-restore cost. 0.65 unpacks per 8-vCPU node gave 15.8 s median; 13 per node gave 50 s; 160
  per 16-vCPU node gave 243 s. Choose the per-node concurrency on purpose and state it.

### The reference cluster

The baselines below were measured on this setup, and section 1's checks confirm it is still so:

- Substrate v1.0.0-rc.1: every component, ate-api-server included, built from that tag by
  `ate-setup deploy` with ko at install time; the bundle's `system_description.json` records as
  `substrate_version` the first image of the first `ate-system` pod the collector lists, in
  practice ate-api-server's digest. ate-api-server at 6 replicas for runs and
  12 while goldens bake; the bundled Postgres at the default footprint with
  `max_connections` raised to 500 and a 64-connection pool per replica; atelet with
  `--image-cache-gc-period=30s --image-cache-high-percent=70 --image-cache-low-percent=60`.
- System components on 3 x e2-standard-16, Postgres on 1 x n2-standard-16, both tainted off the
  sandbox pools.
- Sandbox pool A (the standard run): 40 x n2-standard-8, 200 GB pd-balanced, 8 workers per node,
  its own WorkerPool selected by the template. Pool B (the image-delivery and high-cardinality
  runs): 783 x e2-standard-8, one CPU family, 500 GB pd-balanced, about 2.5 workers per node,
  its own WorkerPool. A template selects one pool through the WorkerPool label; a WorkerPool
  whose node selector spans both pools mixes CPU families, and a golden baked on one family
  boots instead of restoring on the other (check 5 in section 1). The WorkerPool's worker
  container sets no requests, so
  each worker registers the node's allocatable; templates declare 2 CPU / 4 GiB, so a worker
  admits 3 running actors. gVisor; golden snapshots on; the snapshots bucket and the image
  registry in the cluster's region; plain containerd pulls unless the arm says otherwise.
- ate-env-api built from this repository's main (the commit is `sdk_version` in the bundle),
  4 replicas, `-wake-budget=5m`; the loader as a Job in the same cluster, built from the same
  checkout.

A different node type, disk, workers per node or GC setting is a different baseline: label it
and do not pool it with these.

### Named runs

Two runs are the standard: ask for them by name and change nothing but the thing under test.
Each has a manifest built once with `make-dataset.py` (README, "Prepare once per project"),
pinned by digest, and kept in the team's benchmark store next to the results, so every run of
a name uses the same images. Build it once per registry mirror; do not rebuild it per run.

**The standard run** (any change in the trainer loop: control plane, router, scheduler, suspend
and resume, env API, guest). SWE-bench Verified: the README's `swebench-verified.jsonl`, built once
with `make-dataset.py
--revision <dataset commit>` (500 instances, images pinned by digest); `run-in-cluster.sh` passes
`-exclude-file offline-exclusions.txt`, which removes the 55 instances that cannot run offline, so
445 run. On 40 n2-standard-8 with 320 workers, which admit 960 actors at 2 CPU each, so width 890
queues no create. Two arms, about 45 and 7 minutes:

```bash
... run-in-cluster.sh RUN swebench-verified-445.jsonl -mode steps -rollouts 2 -steps 50 -width 890 -park delete -images-per-step 445 -trajectory workload-file-read-one -label arm=startup ...
... run-in-cluster.sh RUN swebench-verified-445.jsonl -mode steps -rollouts 2 -steps 50 -width 890 -park suspend -trajectory workload-file-read-one -label arm=wake ...
```

Baseline: startup create 0.67 s, ttfi 6.4 s p50 / 39.7 s p95, step 20 to 28 s warm and 118 s on
the first cold steps; wake 2.1 s p50 / 5.5 s p95, step 6.6 s, no retries. The images fit the
nodes, so the cold steps end by about step 10 and the step time settles near 20 s by step 33:
this run does not measure image delivery.

**The image-delivery run** (streaming, disks, registries, anything that changes a cold start).
`prime-swe-rl-94.jsonl`, 94 rows: the first 91 lines of the Prime Intellect SWE-RL mirror
manifest (`make-dataset.py --images-file` over the mirror's registry listing of 2026-10-29,
in listing order, one line per distinct digest, 36,841 lines) followed by three of its 9 GB
images, `aweaiteam/scaleswe` at digests `6de814e6da7a…`, `7b8b39ea3420…` and `8e999766f2ba…`
(`head -91`, then the three appended; `-images 94` takes the first 94 rows, so the large ones
must be inside them). One task per image. Width 3 on a two-node pool (1.5 starts per node), 30
on a cluster of hundreds of nodes (0.04 per node). Two arms, 10 to 17 minutes each, bounded by
the large images:

```bash
... run-in-cluster.sh RUN prime-swe-rl-94.jsonl -mode eval -images 94 -tasks-per-image 1 -width 3 -trajectory workload-file-read-one -set READ_FILE=/etc/passwd -label arm=start ...
... run-in-cluster.sh RUN prime-swe-rl-94.jsonl -mode eval -images 94 -tasks-per-image 1 -width 3 -trajectory workload-file-read -set READ_FILES=200 -label arm=read ...
```

Sizing: 1.3 to 1.5 starts in flight per node, e2-standard-8 with a 500 GB pd-balanced disk; that
band is what every baseline used, so the number is the per-start cost and not disk contention.
Width 3 on 2 nodes, width 13 on 10 nodes. Caches must be cold per rep:
recreate the node pool, or use at least 47 images per node, the full run's ratio, so a start never
lands on a node that holds its image. A 500 GB disk holds about 290 unpacked Prime images under
the 70% eviction mark. Goldens are baked once beforehand and not timed.
Baseline, plain pull on pd-balanced, measured by this recipe at width 30 on a 783-node pool: start
arm ttfi 28.7 s p50 / 101 s p95 / 275 s max, unpack 26.8 s p50, 0.99 pulls per start; read arm
ttfi 28.9 s / 91.7 s with the read step 573 ms p50 / 1.1 s p95; the three large images 209, 215
and 275 s. The smoke tier gave 29.8 s p50 in 94 s of wall time. 94 starts per arm, two reps when
the p95 matters. It measures the per-start cost without contention; a width of 160 per node measures
contention instead, and the two are not comparable
on ttfi.

For iterating on a delivery change, a smoke tier on the same manifest: `-images 30
-tasks-per-image 1 -width 3` on two nodes (width 10 on a cluster of hundreds of nodes) takes the
first 30 rows, none of them large; about four minutes, the median within about 10% and nothing
else. Confirm on the 94 before comparing or publishing.

Bundle names: `<date>_simulated_swebench-verified_fast-<arm>` and
`<date>_simulated_prime-swe-rl-94_delivery-<arm>`, the layout `benchmarking/SKILL.md` section 5
describes.

## 3. Read the report

- `ttfi` is create start to first command answered; `retried` on that row counts first commands
  that waited on a bake or a wake.
- The `restore first` block (needs `RESTORE_LOG`) splits each first command: `download` is the
  golden from the bucket, `oci_unpack` is the image onto the node, `worker_restore` is gVisor,
  `outside` is everything atelet does not do (create, placement, router, guest readiness). No
  restore records at all means the actors booted; see check 3.
- The cache line, `misses per first command`, near 1.0 means every environment pulled its own
  image: placement is image-blind, and 16 rollouts of one image land on 13 to 16 nodes.
- `crashed while waking` on a boot is an image that does not start under the guest; on a restore
  it is a golden gVisor could not bring back. Both are deterministic per image; exclude and file.

## 4. Record with every run

The bundle records the cluster, node pools, Kubernetes and Substrate versions and the loader
commit (`bench measure -k8s`, section 2), and the loader records its own flags. Record by hand
what neither does: boot disk type and size, CPU family, workers per node, declared actor limits,
ateapi replicas, atelet GC flags, and whether the templates had goldens at run start. Each of
these moved a number this week.
