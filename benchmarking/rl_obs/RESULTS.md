# RL sandbox observability PoC: results (barkland-brust, 2026-10-21)

Setup: `ate-env-api` built from the `telemetry` branch, deployed as a separate
1-replica `ate-env-api-telemetry` in `ate-env`; atespace `telemetry-poc`
(template 1 CPU / 2 GiB, golden snapshot ready); SWE-bench Verified images from
the `swebench-mirror` (offline exclusions and `-zran` Nydus variants removed);
events in `gs://snapshot-substrate-gke-ai-eco-dev/telemetry-poc/<run>/`;
Substrate records from Cloud Logging. Reports: `x20web/~songsunny/rl-obs/<run>.html`.

| Run | What | Key numbers |
|---|---|---|
| E0 `e0-plumbing` | 16 trajectories | Substrate join 16/16; golden restore with cached image 0.17 s; image-cache miss unpack 12.8 s |
| E1 `e1-baseline` | 640 trajectories, 64 workers, 10 batches | acquire p50 per batch 11.3 s → 0.26 s as node caches fill; 9.1 pulls per image across 19 nodes; cache hit 44 %; wait share 23.8 % |
| E2 `e2-cache-miss` | E1 + 16 new images at batch 3 | batch 3 acquire p50 7.8 s vs 0.26 s elsewhere; boot-from-image stripe across all workers |
| E3 overhead (in-process) | 10k trajectories, mock backend | events (JSONL) +56 µs per SDK call; OTel spans + metrics +155 µs; both +167 µs |
| E3 overhead (cluster, on/off/on) | 3 × 256 trajectories | exec p50 273.9 ms off vs 275.0 / 276.7 ms on; acquire p50 99 ms off vs 104 / 108 ms on (within run-to-run noise); 9.05 events and 6.2 KiB per trajectory |
| E4 `e4-traces-2` | 32 traced episodes | lifecycle calls (create, suspend/delete, checkpoint) carry the episode trace on ateapi, atelet and ateom; the resume triggered by the first guest call did **not** (ate-env-api's guest router connection sent no traceparent) |
| E4 `e4-traces-3` | same, after the guest-connection fix | 32/32 episodes: every Substrate record of the actor (create, resume, restore, checkpoint, suspend, delete, usage samples) carries the episode trace id; no records on other traces |
| E4 `e4-traces-5` | same, rebased on main, Substrate `ddb46651` | 32/32 episodes: the guest-call resume and every other Substrate record of the actor (ateapi create/resume/suspend/delete, atelet and worker checkpoints, usage samples) carry the episode trace id; no records on other traces. No restore records: this Substrate takes no golden snapshot, so fresh actors boot from the image |

E0–E4 `e4-traces-3` ran on Substrate `v0.4.0-70-g579cfcb6`; `e4-traces-5` on
`v0.5.0-107-gddb46651`, the version ate-env pins, where the worker container is
named `worker` (was `ateom`).

Findings:

1. Placement / image locality dominates acquire time: a cached golden restore
   is ~0.17 s, an uncached image costs 6–13 s of OCI unpack, and each image
   was pulled on ~9 of 19 nodes. This is the measured case for image-affinity
   placement.
2. The SDK warm pool (create + suspend) gives almost no warm hits: a created,
   never-run actor has no snapshot, so its first resume boots or restores from
   the golden like any new actor.
3. Fresh actors that boot from the image leave no atelet "Restore timing
   breakdown"; their resume time is derived from ateapi's state changes. On
   Substrate `ddb46651` templates take no golden snapshot, so this is every
   fresh actor's first resume.
4. Substrate's image cache cannot unpack Nydus (`-zran`) layers
   (`magic number mismatch`) and crashed actors cannot be deleted
   (delete suspends first). Both are worth upstream issues.
5. ate-env-api's own router connection (process/filesystem calls) lacked the
   OpenTelemetry client handler, so the wake-on-request resume started a new
   trace; atenet already continues an incoming traceparent. Fixed in this branch.
6. The managed OTel collector listens on its pod IP only; reaching it from
   outside the cluster needs an in-cluster relay.
