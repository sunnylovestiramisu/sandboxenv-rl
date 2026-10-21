# rl_obs: RL sandbox observability report builder

Builds a per-run dashboard from the SDK's `rl_sandbox.*` event stream joined
with Substrate's per-actor restore records.

```
events-*.jsonl (SDK, local dir or gs://)  ─┐
                                           ├─> aggregate.py ─> run.json ─> report.html
Substrate "Restore timing breakdown"       │
(Cloud Logging, or a local JSONL file)    ─┘
```

* **Join:** each `acquire.ready` is matched to the restore record with the
  same `ate.actor.uid` (or `(ate.atespace, ate.actor.name)`) whose time falls
  between the acquire start and the release. The join rate is shown in the
  report header.
* **Acquire source:** from the restore record's `ate.snapshot.kind`
  (`latest`/`local` → warm, `golden` → snapshot, `boot` → cold); without a
  record, an acquire from the SDK's warm pool counts as warm, else unknown.
* **Effective acquire:** SDK wait + the lazy resume Substrate performs on the
  first guest call (restore total, capped by the first step's time).
* **Image-cache miss (derived):** `oci_unpack > 1 s` on the restore record.

## Usage

```bash
# Simulated run (no cluster): see benchmarking/rl_obs/README.md
python tools/rl_obs/build_report.py --events /tmp/rl-obs/demo \
  --substrate-records /tmp/rl-obs/demo/substrate-records.jsonl \
  --out /tmp/rl-obs/demo/report

# Cluster run: events in GCS, Substrate records from Cloud Logging
python tools/rl_obs/build_report.py --events gs://BUCKET/rl-obs/e1-baseline \
  --project PROJECT --atespace rl-obs-poc --out reports/e1-baseline
```

Open `report.html` in a browser (self-contained apart from Tailwind from
gstatic). Tests: `pytest tools/rl_obs`.
