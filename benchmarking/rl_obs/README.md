# RL sandbox observability: load driver

`rl_load_driver.py` produces the `rl_sandbox.*` telemetry stream for a
synthetic GRPO-style rollout workload, without GPUs. It is the driver for the
observability PoC experiments (baseline, image cache-miss injection,
telemetry overhead, trace completeness).

Each trajectory: acquire a sandbox → `--turns` × (simulated LLM time → one
command in the sandbox) → reward command → release. Per batch the driver also
records harness phases (`engine`, `trainer_step`, `weight_sync`, `prefetch`)
so the run timeline shows the training pipeline next to the sandbox lanes.

## Local, simulated backend

No cluster needed. The `sim` backend models warm-pool hits, snapshot restores
and cold starts with a per-node image cache, and writes Substrate-shaped
`Restore timing breakdown` records next to the events so the report builder's
join can be exercised:

```bash
python benchmarking/rl_obs/rl_load_driver.py --backend sim --run-id demo \
  --batches 6 --groups 8 --generations 4 --workers 16 \
  --new-images-at-batch 3 --bad-node 5 --time-scale 0.05 \
  --out /tmp/rl-obs/demo
```

## Substrate (e.g. the barkland-brust dev cluster)

```bash
python benchmarking/rl_obs/rl_load_driver.py --backend substrate \
  --endpoint "$ATE_ENV_API_TARGET" --atespace rl-obs-poc \
  --images-file images.txt --new-images-file new-images.txt \
  --new-images-at-batch 3 --run-id e2-cache-miss \
  --out gs://BUCKET/rl-obs/e2-cache-miss
```

`images.txt` lists digest-pinned task images, one per line (for example from
`benchmarking/swebench`). Add `--trace --otlp-endpoint HOST:4317` to export one
trace per episode (needs `opentelemetry-sdk` and `opentelemetry-exporter-otlp`).

Events land in `<out>/events-<worker>-<pid>.jsonl`; see
`ate_env.telemetry` for the schema.
