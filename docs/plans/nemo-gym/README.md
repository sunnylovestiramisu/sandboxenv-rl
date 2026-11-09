# Plan: NeMo Gym sandbox provider

Status: living document, last checked 2026-11-09 against NeMo Gym `v0.7.0` (the latest release,
2026-10-12) and NeMo Gym `main` at `acb6773`. The criteria are in
[acceptance-criteria.md](./acceptance-criteria.md); the gap issue is #290.

## 1. Goal

[`integrations/nemo-gym`](../../../integrations/nemo-gym/README.md) is a NeMo Gym sandbox provider
(`nemo_gym.sandbox_providers` entry point `substrate`) that runs each Gym sandbox as an ate-env
environment. Any Gym-based framework (NeMo-RL, Nemotron pipelines, Gym's own environment servers)
must run its rollout sandboxes on Substrate with `sandbox: substrate:` as the only configuration
change.

Gym moves fast: `main` was 799 commits past `v0.7.0` four weeks after the release, and two of those
commits broke this provider on the Harbor path without touching the provider protocol's required
methods. This plan records each feature the provider promises, and each Gym capability it does or
does not implement, as a criterion with a status and the tests that check it. Section 4 is the
procedure that finds drift against a new Gym release.

## 2. What the provider covers today

- The seven required methods of `SandboxProvider` (`create`, `exec`, `upload_file`, `download_file`,
  `status`, `close`, `aclose`) over the `ate_env_rl` client.
- Gym's exec error contract: return code 125 with `error_type` `"timeout"`, `"sandbox"` or
  `"user_switch_unavailable"`; a nonzero exit of the command is a plain result.
- `SandboxStatus` mapping from the environment lifecycle, with `PAUSED` for a parked sandbox.
- Images: `image_templates` maps an image to a registered template, else `rootfs_image` derives one
  from the base template.
- Optional capabilities: `SupportsSandboxEndpoint` (port proxy), `SupportsSandboxPauseResume` (idle
  parking with `SUSPEND_MODE_LOCAL` or durable suspend) and `ConnectableProvider` (#294).
- The NeMo runtime layer (#94): NeMo Skills' sandbox server and NeMo Gym's exec server injected into
  any task image, reached through `endpoint()`.

## 3. What it does not cover, and why

| Gym capability                                                                       | Since          | Decision                                                                                                                                            |
| ------------------------------------------------------------------------------------ | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SupportsSandboxIndependentUploads` (`SandboxSession.expire`)                        | after `v0.7.0` | planned (NG-18): needs a check of the client's concurrency limits first                                                                             |
| `SupportsSandboxBackgroundServices`                                                  | `v0.7.0`       | planned check (NG-19): Gym falls back to plain `exec`; correct only if the guest keeps a command's background children alive after the command ends |
| Compose capabilities: network, port forwarding, runtime requirements, shared storage | `v0.7.0`       | not planned: one environment is one container; multi-container Compose tasks are out of scope                                                       |
| Compose capabilities: service options, pull policy                                   | after `v0.7.0` | not planned, same reason                                                                                                                            |
| `SupportsSandboxSidecars`                                                            | after `v0.7.0` | not planned now; runtime layers (guest sidecars in the same rootfs) cover the services we need                                                      |
| `SupportsSandboxPty`, `SupportsSandboxPtyAttach`                                     | `v0.7.0`       | not planned now: only Gym's `shell` CLI uses them; the guest supports a PTY, thus this is possible later                                            |
| Create failures as `NgFailureEvent`                                                  | after `v0.7.0` | deferred (NG-32): Gym still accepts `SandboxCreateError` from providers awaiting migration                                                          |

## 4. Drift check for a new Gym release or `main`

Run this before each Gym bump, and at least once a month while Gym `main` moves this fast. Put the
clone in a directory of its own; do not run code from it.

1. Clone and pin: `git clone --filter=blob:none https://github.com/NVIDIA-NeMo/Gym.git` and note the
   tag or commit.
2. Protocol diff: `git diff <last-checked>..<new> -- nemo_gym/sandbox/providers/base.py`. Look for
   new or changed `Supports*` protocols, `ConnectableProvider`, new `SandboxSpec` fields, new
   `SandboxExecResult.error_type` values, and changes to the docstrings of `create`, `exec` and the
   PTY methods (the contract is partly prose).
3. Facade diff: `git diff <last-checked>..<new> -- nemo_gym/sandbox/api.py`. The facade decides what
   reaches the provider: the `user` default (NG-9) and the `serialize()` requirement (NG-17) both
   came from here, not from `base.py`.
4. Capability callers: for each `Supports*` name and `ConnectableProvider`, list the callers outside
   `nemo_gym/sandbox/providers/` with `git grep -l <name> -- '*.py' ':!*test*'`, and read what a
   caller does when the capability is missing (raise, fall back, or skip).
5. Keyword callers: `git grep -n -E 'exec\([^)]*user='` and `git grep -n 'error_type'` outside the
   providers, to find new arguments the facade passes and new `error_type` values callers read.
6. Imports: check that each name `_compat.py` imports from `nemo_gym.sandbox.providers.base` still
   exists, and the `requires-python` floor in Gym's `pyproject.toml` against the `gym` extra.
7. Registry: check `nemo_gym/sandbox/providers/registry.py` for a built-in provider named
   `substrate`, which would collide with our entry point (NG-31).
8. Update [acceptance-criteria.md](./acceptance-criteria.md): one row per new capability or
   contract, the status of each row, and the "last checked" line above. Open an issue for each
   break.

## 5. Order of the open work

1. #291 (NG-9) and #294 (NG-17): Harbor tasks through Gym `main` work on this provider.
2. NG-27: run one Harbor episode through Gym's Harbor environment server on a cluster, to prove 1.
3. NG-18 and NG-19: the expire capability and the background-services check.
4. NG-26: the first end-to-end Gym rollout with a stock Gym example (the original M1 exit
   criterion).
5. NG-29 and NG-30: the pause-against-never comparison and create budgets under saturation, from the
   comparison with `dims/gym-substrate`.
6. NG-31: agree on one Substrate provider and its entry point name before an upstream proposal.
