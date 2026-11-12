# Plan: NeMo Gym sandbox provider

Status: living document, last checked 2026-11-10 against NeMo Gym `v0.7.0` (the latest release,
2026-10-12) and NeMo Gym `main` at `22f6c79`. The criteria are in
[acceptance-criteria.md](./acceptance-criteria.md). #290 recorded the gaps found on 2026-11-09; the
gaps still open are tracked in #303.

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
  parking with `SUSPEND_MODE_LOCAL` or durable suspend), `ConnectableProvider` (#294) and
  `SupportsSandboxIndependentUploads` (file transfers on a connection of their own, thus
  `SandboxSession.expire` can stop a running harness).
- `preserve_background_services=True` through Gym's fallback to a plain `exec`: a service that
  redirects its output keeps running after its command returns (NG-19).
- The NeMo runtime layer (#94): NeMo Skills' sandbox server and NeMo Gym's exec server injected into
  any task image, reached through `endpoint()`.

## 3. What it does not cover, and why

| Gym capability                                                                                                                                        | Since          | Decision                                                                                                                                                                      |
| ----------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SupportsSandboxBackgroundServices`                                                                                                                   | `v0.7.0`       | not needed (NG-19): Gym falls back to a plain `exec`, and the guest keeps a command's background children alive after the command ends, which is all that the capability adds |
| Compose capabilities: `SupportsSandboxNetwork`, `SupportsSandboxPortForwarding`, `SupportsSandboxRuntimeRequirements`, `SupportsSandboxSharedStorage` | `v0.7.0`       | not planned: one environment is one container; multi-container Compose tasks are out of scope                                                                                 |
| Compose capabilities: `SupportsSandboxServiceOptions`, `SupportsSandboxPullPolicy`                                                                    | after `v0.7.0` | not planned, same reason                                                                                                                                                      |
| `SupportsSandboxSidecars`                                                                                                                             | after `v0.7.0` | not planned now; runtime layers (guest sidecars in the same rootfs) cover the services we need                                                                                |
| `SupportsSandboxPty`, `SupportsSandboxPtyAttach`                                                                                                      | `v0.7.0`       | not planned now: only Gym's `shell` CLI uses them; the guest supports a PTY, thus this is possible later                                                                      |
| Create failures as `NgFailureEvent`                                                                                                                   | after `v0.7.0` | deferred (NG-32): Gym still accepts `SandboxCreateError` from providers awaiting migration                                                                                    |

## 4. Drift check for a new Gym release or `main`

Run this before each Gym bump, and at least once a month while Gym `main` moves this fast. Steps 1
and 2 are mechanical; steps 3 to 7 find what the tests cannot, because a test can only check a
capability the provider already knows about; step 8 records the result.

1. Pin the ref: the release tag, or `git ls-remote https://github.com/NVIDIA-NeMo/Gym.git main`.
2. Run the provider's unit tests with that Gym installed, from `integrations/nemo-gym` (needs uv >=
   0.13.1 and Python >= 3.13.14, Gym's floor):

   ```sh
   uv run --python '>=3.13.14' --extra test --extra gym pytest -rs             # the locked release
   uv run --python '>=3.13.14' --extra test \
     --with 'nemo-gym==<version>' pytest -rs                                   # a release not locked yet
   uv run --python '>=3.13.14' --extra test \
     --with 'nemo-gym @ git+https://github.com/NVIDIA-NeMo/Gym.git@<ref>' pytest -rs  # a main commit
   ```

   `--extra gym` installs the release the workspace `uv.lock` pins (`0.7.0` today), not the newest
   one on PyPI; `--with` installs the version named, for a release the lock does not have yet.
   Without a cluster the expected result is all passed and 4 skipped, the four e2e tests; `-rs`
   prints the reason of each skip. `test_capabilities_match_the_plan` (NG-34) runs only here: it
   fails when Gym adds a capability (a `Protocol` in `base.py`) that section 3 does not decide, when
   the provider stops implementing one or Gym drops or renames one it implements, when a name
   `_compat.py` exports is not Gym's own (step 7), or when a built-in provider takes the name
   `substrate`. Gym `main` reports its version as the current release number with an `rc0` suffix
   (`0.7.0rc0` at `f49b582`), which sorts before the release.

3. For the reading steps, clone into a directory of its own and do not run code from it:
   `git clone --filter=blob:none https://github.com/NVIDIA-NeMo/Gym.git`. Then diff the protocol:
   `git diff <last-checked>..<new> -- nemo_gym/sandbox/providers/base.py`. Look for changed
   `Supports*` protocols, `ConnectableProvider`, new `SandboxSpec` fields, new
   `SandboxExecResult.error_type` values, and changes to the docstrings of `create`, `exec` and the
   PTY methods (the contract is partly prose).
4. Facade diff: `git diff <last-checked>..<new> -- nemo_gym/sandbox/api.py`. The facade decides what
   reaches the provider: the `user` default (NG-9) and the `serialize()` requirement (NG-17) both
   came from here, not from `base.py`, and the existing tests could not see either.
5. Capability callers: for each `Supports*` name and `ConnectableProvider`, list the callers outside
   `nemo_gym/sandbox/providers/` with `git grep -l <name> -- '*.py' ':!*test*'`, and read what a
   caller does when the capability is missing (raise, fall back, or skip). A new caller can turn a
   "not planned" row into a break.
6. Keyword callers: `git grep -n -E 'exec\([^)]*user='` and `git grep -n 'error_type'` outside the
   providers, to find new arguments the facade passes and new `error_type` values callers read.
7. Imports and floor: each name `_compat.py` imports from `nemo_gym.sandbox.providers.base` still
   exists. A missing name does not stop step 2 at import: `_compat.py` catches the `ImportError` and
   the suite runs against its mirrors (`SandboxEndpoint` has a fallback of its own, which leaves
   `NEMO_GYM_AVAILABLE` true); `test_capabilities_match_the_plan` then fails, because it checks that
   each name `_compat.py` exports is the object Gym defines. Gym's `requires-python` must match the
   `gym` extra.
8. Record: add a row to the drift log (section 6), update the status of each row in
   [acceptance-criteria.md](./acceptance-criteria.md) and both "last checked" lines, and open an
   issue for each break.

## 5. Order of the open work

1. NG-27: run one Harbor episode through Gym's Harbor environment server on a cluster
   (`gym dataset validate environments/hello_world` with this provider; the oracle agent needs no
   model). Its prerequisites, NG-9, NG-17 and NG-19, are met.
2. NG-26: the first end-to-end Gym rollout with a stock Gym example (the original M1 exit
   criterion).
3. NG-29 and NG-30: the pause-against-never comparison and create budgets under saturation, from the
   comparison with `dims/gym-substrate`.
4. NG-31: agree on one Substrate provider and its entry point name before an upstream proposal.

## 6. Drift log

One row per drift check, newest first.

| Date       | Gym ref                              | Result                                                                                                                                                                                                                                                                                                    | Action                                      |
| ---------- | ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| 2026-11-10 | `main` `22f6c79` (77 past `f49b582`) | One provider-facing change, #4917: a `SandboxSpec.files` value can be `bytes`, and `create` failed on one. No new capability; `serialize(scope=...)` reaches `serialize_handle`, which already takes `scope`. Unit tests: 103 passed, 4 skipped against `0.7.0` and against `22f6c79` (rerun 2026-11-11). | `create` seeds `bytes`; NG-18 and NG-19 met |
| 2026-11-09 | `main` `f49b582` (11 past `acb6773`) | One sandbox change, #4934 (file transfers are instrumented; the facade still passes a `Path` to `upload_file` and `download_file`). No provider-facing change. Unit tests: 91 passed, 3 skipped against `0.7.0` and against `f49b582`, after one test was adapted to Gym's own port checks.               | NG-28 and NG-34 met; #303 for the open gaps |
| 2026-11-09 | `main` `acb6773` (799 past `v0.7.0`) | Two breaks on the Harbor path: the facade sends `user` on each exec, and the Harbor environment server calls `serialize()` on each episode. Four capabilities added (sidecars, independent uploads, service options, pull policy), none required.                                                         | #290; fixed by #291 and #294                |
| 2026-10-27 | `0.4.0`                              | Error contract and parking compared with `dims/gym-substrate`; unit tests run against the 0.4.0 types.                                                                                                                                                                                                    | #146, #147, #149                            |
