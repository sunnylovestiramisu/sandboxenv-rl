# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Contract tests for the substrate NeMo Gym provider.

Runs against an in-process fake of ate-env-api (tests/fakes.py) reached
through the real ``ate_env_rl`` client over loopback gRPC, asserting the rules
from the adding-a-provider contract: create returns only when the sandbox
executes commands, exec never raises on nonzero exits, close is
cleanup-safe, and provider_options are validated strictly.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path

import grpc
import pytest
from ate_env_rl import Client, EnvironmentStatus, NotFoundError, SuspendMode

from nemo_gym_substrate import provider as provider_mod
from nemo_gym_substrate._compat import (
    SandboxCreateError,
    SandboxCreateVerificationError,
    SandboxEndpoint,
    SandboxSpec,
    SandboxStatus,
)
from nemo_gym_substrate.provider import SubstrateSandboxProvider

from .fakes import FakeAteEnv, serve


@contextlib.asynccontextmanager
async def stack(
    create: dict | None = None,
    *,
    state: FakeAteEnv | None = None,
    connection: dict | None = None,
):
    """A provider dialing a fake ate-env-api; yields ``(provider, fake)``."""
    async with serve(state) as (target, fake):
        config = {
            "connection": {"api_url": target, "request_timeout_s": 5.0, **(connection or {})},
            "create": {
                "ready_poll_interval_s": 0.01,
                "image_templates": {"img:1": "tpl-img1"},
                **(create or {}),
            },
        }
        provider = SubstrateSandboxProvider(config)
        try:
            yield provider, fake
        finally:
            await provider.aclose()


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _no_ambient_token(monkeypatch):
    # A token in the developer's environment would change what the provider sends.
    monkeypatch.delenv("SUBSTRATE_ENV_AUTH_TOKEN", raising=False)


def _probe_count(fake: FakeAteEnv) -> int:
    return sum(1 for s in fake.starts if s["command"][-1:] == ["true"])


def test_create_returns_ready_handle_and_seeds_files():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(
                SandboxSpec(files={"/task/input.json": '{"n": 1}'}, workdir="/task")
            )
            assert handle.provider_name == "substrate"
            assert handle.sandbox_id.startswith("gym-")
            env = fake.envs[handle.sandbox_id]
            assert (env["template"], env["template_atespace"], env["atespace"]) == (
                "default-template",
                "ate-env",
                "ate-env",
            )
            assert fake.files[(handle.sandbox_id, "/task/input.json")] == b'{"n": 1}'
            assert fake.modes[(handle.sandbox_id, "/task/input.json")] == 0o644

    run(t())


def test_create_waits_for_readiness():
    async def t():
        async with stack() as (provider, fake):
            fake.ready_after_probes = 2
            handle = await provider.create(SandboxSpec())
            assert handle.sandbox_id in fake.envs
            assert _probe_count(fake) == 3  # two UNAVAILABLE + one success

    run(t())


def test_create_failure_raises_create_error():
    async def t():
        async with stack() as (provider, fake):
            fake.create_code = grpc.StatusCode.RESOURCE_EXHAUSTED
            with pytest.raises(SandboxCreateError, match="RESOURCE_EXHAUSTED"):
                await provider.create(SandboxSpec())
            assert fake.envs == {}

    run(t())


def test_create_readiness_timeout_cleans_up():
    async def t():
        async with stack() as (provider, fake):
            fake.ready_after_probes = 10_000
            with pytest.raises(SandboxCreateVerificationError, match="UNAVAILABLE"):
                await provider.create(SandboxSpec(ready_timeout_s=0.05))
            assert fake.envs == {}  # the half-created env was deleted

    run(t())


def test_image_resolves_via_mapping_or_derives():
    async def t():
        async with stack() as (provider, fake):
            # A mapped image names a registered template; no rootfs_image is sent.
            handle = await provider.create(SandboxSpec(image="img:1"))
            assert fake.envs[handle.sandbox_id]["template"] == "tpl-img1"
            assert fake.envs[handle.sandbox_id]["rootfs_image"] == ""
            # Any other image goes to the API as rootfs_image on the base
            # template, which derives (and later reuses) a template for it.
            image = "docker.io/library/python@sha256:" + "ab" * 32
            handle = await provider.create(SandboxSpec(image=image))
            env = fake.envs[handle.sandbox_id]
            assert env["rootfs_image"] == image
            assert env["base_template"] == "default-template"
            assert env["template"] == "default-template-" + "ab" * 6

    run(t())


def test_exec_nonzero_exit_returns_result_not_raise():
    async def t():
        async with stack() as (provider, _):
            handle = await provider.create(SandboxSpec())
            result = await provider.exec(handle, "exit 7")
            assert (result.return_code, result.stderr, result.stdout) == (7, "boom", "")
            # A process exit status carries no error_type, whatever its value:
            # Gym agents end a rollout on error_type, not on the code.
            assert result.error_type is None

    run(t())


def test_exec_on_a_gone_sandbox_reports_a_sandbox_failure():
    # Gym's contract for a sandbox that cannot run the command: a result with
    # the runtime return code and error_type "sandbox", not an exception.
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.close(handle)
            result = await provider.exec(handle, "echo hi")
            assert (result.return_code, result.error_type) == (125, "sandbox")
            assert result.stdout is None
            assert "does not exist" in result.stderr and "not found" in result.stderr

    run(t())


def test_exec_transport_failure_reports_a_sandbox_failure():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.refuse_starts["make"] = grpc.StatusCode.UNAVAILABLE
            result = await provider.exec(handle, "make")
            assert (result.return_code, result.error_type) == (125, "sandbox")
            assert "UNAVAILABLE" in result.stderr
            # The sandbox is still there: the next command runs.
            assert (await provider.exec(handle, "echo hi")).return_code == 0

    run(t())


def test_exec_merges_spec_env_and_cwd():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec(workdir="/w", env={"A": "1", "B": "spec"}))
            await provider.exec(handle, "echo hi", env={"B": "call"})
            sent = fake.starts[-1]
            assert sent["command"] == ["sh", "-c", "echo hi"]
            assert sent["env"] == {"A": "1", "B": "call"}
            assert sent["cwd"] == "/w"
            await provider.exec(handle, "echo hi", cwd="/other")
            assert fake.starts[-1]["cwd"] == "/other"

    run(t())


@pytest.mark.parametrize(
    ("timeout_s", "guest_deadline"),
    [(3, 3.0), (0.5, 1.0), (0, 1.0), (2.2, 2.2)],
)
def test_exec_timeout_is_enforced_guest_side(timeout_s, guest_deadline):
    # The deadline travels as the process's own timeout, not as a `timeout`
    # wrapper, so the image needs no coreutils. Zero and sub-second values
    # floor to 1 s: a zero duration would select the guest default instead.
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.exec(handle, "sleep 100", timeout_s=timeout_s)
            assert fake.starts[-1]["command"] == ["sh", "-c", "sleep 100"]
            assert fake.starts[-1]["timeout"] == pytest.approx(guest_deadline)

    run(t())


def test_exec_without_timeout_has_no_guest_deadline():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.exec(handle, "sleep 1")
            assert fake.starts[-1]["command"] == ["sh", "-c", "sleep 1"]
            assert fake.starts[-1]["timeout"] is None

    run(t())


def _commands(fake) -> list[str]:
    return [start["command"][-1] for start in fake.starts]


def test_exec_as_the_image_user_runs_unwrapped_and_probes_once():
    async def t():
        async with stack() as (provider, fake):
            fake.image_user(provider_mod._USER_PROBE, "root", 0, su=False)
            handle = await provider.create(SandboxSpec())
            for user in ("root", 0):
                result = await provider.exec(handle, "id", user=user)
                assert result.return_code == 0 and result.error_type is None
            assert _commands(fake).count(provider_mod._USER_PROBE) == 1
            assert _commands(fake)[-1] == "id"

    run(t())


def test_exec_without_user_does_not_probe():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.exec(handle, "id")
            assert provider_mod._USER_PROBE not in _commands(fake)

    run(t())


def test_exec_as_another_user_wraps_in_sudo_with_the_env():
    async def t():
        async with stack() as (provider, fake):
            fake.image_user(provider_mod._USER_PROBE, "app", 1000, sudo="passwordless")
            handle = await provider.create(SandboxSpec(env={"A": "1"}))
            result = await provider.exec(handle, "id -u", env={"B": "x y"}, user="root")
            assert result.return_code == 0
            assert _commands(fake)[-1] == "sudo -n -u root env -- A=1 'B=x y' sh -c 'id -u'"
            await provider.exec(handle, "true", user=0)
            assert _commands(fake)[-1].startswith("sudo -n -u '#0' ")

    run(t())


def test_exec_as_another_user_from_root_uses_su():
    async def t():
        async with stack() as (provider, fake):
            # A root image whose sudo wants a password switches with su.
            fake.image_user(provider_mod._USER_PROBE, "root", 0, sudo="locked")
            handle = await provider.create(SandboxSpec())
            await provider.exec(handle, "id", user="app")
            assert _commands(fake)[-1] == "su -s /bin/sh -c 'sh -c id' -- app"
            # su wants a name, and the refusal says so.
            result = await provider.exec(handle, "id", user=1000)
            assert result.return_code == 125
            assert result.error_type == "user_switch_unavailable"
            assert "su" in result.stderr and "uid 1000" in result.stderr
            assert "no passwordless sudo" not in result.stderr

    run(t())


@pytest.mark.parametrize("sudo", ["absent", "locked"])
@pytest.mark.parametrize("su", [True, False])
def test_exec_as_another_user_without_a_passwordless_sudo_is_unavailable(sudo, su):
    # su switches from root only, so a non-root image needs a sudo that takes
    # no password. One with a locked sudo must get the same refusal as one
    # without sudo: Gym's transfer helpers fall back on this error_type only.
    async def t():
        async with stack() as (provider, fake):
            fake.image_user(provider_mod._USER_PROBE, "app", 1000, sudo=sudo, su=su)
            handle = await provider.create(SandboxSpec())
            starts = len(fake.starts)
            result = await provider.exec(handle, "id", user="root")
            assert result.return_code == 125
            assert result.error_type == "user_switch_unavailable"
            assert "root" in result.stderr and "passwordless sudo" in result.stderr
            # Only the probe ran; the command did not.
            assert _commands(fake)[starts:] == [provider_mod._USER_PROBE]

    run(t())


_SH = shutil.which("sh")
_ID = shutil.which("id")


@pytest.mark.skipif(not _SH or not _ID, reason="needs a POSIX sh and id on the host")
@pytest.mark.parametrize(
    ("sudo", "su", "tool"),
    [
        ("absent", True, "su"),
        ("absent", False, ""),
        ("passwordless", True, "sudo"),
        ("locked", True, "su"),
        ("locked", False, ""),
    ],
)
def test_user_probe_in_a_real_shell(tmp_path: Path, sudo: str, su: bool, tool: str):
    # The probe script under the host's sh, with a PATH that holds `id` and
    # stubs only. The fake answers the probe from its text, so this is the test
    # that would catch a typo in it, and it pins FakeAteEnv.image_user's
    # mapping: a sudo that wants a password is not reported as sudo.
    import pwd  # POSIX only, as sh is

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    os.symlink(_ID, bin_dir / "id")
    stubs = {
        "passwordless": f'#!{_SH}\n[ "$*" = "-n true" ] && exit 0\nexit 1\n',
        "locked": f'#!{_SH}\necho "sudo: a password is required" >&2\nexit 1\n',
    }
    if sudo in stubs:
        (bin_dir / "sudo").write_text(stubs[sudo])
        (bin_dir / "sudo").chmod(0o700)
    if su:
        # The probe only looks su up.
        (bin_dir / "su").write_text(f"#!{_SH}\nexit 0\n")
        (bin_dir / "su").chmod(0o700)
    ran = subprocess.run(
        [_SH, "-c", provider_mod._USER_PROBE],
        env={"PATH": str(bin_dir)},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert ran.returncode == 0, ran.stderr
    switch = provider_mod._parse_user_probe(ran.stdout)
    assert switch.tool == tool
    assert switch.identities == {pwd.getpwuid(os.getuid()).pw_name, str(os.getuid())}


def test_exec_user_probe_failure_is_a_sandbox_failure_and_not_cached():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.refuse_starts[provider_mod._USER_PROBE] = grpc.StatusCode.UNAVAILABLE
            result = await provider.exec(handle, "id", user="root")
            assert result.error_type == "sandbox"
            del fake.refuse_starts[provider_mod._USER_PROBE]
            fake.image_user(provider_mod._USER_PROBE, "root", 0)
            result = await provider.exec(handle, "id", user="root")
            assert result.return_code == 0 and result.error_type is None

    run(t())


def test_exec_user_probe_timeout_names_the_probe_and_is_not_cached(monkeypatch):
    monkeypatch.setattr(provider_mod, "_PROBE_TIMEOUT_S", 0.01)
    monkeypatch.setattr(provider_mod, "_EXEC_GRACE_S", 0.05)

    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.hang.add(provider_mod._USER_PROBE)
            result = await provider.exec(handle, "id", user="root", timeout_s=30)
            assert (result.return_code, result.error_type) == (125, "timeout")
            # The probe's deadline, not the caller's timeout_s.
            assert "user probe timed out after 0.01s" in result.stderr
            fake.hang.discard(provider_mod._USER_PROBE)
            fake.image_user(provider_mod._USER_PROBE, "root", 0)
            result = await provider.exec(handle, "id", user="root")
            assert result.return_code == 0 and result.error_type is None

    run(t())


def test_exec_client_timeout_returns_sentinel_and_kills(monkeypatch):
    monkeypatch.setattr(provider_mod, "_EXEC_GRACE_S", 0.05)

    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.hang.add("sleepy")
            result = await provider.exec(handle, "sleepy", timeout_s=0.01)
            assert (result.return_code, result.error_type) == (125, "timeout")
            assert result.stdout is None
            assert "timed out" in (result.stderr or "")
            hung = [p for p in fake.procs.values() if p.argv[-1] == "sleepy"]
            assert hung and hung[0].killed

    run(t())


def test_exec_guest_deadline_reports_timeout(monkeypatch):
    # The guest kills the process group at the deadline and reports a plain
    # SIGKILL; the result reports the timeout it was, with the output kept.
    monkeypatch.setattr(provider_mod, "_MIN_GUEST_TIMEOUT_S", 0.05)

    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.hang.add("sleepy")
            result = await provider.exec(handle, "sleepy", timeout_s=0.05)
            assert (result.return_code, result.error_type) == (125, "timeout")
            assert result.stdout == ""  # what the command got out, not None
            assert "timed out" in result.stderr and "guest" in result.stderr
            assert fake.starts[-1]["timeout"] == pytest.approx(0.05)

    run(t())


def test_file_roundtrip(tmp_path: Path):
    async def t():
        async with stack() as (provider, _):
            handle = await provider.create(SandboxSpec())
            src = tmp_path / "up.bin"
            src.write_bytes(b"\x00\x01payload-longer-than-one-chunk")
            await provider.upload_file(handle, src, "/data/up.bin")
            dst = tmp_path / "down" / "up.bin"
            await provider.download_file(handle, "/data/up.bin", dst)
            assert dst.read_bytes() == b"\x00\x01payload-longer-than-one-chunk"

    run(t())


def test_download_missing_file_raises(tmp_path: Path):
    async def t():
        async with stack() as (provider, _):
            handle = await provider.create(SandboxSpec())
            with pytest.raises(NotFoundError):
                await provider.download_file(handle, "/nope", tmp_path / "out")

    run(t())


def test_status_running_stopped_unknown():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            assert await provider.status(handle) == SandboxStatus.RUNNING
            fake.get_code = grpc.StatusCode.UNAVAILABLE
            assert await provider.status(handle) == SandboxStatus.UNKNOWN
            fake.get_code = None
            await provider.close(handle)
            assert await provider.status(handle) == SandboxStatus.STOPPED

    run(t())


@pytest.mark.parametrize(
    ("env_status", "expected"),
    [
        (EnvironmentStatus.RESUMING, SandboxStatus.STARTING),
        # Parked is alive and one call away; Gym has a status for it since 0.7
        # (an older nemo-gym installed here has none, and gets RUNNING).
        (EnvironmentStatus.SUSPENDING, provider_mod._PAUSED),
        (EnvironmentStatus.SUSPENDED, provider_mod._PAUSED),
        (EnvironmentStatus.PAUSING, provider_mod._PAUSED),
        (EnvironmentStatus.PAUSED, provider_mod._PAUSED),
        (EnvironmentStatus.CRASHED, SandboxStatus.ERROR),
        (EnvironmentStatus.DELETING, SandboxStatus.STOPPED),
        (EnvironmentStatus.UNSPECIFIED, SandboxStatus.UNKNOWN),
    ],
)
def test_status_maps_env_lifecycle(env_status, expected):
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.set_status(handle.sandbox_id, env_status)
            assert await provider.status(handle) == expected

    run(t())


def test_status_does_not_touch_the_guest():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            before = len(fake.starts)
            await provider.status(handle)
            assert len(fake.starts) == before  # no probe exec, so no wake-up

    run(t())


def test_close_is_idempotent():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.close(handle)
            await provider.close(handle)  # second delete hits NOT_FOUND; must not raise
            assert handle.sandbox_id not in fake.envs

    run(t())


def test_close_logs_but_does_not_raise_on_delete_failure(caplog):
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.delete_code = grpc.StatusCode.INTERNAL
            with caplog.at_level(logging.WARNING, logger="nemo_gym_substrate.provider"):
                await provider.close(handle)  # best-effort: never raises
            assert any(
                "deleting substrate env" in r.message and "INTERNAL" in r.message
                for r in caplog.records
            )

    run(t())


def test_provider_options_override_template_and_atespace():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(
                SandboxSpec(provider_options={"template": "tpl-x", "atespace": "as-x"})
            )
            env = fake.envs[handle.sandbox_id]
            assert (env["template"], env["template_atespace"], env["atespace"]) == (
                "tpl-x",
                "as-x",
                "as-x",
            )
            # The handle remembers its atespace, so later calls route there.
            assert await provider.status(handle) == SandboxStatus.RUNNING
            await provider.close(handle)
            assert handle.sandbox_id not in fake.envs

    run(t())


def test_namespace_is_accepted_as_alias_for_atespace():
    async def t():
        async with stack({"namespace": "legacy"}) as (provider, fake):
            handle = await provider.create(SandboxSpec(provider_options={"namespace": "per-sb"}))
            assert fake.envs[handle.sandbox_id]["atespace"] == "per-sb"
            handle2 = await provider.create(SandboxSpec())
            assert fake.envs[handle2.sandbox_id]["atespace"] == "legacy"

    run(t())


def test_atespace_and_namespace_together_rejected():
    with pytest.raises(ValueError, match="either atespace or namespace"):
        SubstrateSandboxProvider({"create": {"atespace": "a", "namespace": "b"}})


def test_unknown_provider_option_rejected():
    async def t():
        async with stack() as (provider, _):
            with pytest.raises(ValueError, match="unknown option"):
                await provider.create(SandboxSpec(provider_options={"tempalte": "oops"}))

    run(t())


def test_unknown_config_key_rejected():
    with pytest.raises(ValueError, match="unknown option"):
        SubstrateSandboxProvider({"connection": {"api_urll": "typo"}})


def test_create_timeout_on_unreachable_api():
    async def t():
        # A closed port refuses at once (UNAVAILABLE); either way it must not hang.
        provider = SubstrateSandboxProvider(
            {"connection": {"api_url": "127.0.0.1:9", "request_timeout_s": 0.5}}
        )
        try:
            with pytest.raises(SandboxCreateError):
                await provider.create(SandboxSpec())
        finally:
            await provider.aclose()

    run(t())


def test_aclose_is_safe_to_repeat_and_drops_the_client():
    async def t():
        async with stack() as (provider, _):
            await provider.create(SandboxSpec())
            assert provider._client is not None  # noqa: SLF001 - test seam
            await provider.aclose()
            await provider.aclose()
            assert provider._client is None  # noqa: SLF001

    run(t())


def test_caller_owned_client_is_not_closed():
    async def t():
        async with serve() as (target, _):
            client = Client(target)
            try:
                provider = SubstrateSandboxProvider(client=client)
                handle = await provider.create(SandboxSpec())
                await provider.aclose()
                # Still usable: aclose() left the caller's client open.
                info = await client.get(handle.sandbox_id)
                assert info.id == handle.sandbox_id
            finally:
                await client.close()

    run(t())


def test_https_api_url_uses_a_tls_channel():
    async def t():
        provider = SubstrateSandboxProvider(
            {"connection": {"api_url": "https://ate-env.example:443"}}
        )
        try:
            assert provider._ate() is not None  # noqa: SLF001 - opens the channel, never dials
            assert provider._channel is not None  # noqa: SLF001
        finally:
            await provider.aclose()
        assert provider._channel is None  # noqa: SLF001

    run(t())


# -- endpoint(): services inside the sandbox ---------------------------------


def test_endpoint_routes_a_declared_port_through_the_api():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            assert handle.raw["ports"] == [6000]
            probes = _probe_count(fake)
            ep = await provider.endpoint(handle, 6000)
            assert isinstance(ep, SandboxEndpoint)
            # The port proxy shares ate-env-api's listener.
            assert ep.endpoint == provider._connection.endpoint_base()
            assert ep.endpoint.startswith("http://") and ep.endpoint.endswith(
                provider._connection.api_url.split("://")[-1]
            )
            assert ep.headers == {
                "X-Env-Id": handle.sandbox_id,
                "X-Env-Atespace": "ate-env",
                "X-Env-Port": "6000",
            }
            # Nothing touches the guest: the first request through the proxy
            # wakes a parked sandbox, and the proxy waits for it.
            assert _probe_count(fake) == probes

    run(t())


def test_endpoint_rejects_an_undeclared_port():
    async def t():
        async with stack() as (provider, _fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            with pytest.raises(ValueError, match="not declared in SandboxSpec.ports"):
                await provider.endpoint(handle, 8080)
            bare = await provider.create(SandboxSpec())
            with pytest.raises(ValueError, match=r"declared ports: \[\]"):
                await provider.endpoint(bare, 6000)

    run(t())


def test_endpoint_uses_the_configured_url_and_the_sandbox_atespace():
    async def t():
        async with stack(
            {"atespace": "team-a"},
            connection={"endpoint_url": "https://gym-proxy.example.com/"},
        ) as (provider, _fake):
            handle = await provider.create(SandboxSpec(ports=[6000]))
            ep = await provider.endpoint(handle, 6000)
            assert ep.endpoint == "https://gym-proxy.example.com"
            assert ep.headers["X-Env-Atespace"] == "team-a"

    run(t())


def test_endpoint_raises_for_a_crashed_sandbox():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            fake.set_status(handle.sandbox_id, EnvironmentStatus.CRASHED)
            with pytest.raises(RuntimeError, match="crashed"):
                await provider.endpoint(handle, 6000)

    run(t())


def test_endpoint_does_not_wake_a_parked_sandbox():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            fake.set_status(handle.sandbox_id, EnvironmentStatus.SUSPENDED)
            before = len(fake.starts)
            ep = await provider.endpoint(handle, 6000)
            assert ep.headers["X-Env-Port"] == "6000"
            assert len(fake.starts) == before  # no probe exec, so no wake-up

    run(t())


def test_create_rejects_invalid_ports():
    async def t():
        async with stack() as (provider, fake):
            cases = [
                ((0,), ValueError, "not a TCP port"),
                ((70000,), ValueError, "not a TCP port"),
                ((80,), ValueError, "guest"),
                ((True,), TypeError, "integers"),  # bool is an int subclass
                (("6000",), TypeError, "integers"),
            ]
            for ports, error, why in cases:
                before = len(fake.envs)
                with pytest.raises(error, match=why):
                    await provider.create(SandboxSpec(ports=ports))
                assert len(fake.envs) == before, "a bad spec must fail before the actor starts"

    run(t())


def test_endpoint_rejects_a_bool_port():
    async def t():
        async with stack() as (provider, _fake):
            handle = await provider.create(SandboxSpec(ports=(1,)))
            with pytest.raises(ValueError, match="not declared"):
                await provider.endpoint(handle, True)  # True == 1 in Python

    run(t())


def test_endpoint_stops_at_once_for_a_deleted_sandbox():
    async def t():
        async with stack({"ready_timeout_s": 30}) as (provider, _fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            await provider.close(handle)
            started = time.monotonic()
            with pytest.raises(RuntimeError, match="does not exist"):
                await provider.endpoint(handle, 6000)
            assert time.monotonic() - started < 5, "endpoint() waited out the ready timeout"

    run(t())


def test_endpoint_base_defaults_to_the_api_url():
    cfg = provider_mod.ConnectionConfig.from_mapping({"api_url": "ate-env-api.ate-env:7777"})
    assert cfg.endpoint_base() == "http://ate-env-api.ate-env:7777"
    cfg = provider_mod.ConnectionConfig.from_mapping({"api_url": "https://env.example.com/"})
    assert cfg.endpoint_base() == "https://env.example.com"
    # A bare host:port endpoint_url gets a scheme too; SandboxEndpoint rejects it otherwise.
    cfg = provider_mod.ConnectionConfig.from_mapping(
        {"api_url": "ate-env-api.ate-env:7777", "endpoint_url": "gym-proxy.example.com:8080"}
    )
    assert cfg.endpoint_base() == "http://gym-proxy.example.com:8080"
    SandboxEndpoint(endpoint=cfg.endpoint_base(), headers={})
    with pytest.raises(ValueError, match="unknown option"):
        provider_mod.ConnectionConfig.from_mapping({"endpoint": "x"})


# -- bearer token for an ate-env-api that requires one ------------------------


def _token_file(tmp_path: Path, content: str) -> str:
    path = tmp_path / "token"
    path.write_text(content)
    return str(path)


def test_auth_token_file_reaches_the_api_and_the_endpoint(tmp_path):
    async def t():
        state = FakeAteEnv(require_token="s3cret")
        token_file = _token_file(tmp_path, " s3cret\n")
        async with stack(state=state, connection={"auth_token_file": token_file}) as (provider, fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            result = await provider.exec(handle, "echo hi")
            assert result.return_code == 0
            ep = await provider.endpoint(handle, 6000)
            assert ep.headers["X-Env-Authorization"] == "Bearer s3cret"
            await provider.close(handle)
        # Lifecycle and guest calls, unary and streaming, all carried the token.
        assert fake.auth_seen and all(v == ["Bearer s3cret"] for _, v in fake.auth_seen)
        assert {m.rsplit("/", 1)[-1] for m, _ in fake.auth_seen} >= {
            "CreateEnvironment", "GetEnvironment", "StartProcess", "DeleteEnvironment",
        }
        # The config names the file; the token is not in its repr.
        assert "s3cret" not in repr(provider._connection)

    run(t())


def test_auth_token_comes_from_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", "from-env")

    async def t():
        async with stack(state=FakeAteEnv(require_token="from-env")) as (provider, _fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            ep = await provider.endpoint(handle, 6000)
            assert ep.headers["X-Env-Authorization"] == "Bearer from-env"

    run(t())
    # The file wins over the environment.
    cfg = provider_mod.ConnectionConfig.from_mapping({"auth_token_file": _token_file(tmp_path, "file")})
    assert cfg.resolve_auth_token() == "file"


def test_no_token_sends_no_credential():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            await provider.exec(handle, "echo hi")
            ep = await provider.endpoint(handle, 6000)
            assert "X-Env-Authorization" not in ep.headers
        assert fake.auth_seen and all(v == [] for _, v in fake.auth_seen)

    run(t())


def test_a_required_token_that_is_missing_fails_the_create():
    async def t():
        async with stack(state=FakeAteEnv(require_token="s3cret")) as (provider, _fake):
            with pytest.raises(Exception, match="bearer token"):
                await provider.create(SandboxSpec())

    run(t())


def test_an_empty_or_missing_token_file_is_a_config_error(tmp_path):
    with pytest.raises(ValueError, match="is empty"):
        SubstrateSandboxProvider({"connection": {"auth_token_file": _token_file(tmp_path, " \n")}})
    with pytest.raises(ValueError, match="auth_token_file"):
        SubstrateSandboxProvider({"connection": {"auth_token_file": str(tmp_path / "nope")}})


# -- parking: an idle sandbox holds no worker ---------------------------------

PAUSE = {"suspend_policy": "pause_after_exec", "idle_before_suspend_s": 0}
# What each park puts on the wire: a local suspend names its mode; a durable
# one sends none, so it works against an ate-env-api that predates the field.
LOCAL = int(SuspendMode.LOCAL)
DURABLE = int(SuspendMode.UNSPECIFIED)


def _parks(fake: FakeAteEnv, env_id: str) -> list[int]:
    return [mode for eid, mode in fake.suspends if eid == env_id]


def _env_status(fake: FakeAteEnv, env_id: str) -> EnvironmentStatus:
    return EnvironmentStatus(fake.envs[env_id]["status"])


async def _eventually(predicate, *, timeout_s: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


def test_never_policy_leaves_the_sandbox_running():
    async def t():
        async with stack({"idle_before_suspend_s": 0}) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.exec(handle, "echo hi")
            await asyncio.sleep(0.05)
            assert fake.suspends == []
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.RUNNING

    run(t())


def test_pause_policy_parks_after_create_and_after_each_idle_call(tmp_path: Path):
    async def t():
        async with stack(PAUSE) as (provider, fake):
            handle = await provider.create(SandboxSpec(files={"/task/a": "x"}))
            # create() hands back a parked sandbox: it holds no worker through
            # the policy's first think time. The park is a local suspend.
            assert _parks(fake, handle.sandbox_id) == [LOCAL]
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            assert await provider.status(handle) == provider_mod._PAUSED
            # The next command wakes it (any guest call does), runs, and parks it again.
            result = await provider.exec(handle, "echo hi")
            assert (result.return_code, result.error_type) == (0, None)
            assert len(_parks(fake, handle.sandbox_id)) == 2
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            # Uploads and downloads park too.
            src = tmp_path / "up.bin"
            src.write_bytes(b"data")
            await provider.upload_file(handle, src, "/task/up.bin")
            await provider.download_file(handle, "/task/up.bin", tmp_path / "down.bin")
            assert len(_parks(fake, handle.sandbox_id)) == 4

    run(t())


def test_suspend_policy_parks_durably():
    async def t():
        config = {"suspend_policy": "suspend_after_exec", "idle_before_suspend_s": 0}
        async with stack(config) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            assert _parks(fake, handle.sandbox_id) == [DURABLE]
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.SUSPENDED

    run(t())


def test_back_to_back_calls_pay_no_park_in_between():
    async def t():
        config = {"suspend_policy": "pause_after_exec", "idle_before_suspend_s": 0.2}
        async with stack(config) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            for _ in range(3):
                assert (await provider.exec(handle, "echo hi")).return_code == 0
            # Each call dropped the park the previous one scheduled.
            assert fake.suspends == []
            await _eventually(lambda: len(fake.suspends) == 1)
            # One park per idle period, not one per call that found it idle.
            await asyncio.sleep(0.3)
            assert len(fake.suspends) == 1
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED

    run(t())


def test_a_call_waits_for_a_park_under_way():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            # Three state reads at 0.25 s each keep the sandbox SUSPENDING for
            # a while; a guest call that lands in that window fails in the
            # fake (as it does on the router), so a passing exec proves the
            # provider waited for the park to settle.
            fake.suspend_settle_polls = 3
            pausing = asyncio.create_task(provider.pause(handle))
            await asyncio.sleep(0.05)
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.SUSPENDING
            result = await provider.exec(handle, "echo hi")
            assert (result.return_code, result.error_type) == (0, None)
            await pausing
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.RUNNING

    run(t())


def test_a_failed_park_logs_and_leaves_the_sandbox_running(caplog):
    async def t():
        async with stack(PAUSE) as (provider, fake):
            fake.suspend_code = grpc.StatusCode.INTERNAL
            with caplog.at_level(logging.WARNING, logger="nemo_gym_substrate.provider"):
                handle = await provider.create(SandboxSpec())
                result = await provider.exec(handle, "echo hi")
            assert result.return_code == 0
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.RUNNING
            assert sum("parking substrate env" in r.message and "INTERNAL" in r.message for r in caplog.records) == 2

    run(t())


def test_a_local_park_falls_back_to_durable_on_an_older_api(caplog):
    async def t():
        async with stack(PAUSE) as (provider, fake):
            fake.reject_local = True
            with caplog.at_level(logging.WARNING, logger="nemo_gym_substrate.provider"):
                handle = await provider.create(SandboxSpec())
                await provider.exec(handle, "echo hi")
            # The first LOCAL was refused and retried durably; later parks go durable at once.
            assert _parks(fake, handle.sandbox_id) == [LOCAL, DURABLE, DURABLE]
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.SUSPENDED
            assert sum("does not serve suspend_mode" in r.message for r in caplog.records) == 1

    run(t())


def test_pause_and_resume():
    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.pause(handle)
            assert _parks(fake, handle.sandbox_id) == [LOCAL]
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            assert await provider.status(handle) == provider_mod._PAUSED
            await provider.pause(handle)  # already parked: nothing sent
            assert len(fake.suspends) == 1
            probes = _probe_count(fake)
            await provider.resume(handle)
            assert _probe_count(fake) == probes + 1
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.RUNNING
            # Under the `never` policy a resumed sandbox stays running.
            await asyncio.sleep(0.05)
            assert len(fake.suspends) == 1
            await provider.close(handle)
            with pytest.raises(RuntimeError, match="does not exist"):
                await provider.pause(handle)
            with pytest.raises(RuntimeError, match="does not exist"):
                await provider.resume(handle)

    run(t())


def test_pause_parks_a_sandbox_that_proxy_traffic_woke():
    """Requests through endpoint() reach the sandbox without a guest call
    through the provider: the server wakes it while the provider still holds
    it as parked. pause() must then park it again, not return early."""
    async def t():
        async with stack(PAUSE) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            assert _parks(fake, handle.sandbox_id) == [LOCAL]
            # What the router does for a request through the port proxy.
            fake.envs[handle.sandbox_id]["status"] = int(EnvironmentStatus.RUNNING)
            await provider.pause(handle)
            assert _parks(fake, handle.sandbox_id) == [LOCAL, LOCAL]
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            # Parked for real: a second pause() is a no-op.
            await provider.pause(handle)
            assert _parks(fake, handle.sandbox_id) == [LOCAL, LOCAL]

    run(t())


def test_pause_waits_for_a_command_in_flight(monkeypatch):
    monkeypatch.setattr(provider_mod, "_EXEC_GRACE_S", 0.05)
    monkeypatch.setattr(provider_mod, "_MIN_GUEST_TIMEOUT_S", 0.05)

    async def t():
        async with stack(connection={"request_timeout_s": 0.3}) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.hang.add("sleepy")
            running = asyncio.create_task(provider.exec(handle, "sleepy", timeout_s=0.6))
            await asyncio.sleep(0.05)
            with pytest.raises(RuntimeError, match="in flight"):
                await provider.pause(handle)
            assert fake.suspends == []  # never under a running command
            result = await running
            assert result.error_type == "timeout"
            await provider.pause(handle)
            assert len(fake.suspends) == 1

    run(t())


def test_close_drops_a_pending_park_and_aclose_waits_for_one_under_way():
    async def t():
        async with stack({"suspend_policy": "pause_after_exec", "idle_before_suspend_s": 10}) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.close(handle)
            assert handle.sandbox_id not in fake.envs
            assert fake.suspends == []
        async with stack({"suspend_policy": "pause_after_exec", "idle_before_suspend_s": 0.05}) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            await provider.exec(handle, "echo hi")
            await provider.aclose()  # waits out the pending park instead of abandoning it
            assert _parks(fake, handle.sandbox_id) == [LOCAL]

    run(t())


def test_ports_skip_automatic_idle_parking_while_explicit_pause_works(tmp_path: Path):
    async def t():
        async with stack(PAUSE) as (provider, fake):
            handle = await provider.create(SandboxSpec(ports=(6000,)))
            assert fake.suspends == []
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.RUNNING
            assert (await provider.exec(handle, "echo hi")).return_code == 0
            src = tmp_path / "up.bin"
            src.write_bytes(b"ok")
            await provider.upload_file(handle, src, "/task/up.bin")
            await provider.download_file(handle, "/task/up.bin", tmp_path / "down.bin")
            assert fake.suspends == []
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.RUNNING
            # Explicit pause() and resume() still work between endpoint() turns.
            await provider.pause(handle)
            assert _parks(fake, handle.sandbox_id) == [LOCAL]
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            await provider.resume(handle)
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.RUNNING

    run(t())


def test_failed_file_calls_still_schedule_idle_park(tmp_path: Path):
    async def t():
        async with stack(PAUSE) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            assert len(_parks(fake, handle.sandbox_id)) == 1
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            with pytest.raises(NotFoundError):
                await provider.download_file(handle, "/no-such-remote", tmp_path / "out.bin")
            assert len(_parks(fake, handle.sandbox_id)) == 2
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            with pytest.raises(FileNotFoundError):
                await provider.upload_file(handle, tmp_path / "no-such-local", "/task/dst")
            assert len(_parks(fake, handle.sandbox_id)) == 3
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED

    run(t())


def test_pause_wraps_rpc_errors_and_timeouts_as_runtime_error(monkeypatch):
    monkeypatch.setattr(provider_mod, "_PARK_SETTLE_S", 0.05)
    monkeypatch.setattr(provider_mod, "_PARK_POLL_S", 0.01)

    async def t():
        async with stack(connection={"request_timeout_s": 0.05}) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.suspend_code = grpc.StatusCode.PERMISSION_DENIED
            with pytest.raises(RuntimeError, match="PermissionDeniedError"):
                await provider.pause(handle)
            fake.suspend_code = None
            fake.suspend_settle_polls = 10
            fake.get_delay_s = 0.2
            with pytest.raises(RuntimeError, match="no answer from"):
                await provider.pause(handle)

    run(t())


def test_park_settle_loop_retries_transient_get_errors(monkeypatch):
    monkeypatch.setattr(provider_mod, "_PARK_POLL_S", 0.02)

    async def t():
        async with stack() as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.suspend_settle_polls = 2
            fake.fail_gets = 2
            await provider.pause(handle)
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED
            assert fake.fail_gets == 0

    run(t())


def test_pause_under_never_remembers_local_refusal(caplog):
    async def t():
        async with stack() as (provider, fake):
            fake.reject_local = True
            h1 = await provider.create(SandboxSpec())
            h2 = await provider.create(SandboxSpec())
            with caplog.at_level(logging.WARNING, logger="nemo_gym_substrate.provider"):
                await provider.pause(h1)
                await provider.pause(h2)
            assert _parks(fake, h1.sandbox_id) == [LOCAL, DURABLE]
            assert _parks(fake, h2.sandbox_id) == [DURABLE]
            assert provider._park_mode is None
            assert sum("does not serve suspend_mode" in r.message for r in caplog.records) == 1

    run(t())


def test_aclose_waits_for_suspend_rpc_spawned_during_drain(monkeypatch):
    monkeypatch.setattr(provider_mod, "_PARK_SETTLE_S", 0.02)
    monkeypatch.setattr(provider_mod, "_PARK_POLL_S", 0.01)

    async def t():
        config = {"suspend_policy": "pause_after_exec", "idle_before_suspend_s": 0.03}
        async with stack(config, connection={"request_timeout_s": 0.05}) as (provider, fake):
            handle = await provider.create(SandboxSpec())
            fake.suspend_delay_s = 0.2
            assert (await provider.exec(handle, "echo hi")).return_code == 0
            # _park_after wakes during aclose(), spawns the slow suspend RPC,
            # and finishes after its 0.05 s wait while the RPC is still running.
            await provider.aclose()
            assert provider._park_tasks == set()
            assert _env_status(fake, handle.sandbox_id) == EnvironmentStatus.PAUSED

    run(t())


def test_parking_config_is_validated():
    with pytest.raises(ValueError, match="suspend_policy must be one of"):
        SubstrateSandboxProvider({"create": {"suspend_policy": "sometimes"}})
    with pytest.raises(ValueError, match="idle_before_suspend_s must be >= 0"):
        SubstrateSandboxProvider({"create": {"idle_before_suspend_s": -1}})
    cfg = provider_mod.CreateConfig.from_mapping({"suspend_policy": "suspend_after_exec"})
    assert (cfg.suspend_policy, cfg.idle_before_suspend_s) == ("suspend_after_exec", 1.0)


def test_provider_lifecycle_has_no_telemetry_side_effects():
    """A provider process never constructs an Observer: a create/exec/close
    cycle leaves prometheus_client unimported and the fleet collectors
    unregistered, even where the metrics extra is installed."""
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(
        """
        import asyncio, sys
        from ate_env_rl import observability
        from nemo_gym_substrate._compat import SandboxSpec
        from nemo_gym_substrate.provider import SubstrateSandboxProvider
        from tests.fakes import serve

        async def main():
            async with serve() as (target, _fake):
                p = SubstrateSandboxProvider({"connection": {"api_url": target}})
                h = await p.create(SandboxSpec(ready_timeout_s=10))
                assert (await p.exec(h, "true")).return_code == 0
                await p.close(h)
                await p.aclose()

        asyncio.run(main())
        assert "prometheus_client" not in sys.modules, "provider imported prometheus_client"
        assert observability._METRICS_READY is False and observability.PHASE_LATENCY is None
        print("clean")
        """
    )
    root = Path(__file__).resolve().parents[1]
    out = subprocess.run([sys.executable, "-c", code], cwd=root, capture_output=True,
                         text=True, timeout=60, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "clean"
