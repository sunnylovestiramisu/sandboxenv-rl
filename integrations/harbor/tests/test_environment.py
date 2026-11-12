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

"""AteEnvEnvironment against the local fake: lifecycle, exec, files, errors."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import shutil
import stat
import tarfile
import time
from pathlib import Path

import pytest
from ate_env_rl import FailedPreconditionError, RpcError
from harbor.models.task.config import EnvironmentConfig
from harbor.models.trial.config import ResourceMode

from ate_env_harbor import AteEnvEnvironment, parse_worker_selector
from ate_env_harbor.environment import RECORD_FILE, WHOAMI_COMMAND, pack_dir_as_root

from .conftest import ALPINE


def commands(fake) -> list[str]:
    return [e["command"][-1] for e in fake.state.execs]


TOOLS = ("sh", "mkdir", "tar", "gzip", "rm", "find", "chmod", "cat", "env", "dirname", "id", "sleep", "pwd", "true")


def minimal_bin(tmp_path: Path) -> Path:
    """A PATH with the tools the provider and the tests need, and nothing else."""
    bin_dir = tmp_path / "minbin"
    bin_dir.mkdir()
    for name in TOOLS:
        found = shutil.which(name)
        if found:
            (bin_dir / name).symlink_to(found)
    return bin_dir


async def test_start_creates_from_the_digest_and_the_class_template(fake, make_env):
    env = make_env()
    await env.start(force_build=False)
    try:
        assert fake.state.creates == [
            {
                "id": env.env_id,
                "atespace": "ate-env",
                "template": "harbor-c1-m512",
                "rootfs_image": ALPINE,
                "worker_selector": {},
            }
        ]
        assert env.env_id.startswith("hb-echo-abcdef")
        assert "true" in commands(fake) and WHOAMI_COMMAND in commands(fake)
        record = json.loads((env.trial_paths.trial_dir / RECORD_FILE).read_text())
        assert record["id"] == env.env_id
        assert record["template"] == "harbor-c1-m512"
        assert record["rootfs_image"] == ALPINE
        assert record["image_user"] == "root"
        assert record["session_id"] == env.session_id
    finally:
        await env.stop(delete=True)
    assert fake.state.deletes == [env.env_id]
    assert fake.state.envs == {}


async def test_separate_verifier_environment_keeps_its_own_record(fake, make_env, tmp_path):
    # Harbor's separate verifier mode starts a second environment for the same
    # trial directory; found live: its record overwrote the agent's.
    from harbor.models.trial.paths import TrialPaths

    paths = TrialPaths(trial_dir=tmp_path / "echo__AbCdEf")  # Harbor names the trial dir after the trial
    paths.mkdir()
    agent = make_env(session_id="echo__AbCdEf__env")
    verifier = make_env(session_id="echo__AbCdEf__verifier__trial")
    agent.trial_paths = verifier.trial_paths = paths
    await agent.start(force_build=False)
    await verifier.start(force_build=False)
    try:
        assert agent.record_file == "ate_env.json" and verifier.record_file == "ate_env.verifier.trial.json"
        a = json.loads((paths.trial_dir / "ate_env.json").read_text())
        v = json.loads((paths.trial_dir / "ate_env.verifier.trial.json").read_text())
        assert (a["id"], a["session_id"]) == (agent.env_id, "echo__AbCdEf__env")
        assert (v["id"], v["session_id"]) == (verifier.env_id, "echo__AbCdEf__verifier__trial")
    finally:
        await agent.stop(delete=True)
        await verifier.stop(delete=True)


def test_record_file_survives_harbor_truncation_and_multi_step_verifiers(fake, make_env, tmp_path):
    # Harbor 0.25 truncates a separate verifier's session id to 63 characters:
    # safe[:53] + "__" + sha1[:8] (trial.py, _separate_verifier_session_id),
    # which cuts "__verifier__" off for trial names of 42 characters or more.
    import hashlib

    from harbor.models.trial.paths import TrialPaths

    trial_name = "swebench-verified-django-django-16379-fix__aBcDeFg"  # 50 characters
    assert len(trial_name) == 50
    paths = TrialPaths(trial_dir=tmp_path / trial_name)
    paths.mkdir()

    def verifier_session(key: str) -> str:
        raw = f"{trial_name}__verifier__{key}"
        safe = "".join(c if c.isalnum() or c in "-._" else "_" for c in raw)
        return safe if len(safe) <= 63 else safe[:53] + "__" + hashlib.sha1(raw.encode()).hexdigest()[:8]

    agent = make_env(session_id=f"{trial_name}__env")
    step_a = make_env(session_id=verifier_session("grade"))
    step_b = make_env(session_id=verifier_session("lint"))
    for env in (agent, step_a, step_b):
        env.trial_paths = paths
    assert "__verifier__" not in step_a.session_id  # the marker is cut off
    assert agent.record_file == "ate_env.json"
    assert step_a.record_file.startswith("ate_env.verifier.") and step_a.record_file.endswith(".json")
    assert step_a.record_file != step_b.record_file != agent.record_file
    # A short trial name keeps the step name readable.
    short = make_env(session_id="echo__AbCdEf__verifier__grade")
    short.trial_paths = TrialPaths(trial_dir=tmp_path / "echo__AbCdEf")
    assert short.record_file == "ate_env.verifier.grade.json"


async def test_worker_selector_narrows_the_pool(fake, make_env):
    env = make_env(worker_selector="pool=a, zone=b")
    assert env.worker_selector == {"pool": "a", "zone": "b"}
    await env.start(force_build=False)
    try:
        assert fake.state.creates[-1]["worker_selector"] == {"pool": "a", "zone": "b"}
        record = json.loads((env.trial_paths.trial_dir / RECORD_FILE).read_text())
        assert record["worker_selector"] == {"pool": "a", "zone": "b"}
    finally:
        await env.stop(delete=True)
    # The fallback create under the auto policy carries it too.
    fake.state.missing_templates.add("harbor-c1-m512")
    env = make_env(worker_selector={"pool": "a"})
    await env.start(force_build=False)
    try:
        assert fake.state.creates[-1]["template"] == "default-template"
        assert fake.state.creates[-1]["worker_selector"] == {"pool": "a"}
    finally:
        await env.stop(delete=True)
    assert make_env().worker_selector == {}
    with pytest.raises(ValueError, match="key=value"):
        make_env(worker_selector="pool")


def test_worker_selector_accepts_the_cli_forms():
    assert parse_worker_selector(None) == {} and parse_worker_selector("") == {}
    assert parse_worker_selector(" pool = a ,zone=b") == {"pool": "a", "zone": "b"}
    assert parse_worker_selector("pool=") == {"pool": ""}  # as ate-env create: an empty value
    assert parse_worker_selector({"pool": "a"}) == {"pool": "a"}


@pytest.mark.parametrize(
    "value",
    [
        # Harbor's --ek decodes JSON: a list, a bool and a number arrive as such.
        pytest.param(["d0"], id="list"),
        pytest.param(True, id="bool"),
        pytest.param(123, id="int"),
        pytest.param({"": "a"}, id="empty-key"),
        pytest.param({"gpu": True}, id="bool-value"),
        pytest.param({"pool": None}, id="none-value"),
        # What ate-env create --worker-selector refuses.
        pytest.param("pool=a,pool=b", id="twice"),
        pytest.param("pool=a,pool=a", id="twice-same"),
        pytest.param(",", id="comma"),
        pytest.param("pool=a,,zone=b", id="empty-item"),
        pytest.param("=a", id="no-key"),
        pytest.param(" ", id="blank"),
    ],
)
def test_worker_selector_refuses_what_the_cli_refuses(value, make_env):
    with pytest.raises(ValueError, match="worker_selector"):
        parse_worker_selector(value)
    with pytest.raises(ValueError, match="worker_selector"):
        make_env(worker_selector=value)


async def test_environment_dir_is_uploaded_to_the_workdir(fake, make_env):
    env = make_env()
    await env.start(force_build=False)
    try:
        assert (make_env.work / "hello.txt").read_text() == "hello from environment/\n"
    finally:
        await env.stop(delete=True)


async def test_no_resources_means_the_default_template(fake, make_env):
    cfg = EnvironmentConfig(docker_image=ALPINE, workdir=str(make_env.work))
    env = make_env(config=cfg, template="base")
    await env.start(force_build=False)
    await env.stop(delete=True)
    assert fake.state.creates[-1]["template"] == "base"


async def test_overrides_pick_the_class(fake, make_env):
    env = make_env(override_cpus=2, override_memory_mb=4096)
    await env.start(force_build=False)
    await env.stop(delete=True)
    assert fake.state.creates[-1]["template"] == "harbor-c2-m4096"
    env = make_env(override_cpus=0.5)
    await env.start(force_build=False)
    await env.stop(delete=True)
    assert fake.state.creates[-1]["template"] == "harbor-c500m-m512"


async def test_ignored_resources_use_the_default_template(fake, make_env):
    env = make_env(
        cpu_enforcement_policy=ResourceMode.IGNORE, memory_enforcement_policy=ResourceMode.IGNORE
    )
    await env.start(force_build=False)
    await env.stop(delete=True)
    assert fake.state.creates[-1]["template"] == "default-template"


async def test_missing_class_template_falls_back_under_auto(fake, make_env, caplog):
    fake.state.missing_templates.add("harbor-c1-m512")
    env = make_env()
    with caplog.at_level(logging.WARNING):
        await env.start(force_build=False)
    await env.stop(delete=True)
    assert [c["template"] for c in fake.state.creates] == ["harbor-c1-m512", "default-template"]
    assert "ate-env manifest template --template harbor-c1-m512 --cpu 1 --memory 512Mi" in caplog.text
    assert "register-templates.sh" in caplog.text


async def test_missing_class_template_fails_under_limit(fake, make_env):
    fake.state.missing_templates.add("harbor-c1-m512")
    env = make_env(cpu_enforcement_policy=ResourceMode.LIMIT)
    with pytest.raises(FailedPreconditionError, match="register it with: ate-env manifest template"):
        await env.start(force_build=False)
    assert len(fake.state.creates) == 1
    assert fake.state.envs == {}


async def test_exec_maps_cwd_env_timeout_and_exit_codes(fake, make_env, tmp_path):
    env = make_env(persistent_env={"PERSIST": "yes"})
    await env.start(force_build=False)
    try:
        result = await env.exec("pwd")
        assert result.return_code == 0 and result.stdout.strip() == str(make_env.work)
        other = tmp_path / "other"
        other.mkdir()
        assert (await env.exec("pwd", cwd=str(other))).stdout.strip() == str(other)
        assert (await env.exec("echo $FOO-$PERSIST", env={"FOO": "bar"})).stdout.strip() == "bar-yes"
        result = await env.exec("echo out; echo err >&2; exit 7")
        assert (result.return_code, result.stdout.strip(), result.stderr.strip()) == (7, "out", "err")
        started = time.monotonic()
        result = await env.exec("sleep 20", timeout_sec=1)
        assert result.return_code == 137
        assert time.monotonic() - started < 10
        assert fake.state.execs[-1]["timeout"] == 1
    finally:
        await env.stop(delete=True)


async def test_exec_as_the_image_user_runs_the_command_as_is(fake, make_env):
    env = make_env()
    await env.start(force_build=False)
    try:
        await env.exec("echo hi", user="root")
        assert fake.state.execs[-1]["command"] == ["sh", "-c", "echo hi"]
        await env.exec("echo hi")  # default_user None: as is
        assert fake.state.execs[-1]["command"] == ["sh", "-c", "echo hi"]
    finally:
        await env.stop(delete=True)


async def test_exec_as_another_user_needs_sudo_or_su(fake, make_env, tmp_path):
    fake.state.whoami = "app"
    bin_dir = minimal_bin(tmp_path)  # neither sudo nor su
    fake.state.child_path = str(bin_dir)
    env = make_env()
    await env.start(force_build=False)
    try:
        with pytest.raises(RuntimeError, match="needs sudo"):
            await env.exec("echo hi", user="root")
    finally:
        await env.stop(delete=True)

    # A sudo in the image is used, non-interactively. Like the real one, the
    # stub resets the environment (env_reset), thus the variables Harbor
    # passes reach the command only because the provider inlines them.
    sudo = bin_dir / "sudo"
    sudo.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" > \"$SUDO_LOG\"\n"  # not echo: dash's echo eats a leading -n
        "shift 3\n"  # -n -u <user>
        'exec env -i PATH="$PATH" "$@"\n'
    )
    sudo.chmod(0o755)
    env2 = make_env(persistent_env={"PERSIST": "kept"})
    await env2.start(force_build=False)
    try:
        log = tmp_path / "sudo.log"
        result = await env2.exec("echo $PERSIST:$MARK", user="root", env={"SUDO_LOG": str(log), "MARK": "m"})
        assert (result.return_code, result.stdout.strip()) == (0, "kept:m")
        words = log.read_text().split()
        assert words[:4] == ["-n", "-u", "root", "env"]
        assert "MARK=m" in words and "PERSIST=kept" in words
        assert fake.state.execs[-1]["command"][-1].startswith("sudo -n -u root env ")
    finally:
        await env2.stop(delete=True)


async def test_exec_as_another_user_from_root_uses_su(fake, make_env, tmp_path):
    fake.state.whoami = "root"
    bin_dir = minimal_bin(tmp_path)
    su = bin_dir / "su"
    su.write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$*\" > \"$SU_LOG\"\n"
        '[ "$2" = -s ] && [ "$4" = -c ] || exit 64\n'  # su <user> -s /bin/sh -c <inner>
        'exec env -i PATH="$PATH" /bin/sh -c "$5"\n'
    )
    su.chmod(0o755)
    fake.state.child_path = str(bin_dir)
    env = make_env()
    await env.start(force_build=False)
    try:
        log = tmp_path / "su.log"
        result = await env.exec("echo as=$MARK", user="app", env={"SU_LOG": str(log), "MARK": "m"})
        assert (result.return_code, result.stdout.strip()) == (0, "as=m")
        assert log.read_text().split()[:4] == ["app", "-s", "/bin/sh", "-c"]
        with pytest.raises(RuntimeError, match="su wants a user name"):
            await env.exec("true", user=1000)
    finally:
        await env.stop(delete=True)


async def test_numeric_user_goes_to_sudo_as_a_uid(fake, make_env):
    env = make_env()
    env._image_user = "app"
    wrapped = env._as_user_command("echo hi", 1000, {"A": "1 2"}, "sudo")
    assert wrapped == "sudo -n -u '#1000' env 'A=1 2' sh -c 'echo hi'"
    assert env._as_user_command("echo hi", "app", None, "sudo") == "echo hi"


async def test_exec_without_a_timeout_gets_the_configured_bound(fake, make_env):
    env = make_env()
    await env.start(force_build=False)
    try:
        await env.exec("true")
        assert fake.state.execs[-1]["timeout"] == 24 * 3600
    finally:
        await env.stop(delete=True)
    env = make_env(exec_timeout_sec=5)
    await env.start(force_build=False)
    try:
        await env.exec("true")
        assert fake.state.execs[-1]["timeout"] == 5
    finally:
        await env.stop(delete=True)


async def test_file_and_directory_round_trips(fake, make_env, tmp_path):
    env = make_env()
    await env.start(force_build=False)
    try:
        # upload_file keeps the mode and creates the parent directory.
        script = tmp_path / "run.sh"
        script.write_text("#!/bin/sh\necho ran\n")
        script.chmod(0o755)
        target = tmp_path / "in" / "deep" / "run.sh"
        await env.upload_file(script, str(target))
        assert stat.S_IMODE(target.stat().st_mode) == 0o755
        assert (await env.exec(str(target))).stdout.strip() == "ran"

        # upload_dir keeps modes, empty directories, symlinks and big files.
        src = tmp_path / "src"
        (src / "sub").mkdir(parents=True)
        (src / "empty").mkdir()
        (src / "a.txt").write_text("a")
        (src / "sub" / "big.bin").write_bytes(os.urandom(8 * 1024 * 1024))
        (src / "sub" / "x.sh").write_text("#!/bin/sh\n")
        (src / "sub" / "x.sh").chmod(0o700)
        (src / "link").symlink_to("a.txt")
        dst = tmp_path / "dst"
        await env.upload_dir(src, str(dst))
        assert (dst / "a.txt").read_text() == "a"
        assert (dst / "empty").is_dir()
        assert (dst / "link").is_symlink() and (dst / "link").read_text() == "a"
        assert stat.S_IMODE((dst / "sub" / "x.sh").stat().st_mode) == 0o700
        assert (dst / "sub" / "big.bin").read_bytes() == (src / "sub" / "big.bin").read_bytes()

        # download_file and download_dir bring them back.
        back = tmp_path / "back.txt"
        await env.download_file(str(dst / "a.txt"), back)
        assert back.read_text() == "a"
        back_dir = tmp_path / "back"
        await env.download_dir(str(dst), back_dir)
        assert (back_dir / "sub" / "big.bin").read_bytes() == (src / "sub" / "big.bin").read_bytes()
        assert (back_dir / "empty").is_dir()
        # No archive is left behind in the scratch directory.
        assert not list((tmp_path / "scratch").glob(".harbor-*"))
    finally:
        await env.stop(delete=True)


async def test_upload_dir_removes_the_archive_when_the_unpack_call_raises(fake, make_env, tmp_path):
    env = make_env()
    await env.start(force_build=False)
    try:
        src = tmp_path / "src"
        src.mkdir()
        (src / "a").write_text("a")
        fake.state.fail_exec_containing = "tar -xzf"
        with pytest.raises(RpcError):
            await env.upload_dir(src, str(tmp_path / "dst"))
        archives = [p for p in (tmp_path / "scratch").glob(".harbor-upload-*")]
        assert archives == []
        assert commands(fake)[-1].startswith("rm -f ")
    finally:
        await env.stop(delete=True)


async def test_shared_client_cache_evicts_closed_loops(fake):
    from ate_env_harbor import _client

    def on_its_own_loop() -> None:
        async def make() -> None:
            _client.shared_client(fake.addr, None)

        asyncio.run(make())  # that loop is closed when asyncio.run returns

    await asyncio.to_thread(on_its_own_loop)
    assert any(bound.is_closed() for _, bound in _client._clients.values())
    _client.shared_client(fake.addr, None)
    assert not any(bound.is_closed() for _, bound in _client._clients.values())


def test_uploaded_archives_are_owned_by_root(tmp_path):
    # GNU tar extracting as root restores the archived owner; inside an actor
    # a chown to the host's uid fails. Pack with root ownership instead.
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "empty").mkdir()
    (src / "a.sh").write_text("#!/bin/sh\n")
    (src / "a.sh").chmod(0o750)
    (src / "link").symlink_to("a.sh")
    with tarfile.open(fileobj=io.BytesIO(pack_dir_as_root(src)), mode="r:gz") as tar:
        members = {m.name: m for m in tar.getmembers()}
    assert set(members) == {".", "./sub", "./empty", "./a.sh", "./link"}
    assert all(m.uid == 0 and m.gid == 0 and m.uname == "root" and m.gname == "root" for m in members.values())
    assert members["./a.sh"].mode & 0o777 == 0o750
    assert members["./link"].issym() and members["./link"].linkname == "a.sh"
    assert members["./empty"].isdir()


def test_pack_dir_as_root_follows_a_symlinked_source_dir(tmp_path):
    # tar.add on a symlinked source_dir archives "." as a SYMLINK member and
    # GNU tar then fails to extract it into an existing directory. The top
    # level is resolved; symlinks inside the tree stay symlinks.
    real = tmp_path / "real"
    real.mkdir()
    (real / "a.sh").write_text("#!/bin/sh\n")
    (real / "link").symlink_to("a.sh")
    (tmp_path / "alias").symlink_to(real, target_is_directory=True)
    with tarfile.open(fileobj=io.BytesIO(pack_dir_as_root(tmp_path / "alias")), mode="r:gz") as tar:
        members = {m.name: m for m in tar.getmembers()}
    assert set(members) == {".", "./a.sh", "./link"}
    assert members["."].isdir()
    assert members["./link"].issym() and members["./link"].linkname == "a.sh"


async def test_download_dir_of_a_missing_directory_fails_clearly(fake, make_env, tmp_path):
    env = make_env()
    await env.start(force_build=False)
    try:
        with pytest.raises(RuntimeError, match="packing"):
            await env.download_dir(str(tmp_path / "nope"), tmp_path / "out")
    finally:
        await env.stop(delete=True)


async def test_readiness_waits_through_unavailable_answers(fake, make_env):
    fake.state.unavailable_calls = 3
    env = make_env()
    await env.start(force_build=False)
    await env.stop(delete=True)
    assert fake.state.unavailable_calls == 0


async def test_readiness_timeout_deletes_the_environment(fake, make_env):
    fake.state.unavailable_calls = 10**6
    env = make_env(create_timeout_sec=2)
    with pytest.raises(TimeoutError, match="did not answer within 2s"):
        await env.start(force_build=False)
    assert fake.state.deletes == [env.env_id]
    assert fake.state.envs == {}
    with pytest.raises(RuntimeError, match="not started"):
        await env.exec("true")


async def test_failure_after_create_deletes_the_environment(fake, make_env):
    cfg = EnvironmentConfig(docker_image=ALPINE, workdir="/dev/null/nope")
    env = make_env(config=cfg)
    with pytest.raises(RuntimeError, match="mkdir -p /dev/null/nope failed"):
        await env.start(force_build=False)
    assert fake.state.deletes == [env.env_id]
    assert fake.state.envs == {}


async def test_concurrent_environments_on_one_dockerfile_build_once(fake, make_env, stubs, tmp_path, monkeypatch):
    # Harbor makes one environment per trial; the build and its digest are shared.
    monkeypatch.setenv("STUB_BUILD_SLEEP", "0.3")
    env_dir = tmp_path / "dockerfile-task" / "environment"
    env_dir.mkdir(parents=True)
    (env_dir / "Dockerfile").write_text("FROM alpine:3.20\n")
    cfg = EnvironmentConfig(workdir=str(make_env.work))
    envs = [
        make_env(config=cfg, environment_dir=env_dir, build="local",
                 registry="reg.example/harbor", docker_bin=stubs.docker)
        for _ in range(3)
    ]
    await asyncio.gather(*(e.start(force_build=False) for e in envs))
    try:
        assert len(stubs.builds()) == 1
        images = {c["rootfs_image"] for c in fake.state.creates}
        assert len(images) == 1 and next(iter(images)).startswith("reg.example/harbor/echo@sha256:")
    finally:
        await asyncio.gather(*(e.stop(delete=True) for e in envs))


async def test_create_cancelled_after_the_server_registered_deletes_by_id(fake, make_env):
    """Harbor wraps start() in build_timeout_sec. A create that the server
    completed but that the caller did not wait out must not leak: the id is
    known before the call, thus the cleanup deletes by id."""
    fake.state.create_delay = 2.0
    env = make_env()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(env.start(force_build=False), 0.3)
    assert fake.state.deletes == [env.env_id]
    assert fake.state.envs == {}
    with pytest.raises(RuntimeError, match="not started"):
        await env.exec("true")


async def test_create_timeout_covers_the_create_itself(fake, make_env):
    """create_timeout_sec starts before the create, not after it returns."""
    fake.state.create_delay = 1.0
    env = make_env(create_timeout_sec=0.5)
    fake.state.unavailable_calls = 10**6
    with pytest.raises(TimeoutError, match="did not answer within 0.5s"):
        await env.start(force_build=False)
    assert fake.state.deletes == [env.env_id]


async def test_create_failure_is_raised_as_is(fake, make_env):
    import grpc

    fake.state.create_code = grpc.StatusCode.PERMISSION_DENIED
    env = make_env()
    with pytest.raises(Exception, match="create refused"):
        await env.start(force_build=False)


async def test_stop_without_delete_suspends(fake, make_env, caplog):
    env = make_env()
    await env.start(force_build=False)
    with caplog.at_level(logging.INFO):
        await env.stop(delete=False)
    assert fake.state.suspends == [env.env_id]
    assert fake.state.deletes == []
    assert env.env_id in fake.state.envs
    assert f"ate-env {env.env_id} shell --api {fake.addr} --atespace ate-env 'cat " in caplog.text
    await env.stop(delete=False)  # idempotent
    assert len(fake.state.suspends) == 1


async def test_token_is_sent_and_never_recorded(fake, make_env, caplog):
    fake.state.required_token = "s3cret-token"
    env = make_env(api_token="s3cret-token")
    with caplog.at_level(logging.DEBUG):
        await env.start(force_build=False)
        await env.exec("true")
        await env.stop(delete=True)
    assert "s3cret-token" not in caplog.text
    assert "s3cret-token" not in (env.trial_paths.trial_dir / RECORD_FILE).read_text()
    bare = make_env()
    with pytest.raises(RpcError):
        await bare.start(force_build=False)


async def test_token_kwarg_warns_once_without_the_value(fake, make_env, caplog, monkeypatch):
    import ate_env_harbor.environment as module

    monkeypatch.setattr(module, "_warned_token_kwarg", False)
    with caplog.at_level(logging.WARNING):
        make_env(api_token="s3cret-token")
        make_env(api_token="s3cret-token")
    warnings = [r for r in caplog.records if "result.json" in r.getMessage()]
    assert len(warnings) == 1
    assert "SUBSTRATE_ENV_AUTH_TOKEN" in warnings[0].getMessage()
    assert "s3cret-token" not in caplog.text


async def test_attach_names_the_cli_command(fake, make_env):
    env = make_env()
    with pytest.raises(RuntimeError, match=f"ate-env {env.env_id} shell --api .* '<command>'"):
        await env.attach()


def test_definition_validation(fake, make_env, task_dir, tmp_path):
    compose_dir = tmp_path / "compose" / "environment"
    compose_dir.mkdir(parents=True)
    (compose_dir / "docker-compose.yaml").write_text("services: {}\n")
    with pytest.raises(ValueError, match="Compose"):
        make_env(environment_dir=compose_dir)
    empty = tmp_path / "empty" / "environment"
    empty.mkdir(parents=True)
    cfg = EnvironmentConfig(workdir=str(make_env.work))
    with pytest.raises(FileNotFoundError):
        make_env(environment_dir=empty, config=cfg)
    (empty / "Dockerfile").write_text("FROM alpine\n")
    with pytest.raises(ValueError, match="build=none"):
        make_env(environment_dir=empty, config=cfg)
    with pytest.raises(ValueError, match="build must be one of"):
        make_env(build="remote")


def test_type_and_capabilities(fake, make_env):
    env = make_env()
    assert AteEnvEnvironment.type() == "ate-env"
    assert env.capabilities.mounted is False and env.capabilities.docker_compose is False
    caps = AteEnvEnvironment.resource_capabilities()
    assert caps.cpu_limit and caps.memory_limit and caps.fractional_cpus and not caps.cpu_request


async def test_preflight_checks_the_api_from_the_environment(fake, monkeypatch):
    monkeypatch.delenv("SUBSTRATE_ENV_API", raising=False)
    monkeypatch.delenv("SUBSTRATE_ENV_AUTH_TOKEN", raising=False)
    await asyncio.to_thread(AteEnvEnvironment.preflight)  # nothing configured: no check
    monkeypatch.setenv("SUBSTRATE_ENV_API", fake.addr)
    await asyncio.to_thread(AteEnvEnvironment.preflight)
    fake.state.required_token = "tok"
    with pytest.raises(SystemExit, match="SUBSTRATE_ENV_AUTH_TOKEN"):
        await asyncio.to_thread(AteEnvEnvironment.preflight)
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", "tok")
    await asyncio.to_thread(AteEnvEnvironment.preflight)
    # Harbor 0.26 passes the job kwargs: api_url and api_token select the probe.
    monkeypatch.delenv("SUBSTRATE_ENV_API")
    monkeypatch.delenv("SUBSTRATE_ENV_AUTH_TOKEN")
    await asyncio.to_thread(
        AteEnvEnvironment.preflight, {"api_url": fake.addr, "api_token": "tok"}
    )
    with pytest.raises(SystemExit, match="SUBSTRATE_ENV_AUTH_TOKEN"):
        await asyncio.to_thread(AteEnvEnvironment.preflight, {"api_url": fake.addr})
    monkeypatch.setenv("SUBSTRATE_ENV_API", "127.0.0.1:1")
    with pytest.raises(SystemExit, match="cannot reach|no answer"):
        await asyncio.to_thread(AteEnvEnvironment.preflight)
    # Inside a running loop (Harbor's local validation path) the probe runs on
    # a worker thread instead of being skipped. Only the negative case can run
    # here: the fake API is served by this very loop, which the call blocks.
    with pytest.raises(SystemExit, match="cannot reach|no answer"):
        AteEnvEnvironment.preflight()


def test_task_dir_fixture_is_the_echo_task(task_dir: Path):
    assert (task_dir / "tests" / "test.sh").exists()


# --- tools as a second layer: tool_paths ---


def _layer(tmp_path: Path) -> Path:
    tools = tmp_path / "layer" / "bin"
    tools.mkdir(parents=True)
    tool = tools / "hb-tool"
    tool.write_text("#!/bin/sh\necho from-layer\n")
    tool.chmod(0o755)
    return tools


async def test_tool_paths_go_first_on_path_for_every_exec(fake, make_env, tmp_path):
    tools = _layer(tmp_path)
    env = make_env(tool_paths=str(tools))
    await env.start(force_build=False)
    try:
        result = await env.exec("hb-tool")
        assert (result.return_code, result.stdout.strip()) == (0, "from-layer")
        command = fake.state.execs[-1]["command"][-1]
        assert command.startswith("PATH=") and str(tools) in command and command.endswith("; hb-tool")
        assert "${PATH:+:$PATH}" in command
        result = await env.exec('echo "$PATH"')
        assert result.stdout.startswith(f"{tools}:")
        # An empty PATH gets the tools alone: no trailing colon, no cwd on PATH.
        result = await env.exec("sh -c 'unset PATH; " + env._tooled('echo "$PATH"') + "'")
        assert result.stdout.strip() == str(tools)
        record = json.loads((env.trial_paths.trial_dir / RECORD_FILE).read_text())
        assert record["tool_paths"] == [str(tools)]
        assert record["tool_paths_profile"] == env._tool_paths_profile
    finally:
        await env.stop(delete=True)


def test_tool_paths_accepts_a_list_or_a_colon_separated_string(fake, make_env):
    assert make_env(tool_paths="/opt/a/bin:/opt/b/bin").tool_paths == ("/opt/a/bin", "/opt/b/bin")
    assert make_env(tool_paths=["/opt/a/bin"]).tool_paths == ("/opt/a/bin",)
    assert make_env(tool_paths=["/opt/a/bin:/opt/b/bin"]).tool_paths == ("/opt/a/bin", "/opt/b/bin")
    assert make_env().tool_paths == ()
    with pytest.raises(ValueError, match="absolute"):
        make_env(tool_paths="opt/a/bin")
    with pytest.raises(ValueError, match="absolute"):
        make_env(tool_paths=["/opt/a/bin:rel"])  # a list item that hides a relative path


async def test_missing_tool_path_fails_start_and_deletes_the_environment(fake, make_env, tmp_path):
    absent = tmp_path / "no-layer" / "bin"
    env = make_env(tool_paths=str(absent))
    with pytest.raises(RuntimeError, match="--layer"):
        await env.start(force_build=False)
    assert fake.state.deletes == [env.env_id]
    assert fake.state.envs == {}


async def test_tool_paths_survive_the_user_switch(fake, make_env, tmp_path):
    fake.state.whoami = "app"
    bin_dir = minimal_bin(tmp_path)
    sudo = bin_dir / "sudo"
    sudo.write_text('#!/bin/sh\nshift 3\nexec env -i PATH="$PATH" "$@"\n')  # resets the environment, like sudo
    sudo.chmod(0o755)
    fake.state.child_path = str(bin_dir)
    tools = _layer(tmp_path)
    env = make_env(tool_paths=str(tools))
    await env.start(force_build=False)
    try:
        result = await env.exec("hb-tool", user="root")
        assert (result.return_code, result.stdout.strip()) == (0, "from-layer")
    finally:
        await env.stop(delete=True)


async def test_file_helpers_see_the_tool_path(fake, make_env, tmp_path):
    tools = _layer(tmp_path)
    env = make_env(tool_paths=str(tools))
    await env.start(force_build=False)
    try:
        src = tmp_path / "src"
        src.mkdir()
        (src / "a").write_text("a")
        await env.upload_dir(src, str(tmp_path / "dst"))
        unpack = [c for c in commands(fake) if "tar -xzf" in c][-1]
        assert unpack.startswith("PATH=") and str(tools) in unpack
        assert (tmp_path / "dst" / "a").read_text() == "a"
        await env.download_dir(str(tmp_path / "dst"), tmp_path / "back")
        pack, cleanup = [c for c in commands(fake) if "tar -czf" in c or c.endswith(".tgz")][-2:]
        assert pack.startswith("PATH=") and cleanup.startswith("PATH=") and "rm -f" in cleanup
    finally:
        await env.stop(delete=True)


async def test_tool_paths_profile_gives_login_shells_the_tools(fake, make_env, tmp_path):
    tools = _layer(tmp_path)
    profile = tmp_path / "etc" / "profile.d" / "zz-harbor-tools.sh"
    env = make_env(tool_paths=str(tools), tool_paths_profile=str(profile))
    await env.start(force_build=False)
    try:
        text = profile.read_text()
        assert text.splitlines()[-1] == f"PATH={tools}${{PATH:+:$PATH}}; export PATH"
        # What /etc/profile does on Debian and Alpine: set PATH, then source the drop-in.
        result = await env.exec(f"PATH=/usr/bin:/bin; . {profile}; command -v hb-tool; echo \"$PATH\"")
        assert result.stdout.splitlines() == [f"{tools}/hb-tool", f"{tools}:/usr/bin:/bin"]
    finally:
        await env.stop(delete=True)


async def test_tool_paths_profile_is_a_warning_when_not_writable(fake, make_env, tmp_path, caplog):
    tools = _layer(tmp_path)
    env = make_env(tool_paths=str(tools), tool_paths_profile="/dev/null/zz-harbor-tools.sh")
    with caplog.at_level(logging.WARNING):
        await env.start(force_build=False)
    try:
        assert "could not write /dev/null/zz-harbor-tools.sh" in caplog.text
        assert (await env.exec("hb-tool")).stdout.strip() == "from-layer"
    finally:
        await env.stop(delete=True)


async def test_tool_paths_profile_can_be_disabled(fake, make_env, tmp_path):
    tools = _layer(tmp_path)
    env = make_env(tool_paths=str(tools), tool_paths_profile="")
    await env.start(force_build=False)
    try:
        assert not any(w[0].endswith("zz-harbor-tools.sh") for w in fake.state.writes)
        record = json.loads((env.trial_paths.trial_dir / RECORD_FILE).read_text())
        assert record["tool_paths_profile"] is None
    finally:
        await env.stop(delete=True)
