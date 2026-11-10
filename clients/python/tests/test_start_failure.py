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

"""ate-env-api's start-failure trailer (ate-env-start-failure) becomes a
retryable SandboxStartError with its reason, at the client and through the
runtime a fleet hands to process_fn."""

from __future__ import annotations

import pickle

import grpc
import pytest

from ate_env_rl.errors import (
    START_FAILURE_CRASHED,
    START_FAILURE_KEY,
    START_FAILURE_NO_CAPACITY,
    FailedPreconditionError,
    RpcError,
    SandboxStartError,
    map_rpc_error,
)
from ate_env_rl.runtime.substrate_env_client import SubstrateEnvClientRuntime


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code, details, trailing=None):
        self._code, self._details, self._trailing = code, details, trailing

    def code(self):
        return self._code

    def details(self):
        return self._details

    def trailing_metadata(self):
        return self._trailing


def test_bytes_trailer_values_are_decoded():
    err = map_rpc_error(_FakeRpcError(
        grpc.StatusCode.FAILED_PRECONDITION, "x", ((START_FAILURE_KEY, b"crashed"),)))
    assert isinstance(err, SandboxStartError) and err.reason == START_FAILURE_CRASHED


@pytest.mark.parametrize("trailing", [None, (), (("other-key", "crashed"),)])
def test_unmarked_calls_keep_their_mapping(trailing):
    assert isinstance(
        map_rpc_error(_FakeRpcError(grpc.StatusCode.FAILED_PRECONDITION, "x", trailing)),
        FailedPreconditionError,
    )
    assert isinstance(
        map_rpc_error(_FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "x", trailing)),
        RpcError,
    )


def test_start_error_pickles_with_its_reason():
    err = SandboxStartError("boom", sandbox_id="sb-1", status="grpc RESOURCE_EXHAUSTED",
                            reason=START_FAILURE_NO_CAPACITY)
    again = pickle.loads(pickle.dumps(err))
    assert (str(again), again.sandbox_id, again.status, again.retryable, again.reason) == (
        "boom", "sb-1", "grpc RESOURCE_EXHAUSTED", True, START_FAILURE_NO_CAPACITY)


async def test_exec_on_an_environment_that_crashed_while_waking(fake_stack):
    client, fakes = fake_stack
    await client.create("dev1")
    fakes.processes.start_failure = (
        grpc.StatusCode.FAILED_PRECONDITION,
        "environment ate-env/dev1 crashed while waking: assigned worker is draining",
        START_FAILURE_CRASHED,
    )
    rt = SubstrateEnvClientRuntime(client=client, env_id="dev1")
    with pytest.raises(SandboxStartError) as exc:
        await rt.exec_async("true")
    assert exc.value.reason == START_FAILURE_CRASHED
    assert exc.value.sandbox_id == "dev1"
    assert exc.value.retryable
    assert "assigned worker is draining" in str(exc.value)


async def test_read_file_when_no_worker_has_room(fake_stack):
    client, fakes = fake_stack
    await client.create("dev1")
    fakes.filesystem.start_failure = (
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        "environment ate-env/dev1 cannot start: no worker has had room for it",
        START_FAILURE_NO_CAPACITY,
    )
    rt = SubstrateEnvClientRuntime(client=client, env_id="dev1")
    with pytest.raises(SandboxStartError) as exc:
        await rt.read_file_bytes_async("/x")
    assert exc.value.reason == START_FAILURE_NO_CAPACITY
    assert exc.value.sandbox_id == "dev1"


async def test_write_file_when_no_worker_has_room(fake_stack):
    client, fakes = fake_stack
    await client.create("dev1")
    fakes.filesystem.start_failure = (
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        "environment ate-env/dev1 cannot start: no worker has had room for it",
        START_FAILURE_NO_CAPACITY,
    )
    rt = SubstrateEnvClientRuntime(client=client, env_id="dev1")
    with pytest.raises(SandboxStartError) as exc:
        await rt.write_file_async("/x", "data")
    assert exc.value.reason == START_FAILURE_NO_CAPACITY
    assert exc.value.sandbox_id == "dev1"
    assert "x" not in fakes.filesystem.files and "/x" not in fakes.filesystem.files


async def test_stream_process_raises_the_start_error(fake_stack):
    client, fakes = fake_stack
    await client.create("dev1")
    fakes.processes.start_failure = (
        grpc.StatusCode.FAILED_PRECONDITION, "crashed while waking", START_FAILURE_CRASHED)
    rt = SubstrateEnvClientRuntime(client=client, env_id="dev1")
    with pytest.raises(SandboxStartError) as exc:
        async for _ in rt.stream_process_async(["true"]):
            pass
    assert exc.value.reason == START_FAILURE_CRASHED
