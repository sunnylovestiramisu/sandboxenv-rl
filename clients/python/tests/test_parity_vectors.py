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

import json
from pathlib import Path

import grpc
import pytest

from ate_env_rl import (
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
    ProcessExitedError,
    RpcError,
    SandboxStartError,
)
from ate_env_rl._gen.ateenv.v1alpha import env_pb2
from ate_env_rl.client import Client
from ate_env_rl.errors import map_rpc_error
from ate_env_rl.telemetry import tracing


def repo_root() -> Path:
    d = Path(__file__).resolve().parent
    while True:
        if (d / "proto" / "testdata" / "client-parity").exists():
            return d
        if d.parent == d:
            raise RuntimeError("repo root not found")
        d = d.parent


def load_vector(name: str):
    with open(repo_root() / "proto" / "testdata" / "client-parity" / name) as f:
        return json.load(f)


@pytest.mark.parametrize("vector", load_vector("image_ref.json"))
def test_image_ref_attr(vector):
    # Python client image_ref_attr returns None instead of empty string for empty inputs
    assert tracing.image_ref_attr(vector["ref"]) == vector["want"]


@pytest.mark.parametrize("vector", load_vector("executable.json"))
def test_executable_attr(vector):
    assert tracing.executable_attr(vector["line"]) == vector["want"]


@pytest.mark.parametrize("vector", load_vector("rpc_attrs.json"))
def test_rpc_attrs(vector, monkeypatch):
    mode = vector["mode"]
    monkeypatch.setenv("OTEL_SEMCONV_STABILITY_OPT_IN", mode)
    # The python client uses _RPC_SEMCONV_MODE internally which is set at import
    # we need to reload it or parse it
    if mode == "rpc/old":
        monkeypatch.setattr(tracing, "_RPC_SEMCONV_MODE", tracing._SEMCONV_OLD)
    elif mode == "rpc/dup":
        monkeypatch.setattr(tracing, "_RPC_SEMCONV_MODE", tracing._SEMCONV_DUP)
    else:
        monkeypatch.setattr(tracing, "_RPC_SEMCONV_MODE", tracing._SEMCONV_NEW)

    got = tracing.rpc_attrs(vector["service"], vector["method"], vector["target"])

    # python uses target=None instead of empty string
    if not vector["target"]:
        got = tracing.rpc_attrs(vector["service"], vector["method"], None)

    assert got == vector["want"]


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code, details, trailing=None):
        self._code, self._details, self._trailing = code, details, trailing

    def code(self):
        return self._code

    def details(self):
        return self._details

    def trailing_metadata(self):
        return self._trailing


def error_class_for_name(name):
    return {
        "NotFoundError": NotFoundError,
        "InvalidArgumentError": InvalidArgumentError,
        "PermissionDeniedError": PermissionDeniedError,
        "FailedPreconditionError": FailedPreconditionError,
        "ProcessExitedError": ProcessExitedError,
        "StartError": SandboxStartError,
        "RpcError": RpcError,
    }[name]


@pytest.mark.parametrize("vector", load_vector("error_mapping.json"))
def test_error_mapping(vector):
    code_enum = [c for c in grpc.StatusCode if c.value[0] == vector["code"]][0]
    trailing = (
        (("ate-env-start-failure", vector["trailer"]),) if vector["trailer"] else ()
    )
    err = _FakeRpcError(code_enum, vector["message"], trailing)

    mapped = map_rpc_error(err)

    want = vector["want"]
    kind = want["kind"]
    expected_class = {
        "not_found": NotFoundError,
        "invalid_argument": InvalidArgumentError,
        "permission_denied": PermissionDeniedError,
        "failed_precondition": FailedPreconditionError,
        "process_exited": ProcessExitedError,
        "start_error": SandboxStartError,
        "rpc": RpcError,
    }[kind]

    assert isinstance(mapped, expected_class)

    if kind == "start_error":
        assert mapped.reason == want.get("reason")

    if hasattr(mapped, "retryable"):
        assert mapped.retryable == want["retryable"]


def test_metric_families():
    pytest.importorskip("prometheus_client")
    from ate_env_rl.telemetry import metrics

    # Build the collectors without enable_client_metrics(), which is a
    # one-way, process-global switch.
    metrics._histogram("exec")
    metrics._histogram("suspend")
    metrics._subphase_histogram("ready_to_first_command")

    vectors = load_vector("metric_families.json")
    for v in vectors:
        found = False
        name = v["name"]
        all_hists = {**metrics._HISTOGRAMS, **metrics._SUBPHASE_HISTOGRAMS}
        for key, hist in all_hists.items():
            if hist._name == name:
                found = True
                assert hist._documentation == v["help"]
                assert list(hist._labelnames) == v["labels"]
                assert list(hist._kwargs.get("buckets", [])) == v["buckets"]
                break
        assert found, f"metric family {name} not found in client metrics"


def test_rpc_coverage():
    vectors = load_vector("rpc_coverage.json")

    services = env_pb2.DESCRIPTOR.services_by_name
    env_service = services["EnvironmentService"]

    for method in env_service.methods:
        assert method.name in vectors, f"RPC {method.name} not found in coverage vector"
        mapping = vectors[method.name]
        python_method_name = mapping["python"]
        assert hasattr(Client, python_method_name), (
            f"Python Client missing method {python_method_name} for RPC {method.name}"
        )
