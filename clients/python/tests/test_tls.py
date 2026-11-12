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

"""Client(credentials=...): TLS to ate-env-api against a local gRPC server."""

import shutil
import subprocess

import grpc
import pytest

from ate_env_rl import Client
from ate_env_rl._gen.ateenv.v1alpha import env_pb2, env_pb2_grpc


class _InfoService(env_pb2_grpc.EnvironmentServiceServicer):
    """Answers GetServerInfo and echoes the authorization header it saw."""

    async def GetServerInfo(self, request, context):
        auth = dict(context.invocation_metadata()).get("authorization", "")
        return env_pb2.GetServerInfoResponse(version=f"auth={auth}")


@pytest.fixture(scope="module")
def cert(tmp_path_factory):
    """A throwaway self-signed certificate for localhost: (cert_pem, key_pem)."""
    if shutil.which("openssl") is None:
        pytest.skip("openssl is not installed")
    d = tmp_path_factory.mktemp("tls")
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-days", "1", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost",
            "-keyout", str(d / "key.pem"), "-out", str(d / "cert.pem"),
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    return (d / "cert.pem").read_bytes(), (d / "key.pem").read_bytes()


async def _serve(port_adder):
    server = grpc.aio.server()
    env_pb2_grpc.add_EnvironmentServiceServicer_to_server(_InfoService(), server)
    port = port_adder(server)
    await server.start()
    return server, f"localhost:{port}"


@pytest.fixture
async def tls_server(cert):
    cert_pem, key_pem = cert
    creds = grpc.ssl_server_credentials(((key_pem, cert_pem),))
    server, target = await _serve(lambda s: s.add_secure_port("localhost:0", creds))
    yield target
    await server.stop(0)


@pytest.fixture
async def plaintext_server():
    server, target = await _serve(lambda s: s.add_insecure_port("localhost:0"))
    yield target
    await server.stop(0)


async def _server_info(client: Client) -> str:
    stub = env_pb2_grpc.EnvironmentServiceStub(client._channel)
    resp = await stub.GetServerInfo(env_pb2.GetServerInfoRequest(), timeout=5)
    return resp.version


async def test_tls_client_reaches_tls_server_and_sends_token(tls_server, cert):
    creds = grpc.ssl_channel_credentials(root_certificates=cert[0])
    client = Client(tls_server, credentials=creds, auth_token="tok")
    try:
        assert await _server_info(client) == "auth=Bearer tok"
    finally:
        await client.close()


async def test_https_url_with_credentials_uses_tls(tls_server, cert):
    creds = grpc.ssl_channel_credentials(root_certificates=cert[0])
    client = Client(f"https://{tls_server}", credentials=creds)
    try:
        assert await _server_info(client) == "auth="
    finally:
        await client.close()


async def test_default_is_plaintext_even_for_https_url(tls_server, plaintext_server):
    # Plaintext to a TLS server fails, and an https:// URL alone does not
    # turn TLS on: the same URL shape reaches a plaintext server.
    client = Client(tls_server)
    try:
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await _server_info(client)
        assert err.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        await client.close()
    client = Client(f"https://{plaintext_server}")
    try:
        assert await _server_info(client) == "auth="
    finally:
        await client.close()


async def test_tls_client_to_plaintext_server_fails(plaintext_server, cert):
    creds = grpc.ssl_channel_credentials(root_certificates=cert[0])
    client = Client(plaintext_server, credentials=creds)
    try:
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await _server_info(client)
        assert err.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        await client.close()


async def test_untrusted_server_certificate_fails(tls_server):
    # Default roots do not trust the self-signed certificate.
    client = Client(tls_server, credentials=grpc.ssl_channel_credentials())
    try:
        with pytest.raises(grpc.aio.AioRpcError) as err:
            await _server_info(client)
        assert err.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        await client.close()


async def test_channel_and_credentials_together_raise():
    channel = grpc.aio.insecure_channel("localhost:1")
    try:
        with pytest.raises(ValueError, match="either channel or credentials"):
            Client(channel=channel, credentials=grpc.ssl_channel_credentials())
    finally:
        await channel.close()
