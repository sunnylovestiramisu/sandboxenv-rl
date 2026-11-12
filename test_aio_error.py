import asyncio
import grpc
from ate_env_rl._gen.ateenv.v1alpha import env_pb2, env_pb2_grpc
from clients.python.tests.test_tls import _serve, _server_info, cert

async def main():
    import pytest
    from _pytest.tmpdir import TempPathFactory
    from _pytest.monkeypatch import MonkeyPatch
    # To run this, we need the cert. Let's just create it directly.
    import subprocess
    import tempfile
    import pathlib
    d = pathlib.Path(tempfile.mkdtemp())
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-keyout", str(d / "key.pem"), "-out", str(d / "cert.pem")], check=True, capture_output=True)
    cert_pem = (d / "cert.pem").read_bytes()
    key_pem = (d / "key.pem").read_bytes()
    creds = grpc.ssl_server_credentials(((key_pem, cert_pem),))
    server, target = await _serve(lambda s: s.add_secure_port("localhost:0", creds))

    # 1. untrusted cert
    try:
        stub = env_pb2_grpc.EnvironmentServiceStub(grpc.aio.secure_channel(target, grpc.ssl_channel_credentials()))
        await stub.GetServerInfo(env_pb2.GetServerInfoRequest(), timeout=5)
    except grpc.aio.AioRpcError as e:
        print("UNTRUSTED:", e.code(), e.details())
        print("UNTRUSTED DEBUG:", e.debug_error_string())

    # 2. plaintext to TLS
    try:
        stub = env_pb2_grpc.EnvironmentServiceStub(grpc.aio.insecure_channel(target))
        await stub.GetServerInfo(env_pb2.GetServerInfoRequest(), timeout=5)
    except grpc.aio.AioRpcError as e:
        print("PLAINTEXT_TO_TLS:", e.code(), e.details())

    await server.stop(0)

asyncio.run(main())
