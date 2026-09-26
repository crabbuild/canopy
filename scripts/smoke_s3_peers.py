"""Two live HTTPS peers, remote Git/LFS, and takeover without gateway restart."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import json
import os
import signal
import ssl
import subprocess
import threading
import time
import uuid


@contextmanager
def proxy(upstream, certificate, key):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            connection = http.client.HTTPConnection(upstream, timeout=30)
            try:
                connection.request("POST", self.path, body,
                                   {"Content-Type": "application/octet-stream"})
                response = connection.getresponse()
                reply = response.read()
                self.send_response(response.status)
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)
            except (OSError, http.client.HTTPException):
                self.send_error(502)
            finally:
                connection.close()

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"https://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


def qualify(binary, directory, settings, processes):
    from smoke_s3_process import port, start, create_repository, git, clone_and_verify, api_status, api_get, verify_discovery

    ca, ca_key = directory / "peer-ca.pem", directory / "peer-ca-key.pem"
    certificate, key = directory / "peer-server.pem", directory / "peer-key.pem"
    csr, extensions = directory / "peer.csr", directory / "peer.ext"
    extensions.write_text("basicConstraints=critical,CA:FALSE\n"
                          "keyUsage=critical,digitalSignature,keyEncipherment\n"
                          "extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1\n")
    commands = [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(ca_key),
         "-out", str(ca), "-days", "1", "-subj", "/CN=Canopy Test CA",
         "-addext", "basicConstraints=critical,CA:TRUE"],
        ["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key),
         "-out", str(csr), "-subj", "/CN=127.0.0.1"],
        ["x509", "-req", "-in", str(csr), "-CA", str(ca), "-CAkey", str(ca_key),
         "-set_serial", "1", "-out", str(certificate), "-days", "1",
         "-extfile", str(extensions)],
    ]
    for command in commands:
        subprocess.run(["openssl", *command], check=True, capture_output=True)
    for private_key in (key, ca_key):
        private_key.chmod(0o600)
    a, b = f"127.0.0.1:{port()}", f"127.0.0.1:{port()}"
    settings = {**settings, "storage_url": settings["storage_url"] + "/peers",
                "peer_ca_certificate": str(ca)}
    with proxy(a, certificate, key) as peer_a, proxy(b, certificate, key) as peer_b:
        first, base_a = start(binary, directory, {**settings, "peer_endpoint": peer_a},
                              "peer-first", listen_address=a)
        processes.append(first)
        second, base_b = start(binary, directory,
                               {**settings, "node_id": str(uuid.uuid4()), "peer_endpoint": peer_b},
                               "peer-second", listen_address=b)
        processes.append(second)
        expected = []
        names = ["peer-left", "peer-right", *[f"peer-{index}" for index in range(2, 8)]]
        for index, name in enumerate(names):
            owner, ingress = (base_a, base_b) if index % 2 == 0 else (base_b, base_a)
            create_repository(owner, name)
            local = directory / name
            git("init", "-b", "main", str(local))
            git("config", "user.name", "Canopy Test", cwd=local)
            git("config", "user.email", "canopy@example.invalid", cwd=local)
            git("lfs", "install", "--local", cwd=local)
            git("lfs", "track", "*.lfs", cwd=local)
            readme, lfs = name.encode(), name.encode() * 100_000
            (local / "README.md").write_bytes(readme)
            (local / "asset.lfs").write_bytes(lfs)
            git("add", ".", cwd=local)
            git("commit", "-m", "Remote Cell", cwd=local)
            url = f"{ingress}/canopy/{name}.git"
            git("-c", "http.extraHeader=Authorization: Bearer local-test-token", "push", url, "main", cwd=local)
            oid = git("rev-parse", "HEAD", cwd=local)
            clone_and_verify(url, directory / f"{name}-live", oid, readme, lfs)
            expected.append((name, oid, readme, lfs))
        public_name, public_oid, public_readme, public_lfs = expected[0]
        visibility_path = f"/api/repositories/{public_name}/visibility"
        current = api_get(base_b, visibility_path, "local-test-token")
        assert api_status(base_b, visibility_path, "local-test-token", "PUT", {
            "repository_id": current["repository_id"],
            "expected_generation": current["generation"], "visibility": "public"}) == 200
        verify_discovery(base_b, None, [public_name])
        clone_and_verify(f"{base_b}/canopy/{public_name}.git", directory / "public-live",
                         public_oid, public_readme, public_lfs, token=None)
        first.kill()
        first.wait(timeout=10)
        assert api_status(base_b, "/api/repositories", "local-test-token") == 503
        time.sleep(11)
        for name, oid, readme, lfs in expected:
            clone_and_verify(f"{base_b}/canopy/{name}.git", directory / f"{name}-takeover",
                             oid, readme, lfs)
        verify_discovery(base_b, None, [public_name])
        clone_and_verify(f"{base_b}/canopy/{public_name}.git", directory / "public-takeover",
                         public_oid, public_readme, public_lfs, token=None)
        current = api_get(base_b, visibility_path, "local-test-token")
        assert api_status(base_b, visibility_path, "local-test-token", "PUT", {
            "repository_id": current["repository_id"],
            "expected_generation": current["generation"], "visibility": "private"}) == 200
        verify_discovery(base_b, None, [])
        assert api_status(base_b, f"/canopy/{public_name}.git/info/refs?service=git-upload-pack", None) == 401
        assert api_status(base_b, f"/canopy/{public_name}.git/info/lfs/objects/batch", None,
                          "POST", {"operation": "download", "objects": []}) == 401
        print("PASS: anonymous Git/LFS and public discovery survive cross-node routing and SIGKILL; privatization revokes new reads", flush=True)
        operation = str(uuid.uuid4())
        configuration = directory / "peer-second.json"
        def maintenance(action, operation_id=None):
            arguments = [str(binary), "maintenance", str(configuration), action]
            if operation_id is not None:
                arguments.append(operation_id)
            environment = {key: value for key, value in os.environ.items()
                           if key not in ("CANOPY_GIT_TOKEN", "CANOPY_NODE_SIGNING_KEY_HEX")}
            result = subprocess.run(arguments, check=True, capture_output=True, env=environment)
            return json.loads(result.stdout)
        assert maintenance("begin", operation)["release"]["state"] == "maintenance"
        second.wait(timeout=30)
        assert second.returncode == 0
        assert maintenance("status")["drained"]
        assert maintenance("end", operation)["release"]["state"] == "ready"
        restored, restored_url = start(binary, directory,
            {**settings, "node_id": str(uuid.uuid4()), "peer_endpoint": peer_b},
            "peer-resumed", listen_address=b)
        processes.append(restored)
        clone_and_verify(f"{restored_url}/canopy/{public_name}.git", directory / "maintenance-resume",
                         public_oid, public_readme, public_lfs)
        restored.send_signal(signal.SIGTERM)
        restored.wait(timeout=30)
        assert restored.returncode == 0
        print("PASS: CLI maintenance drains the surviving process, proves closed writers, and resumes the same Git/LFS repository without node secrets", flush=True)
        print("PASS: two live HTTPS nodes serve eight Git/LFS repositories beyond resident capacity through opposite Cell owners; survivor restores Directory and repository after SIGKILL without restarting", flush=True)
