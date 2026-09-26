"""Two live HTTPS peers, remote Git/LFS, and takeover without gateway restart."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
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
    from smoke_s3_process import port, start, create_repository, git, clone_and_verify, api_status

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
        first.kill()
        first.wait(timeout=10)
        assert api_status(base_b, "/api/repositories", "local-test-token") == 503
        time.sleep(11)
        for name, oid, readme, lfs in expected:
            clone_and_verify(f"{base_b}/canopy/{name}.git", directory / f"{name}-takeover",
                             oid, readme, lfs)
        second.send_signal(signal.SIGTERM)
        second.wait(timeout=30)
        assert second.returncode == 0
        print("PASS: two live HTTPS nodes serve eight Git/LFS repositories beyond resident capacity through opposite Cell owners; survivor restores Directory and repository after SIGKILL without restarting", flush=True)
