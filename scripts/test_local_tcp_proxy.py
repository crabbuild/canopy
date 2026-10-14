"""Real socket regressions for transparent three-backend forwarding."""

from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import socket
import socketserver
import threading
import time
import unittest

from local_tcp_proxy import LocalProxy


class Echo(socketserver.BaseRequestHandler):
    def handle(self):
        body = bytearray()
        while block := self.request.recv(65536):
            body.extend(block)
        self.server.received.append(bytes(body))
        self.request.sendall(body)


class HTTP(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.requests.append((self.headers.get("Authorization"), body))
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ProxyTests(unittest.TestCase):
    def servers(self, stack, handler, http=False):
        servers = []
        for _ in range(3):
            kind = ThreadingHTTPServer if http else socketserver.ThreadingTCPServer
            server = kind(("127.0.0.1", 0), handler)
            server.daemon_threads = True
            server.received, server.requests = [], []
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            stack.callback(thread.join)
            stack.callback(server.server_close)
            stack.callback(server.shutdown)
            servers.append(server)
        return servers

    def test_three_backends_preserve_large_binary_body_and_half_close(self):
        with ExitStack() as stack:
            servers = self.servers(stack, Echo)
            proxy = stack.enter_context(LocalProxy([
                f"127.0.0.1:{server.server_address[1]}" for server in servers]))
            for index in range(6):
                body = bytes(range(256)) * 4097 + bytes([index])
                with socket.create_connection(proxy.address, timeout=5) as client:
                    client.sendall(body)
                    client.shutdown(socket.SHUT_WR)
                    result = bytearray()
                    while block := client.recv(65536):
                        result.extend(block)
                self.assertEqual(result, body)
            metrics = proxy.snapshot()
            self.assertEqual(metrics["backend_connections"], [2, 2, 2])
            self.assertEqual(metrics["errors"], {})
            self.assertEqual(metrics["client_bytes"], metrics["backend_bytes"])
            self.assertEqual([len(server.received) for server in servers], [2, 2, 2])

    def test_keep_alive_and_authorization_are_not_rewritten(self):
        with ExitStack() as stack:
            servers = self.servers(stack, HTTP, http=True)
            proxy = stack.enter_context(LocalProxy([
                f"127.0.0.1:{server.server_address[1]}" for server in servers]))
            for _ in range(3):
                client = http.client.HTTPConnection(*proxy.address, timeout=5)
                try:
                    for body in (b"first", bytes(range(256)) * 1000):
                        client.request("POST", "/fixture", body=body,
                                       headers={"Authorization": "Bearer fixture"})
                        response = client.getresponse()
                        self.assertEqual(response.status, 200)
                        self.assertEqual(response.read(), body)
                finally:
                    client.close()
            self.assertEqual([server.requests for server in servers],
                             [[("Bearer fixture", b"first"),
                               ("Bearer fixture", bytes(range(256)) * 1000)]] * 3)

    def test_dead_backend_does_not_retry_a_possible_mutation(self):
        with ExitStack() as stack:
            servers = self.servers(stack, Echo)
            with socket.socket() as unused:
                unused.bind(("127.0.0.1", 0))
                dead_port = unused.getsockname()[1]
            proxy = stack.enter_context(LocalProxy([
                f"127.0.0.1:{dead_port}",
                f"127.0.0.1:{servers[0].server_address[1]}"]))
            with socket.create_connection(proxy.address, timeout=5) as client:
                client.sendall(b"must not replay")
                try:
                    result = client.recv(1024)
                except ConnectionResetError:
                    result = b""
            self.assertEqual(result, b"")
            self.assertEqual(servers[0].received, [])
            self.assertEqual(proxy.snapshot()["backend_connections"], [1, 0])

    def test_reject_non_loopback_upstreams(self):
        with self.assertRaises(ValueError):
            LocalProxy(["0.0.0.0:8080"])

    def test_connection_limit_rejects_without_queueing_or_replaying(self):
        with ExitStack() as stack:
            servers = self.servers(stack, Echo)
            proxy = stack.enter_context(LocalProxy([
                f"127.0.0.1:{servers[0].server_address[1]}"], max_connections=1))
            held = stack.enter_context(socket.create_connection(proxy.address, timeout=5))
            for _ in range(100):
                if proxy.snapshot()["active_connections"] == 1:
                    break
                time.sleep(.01)
            self.assertEqual(proxy.snapshot()["active_connections"], 1)
            with socket.create_connection(proxy.address, timeout=5) as excess:
                excess.sendall(b"must not queue")
                try:
                    result = excess.recv(1024)
                except ConnectionResetError:
                    result = b""
            self.assertEqual(result, b"")
            metrics = proxy.snapshot()
            self.assertEqual(metrics["rejected_connections"], 1)
            self.assertEqual(metrics["peak_connections"], 1)
            self.assertEqual(metrics["accepted_connections"], 1)
            held.shutdown(socket.SHUT_WR)


if __name__ == "__main__":
    unittest.main()
