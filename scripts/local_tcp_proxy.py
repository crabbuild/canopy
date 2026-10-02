"""Bounded loopback stream proxy for local qualification, not a production LB.

Balance TCP connections, not HTTP requests: keep-alive stays on one backend.
Forward bytes without parsing, buffering complete bodies, or retrying requests.
Optional TLS termination also supports Canopy's authenticated peer transport.
"""

import asyncio
from collections import Counter
import copy
import threading


class LocalProxy:
    def __init__(self, upstreams, *, tls_context=None, max_connections=256,
                 connect_timeout=5, buffer_bytes=64 * 1024):
        if not upstreams or not 1 <= max_connections <= 1024:
            raise ValueError("require upstreams and 1..1024 connections")
        if not 1 <= buffer_bytes <= 1024 * 1024 or connect_timeout <= 0:
            raise ValueError("invalid buffer or connect timeout")
        self.upstreams = []
        for address in upstreams:
            host, port = address.rsplit(":", 1)
            if host != "127.0.0.1" or not 1 <= int(port) <= 65535:
                raise ValueError("qualification upstreams must be loopback")
            self.upstreams.append((host, int(port)))
        self.tls_context = tls_context
        self.max_connections = max_connections
        self.connect_timeout = connect_timeout
        self.buffer_bytes = buffer_bytes
        self.started = threading.Event()
        self.tasks = set()
        self.failure = None
        self.metrics = {"accepted_connections": 0, "active_connections": 0,
                        "peak_connections": 0, "rejected_connections": 0,
                        "backend_connections": [0] * len(upstreams),
                        "client_bytes": 0, "backend_bytes": 0, "errors": Counter()}

    async def relay(self, reader, writer, metric):
        while block := await reader.read(self.buffer_bytes):
            self.metrics[metric] += len(block)
            writer.write(block)
            await writer.drain()
        # Propagate half-close: the backend can still send its final response.
        if writer.can_write_eof():
            writer.write_eof()
            await writer.drain()
        else:
            writer.close()

    async def handle(self, reader, writer):
        if len(self.tasks) >= self.max_connections:
            self.metrics["rejected_connections"] += 1
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            return
        task = asyncio.current_task()
        self.tasks.add(task)
        index = self.metrics["accepted_connections"] % len(self.upstreams)
        self.metrics["accepted_connections"] += 1
        self.metrics["backend_connections"][index] += 1
        self.metrics["active_connections"] = len(self.tasks)
        self.metrics["peak_connections"] = max(self.metrics["peak_connections"], len(self.tasks))
        backend = None
        relays = []
        try:
            host, port = self.upstreams[index]
            upstream, backend = await asyncio.wait_for(
                asyncio.open_connection(host, port, limit=self.buffer_bytes),
                timeout=self.connect_timeout)
            relays = [asyncio.create_task(self.relay(reader, backend, "client_bytes")),
                      asyncio.create_task(self.relay(upstream, writer, "backend_bytes"))]
            await asyncio.gather(*relays)
        except (OSError, asyncio.TimeoutError) as error:
            # Only the error class is retained; never headers or request bodies.
            self.metrics["errors"][type(error).__name__] += 1
        finally:
            for relay in relays:
                if not relay.done():
                    relay.cancel()
            await asyncio.gather(*relays, return_exceptions=True)
            for stream in (backend, writer):
                if stream is not None:
                    stream.close()
                    try:
                        await stream.wait_closed()
                    except OSError:
                        pass
            self.tasks.discard(task)
            self.metrics["active_connections"] = len(self.tasks)

    async def serve(self):
        self.loop = asyncio.get_running_loop()
        self.stop = asyncio.Event()
        server = await asyncio.start_server(self.handle, "127.0.0.1", 0,
                                           ssl=self.tls_context, limit=self.buffer_bytes)
        self.address = server.sockets[0].getsockname()
        self.started.set()
        try:
            await self.stop.wait()
        finally:
            server.close()
            await server.wait_closed()
            tasks = list(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def run(self):
        try:
            asyncio.run(self.serve())
        except BaseException as error:
            self.failure = error
            self.started.set()

    def __enter__(self):
        self.worker = threading.Thread(target=self.run, daemon=True)
        self.worker.start()
        if not self.started.wait(10):
            raise RuntimeError("local proxy startup timed out")
        if self.failure is not None:
            raise RuntimeError("local proxy failed") from self.failure
        scheme = "https" if self.tls_context else "http"
        self.url = f"{scheme}://127.0.0.1:{self.address[1]}"
        return self

    def snapshot(self):
        async def capture():
            return copy.deepcopy(self.metrics)
        if self.worker.is_alive():
            return asyncio.run_coroutine_threadsafe(capture(), self.loop).result(timeout=5)
        return copy.deepcopy(self.metrics)

    def __exit__(self, *_):
        if self.worker.is_alive():
            self.loop.call_soon_threadsafe(self.stop.set)
        self.worker.join(timeout=10)
        if self.worker.is_alive():
            raise RuntimeError("local proxy failed to stop")
        if self.failure is not None:
            raise RuntimeError("local proxy failed") from self.failure
