#!/usr/bin/env python3
"""Measure a read-only v0, no-haves, side-band pack POST through the ingress.

This is not a clone benchmark: discovery, identity checks and local Git validation
are outside the POST clock. No retries, remote mutations or automatic warmup.
First-pack-byte excludes HTTP headers, NAK and progress sidebands. Local files
and a failed receipt remain available for inspection if validation fails.
"""

import argparse
import http.client
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.parse import urlsplit
import uuid

import benchmark_repositories as benchmark
import benchmark_three_node_campaign as campaign


def require(condition, message):
    if not condition:
        raise ValueError(message)


def packet(payload):
    require(0 < len(payload) <= 65516, "invalid packet payload size")
    return f"{len(payload) + 4:04x}".encode() + payload


def request_body(oid):
    require(isinstance(oid, str) and re.fullmatch(r"[0-9a-f]{40}", oid),
            "require a SHA-1 commit object ID")
    return packet(f"want {oid} side-band-64k ofs-delta\n".encode()) + b"0000" + packet(b"done\n")


def stream_pack(response, destination, started, *, max_pack_bytes, timeout, clock=time.monotonic):
    """Consume bounded pkt-lines; time channel-1's first byte before its remainder.

    read(1) at that boundary avoids waiting for an entire sideband packet. Times
    are client-observed read boundaries, not kernel arrival or server CPU time.
    """
    wire_bytes = pack_bytes = progress_bytes = 0
    first_pack = last_pack = None
    signature = bytearray()
    nak_seen = False

    def read(size):
        nonlocal wire_bytes
        require(clock() - started < timeout, "pack POST exceeded total deadline")
        value = response.read(size)
        require(len(value) == size, "truncated pkt-line")
        wire_bytes += len(value)
        require(wire_bytes <= max_pack_bytes + 16 * 1024 * 1024,
                "response framing/progress exceeds wire bound")
        require(clock() - started < timeout, "pack POST exceeded total deadline")
        return value

    while True:
        header = read(4)
        require(re.fullmatch(b"[0-9a-fA-F]{4}", header), "invalid pkt-line header")
        size = int(header, 16)
        if size == 0:
            break
        require(5 <= size <= 65520, "invalid v0 sideband packet size")
        channel = read(1)
        remaining = size - 5
        if not nak_seen:
            require(channel + read(remaining) == b"NAK\n", "expected no-haves NAK")
            nak_seen = True
            continue
        require(channel in (b"\x01", b"\x02", b"\x03"), "invalid sideband channel")
        if channel == b"\x03":
            # Do not put server-controlled text (possibly secrets) in reports.
            raise ValueError("remote fatal sideband")
        if channel == b"\x02":
            progress_bytes += len(read(remaining))
            continue
        require(remaining > 0, "empty pack-data packet")
        require(pack_bytes + remaining <= max_pack_bytes, "pack exceeds declared bound")
        first = read(1)
        if first_pack is None:
            first_pack = clock()
        chunks = [first, read(remaining - 1)] if remaining > 1 else [first]
        last_pack = clock()
        for chunk in chunks:
            signature.extend(chunk[:max(0, 4 - len(signature))])
            destination.write(chunk)
            pack_bytes += len(chunk)
    require(nak_seen and first_pack is not None and signature == b"PACK", "missing valid pack prefix")
    require(response.read(1) == b"", "trailing bytes after final flush")
    finished = clock()
    require(finished - started < timeout, "pack POST exceeded total deadline")
    seconds = finished - started
    require(seconds > 0 and started <= first_pack <= last_pack <= finished,
            "invalid measurement clock")
    return {"started_monotonic": started, "first_pack_byte_monotonic": first_pack,
            "last_pack_byte_monotonic": last_pack, "completed_monotonic": finished,
            "response_body_bytes": wire_bytes, "pack_bytes": pack_bytes,
            "progress_payload_bytes": progress_bytes,
            "post_first_pack_byte_ms": (first_pack - started) * 1000,
            "post_last_pack_byte_ms": (last_pack - started) * 1000,
            "post_complete_ms": seconds * 1000,
            "pack_bytes_per_post_second": pack_bytes / seconds,
            "timing_scope": "fresh HTTP connection + POST through final flush/EOF; no discovery or local validation; client read boundaries"}


def download(base_url, entry, oid, token, pack_path, *, timeout, max_pack_bytes):
    # URL validation is also used by the bound benchmark harness.
    benchmark.Client(base_url, token, timeout).close()
    url = urlsplit(base_url)
    require(all(isinstance(entry.get(key), str) and re.fullmatch(r"[a-z0-9_-]{1,64}", entry[key])
                for key in ("owner", "name")), "invalid repository path")
    body = request_body(oid)
    connection_type = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    connection = connection_type(url.hostname, url.port, timeout=timeout)
    request_id = str(uuid.uuid4())
    started = time.monotonic()
    try:
        connection.request("POST", url.path.rstrip("/") + f"/{entry['owner']}/{entry['name']}.git/git-upload-pack",
                           body=body, headers={"Authorization": f"Bearer {token}",
                           "Content-Type": "application/x-git-upload-pack-request",
                           "Accept": "application/x-git-upload-pack-result",
                           "Accept-Encoding": "identity", "Connection": "close",
                           "X-Request-ID": request_id})
        response = connection.getresponse()
        headers_ms = (time.monotonic() - started) * 1000
        require(response.status == 200, f"pack POST HTTP {response.status}")
        require(response.getheader("Content-Type", "").split(";")[0].strip()
                == "application/x-git-upload-pack-result", "unexpected pack content type")
        require(response.getheader("Content-Encoding", "identity") == "identity",
                "encoded pack response not supported")
        with pack_path.open("xb") as output:
            result = stream_pack(response, output, started, max_pack_bytes=max_pack_bytes, timeout=timeout)
        return {**result, "http_headers_ms": headers_ms, "request_id": request_id,
                "pack_sha256": benchmark.file_sha256(pack_path)}
    finally:
        connection.close()


def validate_pack(pack_path, oid, destination, token):
    """Validate the pack and the wanted commit's full closure in an empty ODB."""
    destination.mkdir(parents=True, exist_ok=False)
    benchmark.git("init", "--bare", str(destination), cwd=destination.parent, token=token)
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith(("GIT_", "AWS_", "RUSTFS_", "CANOPY_"))}
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_TERMINAL_PROMPT="0")
    with pack_path.open("rb") as source:
        result = subprocess.run(["git", "index-pack", "--strict", "--stdin"],
                                cwd=destination, env=environment, stdin=source,
                                capture_output=True, timeout=120, check=False)
    require(result.returncode == 0, "strict pack indexing failed")
    require(benchmark.git("cat-file", "-t", oid, cwd=destination, token=token) == "commit",
            "wanted object is not a commit")
    benchmark.git("update-ref", "refs/heads/measured", oid, cwd=destination, token=token)
    benchmark.git("symbolic-ref", "HEAD", "refs/heads/measured", cwd=destination, token=token)
    benchmark.git("fsck", "--strict", "--full", cwd=destination, token=token)
    return {"index_pack_strict": True, "fsck_strict_full": True, "wanted_commit": oid}


def measure(args, token):
    receipt = campaign.read_json(args.receipt)
    require(receipt.get("complete") is True and receipt.get("error") is None,
            "require a complete critical Git receipt")
    entries = receipt.get("repositories")
    require(isinstance(entries, list) and len(entries) == 2, "require two critical repositories")
    entry = entries[args.repository_index]
    require(benchmark.canonical_repository_uuid(entry.get("repository_id")), "invalid repository UUID")
    oid = receipt.get("refs", {}).get("refs/heads/main")
    request_body(oid)
    ready = campaign.read_json(args.fleet_dir / "ready.json")
    ready = campaign.validate_fleet(args.fleet_dir,
                                   {"node_active_limit": ready.get("max_active_repositories_per_node")})
    require(receipt.get("base_url") == ready["proxy_url"]
            and receipt.get("binary_sha256") == ready["binary_sha256"], "critical receipt/fleet binding differs")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    result = {"version": 1, "complete": False, "error": None, "repository": entry,
              "wanted_commit": oid, "protocol": 0, "haves": 0,
              "driver_sha256": benchmark.file_sha256(Path(__file__)),
              "git_driver_sha256": benchmark.file_sha256(Path(benchmark.__file__)),
              "fleet_validator_sha256": benchmark.file_sha256(Path(campaign.__file__)),
              "receipt_sha256": benchmark.file_sha256(args.receipt),
              "fleet_ready_sha256": benchmark.file_sha256(args.fleet_dir / "ready.json"),
              "binary_sha256": ready["binary_sha256"], "samples": [],
              "declared": {"samples": args.samples, "timeout_seconds": args.timeout,
                           "max_pack_bytes": args.max_pack_bytes},
              "qualification": "serial no-haves v0 transfer observations; not stock clone, scheduled load, cold ownership, recovery, CPU or provider cost proof"}
    path = args.output_dir / "measurement.json"
    benchmark.save(path, result)
    client = benchmark.Client(ready["proxy_url"], token, args.timeout)
    try:
        result["validation_git_version"] = benchmark.git("--version", cwd=args.output_dir, token=token)
        status, body = client.request(f"/api/repositories/{entry['name']}", request_id=str(uuid.uuid4()))
        require(status == 200 and json.loads(body).get("repository_id") == entry["repository_id"],
                "repository identity differs")
        for index in range(args.samples):
            # Revalidate live identity/binary/script binding before each sample.
            campaign.validate_fleet(args.fleet_dir,
                                    {"node_active_limit": ready["max_active_repositories_per_node"]})
            pack_path = args.output_dir / f"sample-{index}.pack"
            sample = {"index": index, "complete": False, "error": None}
            result["samples"].append(sample)
            benchmark.save(path, result)
            sample.update(download(ready["proxy_url"], entry, oid, token, pack_path,
                                   timeout=args.timeout, max_pack_bytes=args.max_pack_bytes))
            checking = time.monotonic()
            sample["validation"] = validate_pack(pack_path, oid, args.output_dir / f"sample-{index}.git", token)
            sample["validation_ms"] = (time.monotonic() - checking) * 1000
            sample["complete"] = True
            benchmark.save(path, result)
        result["complete"] = True
    except BaseException as error:
        result["error"] = type(error).__name__
        if result["samples"] and not result["samples"][-1]["complete"]:
            result["samples"][-1]["error"] = type(error).__name__
        raise
    finally:
        client.close()
        benchmark.save(path, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fleet-dir", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repository-index", type=int, choices=(0, 1), default=0)
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--max-pack-bytes", type=int, default=256 * 1024 * 1024)
    args = parser.parse_args()
    require(1 <= args.samples <= 100 and 0 < args.timeout <= 300
            and 32 <= args.max_pack_bytes <= 1024 * 1024 * 1024, "invalid measurement bounds")
    token = os.environ.get("CANOPY_GIT_TOKEN")
    require(bool(token), "set CANOPY_GIT_TOKEN")
    measure(args, token)


if __name__ == "__main__":
    main()
