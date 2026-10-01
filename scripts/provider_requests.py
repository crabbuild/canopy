#!/usr/bin/env python3
"""Read RustFS console S3 counters without changing provider configuration.

Uses signed GET /rustfs/admin/v3/metrics?n=1&types=512 (NDJSON, not
Prometheus). Requests, outcomes and restart boundaries are retained. Counts
include background ownership work and retries; they are not billed USD, bytes,
latency, or per-Canopy-operation cost without matched workload boundaries.
Credentials are read from environment and passed to curl on stdin, never argv.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.parse import urlsplit

import benchmark_repositories as benchmark


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse_metrics(body):
    require(len(body) <= 1024 * 1024, "metrics response exceeds bound")
    lines = body.splitlines()
    require(len(lines) == 1, "require exactly one NDJSON sample")
    record = json.loads(lines[0])
    require(record.get("errors") == [] and record.get("final") is True,
            "provider metrics incomplete or errored")
    http = record.get("aggregated", {}).get("http")
    require(isinstance(http, dict) and isinstance(http.get("collected"), str),
            "missing provider HTTP counters")
    counters = http.get("requests")
    require(isinstance(counters, list) and len(counters) <= 4096, "invalid provider request counters")
    seen = set()
    for counter in counters:
        require(isinstance(counter, dict), "invalid request counter")
        require(counter.get("method") in ("GET", "HEAD", "PUT", "POST", "DELETE", "OPTIONS", "PATCH",
                                          "CONNECT", "TRACE", "OTHER"),
                "invalid request method")
        require(isinstance(counter.get("operation"), str)
                and re.fullmatch(r"(?:s3:[A-Za-z0-9_]{1,64}|unknown)", counter["operation"]),
                "invalid operation label")
        require(counter.get("outcome") in ("1xx", "2xx", "3xx", "4xx", "5xx", "cancelled",
                                           "service_error", "unknown"),
                "invalid outcome label")
        require(isinstance(counter.get("total"), int) and not isinstance(counter["total"], bool)
                and 0 <= counter["total"] < 2**64, "invalid request count")
        key = tuple(counter[name] for name in ("method", "operation", "outcome"))
        require(key not in seen, "duplicate counter series")
        seen.add(key)
    return record


def fetch(endpoint, access_key, secret_key, region):
    url = urlsplit(endpoint)
    require(url.scheme == "http" and url.hostname in ("127.0.0.1", "::1")
            and url.port and not url.username and not url.password and not url.query
            and not url.fragment and url.path in ("", "/"), "require explicit loopback provider endpoint")
    # Bound configuration syntax and credentials so neither can inject curl
    # directives. Do not include curl stderr or config text in error reports.
    require(all(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_+/=.-]{1,256}", value)
                for value in (access_key, secret_key)), "invalid fixture credentials")
    require(re.fullmatch(r"[a-z0-9-]{1,64}", region), "invalid region")
    config = f'user = "{access_key}:{secret_key}"\naws-sigv4 = "aws:amz:{region}:s3"\n'
    started = time.monotonic()
    # Redirects are not followed. max-filesize bounds the body, including
    # chunked responses; max-time bounds the whole request. Status trailer is
    # split from the body, which is never logged alongside signing headers.
    result = subprocess.run(["curl", "--disable", "--config", "-", "--noproxy", "*", "--silent", "--show-error",
        "--max-time", "10", "--max-filesize", "1048576", "--write-out", "\n%{http_code}\n%{content_type}",
        endpoint.rstrip("/") + "/rustfs/admin/v3/metrics?n=1&types=512"],
        input=config.encode(), capture_output=True, timeout=15, check=False)
    finished = time.monotonic()
    require(result.returncode == 0, "provider metrics transfer failed")
    body, status, content_type = result.stdout.rsplit(b"\n", 2)
    require(status == b"200" and content_type.split(b";")[0].strip() == b"application/x-ndjson",
            "provider metrics HTTP status/content type differs")
    metrics = parse_metrics(body)
    return {"utc": datetime.now(timezone.utc).isoformat(),
            "read_started_monotonic": started, "read_finished_monotonic": finished,
            "body_sha256": hashlib.sha256(body).hexdigest(), "metrics": metrics}


def provider_state(binding, docker_config, docker_host):
    result = subprocess.run(["docker", "--config", str(docker_config), "--host", docker_host,
        "inspect", binding["provider_container_id"]], capture_output=True, timeout=10, check=False)
    require(result.returncode == 0, "provider inspect failed")
    values = json.loads(result.stdout)
    require(isinstance(values, list) and len(values) == 1, "ambiguous provider identity")
    value = values[0]
    require(value["Id"] == binding["provider_container_id"] and value["State"]["Running"] is True
            and value["State"]["StartedAt"] == binding["provider_started_at"]
            and value["Image"] == binding["provider_image_id"], "provider identity/start changed")
    require(value["Config"]["Labels"].get("canopy.purpose") == "three-node-candidate-e07670e",
            "provider purpose differs")
    # Never return Docker's full environment: it contains provider credentials.
    return {"id": value["Id"], "started_at": value["State"]["StartedAt"],
            "image": value["Image"], "restart_count": value["RestartCount"]}


def changes(before, after):
    def series(sample):
        return {tuple(row[key] for key in ("method", "operation", "outcome")): row["total"]
                for row in sample["metrics"]["aggregated"]["http"]["requests"]}
    old, new = series(before), series(after)
    require(old.keys() <= new.keys(), "counter series disappeared")
    result = []
    for key, value in sorted(new.items()):
        increment = value - old.get(key, 0)
        require(increment >= 0, "provider counter decreased")
        result.append(dict(zip(("method", "operation", "outcome", "requests"), (*key, increment))))
    return result


def observe(args):
    require(2 <= args.samples <= 86400 and math.isfinite(args.interval) and .1 <= args.interval <= 30,
            "invalid observation bounds")
    binding = json.loads(args.binding.read_text())
    access_key, secret_key = os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_REGION", "us-east-1")
    initial_state = provider_state(binding, args.docker_config, args.docker_host)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    path = args.output_dir / "observation.json"
    samples_path = args.output_dir / "samples.jsonl"
    result = {"version": 1, "complete": False, "error": None, "samples": 0,
              "declared": {"samples": args.samples, "interval_seconds": args.interval},
              "binding_sha256": benchmark.file_sha256(args.binding), "provider": initial_state,
              "driver_sha256": benchmark.file_sha256(Path(__file__)),
              "scope": "provider-wide S3 request/outcome counters including background work/retries; not priced cost, bytes, request latency or per-Canopy-operation attribution"}
    benchmark.save(path, result)
    previous = None
    try:
        with samples_path.open("x") as output:
            for index in range(args.samples):
                state = provider_state(binding, args.docker_config, args.docker_host)
                sample = fetch(binding["provider_endpoint"], access_key, secret_key, region)
                require(provider_state(binding, args.docker_config, args.docker_host) == state == initial_state,
                        "provider restarted during observation")
                sample["index"] = index
                sample["changes"] = changes(previous, sample) if previous else []
                output.write(json.dumps(sample) + "\n")
                output.flush()
                previous = sample
                result["samples"] += 1
                if index + 1 < args.samples:
                    time.sleep(args.interval)
        result["complete"] = True
    except BaseException as error:
        result["error"] = type(error).__name__
        raise
    finally:
        if samples_path.exists():
            result["samples_sha256"] = benchmark.file_sha256(samples_path)
        benchmark.save(path, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--docker-config", type=Path, required=True)
    parser.add_argument("--docker-host", required=True)
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--interval", type=float, default=5)
    observe(parser.parse_args())


if __name__ == "__main__":
    main()
