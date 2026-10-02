"""Validate provider counters, safe signed reads and restart/failure boundaries."""

import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import provider_requests as provider


class ProviderTests(unittest.TestCase):
    def metrics(self, total=10):
        return {"errors": [], "final": True, "hosts": [":9000"], "aggregated": {"http": {
            "collected": "2026-10-01T00:00:00Z", "requests": [{"method": "GET",
            "operation": "s3:GetObject", "outcome": "2xx", "total": total}]}}}

    def sample(self, total=10):
        return {"metrics": self.metrics(total)}

    def test_valid_counter_snapshot_preserves_unknown_operations_and_outcomes(self):
        record = self.metrics()
        record["aggregated"]["http"]["requests"].append(
            {"method": "PUT", "operation": "unknown", "outcome": "4xx", "total": 1})
        for method in ("CONNECT", "TRACE", "OTHER"):
            for outcome in ("cancelled", "service_error", "unknown"):
                record["aggregated"]["http"]["requests"].append(
                    {"method": method, "operation": "unknown", "outcome": outcome, "total": 1})
        self.assertEqual(provider.parse_metrics(json.dumps(record).encode() + b"\n"), record)

    def test_incomplete_duplicate_malformed_or_oversized_metrics_fail(self):
        mutations = [lambda value: value.update(final=False),
            lambda value: value.update(errors=["collection failed"]),
            lambda value: value["aggregated"].update(http=None),
            lambda value: value["aggregated"]["http"]["requests"].append(
                copy.deepcopy(value["aggregated"]["http"]["requests"][0])),
            lambda value: value["aggregated"]["http"]["requests"][0].update(total=True),
            lambda value: value["aggregated"]["http"]["requests"][0].update(total=-1),
            lambda value: value["aggregated"]["http"]["requests"][0].update(total=2**64),
            lambda value: value["aggregated"]["http"]["requests"][0].update(operation="private\nlabel"),
            lambda value: value["aggregated"]["http"]["requests"][0].update(method="BAD"),
            lambda value: value["aggregated"]["http"]["requests"][0].update(outcome="invalid")]
        for mutate in mutations:
            record = self.metrics()
            mutate(record)
            with self.assertRaises(ValueError):
                provider.parse_metrics(json.dumps(record).encode())
        for body in (b"x" * (1024 * 1024 + 1), b"", b"{}\n{}\n"):
            with self.assertRaises(ValueError):
                provider.parse_metrics(body)

    def test_counter_deltas_preserve_new_series_and_reject_reset_or_disappearance(self):
        before, after = self.sample(10), self.sample(20)
        after["metrics"]["aggregated"]["http"]["requests"].append(
            {"method": "PUT", "operation": "s3:PutObject", "outcome": "4xx", "total": 2})
        self.assertEqual([row["requests"] for row in provider.changes(before, after)], [10, 2])
        with self.assertRaisesRegex(ValueError, "decreased"):
            provider.changes(before, self.sample(9))
        after["metrics"]["aggregated"]["http"]["requests"] = []
        with self.assertRaisesRegex(ValueError, "disappeared"):
            provider.changes(before, after)

    def test_fetch_is_credential_safe_bounded_and_configuration_isolated(self):
        body = json.dumps(self.metrics()).encode() + b"\n"
        result = SimpleNamespace(returncode=0, stdout=body + b"\n200\napplication/x-ndjson", stderr=b"")
        with patch.object(provider.subprocess, "run", return_value=result) as run:
            sample = provider.fetch("http://127.0.0.1:32776", "private-access", "private-secret", "us-east-1")
        args, kwargs = run.call_args
        argv = args[0]
        self.assertEqual(argv[0:2], ["curl", "--disable"])
        self.assertIn("--noproxy", argv)
        self.assertNotIn("--location", argv)
        self.assertNotIn("private-secret", " ".join(argv))
        self.assertIn(b"private-access:private-secret", kwargs["input"])
        self.assertNotIn("private-secret", json.dumps(sample))
        self.assertEqual(sample["metrics"], self.metrics())

    def test_unsafe_endpoint_credentials_and_failed_http_are_rejected(self):
        for endpoint in ("http://example.com:9000", "https://127.0.0.1:9000", "http://127.0.0.1",
                         "http://name:secret@127.0.0.1:9000", "http://127.0.0.1:9000/?bad=1"):
            with patch.object(provider.subprocess, "run", side_effect=AssertionError("unexpected request")), \
                 self.assertRaises(ValueError):
                provider.fetch(endpoint, "access", "secret", "us-east-1")
        with self.assertRaises(ValueError):
            provider.fetch("http://127.0.0.1:9000", "access", 'inject"\nheader=bad', "us-east-1")
        for returncode, ending in ((1, b""), (0, b"\n302\napplication/x-ndjson"),
                                   (0, b"\n401\napplication/xml"), (0, b"\n200\ntext/html")):
            result = SimpleNamespace(returncode=returncode, stdout=b"{}" + ending,
                                     stderr=b"private signed header")
            with patch.object(provider.subprocess, "run", return_value=result), self.assertRaises(ValueError) as error:
                provider.fetch("http://127.0.0.1:9000", "access", "secret", "us-east-1")
            self.assertNotIn("private signed header", str(error.exception))

    def test_provider_restart_identity_and_purpose_are_enforced(self):
        binding = {"provider_container_id": "id", "provider_started_at": "start", "provider_image_id": "image"}
        value = {"Id": "id", "State": {"Running": True, "StartedAt": "start"}, "Image": "image",
                 "RestartCount": 0, "Config": {"Labels": {"canopy.purpose": "three-node-candidate-e07670e"}}}
        for mutate in (None, lambda item: item["State"].update(StartedAt="new"),
                       lambda item: item["State"].update(Running=False),
                       lambda item: item.update(Id="different"),
                       lambda item: item.update(Image="different"),
                       lambda item: item["Config"]["Labels"].update({"canopy.purpose": "unrelated"})):
            changed = copy.deepcopy(value)
            if mutate:
                mutate(changed)
            result = SimpleNamespace(returncode=0, stdout=json.dumps([changed]).encode())
            with patch.object(provider.subprocess, "run", return_value=result):
                if mutate:
                    with self.assertRaises(ValueError):
                        provider.provider_state(binding, Path("unused"), "unused")
                else:
                    self.assertEqual(provider.provider_state(binding, Path("unused"), "unused")["id"], "id")

    def test_observation_restarts_and_failed_reads_leave_incomplete_receipts(self):
        for failure in (None, "read", "restart"):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binding = root / "binding.json"
                binding.write_text(json.dumps({"provider_endpoint": "http://127.0.0.1:9000"}))
                args = SimpleNamespace(binding=binding, docker_config=root / "docker", docker_host="unused",
                    output_dir=root / "observation", samples=2, interval=.1)
                samples = [self.sample(10), ValueError("private diagnostic") if failure == "read" else self.sample(20)]
                states = [{"id": "id", "restart_count": 0}] * 4
                states += [{"id": "id", "restart_count": 1 if failure == "restart" else 0}]
                with patch.object(provider, "provider_state", side_effect=states), \
                     patch.object(provider, "fetch", side_effect=samples), patch.object(provider.time, "sleep"):
                    if failure:
                        with self.assertRaises(ValueError):
                            provider.observe(args)
                    else:
                        self.assertTrue(provider.observe(args)["complete"])
                record = json.loads((args.output_dir / "observation.json").read_text())
                self.assertEqual(record["complete"], failure is None)
                self.assertEqual(record["samples"], 1 if failure else 2)
                self.assertNotIn("private diagnostic", json.dumps(record))
                self.assertEqual(record["samples_sha256"], provider.benchmark.file_sha256(args.output_dir / "samples.jsonl"))


if __name__ == "__main__":
    unittest.main()
