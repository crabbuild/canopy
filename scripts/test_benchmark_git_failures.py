"""Bounded Git failure evidence; local fixtures do not measure server capacity."""
import hashlib
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import benchmark_repositories as benchmark
import audit_three_node_campaign as auditor


class GitFailureEvidenceTests(unittest.TestCase):
    def test_nonzero_exit_retains_redacted_stage_and_stderr(self):
        token = 'fixture-private-token'
        stderr = (f'Authorization: Bearer {token}\n'
                  'Proxy-Authorization: Basic Y3JlZGVudGlhbHM=\n'
                  'fatal: https://user:password@example.invalid/remote denied\n'
                  'https://example.invalid/?TOKEN=query-private&secret=other-private\n'
                  f'{token}\n').encode()
        result = subprocess.CompletedProcess(['git'], 128, stdout=b'', stderr=stderr)
        with patch.object(benchmark, 'git_result', return_value=result), self.assertRaises(RuntimeError) as raised:
            benchmark.git('-c', 'protocol.version=2', 'fetch', 'remote', cwd=Path('.'), token=token)
        details = getattr(raised.exception, 'git_failure', None)
        self.assertIsInstance(details, dict)
        self.assertEqual((details['kind'], details['command'], details['exit_code']), ('exit', 'fetch', 128))
        encoded = json.dumps(details)
        for secret in [token, 'user:password', 'Y3JlZGVudGlhbHM=', 'query-private', 'other-private']:
            self.assertNotIn(secret, encoded)
        self.assertIn('denied', details['stderr_excerpt'])

    def test_excerpt_bound_and_secret_crossing_truncation_boundary(self):
        token = 'fixture-private-token'
        result = subprocess.CompletedProcess(['git'], 1, stdout=b'', stderr=b'x' * 2040 + token.encode() + b'\x01' * 8000)
        with patch.object(benchmark, 'git_result', return_value=result), self.assertRaises(RuntimeError) as raised:
            benchmark.git('push', 'remote', cwd=Path('.'), token=token)
        details = getattr(raised.exception, 'git_failure', None)
        self.assertIsInstance(details, dict)
        self.assertLessEqual(len(details['stderr_excerpt'].encode()), 2048)
        self.assertTrue(details['stderr_truncated'])
        self.assertNotIn(token[:8], details['stderr_excerpt'])
        self.assertLess(len(json.dumps(details)), 13 * 1024)
        auditor.audit_git_failure({'result': 'git_error', 'git_failure': details}, {'git_timeout_seconds': 120})

    def test_invalid_utf8_and_empty_stderr_are_safe(self):
        for stderr in [b'', b'\xff\xfe fatal: denied']:
            with self.subTest(stderr=stderr), patch.object(benchmark, 'git_result', return_value=
                    subprocess.CompletedProcess(['git'], 1, stdout=b'', stderr=stderr)), self.assertRaises(RuntimeError) as raised:
                benchmark.git('clone', 'remote', cwd=Path('.'), token='fixture-token')
            details = getattr(raised.exception, 'git_failure', None)
            self.assertIsInstance(details, dict)
            if not stderr:
                self.assertEqual(details['redacted_stderr_sha256'], hashlib.sha256(b'').hexdigest())
            self.assertLessEqual(len(details['stderr_excerpt'].encode()), 2048)

    def test_timeout_remains_timeout_with_same_deadline(self):
        error = subprocess.TimeoutExpired(['git', 'fetch'], 120, stderr=b'fatal: fixture timeout')
        with patch.object(benchmark, 'git_result', side_effect=error), self.assertRaises(subprocess.TimeoutExpired) as raised:
            benchmark.git('fetch', 'remote', cwd=Path('.'), token='fixture-token', timeout=120)
        self.assertIs(raised.exception, error)
        details = getattr(error, 'git_failure', None)
        self.assertIsInstance(details, dict)
        self.assertEqual((details['kind'], details['command'], details['timeout_seconds']), ('timeout', 'fetch', 120))
        self.assertIsNone(details['exit_code'])

    def fixture(self, root):
        manifest = root / 'corpus.json'
        benchmark.save(manifest, {'version': 1, 'complete': True, 'requested_repositories': 1,
            'repositories': [{'name': 'absent', 'owner': 'canopy', 'repository_id': str(uuid.uuid4()),
                              'commit': 'a' * 40, 'readme_sha256': 'b' * 64}]})
        args = SimpleNamespace(operation='ls_remote', manifest=manifest, seed=20260926,
            active_repositories=1, distribution='uniform', rate=1, duration=1, concurrency=1,
            timeout=30, git_timeout=120, work_dir=root / 'clients', output=root / 'report.json')
        return args, SimpleNamespace(base_url=(root / 'nonexistent-backend').as_uri())

    def test_real_local_git_failure_remains_failed_and_retains_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); args, client = self.fixture(root)
            report = benchmark.measure(args, client, 'fixture-token')
            self.assertEqual(report['outcomes'], {'git_error': 1})
            self.assertEqual(report['failed_arrivals'], 1)
            sample = json.loads(args.output.with_suffix('.samples.jsonl').read_text())
            self.assertEqual(sample['git_failure']['command'], 'ls-remote')
            self.assertNotEqual(sample['git_failure']['exit_code'], 0)
            self.assertIn('fatal:', sample['git_failure']['stderr_excerpt'])
            self.assertEqual(benchmark.file_sha256(args.output.with_suffix('.samples.jsonl')), report['samples_sha256'])

    def test_measured_timeout_is_not_relabelled_as_git_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); args, client = self.fixture(root)
            with patch.object(benchmark, 'git_result', side_effect=subprocess.TimeoutExpired(['git'], 120, stderr=b'fixture-timeout')):
                report = benchmark.measure(args, client, 'fixture-token')
            self.assertEqual(report['outcomes'], {'client_timeout': 1})
            sample = json.loads(args.output.with_suffix('.samples.jsonl').read_text())
            self.assertEqual(sample['git_failure']['timeout_seconds'], 120)
            self.assertEqual(sample['result'], 'client_timeout')
            auditor.audit_git_failure(sample, report)

    def test_independent_auditor_rejects_corrupted_failure_details(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); args, client = self.fixture(root)
            report = benchmark.measure(args, client, 'fixture-token')
            path = args.output.with_suffix('.samples.jsonl')
            sample = json.loads(path.read_text())
            window = {'operation': 'ls_remote', 'distribution': 'uniform', 'active_repositories': 1,
                      'rate': 1, 'duration': 1, 'concurrency': 1}
            def audit():
                return auditor.audit_window(args.output, window, benchmark.file_sha256(args.manifest),
                    benchmark.file_sha256(Path(benchmark.__file__)), {sample['repository_id']}, {0: sample['repository_id']})
            audit()
            for key, value in [('exit_code', 0), ('command', 'secret-command'),
                               ('stderr_excerpt', 'x' * 2049), ('redacted_stderr_sha256', 'bad'),
                               ('arguments', ['secret']), ('timeout_seconds', 120),
                               ('exit_code', True), ('redacted_stderr_bytes', False),
                               ('stderr_truncated', 'yes'), ('kind', 'unknown'),
                               ('redacted_stderr_sha256', 'a' * 64)]:
                changed = deepcopy(sample); changed['git_failure'][key] = value
                path.write_text(json.dumps(changed) + '\n')
                report['samples_sha256'] = benchmark.file_sha256(path); benchmark.save(args.output, report)
                with self.subTest(key=key), self.assertRaises(ValueError):
                    audit()
            legacy = deepcopy(sample); del legacy['git_failure']
            path.write_text(json.dumps(legacy) + '\n')
            report['samples_sha256'] = benchmark.file_sha256(path); benchmark.save(args.output, report)
            audit()


if __name__ == '__main__':
    unittest.main()
