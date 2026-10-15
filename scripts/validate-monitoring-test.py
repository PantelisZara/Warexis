#!/usr/bin/env python3
"""Behavioral regression tests for private runtime generation boundaries."""
import argparse
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('prepare_monitoring', Path(__file__).with_name('prepare-monitoring.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class MonitoringPreparationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='warexis-monitoring-test-')
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.secret = self.base / 'secret'
        self.secret.write_text('test-only-not-a-real-credential')
        self.secret.chmod(0o600)
        self.args = argparse.Namespace(runtime_dir=str(self.base / 'runtime'), collector_secret_file=str(self.secret),
            issuer='http://127.0.0.1:18080/realms/warexis', order_url='http://127.0.0.1:18081',
            inventory_url='http://127.0.0.1:18082', fulfillment_url='http://127.0.0.1:18083',
            collector_client_id='warexis-metrics-collector', prometheus_port=19090, grafana_port=13000)

    def prepare(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            MODULE.prepare(self.args)
        return output.getvalue()

    def test_private_runtime_uses_three_authenticated_loopback_targets_without_copying_secret(self):
        output = self.prepare()
        runtime = Path(self.args.runtime_dir)
        self.assertEqual(runtime.stat().st_mode & 0o777, 0o700)
        config = json.loads((runtime / 'prometheus.json').read_text())
        self.assertEqual([job['job_name'] for job in config['scrape_configs']], ['warexis-order', 'warexis-inventory', 'warexis-fulfillment'])
        for job in config['scrape_configs']:
            self.assertEqual(job['oauth2']['client_secret_file'], '/run/secrets/collector-secret')
            self.assertEqual(job['metrics_path'], '/actuator/prometheus')
            self.assertTrue(job['honor_labels'])
        for path in runtime.iterdir():
            self.assertEqual(path.stat().st_mode & 0o777, 0o700 if path.is_dir() else 0o600)
            if path.is_file():
                self.assertNotIn(self.secret.read_text(), path.read_text())
        self.assertNotIn(self.secret.read_text(), output)
        self.assertGreater(len((runtime / 'grafana-admin-password').read_text().strip()), 32)

    def test_existing_runtime_is_never_overwritten(self):
        self.prepare()
        password = (Path(self.args.runtime_dir) / 'grafana-admin-password').read_bytes()
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.prepare()
        self.assertEqual((Path(self.args.runtime_dir) / 'grafana-admin-password').read_bytes(), password)

    def test_runtime_inside_checkout_is_rejected(self):
        (self.base / '.git').mkdir()
        with self.assertRaisesRegex(ValueError, 'outside Git'):
            self.prepare()
        self.assertFalse(Path(self.args.runtime_dir).exists())

    def test_symbolic_link_path_is_rejected(self):
        alias = self.base / 'alias'
        alias.symlink_to(self.base, target_is_directory=True)
        self.args.runtime_dir = str(alias / 'runtime')
        with self.assertRaisesRegex(ValueError, 'symbolic links'):
            self.prepare()

    def test_public_secret_permissions_are_rejected(self):
        self.secret.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'mode0600'):
            self.prepare()

    def test_linked_secret_is_rejected(self):
        os.link(self.secret, self.base / 'secret-copy')
        with self.assertRaisesRegex(ValueError, 'one link'):
            self.prepare()

    def test_secret_inside_another_checkout_is_rejected(self):
        checkout = self.base / 'checkout'
        checkout.mkdir()
        (checkout / '.git').write_text('gitdir: ignored')
        self.secret.rename(checkout / 'secret')
        self.args.collector_secret_file = str(checkout / 'secret')
        with self.assertRaisesRegex(ValueError, 'outside Git'):
            self.prepare()

    def test_non_loopback_and_credential_bearing_urls_are_rejected(self):
        for url in ['http://0.0.0.0:18081', 'http://example.com:18081', 'http://user:password@127.0.0.1:18081',
                    'http://127.0.0.1:18081?token=bad', 'http://127.0.0.1:18081/actuator/prometheus']:
            with self.subTest(url=url), self.assertRaises(ValueError):
                MODULE.loopback_url(url)

    def test_issuer_requires_realm_and_monitoring_ports_are_distinct(self):
        with self.assertRaises(ValueError):
            MODULE.loopback_url('http://127.0.0.1:18080', issuer=True)
        self.args.grafana_port = self.args.prometheus_port
        with self.assertRaisesRegex(ValueError, 'distinct'):
            self.prepare()


if __name__ == '__main__':
    unittest.main()
