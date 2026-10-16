#!/usr/bin/env python3
"""Regression checks for private tracing runtime boundaries."""
import argparse
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('prepare_tracing', Path(__file__).with_name('prepare-tracing.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TracingPreparationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='warexis-tracing-test-')
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.args = argparse.Namespace(runtime_dir=str(self.base / 'runtime'), query_port=13200, otlp_http_port=14318)

    def prepare(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            MODULE.prepare(self.args)
        return output.getvalue()

    def test_private_runtime_copies_config_and_uses_callers_identity(self):
        output = self.prepare()
        runtime = Path(self.args.runtime_dir)
        self.assertEqual((runtime / 'tempo.yaml').read_bytes(), MODULE.CONFIG.read_bytes())
        self.assertEqual(runtime.stat().st_mode & 0o777, 0o700)
        for item in runtime.iterdir():
            self.assertEqual(item.stat().st_mode & 0o777, 0o700 if item.is_dir() else 0o600)
            self.assertEqual(item.stat().st_uid, os.getuid())
        env = (runtime / 'tracing.env').read_text()
        self.assertIn(f"WAREXIS_TRACING_UID='{os.getuid()}'", env)
        self.assertIn(f"WAREXIS_TRACING_GID='{os.getgid()}'", env)
        self.assertIn("WAREXIS_TEMPO_QUERY_PORT='13200'", env)
        self.assertIn("WAREXIS_TEMPO_OTLP_HTTP_PORT='14318'", env)
        self.assertNotIn(str(runtime), output)

    def test_optional_provisioning_preserves_accepted_monitoring_configs(self):
        self.prepare()
        runtime = Path(self.args.runtime_dir)
        provisioned = runtime / 'grafana-provisioning'
        for relative in ('dashboards/warexis.yaml', 'datasources/prometheus.yaml'):
            self.assertEqual((provisioned / relative).read_bytes(),
                             (MODULE.ROOT / 'infrastructure/monitoring/grafana/provisioning' / relative).read_bytes())
        for item in provisioned.rglob('*'):
            self.assertEqual(item.stat().st_mode & 0o777, 0o700 if item.is_dir() else 0o600)
        tempo = (provisioned / 'datasources/tempo.yaml').read_text()
        self.assertIn('uid: warexis-tempo', tempo)
        self.assertIn('url: ${WAREXIS_TEMPO_URL}', tempo)
        self.assertIn('isDefault: false', tempo)
        self.assertIn('editable: false', tempo)
        self.assertNotIn('password', tempo.lower())

    def test_optional_datasource_is_not_installed_in_base_monitoring(self):
        self.assertFalse((MODULE.ROOT / 'infrastructure/monitoring/grafana/provisioning/datasources/tempo.yaml').exists())
        self.prepare()
        runtime = Path(self.args.runtime_dir)
        self.assertTrue((runtime / 'grafana-provisioning/datasources/tempo.yaml').is_file())

    def test_existing_runtime_is_never_overwritten(self):
        self.prepare()
        config = Path(self.args.runtime_dir) / 'tempo.yaml'
        config.write_text('preserved')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.prepare()
        self.assertEqual(config.read_text(), 'preserved')

    def test_checkout_and_worktree_paths_are_rejected(self):
        for marker in ('directory', 'file'):
            with self.subTest(marker=marker):
                checkout = self.base / marker
                checkout.mkdir()
                git = checkout / '.git'
                git.mkdir() if marker == 'directory' else git.write_text('gitdir: unused')
                self.args.runtime_dir = str(checkout / 'runtime')
                with self.assertRaisesRegex(ValueError, 'outside Git'):
                    self.prepare()
                self.assertFalse(Path(self.args.runtime_dir).exists())

    def test_normalized_checkout_path_is_rejected(self):
        checkout = self.base / 'checkout'
        checkout.mkdir()
        (checkout / '.git').mkdir()
        self.args.runtime_dir = str(self.base / 'missing' / '..' / 'checkout' / 'runtime')
        with self.assertRaisesRegex(ValueError, 'outside Git'):
            self.prepare()

    def test_symbolic_link_components_are_rejected(self):
        alias = self.base / 'alias'
        alias.symlink_to(self.base, target_is_directory=True)
        for path in (alias / 'runtime', alias / '..' / 'runtime', self.base / 'missing' / '..' / 'alias' / 'runtime'):
            self.args.runtime_dir = str(path)
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, 'symbolic links'):
                self.prepare()

    def test_environment_delimiters_are_rejected(self):
        for suffix in ("a'b", 'a\nb', 'a\rb'):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                MODULE.external_path(str(self.base / suffix))

    def test_invalid_or_duplicate_ports_leave_no_runtime(self):
        for query, otlp in ((1023, 14318), (13200, 65536), (13200, 13200)):
            self.args.query_port, self.args.otlp_http_port = query, otlp
            with self.subTest(query=query, otlp=otlp), self.assertRaisesRegex(ValueError, 'distinct unprivileged'):
                self.prepare()
            self.assertFalse(Path(self.args.runtime_dir).exists())

    def test_missing_parent_is_rejected(self):
        self.args.runtime_dir = str(self.base / 'missing' / 'runtime')
        with self.assertRaisesRegex(ValueError, 'parent must already exist'):
            self.prepare()


if __name__ == '__main__':
    unittest.main()
