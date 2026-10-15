"""Collector isolation, exclusive secret ownership and bounded identity provisioning."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('metrics_identity', Path(__file__).with_name('configure-metrics-client.py'))
CONFIG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONFIG)


class FakeAdmin:
    def __init__(self, exists=False):
        self.exists = exists
        self.created = False
        self.calls = []

    def realm(self, method, path, data=None):
        self.calls.append((method, path, data))
        if path.startswith('/clients?'):
            return [{'id': 'collector'}] if self.exists or self.created else []
        if path == '/roles':
            return [{'name': CONFIG.ROLE}]
        if path == '/roles/' + CONFIG.ROLE:
            return {'id': 'metrics', 'name': CONFIG.ROLE}
        if method == 'POST' and path == '/clients':
            self.created = True
        if path.endswith('/service-account-user'):
            return {'id': 'account'}
        return None


class MetricsCollectorTests(unittest.TestCase):
    def test_new_collector_gets_exact_metrics_role_scopes_and_no_interactive_grants(self):
        with tempfile.TemporaryDirectory() as folder:
            target = CONFIG.secret_destination(str(Path(folder) / 'collector.secret'))
            admin = FakeAdmin()
            CONFIG.provision(admin, target)
            client = next(data for method, path, data in admin.calls if method == 'POST' and path == '/clients')
            self.assertFalse(client['publicClient'])
            self.assertTrue(client['serviceAccountsEnabled'])
            for flag in ('standardFlowEnabled', 'implicitFlowEnabled', 'directAccessGrantsEnabled', 'fullScopeAllowed'):
                self.assertFalse(client[flag])
            self.assertEqual(client['defaultClientScopes'], ['roles', 'warexis-api'])
            self.assertEqual(target.read_text(), client['secret'])
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            grants = [data for method, path, data in admin.calls if method == 'POST' and 'mappings/realm' in path]
            self.assertEqual(grants, [[{'id': 'metrics', 'name': CONFIG.ROLE}]] * 2)

    def test_existing_client_refused_before_secret_or_identity_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'collector.secret'
            admin = FakeAdmin(exists=True)
            with self.assertRaises(ValueError):
                CONFIG.provision(admin, target)
            self.assertFalse(target.exists())
            self.assertTrue(all(method == 'GET' for method, _, _ in admin.calls))

    def test_existing_file_never_overwritten(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'collector.secret'
            target.write_text('user-owned')
            with self.assertRaises(ValueError):
                CONFIG.secret_destination(str(target))
            with self.assertRaises(FileExistsError):
                CONFIG.provision(FakeAdmin(), target)
            self.assertEqual(target.read_text(), 'user-owned')

    def test_rejects_git_directory_or_symlink_before_writing(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / '.git').mkdir()
            with self.assertRaises(ValueError):
                CONFIG.secret_destination(str(root / 'collector.secret'))
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            link = root / 'alias'
            link.symlink_to(root, target_is_directory=True)
            with self.assertRaises(ValueError):
                CONFIG.secret_destination(str(link / 'collector.secret'))

    def test_rejects_nonprivate_parent(self):
        with tempfile.TemporaryDirectory() as folder:
            Path(folder).chmod(0o755)
            with self.assertRaises(ValueError):
                CONFIG.secret_destination(str(Path(folder) / 'collector.secret'))

    def test_rejects_remote_admin_before_network(self):
        for url in ('https://example.invalid', 'http://localhost:18080', 'http://user@127.0.0.1:18080'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                CONFIG.IDENTITY.Admin(url, 'unused', 'unused')


if __name__ == '__main__':
    unittest.main()
