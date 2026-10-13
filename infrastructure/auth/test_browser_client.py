"""Regression guards for the frozen local browser identity boundary."""
import copy
import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).with_name('configure-browser-client.py')
SPEC = importlib.util.spec_from_file_location('browser_client', MODULE)
CONFIG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONFIG)


class BrowserClientContractTests(unittest.TestCase):
    def setUp(self):
        self.realm, self.client = CONFIG.load_contract()

    def test_import_has_no_users_or_secrets(self):
        self.assertFalse(self.realm.get('users'))
        self.assertTrue(all(not client.get('secret') for client in self.realm['clients']))

    def test_rejects_alternate_origins_redirects_or_logout_wildcards(self):
        changes = [('webOrigins', ['*']), ('webOrigins', ['http://localhost:5173']),
                   ('redirectUris', ['http://127.0.0.1:5173/*']),
                   ('redirectUris', ['http://localhost:5173/'])]
        for field, value in changes:
            with self.subTest(field=field, value=value):
                client = copy.deepcopy(self.client)
                client[field] = value
                with self.assertRaises(ValueError):
                    CONFIG.client_contract(client)
        client = copy.deepcopy(self.client)
        client['attributes']['post.logout.redirect.uris'] = '+'
        with self.assertRaises(ValueError):
            CONFIG.client_contract(client)

    def test_rejects_client_credentials_password_implicit_or_unscoped_access(self):
        for field in ('implicitFlowEnabled', 'directAccessGrantsEnabled', 'serviceAccountsEnabled', 'fullScopeAllowed'):
            with self.subTest(field=field):
                client = copy.deepcopy(self.client)
                client[field] = True
                with self.assertRaises(ValueError):
                    CONFIG.client_contract(client)
        for field, value in [('publicClient', False), ('standardFlowEnabled', False), ('secret', 'fixture-only')]:
            client = copy.deepcopy(self.client)
            client[field] = value
            with self.assertRaises(ValueError):
                CONFIG.client_contract(client)

    def test_rejects_missing_or_plain_pkce(self):
        for method in (None, 'plain'):
            client = copy.deepcopy(self.client)
            client['attributes']['pkce.code.challenge.method'] = method
            with self.assertRaises(ValueError):
                CONFIG.client_contract(client)

    def test_requires_audience_and_role_claim_scopes(self):
        for scopes in (['roles'], ['warexis-api'], ['roles', 'warexis-api', 'unrelated']):
            client = copy.deepcopy(self.client)
            client['defaultClientScopes'] = scopes
            with self.assertRaises(ValueError):
                CONFIG.client_contract(client)

    def test_refuses_remote_admin_url_before_request(self):
        for base in ('https://example.invalid', 'http://localhost:18080',
                     'http://127.0.0.1:18080/other', 'http://user@127.0.0.1:18080'):
            with self.subTest(base=base), self.assertRaises(ValueError):
                CONFIG.Admin(base, 'unused', 'unused')

    def test_refuses_incompatible_existing_client_before_updates(self):
        self.assert_sync_refused({'publicClient': False, 'serviceAccountsEnabled': True}, [], {})

    def test_refuses_unexpected_existing_role_scopes_before_updates(self):
        self.assert_sync_refused({'publicClient': True, 'serviceAccountsEnabled': False},
                                 [{'name': 'unrelated-role'}], {})
        self.assert_sync_refused({'publicClient': True, 'serviceAccountsEnabled': False}, [],
                                 {'another-client': {'mappings': [{'name': 'admin'}]}})

    def assert_sync_refused(self, current, mappings, client_mappings):
        self_test = self
        class ReadOnlyAdmin:
            def realm(self, method, path, data=None):
                self_test.assertEqual(method, 'GET', 'Unexpected write before incompatible mapping review')
                if path == '':
                    return {'realm': 'warexis'}
                if path.startswith('/roles/'):
                    return {'name': path.rsplit('/', 1)[1]}
                if path == '/client-scopes':
                    return [{'name': name, 'id': name} for name in CONFIG.SCOPES]
                if path.startswith('/clients?'):
                    return [{'id': 'existing'}]
                if path == '/clients/existing':
                    return current
                if path.endswith('/scope-mappings/realm'):
                    return mappings
                if path.endswith('/scope-mappings'):
                    return {'clientMappings': client_mappings}
                raise AssertionError('Unexpected administrative path')
        with self.assertRaises(ValueError):
            CONFIG.sync_client(ReadOnlyAdmin(), self.client)

    def test_existing_user_is_never_updated(self):
        class ExistingUserAdmin:
            def realm(self, method, path, data=None):
                self_test.assertEqual(method, 'GET')
                return [{'id': 'existing'}]
        self_test = self
        with self.assertRaises(ValueError):
            CONFIG.create_user(ExistingUserAdmin(), 'existing-user', 'WAREXIS_VIEWER', 'unused')


if __name__ == '__main__':
    unittest.main()
