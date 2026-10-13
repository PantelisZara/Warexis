#!/usr/bin/env python3
"""Check or provision the loopback-only public browser client without resetting a realm."""
import argparse
import getpass
import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

CLIENT = 'warexis-operations'
ORIGIN = 'http://127.0.0.1:5173'
ROLES = {'WAREXIS_VIEWER', 'WAREXIS_OPERATOR'}
SCOPES = {'roles', 'warexis-api'}
REALM_FILE = Path(__file__).with_name('warexis-realm.json')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def client_contract(client):
    expected = {'clientId': CLIENT, 'enabled': True, 'protocol': 'openid-connect',
                'publicClient': True, 'bearerOnly': False, 'standardFlowEnabled': True,
                'implicitFlowEnabled': False, 'directAccessGrantsEnabled': False,
                'serviceAccountsEnabled': False, 'fullScopeAllowed': False}
    for key, value in expected.items():
        require(client.get(key) == value, 'Unexpected browser client setting: ' + key)
    require(not client.get('secret'), 'A public browser client must have no secret')
    require(client.get('redirectUris') == [ORIGIN + '/'], 'Only the exact root redirect is allowed')
    require(client.get('webOrigins') == [ORIGIN], 'Only the exact loopback origin is allowed')
    require(client.get('attributes', {}).get('pkce.code.challenge.method') == 'S256', 'S256 must be enforced')
    require(client.get('attributes', {}).get('post.logout.redirect.uris') == ORIGIN + '/', 'Exact logout redirect required')
    require(set(client.get('defaultClientScopes', [])) == SCOPES, 'Explicit role and API audience scopes required')
    require(client.get('optionalClientScopes', []) == [], 'No optional scopes permitted')


def load_contract():
    realm = json.loads(REALM_FILE.read_text())
    clients = [c for c in realm['clients'] if c['clientId'] == CLIENT]
    require(len(clients) == 1, 'Exactly one operations client required')
    client_contract(clients[0])
    mappings = [m for m in realm.get('scopeMappings', []) if m.get('client') == CLIENT]
    require(len(mappings) == 1 and set(mappings[0]['roles']) == ROLES, 'Browser realm-role scope must be exact')
    require(not realm.get('users'), 'Do not import user credentials')
    return realm, clients[0]


class NoRedirect(HTTPRedirectHandler):
    # Never forward an administrative Bearer credential to a redirect target.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Admin:
    def __init__(self, base, client_id, secret):
        parts = urlsplit(base)
        require(parts.scheme == 'http' and parts.hostname == '127.0.0.1' and not parts.username
                and not parts.password and not parts.query and not parts.fragment and parts.path in ('', '/'),
                'Use an explicit http://127.0.0.1:<port> local Keycloak URL')
        self.base = base.rstrip('/')
        self.opener = build_opener(ProxyHandler({}), NoRedirect())
        self.token = None
        result = self.call('POST', '/realms/master/protocol/openid-connect/token',
                           {'grant_type': 'client_credentials', 'client_id': client_id, 'client_secret': secret}, form=True)
        self.token = result['access_token']

    def call(self, method, path, data=None, form=False):
        headers = {}
        if self.token:
            headers['Authorization'] = 'Bearer ' + self.token
        if data is not None:
            headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
            data = urlencode(data).encode() if form else json.dumps(data).encode()
        try:
            with self.opener.open(Request(self.base + path, data=data, headers=headers, method=method), timeout=15) as response:
                body = response.read()
                return json.loads(body) if body else None
        except HTTPError as error:
            # Do not expose response bodies, credentials, tokens or request headers.
            raise ValueError('Local identity request failed with HTTP ' + str(error.code)) from None
        except URLError:
            raise ValueError('Local identity server is unavailable') from None

    def realm(self, method, path, data=None):
        return self.call(method, '/admin/realms/warexis' + path, data)


def sync_client(admin, desired):
    # Read and check prerequisites. Never recreate or modify the realm, roles or shared mappers.
    admin.realm('GET', '')
    roles = [admin.realm('GET', '/roles/' + name) for name in sorted(ROLES)]
    scopes = admin.realm('GET', '/client-scopes')
    scope_ids = {s['name']: s['id'] for s in scopes if s['name'] in SCOPES}
    require(set(scope_ids) == SCOPES, 'Existing roles and warexis-api client scopes are required')
    clients = admin.realm('GET', '/clients?' + urlencode({'clientId': CLIENT}))
    require(len(clients) <= 1, 'Ambiguous operations client')
    if not clients:
        admin.realm('POST', '/clients', desired)
        clients = admin.realm('GET', '/clients?' + urlencode({'clientId': CLIENT}))
    require(len(clients) == 1, 'Operations client was not created')
    path = '/clients/' + clients[0]['id']
    current = admin.realm('GET', path)
    # Do not convert an unrelated confidential/service-account client into a public SPA.
    require(current.get('publicClient') is True and current.get('serviceAccountsEnabled') is False,
            'Existing operations client is incompatible; review it in the Admin Console first')
    mappings = admin.realm('GET', path + '/scope-mappings/realm')
    require(all(role['name'] in ROLES for role in mappings),
            'Existing unexpected realm-role scopes require manual review before provisioning')
    client_mappings = admin.realm('GET', path + '/scope-mappings').get('clientMappings', {})
    require(not client_mappings, 'Existing client-role scopes require manual review before provisioning')
    representation = dict(current)
    representation.update(desired)
    representation.pop('secret', None)
    admin.realm('PUT', path, representation)
    for attachment in ('default-client-scopes', 'optional-client-scopes'):
        attached = admin.realm('GET', path + '/' + attachment)
        for scope in attached:
            if attachment == 'optional-client-scopes' or scope['name'] not in SCOPES:
                admin.realm('DELETE', path + '/' + attachment + '/' + scope['id'])
    for scope_id in scope_ids.values():
        admin.realm('PUT', path + '/default-client-scopes/' + scope_id)
    admin.realm('POST', path + '/scope-mappings/realm', roles)
    final = admin.realm('GET', path)
    final['defaultClientScopes'] = [s['name'] for s in admin.realm('GET', path + '/default-client-scopes')]
    final['optionalClientScopes'] = [s['name'] for s in admin.realm('GET', path + '/optional-client-scopes')]
    client_contract(final)
    require({r['name'] for r in admin.realm('GET', path + '/scope-mappings/realm')} == ROLES,
            'Saved browser realm-role scopes differ')


def create_user(admin, username, role, password):
    # Existing users are left untouched; role assignment happens only for a newly created local user.
    users = admin.realm('GET', '/users?' + urlencode({'username': username, 'exact': 'true'}))
    require(not users, 'User already exists; manage existing users in the Admin Console')
    role_representation = admin.realm('GET', '/roles/' + role)
    admin.realm('POST', '/users', {'username': username, 'enabled': True, 'requiredActions': [],
                                 'credentials': [{'type': 'password', 'value': password, 'temporary': False}]})
    users = admin.realm('GET', '/users?' + urlencode({'username': username, 'exact': 'true'}))
    require(len(users) == 1, 'New user lookup failed; review in the Admin Console')
    admin.realm('POST', '/users/' + users[0]['id'] + '/role-mappings/realm', [role_representation])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['check', 'sync-client', 'create-user'])
    parser.add_argument('--url', default='http://127.0.0.1:18080')
    parser.add_argument('--admin-client', default=os.environ.get('KEYCLOAK_BOOTSTRAP_ADMIN_CLIENT_ID', 'warexis-bootstrap-admin'))
    parser.add_argument('--username')
    parser.add_argument('--role', choices=sorted(ROLES))
    args = parser.parse_args()
    try:
        _, desired = load_contract()
        if args.action == 'check':
            print('Browser client contract: PASS')
            return
        if args.action == 'create-user':
            require(args.username and args.role, '--username and --role are required')
        admin = Admin(args.url, args.admin_client, getpass.getpass('Local administrative client secret: '))
        if args.action == 'sync-client':
            sync_client(admin, desired)
            print('Public browser client and explicit role scopes verified')
        else:
            password = getpass.getpass('New local user password: ')
            require(password and password == getpass.getpass('Confirm password: '), 'Passwords must match and be nonempty')
            create_user(admin, args.username, args.role, password)
            print('Local user created with the requested realm role')
    except (ValueError, KeyError, OSError) as error:
        print('Provisioning stopped: ' + str(error), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
