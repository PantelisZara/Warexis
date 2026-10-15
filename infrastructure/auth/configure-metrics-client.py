#!/usr/bin/env python3
"""Provision a new loopback development collector with metrics-only JWT authority."""
import argparse
import getpass
import importlib.util
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
from urllib.parse import urlencode

CLIENT = 'warexis-metrics-collector'
ROLE = 'WAREXIS_METRICS'
SPEC = importlib.util.spec_from_file_location('browser_identity', Path(__file__).with_name('configure-browser-client.py'))
IDENTITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(IDENTITY)


def secret_destination(value):
    path = Path(value).absolute()
    for item in (path, *path.parents):
        if item.is_symlink():
            raise ValueError('Secret destination must not traverse symlinks')
    if not path.parent.is_dir() or (path.parent.stat().st_mode & 0o777) != 0o700:
        raise ValueError('Create an external private 0700 parent directory first')
    probe = subprocess.run(['git', '-C', str(path.parent), 'rev-parse', '--show-toplevel'],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    if probe.returncode == 0 or any((parent / '.git').exists() for parent in path.parents):
        raise ValueError('Collector secrets must be outside Git checkouts')
    if path.exists():
        raise ValueError('Secret destination already exists; refusing to overwrite')
    return path


def provision(admin, destination):
    existing = admin.realm('GET', '/clients?' + urlencode({'clientId': CLIENT}))
    if existing:
        raise ValueError('Collector already exists; preserve its private secret and inspect it before changing anything')
    secret = secrets.token_urlsafe(48)
    # Reserve the external file exclusively before changing identity state. On partial
    # provisioning failure keep it for recovery; never silently rotate an existing client.
    fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(secret)
    roles = admin.realm('GET', '/roles')
    if not any(role.get('name') == ROLE for role in roles):
        admin.realm('POST', '/roles', {'name': ROLE, 'description': 'Read service metrics only'})
    role = admin.realm('GET', '/roles/' + ROLE)
    admin.realm('POST', '/clients', {'clientId': CLIENT, 'enabled': True, 'protocol': 'openid-connect',
                                  'publicClient': False, 'bearerOnly': False, 'secret': secret,
                                  'serviceAccountsEnabled': True, 'standardFlowEnabled': False,
                                  'implicitFlowEnabled': False, 'directAccessGrantsEnabled': False,
                                  'fullScopeAllowed': False, 'defaultClientScopes': ['roles', 'warexis-api'],
                                  'optionalClientScopes': []})
    clients = admin.realm('GET', '/clients?' + urlencode({'clientId': CLIENT}))
    if len(clients) != 1:
        raise ValueError('Collector lookup failed; private secret retained for recovery')
    client = clients[0]['id']
    account = admin.realm('GET', '/clients/' + client + '/service-account-user')
    admin.realm('POST', '/users/' + account['id'] + '/role-mappings/realm', [role])
    admin.realm('POST', '/clients/' + client + '/scope-mappings/realm', [role])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:18080')
    parser.add_argument('--admin-client', default='warexis-bootstrap-admin')
    parser.add_argument('--secret-file', required=True)
    args = parser.parse_args()
    try:
        destination = secret_destination(args.secret_file)
        admin = IDENTITY.Admin(args.url, args.admin_client, getpass.getpass('Local administrative client secret: '))
        provision(admin, destination)
        print('Metrics-only collector provisioned. Private secret saved; no business roles assigned.')
    except (ValueError, KeyError, OSError):
        print('Collector provisioning stopped. No secrets printed. Preserve any newly created private secret file and inspect local identity state before retrying.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
