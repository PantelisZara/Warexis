#!/usr/bin/env python3
"""Focused acceptance for the dedicated local kind Warexis deployment."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import secrets
import stat
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, ProxyHandler, build_opener

# Import the existing helper without writing generated files into the checkout.
sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
AUTH_SPEC = importlib.util.spec_from_file_location('browser_identity', ROOT / 'infrastructure/auth/configure-browser-client.py')
AUTH = importlib.util.module_from_spec(AUTH_SPEC)
AUTH_SPEC.loader.exec_module(AUTH)
ORIGIN = 'http://127.0.0.1:5173'
IDENTITY = 'http://127.0.0.1:18080'
ISSUER = IDENTITY + '/realms/warexis'
CONTEXT = 'kind-warexis-phase11'
SERVICES = ('order', 'inventory', 'fulfillment')
# Reuse the identity helper's redirect rejection for all credential-bearing calls.
HTTP = build_opener(ProxyHandler({}), AUTH.NoRedirect())


def require(condition, message):
    if not condition:
        raise ValueError(message)


def private_file(path):
    require(path.is_absolute() and path.is_file() and not path.is_symlink(), 'Expected an absolute private regular file')
    require(stat.S_IMODE(path.stat().st_mode) == 0o600, 'Private input must have mode 0600')
    return path


def runtime_directory(value):
    path = Path(value)
    require(path.is_absolute() and path == path.resolve(), 'Runtime directory must be an absolute normalized path without symlinks')
    for parent in (path, *path.parents):
        require(not parent.is_symlink() and not (parent / '.git').exists(), 'Keep private runtime state outside Git and symlinks')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    require(stat.S_IMODE(path.stat().st_mode) == 0o700, 'Runtime directory must have mode 0700')
    return path


def save(path, value):
    # O_NOFOLLOW and an exclusive temporary file prevent symlink replacement.
    temporary = path.with_name(path.name + '.tmp-' + secrets.token_hex(6))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(value, output, indent=2)
        output.write('\n')
    os.replace(temporary, path)


def call(method, url, data=None, token=None, expected=(200,), form=False):
    require(url.startswith(ORIGIN + '/') or url.startswith(IDENTITY + '/'), 'Only fixed local deployment URLs are allowed')
    headers = {}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    if data is not None:
        headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
        data = urlencode(data).encode() if form else json.dumps(data).encode()
    try:
        response = HTTP.open(Request(url, data=data, headers=headers, method=method), timeout=15)
    except HTTPError as error:
        response = error
    except URLError:
        raise ValueError('Local deployment HTTP request unavailable') from None
    with response:
        require(response.code in expected, 'Local deployment HTTP status ' + str(response.code))
        body = response.read()
        return json.loads(body) if body else None


def api(service, path, token, method='GET', data=None, expected=(200,)):
    return call(method, ORIGIN + '/api/' + service + path, data, token, expected)


def token(runtime, role):
    client = runtime['clients'][role]
    return call('POST', ISSUER + '/protocol/openid-connect/token',
                {'grant_type': 'client_credentials', 'client_id': client['id'], 'client_secret': client['secret']},
                form=True)['access_token']


def wait_for(fetch, predicate, description, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = fetch()
            if predicate(value):
                return value
        except ValueError:
            pass
        time.sleep(1)
    raise ValueError('Timed out waiting for ' + description)


def kubectl(args, command, data=None):
    result = subprocess.run([args.kubectl, '--kubeconfig', args.kubeconfig, '--context', CONTEXT, '-n', 'warexis', *command],
                            input=data, capture_output=True, text=True, timeout=90, check=False)
    require(result.returncode == 0, 'Scoped kubectl check failed; inspect dedicated cluster events/logs')
    return result.stdout.strip()


def sql(args, service, query):
    # Local socket authentication inside the owning PostgreSQL pod; no password argv.
    return kubectl(args, ['exec', '-i', 'statefulset/' + service + '-postgres', '--',
                          'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'warexis_' + service,
                          '-d', 'warexis_' + service, '-At'], query + '\n')


def setup(args, directory):
    destination = directory / 'runtime.json'
    if destination.exists():
        existing = json.loads(private_file(destination).read_text())
        require(existing.get('status') == 'READY' and existing.get('bootstrapRemoved') is True, 'Partial provisioning requires inspection of the private identity inventory')
        return {'status': 'PASS', 'reused': True, 'bootstrapRemoved': True, 'tokensPersisted': False}
    env = {}
    for line in private_file(Path(args.bootstrap_file)).read_text().splitlines():
        if line and not line.startswith('#'):
            key, separator, value = line.partition('=')
            require(bool(separator), 'Invalid private environment input')
            env[key] = value
    admin = AUTH.Admin(IDENTITY, 'warexis-bootstrap-admin', env['KC_BOOTSTRAP_ADMIN_CLIENT_SECRET'])
    _, desired = AUTH.load_contract()
    AUTH.sync_client(admin, desired)
    suffix = secrets.token_hex(5)
    runtime = {'status': 'PROVISIONING', 'browserOrigin': ORIGIN, 'issuer': ISSUER, 'users': {}, 'clients': {}}
    # Persist generated values before provisioning so a partial failure has a private inventory.
    for role in ('operator', 'viewer'):
        runtime['users'][role] = {'username': 'p11-' + role + '-' + suffix, 'password': secrets.token_urlsafe(24)}
        runtime['clients'][role] = {'id': 'p11-api-' + role + '-' + suffix, 'secret': secrets.token_urlsafe(32)}
    save(destination, runtime)
    for role in ('operator', 'viewer'):
        user = runtime['users'][role]
        role_name = 'WAREXIS_' + role.upper()
        AUTH.create_user(admin, user['username'], role_name, user['password'])
        user_id = admin.realm('GET', '/users?' + urlencode({'username': user['username'], 'exact': 'true'}))[0]['id']
        admin.realm('PUT', '/users/' + user_id, {'firstName': 'Local', 'lastName': role.title(),
                    'email': user['username'] + '@example.invalid', 'emailVerified': True, 'requiredActions': []})
        client = runtime['clients'][role]
        admin.realm('POST', '/clients', {'clientId': client['id'], 'secret': client['secret'],
                    'protocol': 'openid-connect', 'publicClient': False, 'serviceAccountsEnabled': True,
                    'standardFlowEnabled': False, 'implicitFlowEnabled': False, 'directAccessGrantsEnabled': False,
                    'fullScopeAllowed': False, 'defaultClientScopes': ['roles', 'warexis-api'], 'optionalClientScopes': []})
        client_id = admin.realm('GET', '/clients?' + urlencode({'clientId': client['id']}))[0]['id']
        account = admin.realm('GET', '/clients/' + client_id + '/service-account-user')
        representation = admin.realm('GET', '/roles/' + role_name)
        admin.realm('POST', '/users/' + account['id'] + '/role-mappings/realm', [representation])
        admin.realm('POST', '/clients/' + client_id + '/scope-mappings/realm', [representation])
        # Token acquisition proves the explicit service account grant, without storing tokens.
        token(runtime, role)
    bootstrap = admin.call('GET', '/admin/realms/master/clients?' + urlencode({'clientId': 'warexis-bootstrap-admin'}))
    require(len(bootstrap) == 1, 'Expected exactly one disposable bootstrap client')
    admin.call('DELETE', '/admin/realms/master/clients/' + bootstrap[0]['id'])
    runtime['bootstrapRemoved'] = True
    runtime['status'] = 'READY'
    save(destination, runtime)
    return {'status': 'PASS', 'bootstrapRemoved': True, 'browserUsers': ['operator', 'viewer'], 'tokensPersisted': False}


def workflow(runtime, evidence, evidence_path):
    bearer = token(runtime, 'operator')
    sku = 'K8S-' + secrets.token_hex(6)
    stock = api('inventory', '/api/stock-items', bearer, 'POST', {'sku': sku, 'onHand': 12}, (201,))
    require(stock['onHand'] == 12 and stock['reserved'] == 0, 'Fresh synthetic stock must be empty')
    order = api('order', '/api/orders', bearer, 'POST', {'lines': [{'sku': sku, 'quantity': 3}]}, (201,))
    evidence.update({'sku': sku, 'orderId': order['id']})
    save(evidence_path, evidence)
    return finish_order(runtime, evidence, bearer, order['id'], ship=True)


def finish_order(runtime, evidence, bearer, order_id, ship=False):
    order = wait_for(lambda: api('order', '/api/orders/' + order_id, bearer),
                     lambda value: value['status'] == 'CONFIRMED', 'confirmed order')
    fulfillment = wait_for(lambda: api('fulfillment', '/api/fulfillments/orders/' + order_id, bearer),
                           lambda value: value['status'] == 'PENDING', 'pending fulfillment')
    require(fulfillment['orderId'] == order['id'], 'Fulfillment must belong to confirmed order')
    if ship:
        evidence['fulfillmentId'] = fulfillment['id']
        reference = 'K8S-SHIP-' + secrets.token_hex(6)
        shipped = api('fulfillment', '/api/fulfillments/' + fulfillment['id'] + '/ship', bearer,
                      'POST', {'shipmentReference': reference})
        require(shipped['status'] == 'SHIPPED', 'Dispatch must persist SHIPPED')
        evidence['shipmentReference'] = reference
    return {'status': 'PASS', 'orderStatus': order['status'], 'fulfillmentStatus': 'SHIPPED' if ship else fulfillment['status']}


def rejected(runtime, evidence, evidence_path):
    bearer = token(runtime, 'operator')
    order = api('order', '/api/orders', bearer, 'POST', {'lines': [{'sku': evidence['sku'], 'quantity': 100}]}, (201,))
    evidence['rejectedOrderId'] = order['id']
    save(evidence_path, evidence)
    value = wait_for(lambda: api('order', '/api/orders/' + order['id'], bearer),
                     lambda value: value['status'] == 'REJECTED', 'rejected reservation')
    api('fulfillment', '/api/fulfillments/orders/' + order['id'], bearer, expected=(404,))
    return {'status': 'PASS', 'orderStatus': value['status'], 'fulfillmentAbsent': True}


def check(args, runtime, evidence):
    bearer = token(runtime, 'viewer')
    order = api('order', '/api/orders/' + evidence['orderId'], bearer)
    fulfillment = api('fulfillment', '/api/fulfillments/' + evidence['fulfillmentId'], bearer)
    stock = api('inventory', '/api/stock-items/' + evidence['sku'], bearer)
    require(order['status'] == 'CONFIRMED' and fulfillment['status'] == 'SHIPPED', 'Persisted workflow changed')
    require(fulfillment['shipmentReference'] == evidence['shipmentReference'], 'Shipment reference must survive reload')
    require(stock['onHand'] == 12 and stock['reserved'] >= 3 and stock['available'] == 12 - stock['reserved'], 'Persisted stock mismatch')
    api('inventory', '/api/stock-items/' + evidence['sku'], bearer, 'PUT', {'onHand': 99}, (403,))
    api('order', '/api/orders', None, 'POST', {'lines': [{'sku': evidence['sku'], 'quantity': 1}]}, (401,))
    # IDs are UUID-shaped before constructing synthetic-data-only SQL.
    import uuid
    order_id = str(uuid.UUID(evidence['orderId']))
    fulfillment_id = str(uuid.UUID(evidence['fulfillmentId']))
    require(sql(args, 'order', f"SELECT status FROM orders WHERE id='{order_id}';") == 'CONFIRMED', 'Order SQL persistence mismatch')
    require(sql(args, 'fulfillment', f"SELECT status FROM fulfillments WHERE id='{fulfillment_id}' AND order_id='{order_id}';") == 'SHIPPED', 'Fulfillment SQL persistence mismatch')
    require(sql(args, 'inventory', f"SELECT status FROM inventory_reservations WHERE order_id='{order_id}';") == 'RESERVED', 'Inventory SQL reservation mismatch')
    published = {}
    for service, expected in [('order', 2), ('inventory', 1), ('fulfillment', 1)]:
        published[service] = int(wait_for(
            lambda service=service: sql(args, service, f"SELECT count(*) FROM outbox_events WHERE correlation_id='{order_id}' AND published_at IS NOT NULL;"),
            lambda count: count == str(expected), service + ' acknowledged outbox', timeout=30))
    if 'rejectedOrderId' in evidence:
        rejected_id = str(uuid.UUID(evidence['rejectedOrderId']))
        require(sql(args, 'inventory', f"SELECT status||':'||reason_code FROM inventory_reservations WHERE order_id='{rejected_id}';") == 'REJECTED:INSUFFICIENT_STOCK', 'Rejected reservation SQL mismatch')
        require(sql(args, 'fulfillment', f"SELECT count(*) FROM fulfillments WHERE order_id='{rejected_id}';") == '0', 'Rejected order must not create fulfillment')
    return {'status': 'PASS', 'viewerMutationStatus': 403, 'anonymousMutationStatus': 401, 'publishedOutboxes': published,
            'sql': {'order': 'CONFIRMED', 'fulfillment': 'SHIPPED', 'reservation': 'RESERVED'}, 'stock': stock}


def infrastructure(args):
    pods = json.loads(kubectl(args, ['get', 'pods', '-o', 'json']))['items']
    require(len(pods) >= 10, 'Expected all dedicated local workloads')
    require(all(any(c['type'] == 'Ready' and c['status'] == 'True' for c in p['status'].get('conditions', [])) for p in pods), 'All deployment pods must be Ready')
    claims = json.loads(kubectl(args, ['get', 'pvc', '-o', 'json']))['items']
    require(len(claims) == 5 and all(p['status']['phase'] == 'Bound' for p in claims), 'Five durable claims must be Bound')
    for service in (*SERVICES, 'frontend', 'kafka', 'keycloak', 'order-postgres', 'inventory-postgres', 'fulfillment-postgres', 'keycloak-postgres'):
        require(bool(kubectl(args, ['exec', 'statefulset/order-postgres', '--', 'getent', 'hosts', service])), 'Internal Kubernetes DNS failed')
        endpoints = json.loads(kubectl(args, ['get', 'endpointslices', '-l', 'kubernetes.io/service-name=' + service, '-o', 'json']))['items']
        require(any(e.get('conditions', {}).get('ready') is True for item in endpoints for e in item.get('endpoints', [])), 'Service lacks ready endpoints')
    migrations = {}
    for service, count in [('order', 4), ('inventory', 4), ('fulfillment', 2)]:
        migrations[service] = int(sql(args, service, 'SELECT count(*) FROM flyway_schema_history WHERE success;'))
        require(migrations[service] == count and sql(args, service, 'SELECT count(*) FROM flyway_schema_history WHERE NOT success;') == '0', 'Flyway migration mismatch')
        require(call('GET', ORIGIN + '/api/' + service + '/actuator/health')['status'] == 'UP', 'Service health is not UP')
    discovery = call('GET', ISSUER + '/.well-known/openid-configuration')
    require(discovery['issuer'] == ISSUER and discovery['jwks_uri'] == ISSUER + '/protocol/openid-connect/certs', 'Public OIDC issuer/JWKS mismatch')
    return {'status': 'PASS', 'readyPods': len(pods), 'boundPvcs': len(claims), 'migrations': migrations, 'internalDns': 'PASS', 'serviceEndpoints': 'PASS', 'issuer': discovery['issuer']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup', 'infrastructure', 'workflow', 'rejected', 'check', 'pending-start', 'pending-finish'])
    parser.add_argument('--runtime-dir', required=True)
    parser.add_argument('--bootstrap-file')
    data_home = Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local/share'))
    state_home = Path(os.environ.get('XDG_STATE_HOME', Path.home() / '.local/state'))
    parser.add_argument('--kubectl', default=str(data_home / 'warexis/kubernetes/bin/kubectl'))
    parser.add_argument('--kubeconfig', default=str(state_home / 'warexis-supervisor/phase11/kind/kubeconfig'))
    args = parser.parse_args()
    try:
        directory = runtime_directory(args.runtime_dir)
        private_file(Path(args.kubeconfig))
        # The lifecycle owner must match the exact Docker node identity and dedicated context.
        guard_env = dict(os.environ, WAREXIS_K8S_STATE_DIR=str(Path(args.kubeconfig).parent),
                         WAREXIS_K8S_TOOLS_DIR=str(Path(args.kubectl).parent))
        guard = subprocess.run(['bash', str(Path(__file__).with_name('cluster.sh')), 'status'],
                               env=guard_env, capture_output=True, timeout=90, check=False)
        require(guard.returncode == 0, 'Dedicated cluster ownership/status guard failed')
        if args.action == 'setup':
            require(bool(args.bootstrap_file), 'setup requires --bootstrap-file')
            result = setup(args, directory)
        elif args.action == 'infrastructure':
            result = infrastructure(args)
        else:
            runtime = json.loads(private_file(directory / 'runtime.json').read_text())
            require(runtime['status'] == 'READY' and runtime['issuer'] == ISSUER, 'Expected READY dedicated runtime')
            evidence_path = directory / 'workflow.json'
            evidence = json.loads(private_file(evidence_path).read_text()) if evidence_path.exists() else {}
            if args.action == 'workflow':
                require(not evidence, 'Workflow already exists; use check without duplicating workload')
                result = workflow(runtime, evidence, evidence_path)
            elif args.action == 'rejected':
                require('rejectedOrderId' not in evidence, 'Rejected workflow already exists')
                result = rejected(runtime, evidence, evidence_path)
            elif args.action == 'check':
                result = check(args, runtime, evidence)
            elif args.action == 'pending-start':
                require('pendingOrderId' not in evidence, 'Pending workflow already exists')
                bearer = token(runtime, 'operator')
                order = api('order', '/api/orders', bearer, 'POST', {'lines': [{'sku': evidence['sku'], 'quantity': 2}]}, (201,))
                evidence['pendingOrderId'] = order['id']
                save(evidence_path, evidence)
                require(order['status'] == 'PENDING_INVENTORY', 'Stop Inventory before pending-start')
                result = {'status': 'PASS', 'orderStatus': order['status']}
            else:
                result = finish_order(runtime, evidence, token(runtime, 'operator'), evidence['pendingOrderId'])
            save(evidence_path, evidence)
        save(directory / (args.action + '-result.json'), result)
        print(json.dumps({'action': args.action, **result}))
    except (ValueError, KeyError, OSError, subprocess.TimeoutExpired) as error:
        # Never render exception details which could contain input credentials or token responses.
        print('Acceptance failed: ' + (str(error) if isinstance(error, ValueError) else type(error).__name__), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
