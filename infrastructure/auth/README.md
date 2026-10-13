# Local Keycloak identity

This is a loopback development setup. Keycloak 26.8.0 runs `start-dev --import-realm`
with a separate PostgreSQL 18.6 database and persistent volume. Its management port
9000 and identity database port are not published. This setup does not provide
production TLS, production identity operations or browser login.

Use the [local infrastructure instructions](../local/README.md) with an ignored,
trusted `.env`. Replace the public example passwords and bootstrap service-account
secret; do not commit actual credentials. `KEYCLOAK_HTTP_PORT` defaults to 18080.
If it changes, update `WAREXIS_OAUTH2_ISSUER_URI` to the matching full URL.

```sh
docker compose --env-file .env up -d --wait keycloak
```

The imported realm is `warexis`. Discovery is at
`http://127.0.0.1:18080/realms/warexis/.well-known/openid-configuration` and its
`jwks_uri` supplies signing keys. All three resource servers require:

```sh
export WAREXIS_OAUTH2_ISSUER_URI=http://127.0.0.1:18080/realms/warexis
export WAREXIS_OAUTH2_AUDIENCE=warexis-api
```

The realm import defines `WAREXIS_VIEWER`, `WAREXIS_OPERATOR` (a composite including
VIEWER), a bearer-only `warexis-api` audience client, and explicit `roles` and
`warexis-api` client scopes. The latter adds the API audience to access tokens;
the former maps scoped realm roles into `realm_access.roles`. There are no imported
users, confidential test clients or client secrets. Startup import skips an
existing realm; editing the JSON does not silently replace persistent identity data.
Keycloak and identity PostgreSQL restart persistence is covered by local verification.

## Local test credentials

`KC_BOOTSTRAP_ADMIN_CLIENT_ID` / `KC_BOOTSTRAP_ADMIN_CLIENT_SECRET` create a temporary
master-realm administrative service account only when the master realm is first
initialized. Compose maps these from the corresponding `KEYCLOAK_BOOTSTRAP_*`
variables. This uses `client_credentials`, not a password grant. Remove that
temporary bootstrap client through Admin REST after provisioning the local access
you need. Keeping its environment variables does not recreate it on an existing
database. The packaged verifier removes its own temporary bootstrap account after
provisioning clients and destroys only its disposable identity stack afterward.

For a scoped test service account, assign the intended realm role to both its
service-account user and the client's realm-role scope mapping. Attach the `roles`
and `warexis-api` default client scopes, disable browser/password flows, and keep
`fullScopeAllowed=false`. An unscoped account or an account lacking the audience
does not gain Warexis access. See the [Keycloak Admin REST API](https://www.keycloak.org/docs-api/26.8.0/rest-api/index.html).

The following local example creates one temporary OPERATOR test client, acquires a
real signed token in memory, makes a protected read, then deletes the test client.
It prompts for the local bootstrap secret and prints only the HTTP status. It does
not print/store the token or secret. Use synthetic local data; a missing SKU yields
404 after successful authentication.

```sh
export WAREXIS_API_URL=http://127.0.0.1:8082/api/stock-items/LOCAL-SKU
python3 - <<'PY'
import getpass, json, os, uuid
from urllib.request import Request, urlopen
from urllib.parse import urlencode, urlsplit
from urllib.error import HTTPError

issuer = os.environ['WAREXIS_OAUTH2_ISSUER_URI'].rstrip('/')
api_url = os.environ['WAREXIS_API_URL']
for url in (issuer, api_url):
    assert urlsplit(url).scheme == 'http' and urlsplit(url).hostname in ('127.0.0.1', 'localhost')
assert urlsplit(api_url).path.startswith('/api/')
base = issuer.rsplit('/realms/', 1)[0]
def call(method, url, data=None, bearer=None, form=False):
    headers = {}
    if bearer: headers['Authorization'] = 'Bearer ' + bearer
    if data is not None:
        headers['Content-Type'] = 'application/x-www-form-urlencoded' if form else 'application/json'
        data = urlencode(data).encode() if form else json.dumps(data).encode()
    with urlopen(Request(url, data=data, headers=headers, method=method), timeout=10) as r:
        body = r.read()
        return json.loads(body) if body else None
bootstrap = os.environ.get('KEYCLOAK_BOOTSTRAP_ADMIN_CLIENT_ID', 'warexis-bootstrap-admin')
admin = call('POST', base + '/realms/master/protocol/openid-connect/token',
             {'grant_type': 'client_credentials', 'client_id': bootstrap,
              'client_secret': getpass.getpass('Local bootstrap secret: ')}, form=True)['access_token']
client_name, client_secret = 'local-test-' + str(uuid.uuid4()), str(uuid.uuid4())
admin_url = base + '/admin/realms/warexis'
client_id = None
try:
    call('POST', admin_url + '/clients', {'clientId': client_name, 'secret': client_secret,
         'protocol': 'openid-connect', 'publicClient': False, 'serviceAccountsEnabled': True,
         'standardFlowEnabled': False, 'implicitFlowEnabled': False, 'directAccessGrantsEnabled': False,
         'fullScopeAllowed': False, 'defaultClientScopes': ['roles', 'warexis-api'], 'optionalClientScopes': []}, admin)
    client_id = call('GET', admin_url + '/clients?clientId=' + client_name, bearer=admin)[0]['id']
    account = call('GET', admin_url + '/clients/' + client_id + '/service-account-user', bearer=admin)
    role = call('GET', admin_url + '/roles/WAREXIS_OPERATOR', bearer=admin)
    call('POST', admin_url + '/users/' + account['id'] + '/role-mappings/realm', [role], admin)
    call('POST', admin_url + '/clients/' + client_id + '/scope-mappings/realm', [role], admin)
    token = call('POST', issuer + '/protocol/openid-connect/token',
                 {'grant_type': 'client_credentials', 'client_id': client_name,
                  'client_secret': client_secret}, form=True)['access_token']
    try:
        with urlopen(Request(api_url, headers={'Authorization': 'Bearer ' + token}), timeout=10) as r:
            print('API status:', r.status)
    except HTTPError as r:
        print('API status:', r.code)
finally:
    if client_id: call('DELETE', admin_url + '/clients/' + client_id, bearer=admin)
PY
```

Normal service tests use ephemeral test-only RSA credentials and need no Keycloak.
`scripts/verify-security.sh` runs the real-Keycloak packaged security cases;
`scripts/verify-workflow.sh` runs those cases plus the preserved Kafka/PostgreSQL
workflow. Their test clients use runtime-generated secrets and do not use password
grants. The services remain stateless Bearer resource servers: OIDC discovery is
for issuer/key configuration, not implemented backend user sessions. Future browser
access will use Authorization Code with PKCE; no frontend client/login is provided here.
