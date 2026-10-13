# Local Keycloak identity

This is a loopback development setup. Keycloak 26.8.0 runs `start-dev --import-realm`
with a separate PostgreSQL 18.6 database and persistent volume. Its management port
9000 and identity database port are not published. This setup does not provide
production TLS or production identity operations. The local browser client below
uses Authorization Code with PKCE for the operations frontend.

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
`warexis-api` client scopes, plus a dedicated public `warexis-operations` browser
client. The `warexis-api` scope adds the API audience to access tokens; `roles` maps
scoped realm roles into `realm_access.roles`. There are no imported
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
for issuer/key configuration, not implemented backend user sessions. The browser
client uses Authorization Code with PKCE; its real-browser verification is a
separate gate.

## Operations browser client

`warexis-operations` is a public OpenID Connect client with standard Code flow and
server-enforced PKCE `S256`. Implicit flow, direct password grants, service accounts
and full role scope are disabled. It has no client secret. Its default client scopes
are exactly `roles` and `warexis-api`, with explicit `WAREXIS_VIEWER` and
`WAREXIS_OPERATOR` realm-role scope mappings. Scope mappings permit claims only for
roles the user actually owns; they do not grant either role to a user. OPERATOR
continues to include VIEWER through the existing realm composite.

The only allowed web origin is `http://127.0.0.1:5173`. The login and post-logout
redirect URI is exactly `http://127.0.0.1:5173/`, with no wildcard. Login initiated on
a nested frontend route must still pass that exact root redirect to Keycloak.
Use the root redirect explicitly for logout too. The adapter must use Code flow,
`pkceMethod: 'S256'`, and in-memory tokens; it must not persist access/refresh tokens
in local storage, session storage, IndexedDB or application cookies. The frontend
refreshes before protected API calls and uses its development proxy to reach the
three APIs. This addition does not enable backend CORS or change API authorization.

Open both the UI and identity server using `127.0.0.1`, not `localhost`. Their
origins and the issuer are exact strings: `http://localhost:18080/realms/warexis`
does not match the configured `http://127.0.0.1:18080/realms/warexis` issuer.
If using a non-default local Keycloak port, set the frontend's public Keycloak URL
and all three service issuer values consistently; the UI origin and redirects stay
at `http://127.0.0.1:5173` unless intentionally reconfigured and reviewed.

### New or existing local identity data

A new database imports the browser client and its scope mapping from
`warexis-realm.json`. An existing `warexis` realm is skipped by startup import.
Do not reset the realm, delete its database or remove volumes to apply this change.
Use the narrow local administrative procedure below instead. It only creates or
updates `warexis-operations` and its own scope attachments; it does not alter shared
roles/mappers, the `warexis-api` client, users or other clients.

```sh
python3 infrastructure/auth/configure-browser-client.py check
python3 -m unittest discover -s infrastructure/auth -p 'test_*.py'
python3 infrastructure/auth/configure-browser-client.py sync-client
```

The sync command prompts for the local administrative service-account secret; no
secret or token is printed or written. It uses the temporary bootstrap client by
default. If it was already removed, provide an existing authorized local master
realm administrative service-account client ID with `--admin-client`; its secret is
still prompted. It must have permission to read the realm, roles/scopes and manage
this client. The script rejects remote URLs, refuses to repurpose a confidential or
service-account client, and stops on unexpected existing realm/client-role scope
mappings for manual review. A rerun is safe: the same exact client settings/scopes
are reapplied and checked through Admin REST. No realm reset is performed.

`--url http://127.0.0.1:<port>` selects a non-default local identity port. An existing
client's extra default/optional scope attachments are removed only from that browser
client to restore its explicit scope boundary. Unexpected role scope mappings must
be investigated before continuing; the script does not silently delete them.

### Local browser users

Do not add users or passwords to the realm import. To create a new local user,
choose the least privilege role needed and enter a password at the prompts:

```sh
python3 infrastructure/auth/configure-browser-client.py create-user \
  --username local-viewer --role WAREXIS_VIEWER
python3 infrastructure/auth/configure-browser-client.py create-user \
  --username local-operator --role WAREXIS_OPERATOR
```

Passwords stay in process memory and are sent only to the loopback Admin REST
endpoint. Existing users are never overwritten. The command creates the requested
user with one realm role; additional profile requirements may be completed through
the Keycloak login screen. Review the new user in the local Admin Console if an
administrative request fails after creation. Use the console to manage existing
users or an authorized administrative service-account client, and remove temporary
bootstrap credentials when provisioning is finished. Never use real user credentials
in disposable browser verification or commit local passwords/secrets.

The configuration guard and Admin REST checks establish the imported/provisioned
identity settings. Successful browser login, callback, refresh, role-specific API
access and logout still require the real-browser end-to-end gate; they cannot be
claimed from these configuration checks alone. See the official
[JavaScript adapter guidance](https://www.keycloak.org/securing-apps/javascript-adapter)
and [Admin REST API](https://www.keycloak.org/docs-api/26.8.0/rest-api/index.html).
