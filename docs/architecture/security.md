# HTTP API security

Order, Inventory and Fulfillment are stateless Spring Security OAuth2 Resource
Servers. Local Keycloak 26.8.0 issues signed JWT access tokens. OIDC discovery
provides issuer metadata and the JWKS signing-key location; the backend does not
implement user login or maintain authenticated browser sessions.

Every service requires external `WAREXIS_OAUTH2_ISSUER_URI` and
`WAREXIS_OAUTH2_AUDIENCE`. Missing or blank settings fail startup. The local realm
is `warexis`, its issuer defaults in the example configuration to
`http://127.0.0.1:18080/realms/warexis`, and the API audience is `warexis-api`.
The framework verifies signatures, issuer, audience, expiration and not-before;
an additional framework validator requires an expiration claim. Invalid or absent
access credentials do not reach business handlers. Normal timestamp validation
retains the framework's 60-second clock skew.

## Authorization matrix

Only the exact `WAREXIS_VIEWER`, `WAREXIS_OPERATOR` and `WAREXIS_METRICS` strings in
`realm_access.roles` become Warexis authorities. OAuth scopes, client roles,
lookalike names and malformed role claims grant no Warexis authority. OPERATOR
is a Keycloak composite including VIEWER; services also explicitly allow either
exact role for reads. The current resources are shared warehouse resources;
per-user ownership filtering is not implemented.

| Service | Method | Route | Access |
| --- | --- | --- | --- |
| All three | GET | `/actuator/health` | Anonymous, without components or details |
| All three | GET | `/actuator/prometheus` | `WAREXIS_METRICS` only; viewer/operator alone denied |
| Order | POST | `/api/orders` | OPERATOR |
| Order | GET | `/api/orders/{id}` | VIEWER or OPERATOR |
| Inventory | POST | `/api/stock-items` | OPERATOR |
| Inventory | GET | `/api/stock-items/{sku}` | VIEWER or OPERATOR |
| Inventory | PUT | `/api/stock-items/{sku}` | OPERATOR |
| Fulfillment | GET | `/api/fulfillments/{id}` | VIEWER or OPERATOR |
| Fulfillment | GET | `/api/fulfillments/orders/{orderId}` | VIEWER or OPERATOR |
| Fulfillment | POST | `/api/fulfillments/{id}/ship` | OPERATOR |

All other external routes and methods deny access, including additional Actuator
paths, direct `/error`, `/login`, `/logout`, HEAD and OPTIONS. Internal error
dispatch can render an already decided error without making `/error` a public API.

Missing/invalid Bearer authentication returns **401**. Authenticated callers
without the required role return **403**. Ambiguous duplicate Authorization
headers retain standard `invalid_request` **400** semantics. Security errors use
generic `application/problem+json` and Bearer `WWW-Authenticate` challenges;
responses do not disclose tokens or validation exceptions. Authorized requests
retain each service's domain validation and 400/404/409 behavior.

## Stateless boundary

Requests supply credentials explicitly in `Authorization: Bearer <access-token>`.
HTTP Basic, form login, logout handlers, HTTP sessions and request caching are
disabled. The backend does not issue authentication cookies. CSRF protection is
disabled for this explicit-header Bearer model because browsers do not
automatically attach its credentials as cookies. This rationale does not apply
to a future cookie-authenticated design. The local browser client uses Vite's
same-origin proxy for business API requests; the services do not enable browser
CORS. See the [browser networking contract](../frontend/api-contracts.md#browser-networking).

Security applies at the HTTP boundary. Kafka carries domain facts, never user
JWTs, authorization headers or access-token fields. Listeners still use their
existing validation, local transactions, idempotency, bounded durable retries
and DLT recovery; HTTP roles do not authenticate Kafka consumers. Local Kafka
remains plaintext development infrastructure.

## Local identity and tokens

Follow the [local Keycloak guide](../../infrastructure/auth/README.md) for the
separate persistent identity database, reproducible realm import, safe bootstrap
service-account handling and scoped local test credentials. The local verification
uses `client_credentials`, with runtime-generated secrets and temporary clients;
it never uses password grants. The documented Python example keeps its access
token in memory and prints only an API status. It does not populate a shell token
variable. Service curl examples assume an authorized short-lived token already
provided by a trusted local OIDC client in `WAREXIS_ACCESS_TOKEN`; do not commit,
save in an environment file, or log that token.

Keycloak runs development HTTP on loopback. The [operations frontend](../../frontend/README.md)
uses the public `warexis-operations` client with Authorization Code and PKCE S256.
Tokens stay in adapter memory and refresh before protected requests; backend
issuer, audience and role checks remain authoritative. Local redirect, logout and
web origins are configured as described in the [identity guide](../../infrastructure/auth/README.md).
Production TLS and production identity operations remain outside this implementation.

## Verification

Each service's full `test` and `verify` lifecycles preserve the business and
reliability suites and add signed test-only RSA/filter-chain cases without
requiring Keycloak. The [packaged verifier](../../tests/workflow/README.md) starts
actual Keycloak, a separate identity PostgreSQL, Kafka and three independent
business PostgreSQL databases, then launches the current executable JARs.

Its security cases verify discovery/JWKS and real signed token issuer,
audience and realm-role claims, missing/invalid/expired credentials, wrong and
missing audience, a different issuer with the intended API audience, VIEWER
reads/denied mutations, OPERATOR reads/mutations, role conversion negatives,
health, additional-route denial, duplicate-header semantics and absence of
authentication cookies or access credentials in stored domain events.
The full run also exercises authenticated business commands, operational metrics,
trace causality and telemetry privacy. The internal three-service chain and its
DLT, restart, retry, offset and idempotency assertions remain in place.
Local verification does not establish hosted CI success or production readiness.

## Local metrics collector

The metrics role grants only the Prometheus scrape route. It grants no business
read or mutation permissions and is not assigned to the browser client. The same
signature, issuer, audience and expiration checks apply to collector access tokens.
All other Actuator routes remain denied; health continues to disclose only status.

For an existing development realm, `infrastructure/auth/configure-metrics-client.py`
creates a new `warexis-metrics-collector` service account with only the metrics role
in its explicit role scope. It prompts for the existing local administrative client
secret and writes a new collector secret exclusively to a private external file.
It refuses to overwrite either an existing collector or a secret file. This is a
local HTTP development tool, not a production credential distribution mechanism.
Prometheus refreshes signed access tokens with OAuth2 client credentials; tokens and
client secrets must never be copied into Git or browser configuration.
