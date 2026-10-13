# Disposable browser test stack

This test-only Maven module holds a real Kafka, three business PostgreSQL databases, a separate Keycloak PostgreSQL database, Keycloak 26.8.0, and the three packaged Warexis services open for official in-app Browser QA. It does not launch, install, or automate a browser. All published infrastructure and HTTP ports bind to loopback. Existing developer containers are never discovered, stopped, or reused.

The fixture creates ephemeral Viewer and Operator users through Keycloak administration. Login still uses the public `warexis-operations` client and authorization code flow with PKCE S256. There is no SPA password grant, test business service account, token file, or SQL business-data seed. Create stock through the operator UI during the test.

## Launch and stop

Prerequisites: Java 21, Docker, the reviewed Phase 8 realm containing the public operations client, and freshly built executable Order, Inventory and Fulfillment JARs in the chosen integrated source tree. The fixture rejects a missing JAR or a realm without the public PKCE client before starting Docker resources.

From the checkout containing this fixture:

```sh
./services/order-service/mvnw --batch-mode --no-transfer-progress -f tests/browser-fixture/pom.xml test -Dtest=FixtureConfigTests
./scripts/start-browser-test-stack.sh
```

Optional positional arguments are the integrated source root, a private runtime JSON path, and the realm JSON path. The default private runtime file is under the user's local state directory. It must not already exist. Its containing directory must be mode `0700`; every generated credential/log file is mode `0600`. Symlinked paths and all paths within a Git checkout are rejected. Choose a fresh session directory when retaining previous private logs.

The equivalent standalone Maven launch, with explicitly selected paths, is:

```sh
./services/order-service/mvnw --batch-mode --no-transfer-progress -f tests/browser-fixture/pom.xml test -Dtest=BrowserStackSessionTest -Dfixture.start=true -Dfixture.repository="$source_root" -Dfixture.realm="$realm_file" -Dfixture.runtime="$runtime_file"
```

`READY` appears only after discovery issuer validation, user provisioning, and all three real service health responses are `UP`. Console output contains public URLs only. The private JSON contains `issuer`, `services`, `frontendEnvironment`, `users.viewer`, `users.operator`, and `stopFile`. Never print the JSON, passwords, or browser tokens into public logs or test reports.

Start Vite at exactly `http://127.0.0.1:5173`. Its environment must contain the six public values from `frontendEnvironment`: `VITE_KEYCLOAK_URL`, `VITE_KEYCLOAK_REALM`, `VITE_KEYCLOAK_CLIENT_ID`, `WAREXIS_ORDER_PROXY_TARGET`, `WAREXIS_INVENTORY_PROXY_TARGET`, and `WAREXIS_FULFILLMENT_PROXY_TARGET`. The dynamic Keycloak issuer matches all three resource servers; the fixed development port is not used for this disposable session. Public frontend environment values contain no login credentials or tokens.

Stop from another terminal by creating the exact `stopFile` recorded in private JSON, or interrupt the foreground Maven process. Normal shutdown and the JVM shutdown hook stop only owned service JVMs, owned containers and the owned network. They remove the generated runtime credential JSON. Private service logs remain for investigation. The session also has an eight-hour bounded safety deadline. Hard process termination or host failure relies on Testcontainers resource cleanup; do not run global Docker prune or compose down to recover a fixture.

## Required real-browser flow

Use the official in-app Browser plugin. CLI/headless Playwright execution is not authorized by this fixture. Browser QA remains a separate gate and must not be claimed passed from fixture unit tests or health checks.

1. Open the SPA anonymously; verify navigation, public health, explicit login and no business data disclosed.
2. Log in as the ephemeral Operator through the real Keycloak page. Verify the redirect/code exchange and PKCE, with tokens only in application memory.
3. Create a unique SKU with on-hand stock using `POST /api/stock-items`, then read it using `GET /api/stock-items/{sku}`. Update it through `PUT /api/stock-items/{sku}` and verify the returned quantities.
4. Create an order using `POST /api/orders`, preserve its actual returned UUID, and refresh `GET /api/orders/{id}` until the real Kafka workflow returns `CONFIRMED`. Check that reserved and available stock reflect the reservation.
5. Find fulfillment using `GET /api/fulfillments/orders/{orderId}`, then verify the same resource through `GET /api/fulfillments/{id}`. Dispatch using `POST /api/fulfillments/{id}/ship` and a unique shipment reference. Repeating that same reference must remain idempotent; a conflicting reference must return a visible conflict.
6. Log out, log in as Viewer, and read the actual order, stock and fulfillment. Mutation controls must be unavailable. Where a browser-origin direct mutation is checked, the server must return `403` without changing business facts.
7. Check malformed and absent identifiers, validation, stock conflict, business rejection, pending states and retry/manual refresh behavior against actual responses. Assert eventual states with bounded polling rather than fixed sleeps; never invent list APIs or metrics.
8. Verify refresh/session expiry recovery, failed-refresh authentication clearing, `401` behavior, logout and return navigation. Inspect browser storage for absence of tokens. Verify responsive keyboard use and accessible form/error behavior through the same real SPA.

Service paths above are the eight existing backend route shapes. The SPA uses its Vite same-origin service prefixes; no backend CORS change or fake API is required. Record actual observed statuses, UUIDs and UI behavior without including credentials or JWTs in evidence.
