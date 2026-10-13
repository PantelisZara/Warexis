# Operations interface API contracts

The operations interface uses the existing Order, Inventory and Fulfillment REST APIs. It does not provide a store, analytics, customer records or carrier integrations. Each service owns its database; browser requests do not access Kafka or databases directly.

## HTTP endpoints

All business responses are JSON. Read endpoints require `WAREXIS_VIEWER` or `WAREXIS_OPERATOR`; mutations require `WAREXIS_OPERATOR`. Frontend role controls assist users; the backend enforces permission.

| Service | Method and path | Request | Response |
| --- | --- | --- | --- |
| Order | `POST /api/orders` | `{lines:[{sku,quantity}]}` | 201 Order, with Location |
| Order | `GET /api/orders/{id}` | canonical UUID | 200 Order |
| Inventory | `POST /api/stock-items` | `{sku,onHand}` | 201 Stock item, with Location |
| Inventory | `GET /api/stock-items/{sku}` | SKU | 200 Stock item |
| Inventory | `PUT /api/stock-items/{sku}` | `{onHand}` | 200 Stock item |
| Fulfillment | `GET /api/fulfillments/{id}` | lowercase canonical UUID | 200 Fulfillment |
| Fulfillment | `GET /api/fulfillments/orders/{orderId}` | lowercase canonical UUID | 200 Fulfillment |
| Fulfillment | `POST /api/fulfillments/{id}/ship` | `{shipmentReference}` | 200 Fulfillment |
| Each service | `GET /actuator/health` | none; anonymous | health status without details |

Order includes `id`, `status`, `createdAt`, `updatedAt`, and `lines` containing `id`, `sku`, `quantity`. Its status is `PENDING_INVENTORY`, `CONFIRMED`, or `REJECTED`. Creating an order returns its actual initial state; the asynchronous inventory decision must be read from the API afterward.

Stock item includes `id`, `sku`, `onHand`, `reserved`, `available`. Available stock equals on-hand minus reserved stock. There is no public mutation for reservation state.

Fulfillment includes `id`, `orderId`, `reservationId`, `confirmationEventId`, `status`, `createdAt`, `updatedAt`, nullable `shippedAt`, nullable `shipmentReference`, and `lines` containing `orderLineId`, `sku`, `quantity`. Status is `PENDING` or `SHIPPED`. A fulfillment is created asynchronously after order confirmation. A temporary 404 while waiting is not proof of failure. Shipping the same fulfillment with the same reference is idempotent; a different committed reference conflicts.

## Input and error contracts

SKUs and shipment references contain 1–64 ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit: `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`. An order has 1–100 non-null lines and positive integer quantities. Duplicate SKUs are accepted by the existing order contract. Quantities and on-hand stock are Java integers, at most 2,147,483,647. Stock must be nonnegative and cannot be reduced below reserved quantity.

Errors use Problem Details where defined by the service. Invalid bodies or identifiers return 400; unknown resources return 404; stock or shipment conflicts return 409. Missing, invalid or expired Bearer credentials return 401; insufficient permission returns 403. The current APIs provide general validation descriptions, not a stable per-field error map. A network failure during a mutation can leave its outcome unknown; clients must not silently retry it.

## Screen mapping and deferred capabilities

Orders supports creation and lookup by ID, with explicit refresh to read actual workflow state. Inventory supports SKU lookup and protected stock changes. Fulfillment supports lookup by its own ID or order ID and protected manual dispatch. The overview provides operational navigation and service health. There are no collection, pagination, filtering, aggregate statistics, event history or reservation-detail endpoints in this contract. These capabilities are deferred; the UI must not manufacture them.

## Browser networking

Development uses the origin `http://127.0.0.1:5173`. Browser requests use `/api/order`, `/api/inventory`, or `/api/fulfillment` proxy prefixes followed by the real path above. Vite removes the proxy prefix and forwards to the respective host service (8081, 8082, 8083 by default). Backend CORS policies remain unchanged. Keycloak is accessed directly at its browser-visible origin; development defaults to `http://127.0.0.1:18080`, realm `warexis`, audience `warexis-api`. A disposable verification stack may use a different local Keycloak port, consistently shared by all services and the adapter.

Browser identity uses a dedicated public client, `warexis-operations`, with standard Authorization Code Flow and PKCE S256. Tokens stay in adapter memory and are refreshed before authenticated API requests. API audience and realm-role claims must match the backend contract. Only documented local redirect and web origins are configured. This is local development networking, not a production deployment design.
