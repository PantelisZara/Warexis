# Local development infrastructure

Run from the repository root with Docker Engine and Docker Compose available:

```sh
docker compose --env-file .env config --quiet
docker compose --env-file .env up -d
docker compose --env-file .env ps
```

Create an ignored `.env` from `.env.example` if it does not already exist, and
replace the example database passwords and Keycloak bootstrap secret before
starting. Keep an existing environment file and initialized volume credentials.
The example file contains public development values. These commands do not create
a `.env` file. To use other credentials or ports, export the corresponding
environment variables before running the commands; shell values override the
example file. Never commit real credentials or a `.env` file.

| Service | Image | Host endpoint | Container endpoint | Database / user |
| --- | --- | --- | --- | --- |
| Order PostgreSQL | `postgres:18.6` | `127.0.0.1:15432` | `order-postgres:5432` | `warexis_order` |
| Inventory PostgreSQL | `postgres:18.6` | `127.0.0.1:15433` | `inventory-postgres:5432` | `warexis_inventory` |
| Fulfillment PostgreSQL | `postgres:18.6` | `127.0.0.1:15434` | `fulfillment-postgres:5432` | `warexis_fulfillment` |
| Identity PostgreSQL | `postgres:18.6` | Not published | `keycloak-postgres:5432` | `warexis_identity` |
| Keycloak | `quay.io/keycloak/keycloak:26.8.0` | `127.0.0.1:18080` | `keycloak:8080` | Realm `warexis` |
| Kafka | `apache/kafka:4.2.2` | `127.0.0.1:19092` | `kafka:29092` | N/A |

Each PostgreSQL service owns an independent database cluster and data volume.
The local bootstrap users are administrators of their respective development
clusters. Business schemas are managed by service-owned Flyway migrations. Separate least-privilege application roles remain future work.

Kafka runs one combined KRaft broker/controller without ZooKeeper. Host clients
use the loopback endpoint; containers on the project network use `kafka:29092`.
The controller listener at `kafka:29093` is not published to the host. Kafka uses
plaintext transport and single-replica internal topics for local development.
Changing `KAFKA_HOST_PORT` also updates the advertised host endpoint.

All six infrastructure services use the `warexis-local_local` bridge network and restart unless
explicitly stopped. Keycloak waits for its separate identity PostgreSQL to be healthy;
the business databases and Kafka start independently. Keycloak readiness is checked
on its unpublished management port 9000. See [local identity setup and token acquisition](../auth/README.md).
PostgreSQL health checks use `pg_isready` over TCP; Kafka's health check lists
topics through the internal broker listener. Application containers should
wait for their required infrastructure services to be healthy.

Persistent named volumes under the default `warexis-local` project name:

- `warexis-local_order-postgres-data`, mounted at `/var/lib/postgresql`
- `warexis-local_inventory-postgres-data`, mounted at `/var/lib/postgresql`
- `warexis-local_fulfillment-postgres-data`, mounted at `/var/lib/postgresql`
- `warexis-local_kafka-data`, mounted at `/var/lib/kafka/data`
- `warexis-local_keycloak-postgres-data`, mounted at `/var/lib/postgresql`

PostgreSQL 18 stores data under `/var/lib/postgresql/18/docker`. Kafka stores its
data and KRaft metadata in its data volume; unused image configuration/secret
mounts use temporary memory filesystems. Changing the Compose project name also
changes its resource names. PostgreSQL initialization credentials/database names
only apply when its volume is first initialized. Keep `KAFKA_CLUSTER_ID` stable
when reusing the Kafka volume.

Check authenticated database connections and broker connectivity:

```sh
docker compose --env-file .env exec -T order-postgres sh -ec \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -w -c "SELECT current_database(), current_user;"'
docker compose --env-file .env exec -T inventory-postgres sh -ec \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -w -c "SELECT current_database(), current_user;"'
docker compose --env-file .env exec -T kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:29092
```

Stop the local containers without removing them or their data:

```sh
docker compose --env-file .env stop
```

## Start the Spring Boot services from the host

Use Java 21 and a checkout containing `services/order-service` and
`services/inventory-service` and `services/fulfillment-service`. Start the infrastructure first and confirm all
six containers are healthy. Then run each block below in a separate terminal,
starting from the repository root.

`docker compose --env-file .env` supplies values to Compose only; it does
**not** export application environment variables to host Java processes. Each
recipe explicitly loads the trusted local values into a subshell and
passes the required application variables to the Maven wrapper. In particular,
`ORDER_POSTGRES_PASSWORD` becomes `ORDER_DB_PASSWORD`, and
`INVENTORY_POSTGRES_PASSWORD` becomes `INVENTORY_DB_PASSWORD`, and
`FULFILLMENT_POSTGRES_PASSWORD` becomes `FULFILLMENT_DB_PASSWORD`.

If the databases were initialized with different passwords, source the same
trusted local infrastructure environment file in each recipe. The passwords must match the existing databases. The values in
`.env.example` are local examples only; never commit real credentials.

The recipes bind the secured HTTP APIs to loopback for local development. Every
service requires the same issuer and API audience; only exact health reads are
anonymous. Authenticate business calls with the roles described in the
[security contract](../../docs/architecture/security.md).

Start Order Service on HTTP port 8081:

```sh
(
  . ./.env
  cd services/order-service || exit
  WAREXIS_OAUTH2_ISSUER_URI="${WAREXIS_OAUTH2_ISSUER_URI:?Set the Keycloak issuer}" \
  WAREXIS_OAUTH2_AUDIENCE="${WAREXIS_OAUTH2_AUDIENCE:?Set the API audience}" \
  WAREXIS_MESSAGING_ENABLED=true \
  KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${KAFKA_HOST_PORT}" \
  SERVER_ADDRESS=127.0.0.1 \
  SERVER_PORT=8081 \
  ORDER_DB_URL="jdbc:postgresql://127.0.0.1:${ORDER_POSTGRES_PORT}/${ORDER_POSTGRES_DB}" \
  ORDER_DB_USERNAME="${ORDER_POSTGRES_USER}" \
  ORDER_DB_PASSWORD="${ORDER_POSTGRES_PASSWORD:?Set ORDER_POSTGRES_PASSWORD}" \
  ./mvnw spring-boot:run
)
```

Start Inventory Service on HTTP port 8082:

```sh
(
  . ./.env
  cd services/inventory-service || exit
  WAREXIS_OAUTH2_ISSUER_URI="${WAREXIS_OAUTH2_ISSUER_URI:?Set the Keycloak issuer}" \
  WAREXIS_OAUTH2_AUDIENCE="${WAREXIS_OAUTH2_AUDIENCE:?Set the API audience}" \
  WAREXIS_MESSAGING_ENABLED=true \
  KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${KAFKA_HOST_PORT}" \
  SERVER_ADDRESS=127.0.0.1 \
  SERVER_PORT=8082 \
  INVENTORY_DB_URL="jdbc:postgresql://127.0.0.1:${INVENTORY_POSTGRES_PORT}/${INVENTORY_POSTGRES_DB}" \
  INVENTORY_DB_USERNAME="${INVENTORY_POSTGRES_USER}" \
  INVENTORY_DB_PASSWORD="${INVENTORY_POSTGRES_PASSWORD:?Set INVENTORY_POSTGRES_PASSWORD}" \
  ./mvnw spring-boot:run
)
```

Start Fulfillment Service on HTTP port 8083:

```sh
(
  . ./.env
  cd services/fulfillment-service || exit
  WAREXIS_OAUTH2_ISSUER_URI="${WAREXIS_OAUTH2_ISSUER_URI:?Set the Keycloak issuer}" \
  WAREXIS_OAUTH2_AUDIENCE="${WAREXIS_OAUTH2_AUDIENCE:?Set the API audience}" \
  WAREXIS_MESSAGING_ENABLED=true \
  KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:${KAFKA_HOST_PORT}" \
  SERVER_ADDRESS=127.0.0.1 \
  SERVER_PORT=8083 \
  FULFILLMENT_DB_URL="jdbc:postgresql://127.0.0.1:${FULFILLMENT_POSTGRES_PORT}/${FULFILLMENT_POSTGRES_DB}" \
  FULFILLMENT_DB_USERNAME="${FULFILLMENT_POSTGRES_USER}" \
  FULFILLMENT_DB_PASSWORD="${FULFILLMENT_POSTGRES_PASSWORD:?Set FULFILLMENT_POSTGRES_PASSWORD}" \
  ./mvnw spring-boot:run
)
```

Deploy Inventory's OrderConfirmed compatibility before enabling the new Order producer. Fulfillment is a separate Order-stream consumer and can start first. The packaged verification suite exercises this three-service workflow using separate disposable infrastructure.

After all three applications have started, their local health endpoints are:

- Order: <http://127.0.0.1:8081/actuator/health>
- Inventory: <http://127.0.0.1:8082/actuator/health>
- Fulfillment: <http://127.0.0.1:8083/actuator/health>

These recipes enable the Kafka workflow explicitly. Set
`WAREXIS_MESSAGING_ENABLED=false` to run independent REST APIs without messaging.
Kafka remains available to host clients at `127.0.0.1:19092` with the example port.
Creating an order returns `PENDING_INVENTORY`; poll its read endpoint for eventual
`CONFIRMED` or `REJECTED`. Use the stock API to seed synthetic local stock first.
Poll `GET /api/fulfillments/orders/{orderId}` for the resulting `PENDING` plan.
Use the [Fulfillment API](../../services/fulfillment-service/README.md) to record a
manual shipment; Order stays `CONFIRMED` and Inventory stock is unchanged.
See [workflow startup and explicit failure recovery](../../docs/architecture/kafka-workflow.md)
and [disposable packaged-service verification](../../tests/workflow/README.md).
