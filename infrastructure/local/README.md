# Local development infrastructure

Run from the repository root with Docker Engine and Docker Compose available:

```sh
docker compose --env-file .env.example config
docker compose --env-file .env.example up -d
docker compose --env-file .env.example ps
```

The example file contains public development values. These commands do not create
a `.env` file. To use other credentials or ports, export the corresponding
environment variables before running the commands; shell values override the
example file. Never commit real credentials or a `.env` file.

| Service | Image | Host endpoint | Container endpoint | Database / user |
| --- | --- | --- | --- | --- |
| Order PostgreSQL | `postgres:18.6` | `127.0.0.1:15432` | `order-postgres:5432` | `warexis_order` |
| Inventory PostgreSQL | `postgres:18.6` | `127.0.0.1:15433` | `inventory-postgres:5432` | `warexis_inventory` |
| Kafka | `apache/kafka:4.2.2` | `127.0.0.1:19092` | `kafka:29092` | N/A |

Each PostgreSQL service owns an independent database cluster and data volume.
The local bootstrap users are administrators of their respective development
clusters. Application roles and migrations belong to future service work.

Kafka runs one combined KRaft broker/controller without ZooKeeper. Host clients
use the loopback endpoint; containers on the project network use `kafka:29092`.
The controller listener at `kafka:29093` is not published to the host. Kafka uses
plaintext transport and single-replica internal topics for local development.
Changing `KAFKA_HOST_PORT` also updates the advertised host endpoint.

All three services use the `warexis-local_local` bridge network and restart unless
explicitly stopped. They start independently because none depends on another.
PostgreSQL health checks use `pg_isready` over TCP; Kafka's health check lists
topics through the internal broker listener. Future application containers should
wait for their required infrastructure services to be healthy.

Persistent named volumes under the default `warexis-local` project name:

- `warexis-local_order-postgres-data`, mounted at `/var/lib/postgresql`
- `warexis-local_inventory-postgres-data`, mounted at `/var/lib/postgresql`
- `warexis-local_kafka-data`, mounted at `/var/lib/kafka/data`

PostgreSQL 18 stores data under `/var/lib/postgresql/18/docker`. Kafka stores its
data and KRaft metadata in its data volume; unused image configuration/secret
mounts use temporary memory filesystems. Changing the Compose project name also
changes its resource names. PostgreSQL initialization credentials/database names
only apply when its volume is first initialized. Keep `KAFKA_CLUSTER_ID` stable
when reusing the Kafka volume.

Check authenticated database connections and broker connectivity:

```sh
docker compose --env-file .env.example exec -T order-postgres sh -ec \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -w -c "SELECT current_database(), current_user;"'
docker compose --env-file .env.example exec -T inventory-postgres sh -ec \
  'PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -w -c "SELECT current_database(), current_user;"'
docker compose --env-file .env.example exec -T kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:29092
```

Stop the local containers without removing them or their data:

```sh
docker compose --env-file .env.example stop
```

## Start the Spring Boot services from the host

Use Java 21 and a checkout containing `services/order-service` and
`services/inventory-service`. Start the infrastructure first and confirm all
three containers are healthy. Then run each block below in a separate terminal,
starting from the repository root.

`docker compose --env-file .env.example` supplies values to Compose only; it does
**not** export application environment variables to host Java processes. Each
recipe explicitly loads the public local example values into a subshell and
passes the required application variables to the Maven wrapper. In particular,
`ORDER_POSTGRES_PASSWORD` becomes `ORDER_DB_PASSWORD`, and
`INVENTORY_POSTGRES_PASSWORD` becomes `INVENTORY_DB_PASSWORD`.

If the databases were initialized with different passwords, source the same
trusted local infrastructure environment file instead of `.env.example` in each
recipe. The passwords must match the existing databases. The values in
`.env.example` are local examples only; never commit real credentials.

Start Order Service on HTTP port 8081:

```sh
(
  . ./.env.example
  cd services/order-service || exit
  SERVER_PORT=8081 \
  ORDER_DB_URL='jdbc:postgresql://127.0.0.1:15432/warexis_order' \
  ORDER_DB_USERNAME=warexis_order \
  ORDER_DB_PASSWORD="${ORDER_POSTGRES_PASSWORD:?Set ORDER_POSTGRES_PASSWORD}" \
  ./mvnw spring-boot:run
)
```

Start Inventory Service on HTTP port 8082:

```sh
(
  . ./.env.example
  cd services/inventory-service || exit
  SERVER_PORT=8082 \
  INVENTORY_DB_URL='jdbc:postgresql://127.0.0.1:15433/warexis_inventory' \
  INVENTORY_DB_USERNAME=warexis_inventory \
  INVENTORY_DB_PASSWORD="${INVENTORY_POSTGRES_PASSWORD:?Set INVENTORY_POSTGRES_PASSWORD}" \
  ./mvnw spring-boot:run
)
```

After both services have started, their local health endpoints are:

- Order: <http://127.0.0.1:8081/actuator/health>
- Inventory: <http://127.0.0.1:8082/actuator/health>

Kafka remains available to host clients at `127.0.0.1:19092`.
