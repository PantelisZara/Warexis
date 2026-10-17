# Local service metrics

This optional Linux-only stack scrapes the three host service JVMs over loopback.
It uses Prometheus `v3.15.0` and Grafana OSS `13.2.2`, independently of the business
Compose project. It neither starts nor stops business databases, Kafka or Keycloak.
Host networking allows access to services bound to `127.0.0.1`; both monitoring
HTTP listeners also bind to `127.0.0.1`. This is a local development arrangement,
not a production transport or multi-user isolation policy. Local processes can
read Prometheus and its ingested metrics without authentication.

## Prepare and run

Use a disposable test realm or a deliberately configured local realm. Provision a
confidential `warexis-metrics-collector` client with client credentials enabled,
only the `WAREXIS_METRICS` role, and the service's accepted `warexis-api` audience.
Viewer/operator credentials cannot scrape metrics; the collector cannot access
business APIs. Keep its client secret in an owned regular mode0600 file outside
Git. Never pass the secret value as a command argument or add it to `.env`.

Create a fresh runtime directory through the generator, beneath an existing
private parent outside Git. Replace the example service/issuer ports with the
actual local or disposable-fixture ports. The following commands assume the
runtime parent and collector secret file already exist:

```sh
python3 scripts/prepare-monitoring.py \
  --runtime-dir "$HOME/.local/state/warexis-monitoring/example" \
  --order-url http://127.0.0.1:18081 \
  --inventory-url http://127.0.0.1:18082 \
  --fulfillment-url http://127.0.0.1:18083 \
  --issuer http://127.0.0.1:18080/realms/warexis \
  --collector-secret-file "$HOME/.local/state/warexis-monitoring/collector-secret" \
  --prometheus-port 9090 --grafana-port 3000

python3 scripts/validate-monitoring.py --promtool \
  --runtime-dir "$HOME/.local/state/warexis-monitoring/example"

docker compose --env-file "$HOME/.local/state/warexis-monitoring/example/monitoring.env" \
  -f compose.monitoring.yaml -p warexis-monitoring-example up -d
```

Use the same `--env-file`, compose file and explicit project name for later
`ps`, `logs`, `restart` or `down`. `down` stops only that monitoring project;
private bind-mounted data remains. Do not use global Docker cleanup commands.
The generator refuses existing runtime directories; restarting uses their saved
`monitoring.env`. Its paths and ports are public configuration, not credentials.

Grafana is at `http://127.0.0.1:3000`, user `warexis-admin`. The generated random
admin password is in the private runtime's `grafana-admin-password` file. Enter
it locally without copying it into logs, screenshots, source or shell arguments.
It initializes a new Grafana database; changing the file does not rotate an
already initialized account. Provisioned dashboards live in the Warexis folder:
**Warexis overview**, **Warexis service resources**, and **Warexis Kafka and
outbox**. The read-only datasource UID is `warexis-prometheus`. Database settings
persist in the private `grafana-data` directory; dashboard definitions come from
Git on every start. No anonymous access or signup is enabled. Automatic plugin installation and updates
are disabled: the pinned image supplies its bundled Prometheus plugin, and a
read-only root filesystem must not attempt to replace that running plugin.

Prometheus is at `http://127.0.0.1:9090`. Check all three `warexis-*` targets under
Targets. It obtains and refreshes collector access tokens through the realm's
OAuth2 token endpoint. Neither token values nor the client secret are present in
tracked configuration. Scrapes honor the fixed service labels emitted by these
trusted loopback targets, avoiding duplicate `exported_service` labels. A realm/client/audience problem makes scrapes fail; it
must not be hidden as an empty backlog or healthy target.

## Collection and dashboard meaning

Scrapes run every 15 seconds with a 10 second timeout. Prometheus retains at most
24 hours or 512MB of TSDB blocks, whichever limit becomes effective first; WAL/head
storage adds overhead, so this is not a strict filesystem quota. Both databases
persist in private runtime directories using the invoking user's UID/GID. No
remote write, telemetry exporter, collector, cloud account or tracing backend is
configured in this metrics batch.

The overview excludes `/actuator.*` HTTP requests so scrapes and health probes do
not count as business traffic. Throughput and 5xx panels use requests/second;
p95 latency uses seconds and the framework histogram. Service panels use JVM,
process and Hikari metrics emitted by Spring Boot/Micrometer. There are no
hand-maintained substitutes for framework meters.

The messaging dashboard displays the frozen application observations:

| Prometheus metric | Meaning |
| --- | --- |
| `warexis_outbox_pending` | Bounded unpublished row count; 10001 means at least 10001. |
| `warexis_outbox_oldest_age_seconds` | Age of the oldest unpublished event; successful empty snapshot is zero. |
| `warexis_outbox_snapshot_available` | 1 for an available snapshot, 0 for unavailable/stale. |
| `warexis_outbox_publications_total{result}` | Published after broker ACK and durable metadata completion, or actual failed publication attempt. |
| `warexis_outbox_retries_total` | Actual sends with durable prior publication failures. |
| `warexis_messaging_deliveries_total{result}` | Completed processed, duplicate, ignored or DLT observations. |
| `warexis_messaging_failures_total{stage}` | Actual process/DLT failures. |
| `warexis_messaging_retries_total{stage}` | Actual process/DLT retries after prior durable failures. |
| `warexis_messaging_pauses_total` | Listener pause requests only, excluding rebalance restoration; potentially repeated on replay; not current durably paused partitions. |

Counters are process-local observations and reset on service restart. Five-minute
`increase` panels are extrapolated observations, not exact durable business
totals. Low-traffic/new series may lack enough samples; missing data and NaN are
unknown, never silently replaced with zero. A failed scrape is `up=0`; an absent
target is missing. These dashboards do not claim broker consumer lag. Queryable
tracing and durable asynchronous context are implemented separately; see the
[local tracing guide](../tracing/README.md).

Normal success, rejection, duplicates, an outbox backlog/recovery, a safe retry
and DLT exercise should change the corresponding series in the real disposable
stack. Stop monitoring to confirm business work continues; stop a service to
observe its failed scrape. Merely provisioning a dashboard does not prove these
runtime acceptance checks passed.

## Local checks

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/validate-monitoring-test.py
python3 scripts/validate-monitoring.py
python3 scripts/validate-monitoring.py --promtool
```

The tests exercise secret/path permissions and loopback boundaries without
Prometheus or Grafana. If the operating system's temporary directory itself is a
Git checkout, select an external temporary location with `TMPDIR`. Structural
checks require only Python's standard library. `--promtool` uses the pinned
Prometheus Docker image to parse all dashboard expressions; `--runtime-dir` also
checks the actual authenticated scrape configuration and Compose interpolation.
Neither check certifies live scrapes, real workloads or browser rendering. With
the runtime running and scraped at least once, add `--grafana-api-check` alongside
`--runtime-dir` to verify all three provisioned dashboards, anonymous API401, and
an actual Grafana-to-Prometheus `up` query. This catches a missing/stopped bundled
datasource plugin; it does not replace the official Browser acceptance gate or
prove the three business targets are healthy.
