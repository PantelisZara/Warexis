# Local tracing foundation

This optional Linux stack runs a single Tempo 3.1.0 process, pinned by image digest,
with local WAL and block storage and 24 hour block retention. Retention is not a
strict disk quota: WAL, active blocks and compaction need additional space. Usage
reporting is disabled. The stack has no business Kafka dependency and starts no
application, database, collector or metrics service.

The container runs as the invoking user's UID/GID with a read-only root filesystem,
all capabilities dropped and no-new-privileges. Only query HTTP and OTLP HTTP are
published, both on host `127.0.0.1`. Internal Tempo gRPC is not published; no OTLP
gRPC receiver is configured. Local host processes can ingest and query traces
without authentication. This is a local development arrangement.

## Prepare and run

Choose a fresh directory beneath an existing private parent outside every Git
checkout. The generator refuses symlink paths and existing runtime directories.
It creates mode0700 directories and mode0600 configuration/environment files;
Tempo's storage remains beneath the private runtime. No private host path is
stored in source.

```sh
python3 scripts/prepare-tracing.py \
  --runtime-dir "$HOME/.local/state/warexis-tracing/example" \
  --query-port 3200 --otlp-http-port 4318

docker compose --env-file "$HOME/.local/state/warexis-tracing/example/tracing.env" \
  -f compose.tracing.yaml -p warexis-tracing-example config --quiet

docker compose --env-file "$HOME/.local/state/warexis-tracing/example/tracing.env" \
  -f compose.tracing.yaml -p warexis-tracing-example up -d
```

Check `http://127.0.0.1:3200/ready` for readiness. The OTLP HTTP trace endpoint is
`http://127.0.0.1:4318/v1/traces`; the query base URL is `http://127.0.0.1:3200`.
Use the same env file, Compose file and project name for `ps`, `logs`, `restart`
or `down`. Stop only the owned project; bind-mounted trace data remains for reuse.
Restart with the saved `tracing.env`; preparation never overwrites existing data.

Application export stays opt-in and defaults off. Starting Tempo does not enable
export in application processes; configure those processes separately as below.
Normal configured sampling is 0.1; sampling1.0 belongs only in disposable acceptance.
The existing authenticated Grafana, Prometheus datasource and dashboards are
unchanged. Preparation also creates private Grafana provisioning for an optional
monitoring overlay: it copies the accepted Prometheus datasource and dashboard
provider unchanged, and adds the read-only `warexis-tempo` datasource. The base
monitoring stack does not receive this datasource.

To use it, prepare a separate monitoring runtime with
`scripts/prepare-monitoring.py`, then combine both env files and the three Compose
files. Existing monitoring authentication settings remain inherited:

```sh
docker compose --env-file "$HOME/.local/state/warexis-monitoring/example/monitoring.env" \
  --env-file "$HOME/.local/state/warexis-tracing/example/tracing.env" \
  -f compose.monitoring.yaml -f compose.tracing.yaml \
  -f compose.tracing-monitoring.yaml -p warexis-observability-example up -d
```

Grafana reaches Tempo over its host loopback query port; services send traces to
the loopback OTLP HTTP endpoint. The overlay replaces only the provisioning mount
and sets the Tempo query URL. It does not enable anonymous login or change the
Prometheus datasource, dashboard files, or application endpoint security.
Actual datasource query and rendered-trace acceptance still require the running
stack and remain separate from these preparation checks.

## Focused checks

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/prepare-tracing-test.py
```

If the operating system's temporary directory is a Git checkout, use `TMPDIR`
to select an external temporary directory.

These checks cover external paths, private modes, caller identity, configuration
copying, fresh-only generation and port validation. Compose parsing and an actual
owned Tempo readiness check remain separate runtime checks. Full application
export, stored-span privacy, asynchronous causality, persistence/restart, outage,
Grafana and official Browser acceptance remain required before Phase9C completion.

## Application tracing

Order, Inventory and Fulfillment use Spring Boot's managed OpenTelemetry starter.
No Java agent is required. Set these variables on each service JVM:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WAREXIS_TRACING_EXPORT_ENABLED` | `false` | Enable trace export explicitly. |
| `WAREXIS_OTLP_ENDPOINT` | `http://127.0.0.1:4318/v1/traces` | Tempo OTLP HTTP trace endpoint, including path. |
| `WAREXIS_TRACING_SAMPLING_PROBABILITY` | `0.1` | Sampling probability for new roots; use `1.0` only for disposable acceptance. |

Service identities are fixed to `order-service`, `inventory-service` and
`fulfillment-service`. OTLP metrics and log export stay disabled; Prometheus
remains the metrics collector. SDK environment mapping is disabled. The batch
export queue holds at most 16 spans, batches at most four, with a 50ms schedule,
500ms processor timeout and 250ms HTTP exporter timeout (100ms connection timeout).
Backend outages or queue saturation can drop telemetry; they do not retry business
operations or change their transaction and acknowledgment behavior.

Native HTTP spans establish request context. The nullable internal `traceparent`
outbox column captures the context in the business transaction; relay producer
spans use that durable origin after restart. Kafka processing attempts use the
actual incoming producer as parent. Retries are sibling attempts under the same
incoming parent, and a DLT span covers its own broker acknowledgment. Completion
metadata and source-offset acknowledgment retain their existing boundaries.
Missing, malformed or duplicate Kafka context creates a new trace root; legacy
rows without context remain publishable. Unsampled context remains unsampled.
Business event payloads, keys and event identities are unchanged.

Fulfillment stores its first confirmation origin immutably. Manual dispatch
follows the current HTTP request and links to that origin; it does not reparent
the new command under an old confirmation. The shipping outbox captures the
current dispatch context. Duplicate delivery retains the original durable facts.

Export reconstructs span data with fixed service identities, scope and finite
operation/outcome attributes. It drops baggage, tracestate, arbitrary resource
attributes, URLs, SQL, payloads, identifiers, exception events and status text.
Trace/span IDs remain structural context, never metric-label dimensions. Stored
trace data contains no business identifiers, so locate a trace through its
structural trace ID rather than searching for an order UUID or shipment reference.

## Verification scope

The packaged workflow launches real Kafka, PostgreSQL, Keycloak and Tempo using
fresh executable service JARs. Run `scripts/verify-workflow.sh` from the repository
root. It rebuilds the JARs, preserves the existing authenticated business and
privacy cases, and adds causal-flow, dispatch-link, exporter-outage and transport
context scenarios. Existing poison, retry, partition and restart cases also
inspect durable context and actual stored attempt relationships where applicable.
This command does not replace each service's full `./mvnw test` and `./mvnw verify`
lifecycles or the separate frontend and official Browser acceptance gates.
