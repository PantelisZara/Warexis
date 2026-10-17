# Local Kubernetes deployment

This single-node **kind** environment runs the existing Order, Inventory,
Fulfillment and React applications with Kafka, Keycloak and four separate
PostgreSQL stores. It is a local development deployment: HTTP, one Kafka broker,
one replica per workload and node-local storage. It provides neither production
TLS nor high availability. [Docker Compose](../local/README.md) remains the
reference alternative. Prometheus, Grafana and Tempo are **not deployed here**;
the existing application instrumentation remains available for a later optional
monitoring layer.

## Prerequisites and startup

Use Linux with Docker, Python 3 plus PyYAML, a Java 21 JDK and enough free resources
for the real stack. The audited workstation had 4 CPUs, 31 GiB RAM and about 37 GiB
free Docker storage. Plan roughly 7–10 GiB for workloads, the control plane and
rolling surge. Steady workload requests total 1500m CPU; memory limits total 8 GiB.
A second application pod can run briefly during an update. Inspect `docker info`,
`docker ps`, available RAM and Docker disk space before starting. Do not stop or
prune unrelated containers to make room. Host ports 5173 and 18080 must be free.

The tooling pins kind **v0.33.0** and kubectl/Kubernetes **v1.37.0**; the node image
has a committed digest. Tools install under the user-local data directory, with
no system package changes. Run from the repository root:

```sh
bash infrastructure/kubernetes/scripts/install-tools.sh
export WAREXIS_K8S_TOOLS_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/warexis/kubernetes/bin"
export WAREXIS_K8S_STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/warexis-supervisor/phase11/kind"
export PATH="$WAREXIS_K8S_TOOLS_DIR:$PATH"
bash infrastructure/kubernetes/scripts/cluster.sh create
# JAVA_HOME must select an installed Java 21 JDK.
bash infrastructure/kubernetes/scripts/images.sh build --package
bash infrastructure/kubernetes/scripts/images.sh load
python3 infrastructure/kubernetes/scripts/validate.py
bash infrastructure/kubernetes/scripts/deploy.sh apply
python3 infrastructure/kubernetes/scripts/validate.py --server-dry-run \
  --kubeconfig "$WAREXIS_K8S_STATE_DIR/kubeconfig"
```

`--package` builds the three executable JARs with tests skipped; it does not claim
backend test coverage. The frontend image uses Node 24.21, `npm ci` and a production
build. Images use explicit local `p11-14fa97e6` tags and are loaded directly into
kind, without pushing a registry. Application `imagePullPolicy: Never` makes a
missing local image fail visibly. Avoid rebuilding unchanged images. To reuse
already verified, source-matched JARs instead, pass
`images.sh build --artifacts /absolute/path/to/checkout-or-jar-directory`; verify
their source fingerprints first. Existing image caches can be reused explicitly.

`deploy.sh apply` creates the namespace, generates/reuses a private `runtime.env`,
applies the runtime Secret and public realm ConfigMap, asks the Kubernetes API for
a schema dry-run, applies Kustomize manifests and waits for each workload rollout.
A successful apply alone does not prove the business workflow works.

## Configuration, networking and identity

The namespace is `warexis`, context `kind-warexis-phase11`, and cluster
`warexis-phase11`. The dedicated kubeconfig and recorded Docker node ID stay under
`$WAREXIS_K8S_STATE_DIR`; scripts check this ownership before mutations and do not
change the user-global kubeconfig. Internal Services use Kubernetes DNS names such
as `order:8081`, `inventory:8082`, `fulfillment:8083`, `kafka:29092` and
`order-postgres:5432`. Each business service owns a separate database; Keycloak
owns a fourth. Database and Kafka ports are not published to the host.
Kafka advertises `kafka-0.kafka-headless:29092` through a headless Service that
publishes unready addresses, so metadata discovery works during startup; the
`kafka:29092` bootstrap Service still routes only to a ready broker.

Open the frontend at **http://127.0.0.1:5173** and Keycloak at
**http://127.0.0.1:18080**. Use `127.0.0.1` consistently. The frontend Nginx proxy
forwards `/api/order/api/...` to Order's `/api/...`, and similarly for Inventory
and Fulfillment, so browsers use one API origin without broad CORS. Only exact
aggregate `/actuator/health` paths are also proxied; other management paths are
rejected. Nginx runs as UID 101 on port 8080 with a read-only filesystem and `/tmp`
scratch storage. Java applications run as UID 10001.

The public issuer is `http://127.0.0.1:18080/realms/warexis`. Keycloak advertises that
public hostname with dynamic backchannel support. Pods obtain signing keys from
`http://keycloak:8080/realms/warexis/protocol/openid-connect/certs`; resource servers
still validate the exact public issuer **and** `warexis-api` audience. Pod localhost
is never used to reach the host identity endpoint. The existing public browser
client retains Authorization Code with PKCE S256 and the exact root login/logout
redirect `http://127.0.0.1:5173/`. Tokens remain in memory.

`warexis-config` holds public configuration. Secret values are generated at runtime
by `scripts/generate-secrets.py DIRECTORY`, in a 0700 directory and 0600 `runtime.env`
outside Git. `deploy.sh` handles this automatically and sends the Secret manifest
directly to kubectl without printing values or placing passwords in arguments.
Kubernetes Secrets are access-controlled API objects; base64 is not encryption.
Keep the matching private input while retaining the PVCs: redeploy refuses to
replace existing credentials with different ones. Do not print Secret YAML or
kubeconfig credentials. The public realm import contains no user passwords.
Changing a stable-name ConfigMap does not restart processes; after a frontend
Nginx configuration change, restart its Deployment and wait for readiness.

## Inspect and verify

A **Pod** runs containers; a **Deployment** replaces and rolls application Pods.
A **Service** routes to ready Pods through stable DNS. A **ConfigMap** supplies
public settings, while a **Secret** supplies credentials. A **StatefulSet** keeps
stable identities for databases/Kafka; each **PVC** requests its own durable disk.
The five claims survive Pod replacement but reside inside this disposable kind
node. Deleting the cluster deletes that local data.

Startup probes allow up to 450 seconds for Java startup and dependencies.
Readiness uses the actual public aggregate health endpoint to decide traffic
eligibility. Application liveness checks the TCP listener so a database outage
does not trigger repeated application restarts; health subpaths require
credentials in the existing security configuration. Stateful workloads have their
own appropriate probes. These probes do not establish an availability guarantee.

Use an explicitly scoped shell function:

```sh
k() { kubectl --kubeconfig "$WAREXIS_K8S_STATE_DIR/kubeconfig" --context kind-warexis-phase11 -n warexis "$@"; }
bash infrastructure/kubernetes/scripts/cluster.sh status
k get pods,deployments,statefulsets,services,pvc
k get endpointslices
k get events --sort-by=.metadata.creationTimestamp
k logs deployment/order --tail=80
```

Inspect logs privately; do not publish raw operational output without checking it
for credentials. `ImagePullBackOff`/`ErrImageNeverPull` calls for checking the exact
local image tag and loading it. Pending PVCs call for checking the `standard`
StorageClass and node disk. For startup failures, check events, current/previous
Pod logs, database migration status and internal Service endpoints. Do not delete
PVCs to hide an authentication or migration failure.

The focused acceptance helper uses Python's standard library, the existing
identity provisioning helper and real HTTP/Kubernetes APIs. It never starts a
parallel Compose fixture. Run its stages once against this isolated cluster:

```sh
export WAREXIS_K8S_ACCEPTANCE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/warexis-supervisor/phase11/acceptance"
a() { python3 infrastructure/kubernetes/scripts/acceptance.py "$@" --runtime-dir "$WAREXIS_K8S_ACCEPTANCE_DIR" --kubeconfig "$WAREXIS_K8S_STATE_DIR/kubeconfig"; }
a infrastructure
a setup --bootstrap-file "$WAREXIS_K8S_STATE_DIR/secrets/runtime.env"
a workflow
a rejected
a check
```

`infrastructure` verifies Ready Pods, five Bound PVCs, ready Service endpoints,
internal DNS, Flyway migration counts 4/4/2, public health and OIDC discovery.
`setup` creates synthetic Operator/Viewer browser users and scoped service
accounts, then removes the temporary master bootstrap client. It stores only
private 0600 credentials in `runtime.json`; existing READY state is reused.
`workflow` creates stock, confirms a real order through Kafka, observes PENDING
fulfillment and ships it. `rejected` checks insufficient stock and absent
fulfillment. `check` reloads the records, checks Viewer 403 and anonymous 401, and
reads persisted state directly from the owning databases. Result files contain
synthetic IDs and sanitized status. Tokens are never persisted. If provisioning
or a workflow stops partway through, inspect the saved private inventory before
retrying; do not blindly recreate accounts or business records.

Use the official in-app Browser for frontend acceptance: Operator login, existing
order/fulfillment lookup, persisted stock, Viewer read-only access, reload, token
refresh and logout. The same `runtime.json` fits the existing frontend disposable
fixture shape. Browser evidence remains a separate gate from successful HTTP
checks. Reuse unchanged source-matched backend/frontend historical verification;
do not run all suites merely to populate this cluster.

## Controlled restart, recovery and rollout

Only run these commands against the dedicated cluster after its initial checks
pass. Capture before/after Pod UID, PVC UID, resource revision and business state.

```sh
k rollout restart deployment/order
k rollout status deployment/order --timeout=600s
a check
# A pending order must recover while Inventory processing is temporarily stopped.
k scale deployment/inventory --replicas=0
k wait --for=delete pod -l app.kubernetes.io/name=inventory --timeout=120s
a pending-start
k scale deployment/inventory --replicas=1
k rollout status deployment/inventory --timeout=600s
a pending-finish
a check
```

Always restore Inventory to one replica, including after a failed pending check.
For persistence, replace the owned `order-postgres-0` Pod, then `k rollout status
statefulset/order-postgres --timeout=600s` and `a check`. Record unchanged PVC UID.
Repeat relevant controlled replacement for `kafka-0` and restart
`deployment/keycloak`; verify readiness, existing authentication and business data
after each. Never delete database/Kafka PVCs for restart testing.

For a distinct locally built rollout image of only Order:

```sh
bash infrastructure/kubernetes/scripts/images.sh build --service order --tag p11-14fa97e6-r2
bash infrastructure/kubernetes/scripts/images.sh load --service order --tag p11-14fa97e6-r2
k set image deployment/order order=warexis/order:p11-14fa97e6-r2
k rollout status deployment/order --timeout=600s
k get pods -l app.kubernetes.io/name=order -o wide
a check
```

The new image uses a different build label/tag and image ID while retaining the
same accepted application JAR. This verifies image selection and controlled
readiness-aware replacement, not new business code or proven zero downtime.
Supply `--artifacts` on this build too if the checked JARs live elsewhere.
Desired manifests still pin the baseline tag; reapplying them rolls back to that
tag. Inspect the selected Pod image/imageID and rollout before reporting success.

## Safe cleanup

```sh
bash infrastructure/kubernetes/scripts/cluster.sh delete --confirm warexis-phase11
```

This command checks ownership and deletes only the named cluster and its two
local ownership/kubeconfig records. It does not run Docker prune, remove unrelated
containers, delete source worktrees or push images. Cluster deletion destroys its
five local database/Kafka volumes. After the cluster is gone, separately remove
only its generated private Secret input and acceptance credential files; keep
sanitized verification evidence. Leave unrelated Docker/Compose resources alone.
