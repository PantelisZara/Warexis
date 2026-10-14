# Warexis operations frontend

React, strict TypeScript, and Vite provide the operations workspace. Dependencies are pinned in `package.json` and the committed npm lockfile. Use Node 24 (at least 24.15) and npm.

```sh
npm ci
npm run dev
npm run lint
npm run typecheck
npm test
npm run build
```

The development server binds only to `127.0.0.1:5173` and refuses a different port. Its same-origin proxy strips `/api/order`, `/api/inventory`, or `/api/fulfillment`, then forwards to the configured local service. For example, `/api/order/api/orders/{id}` reaches Order's `/api/orders/{id}`. Health uses `/api/order/actuator/health` and the equivalent Inventory and Fulfillment paths. Proxy destinations are server-side variables in `.env.example`; `VITE_*` variables are public configuration and must never contain credentials or tokens.

The application provides Orders, Inventory and Fulfillment workspaces through the audited service APIs. Orders supports creation and ID lookup; Inventory supports SKU lookup and on-hand stock changes; Fulfillment supports lookup and manual dispatch. The overview queries actual anonymous service health. It contains no invented counts, lists, history or business facts.

Copy the public example configuration to an ignored local `.env` before starting Vite (`cp .env.example .env` from this directory). Start the three APIs with the matching issuer/audience and configure the public browser client as described in [the identity setup](../infrastructure/auth/README.md). For disposable verification use [the isolated real stack fixture](../tests/browser-fixture/README.md). The original user-owned branding directory is never required to run the copied frontend assets.

Authentication uses the official `keycloak-js` adapter, initialized before the router with standard Authorization Code Flow and PKCE S256. Login/logout return to the exact allowed root. Tokens stay in adapter memory and are refreshed before protected requests. Viewer and Operator controls follow recognized realm roles, while the backend remains the authorization authority. Refresh failure/401 clears the applicable session;403 stays a permission error. Principal/permission changes discard previous business views. An unconfirmed provider logout offers an explicit retry while local credentials and business access remain cleared.

Mutations have synchronous duplicate guards and no automatic retries. An uncertain order creation requires deliberate acknowledgement and a blank new form before another creation. Stock/dispatch uncertainty requires same-resource reconciliation. Errors use the actual API messages and status; asynchronous business decisions are read from the backend, not inferred by the UI.

Unit/component tests mock the adapter or API where appropriate. They establish available source-level behavior, not real login redirect or backend browser integration evidence. Official in-app Browser exploratory verification and the automated real-stack Playwright suite are separate gates. Desktop/mobile rendering, real PKCE login/logout/refresh and the complete browser business workflow require recorded live results before declaring the phase complete.

## Shared presentation contracts

- `AppShell` renders the route outlet and accepts an optional `session: ReactNode` for the authentication controls.
- `PageHeader`: `title`, `description`, optional `eyebrow` and `actions`.
- `StatusBadge`: children and optional `tone` (`neutral`, `success`, `warning`, `danger`, `info`). Callers supply meaningful status text; color is supplementary.
- `ErrorMessage`: `message` and optional `title`; uses an alert region.
- `LoadingState`: optional `label`; uses a status region.
- `Field`: `id`, `label`, optional `hint` / `error`, and a child control. The control must use that `id`; callers set `aria-describedby` to `${id}-hint` / `${id}-error` when present and `aria-invalid` when invalid.
- CSS provides `panel`, `panel-heading`, `page-header`, `button` / `button--secondary`, `button-group`, `field`, `form-grid`, `content-grid`, `record-details`, `table-scroll`, `inline-code`, `empty-state`, and `sr-only`. CSS variables in `src/styles/tokens.css` define the surface, text, border, focus, and status-independent brand colors.

## Official branding

`public/brand/` contains byte-for-byte copies of the selected official exports: dark wordmark, red X icon, SVG / PNG favicon, and Apple touch icon. Originals remain user-owned outside the tracked frontend. The local Git attributes preserve the original CRLF export bytes and treat the carriage return as a line ending for whitespace checks. No master artwork, editable source, unrelated exports, or generated replacement logos are included. The dark wordmark is displayed on light surfaces; its aspect ratio is preserved. Font styling uses system fallbacks and no external font request.

Unit tests use jsdom and Testing Library. They verify accessible navigation, unknown-route recovery, honest availability presentation, and shared field / error semantics. They do not substitute for the required real-browser authentication and business workflow gate. The unit-test command does not execute browser automation.

## Automated real-stack regression

The opt-in Playwright suite uses the already-running [disposable fixture](../tests/browser-fixture/README.md) and Vite at exactly `http://127.0.0.1:5173`. It never starts or stops infrastructure. Start Vite with the fixture-provided public frontend environment first; keep the runtime JSON private and outside Git.

```sh
npm run typecheck:e2e
WAREXIS_E2E_RUNTIME_FILE=/absolute/private/session/runtime.json npm run test:e2e
```

Use an installed compatible Chromium browser. When using a system browser instead of Playwright's existing browser cache, set `WAREXIS_E2E_BROWSER_EXECUTABLE` to its absolute executable path. The configuration enables the Chromium sandbox and adds no security-disabling flags. No browser download or installation is performed by the test commands.

Tests use ephemeral Operator/Viewer identities through the real Keycloak login form and assert Authorization Code + PKCE S256. Business setup and state assertions use the actual UI, APIs, PostgreSQL and Kafka. Unique SKUs isolate each run; the entire fixture owns cleanup. One worker, no retries and bounded eventual UI assertions avoid replaying mutations. The suite verifies stock changes, confirmation/reservation, fulfillment dispatch/reload, Viewer reads and real server `403`, anonymous `401`, invalid/missing inputs, conflicts, rejection and offline read recovery. One fault-injection case forwards a genuine order POST to the real backend, verifies its `201` response, then drops only its response to verify duplicate guards and uncertain-outcome acknowledgement. It does not synthesize a successful business response.

Credentials are read only from a mode `0600` runtime file in a mode `0700` directory outside Git. Traces, screenshots, video, failure-time ARIA snapshots and storage-state exports are disabled; do not enable them or run with protocol/API debug logging against credentialed sessions. Token inspection reports only a boolean, and no token is attached to test output. A bounded lifecycle case derives the actual bearer expiry in Node memory, waits for the real renewal window, observes a successful refresh grant, and revokes shared SSO using ordinary sign-out in a second page. The first page must then fail real refresh, clear business access and recover through normal login. With the fixture's five-minute token lifetime this case takes about nine minutes and has a twelve-minute limit; no realm settings, clocks or token responses are fabricated. Responsive rendering and keyboard inspection remain separate practical exploratory checks.
