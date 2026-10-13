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

The foundation contains a responsive shell, navigation, operational overview, and shared presentation components. Feature routes currently state that integration is pending; authentication and API requests are added by the following dependency batch. The overview accepts actual service availability through its `health` prop; without a supplied result it explicitly says health has not been checked. It contains no invented counts, lists, history, or business facts.

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

Unit tests use jsdom and Testing Library. They verify accessible navigation, unknown-route recovery, honest availability presentation, and shared field / error semantics. They do not substitute for the required real-browser authentication and business workflow gate. Browser automation is not executed by these scripts.
