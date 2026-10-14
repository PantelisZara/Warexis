// Tab-scoped navigation metadata only. The identity adapter owns OAuth state and
// credentials; a saved route never grants access to the page it identifies.
const storageKey = 'warexis.auth.return-route';
const maximumAge = 5 * 60 * 1000;
const orderIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
type Route = { path: '/orders' | '/inventory' | '/fulfillment'; orderId?: string };

function validatedRoute(value: unknown): Route | undefined {
  if (!value || typeof value !== 'object') return;
  const route = value as Record<string, unknown>;
  if (route.path !== '/orders' && route.path !== '/inventory' && route.path !== '/fulfillment') return;
  if (route.orderId !== undefined && (route.path !== '/fulfillment' ||
      typeof route.orderId !== 'string' || !orderIdPattern.test(route.orderId))) return;
  return { path: route.path, ...(route.orderId === undefined ? {} : { orderId: route.orderId as string }) };
}
export function clearReturnRoute(): void {
  try { window.sessionStorage.removeItem(storageKey); } catch { /* Storage is optional. */ }
}
export function rememberReturnRoute(): void {
  clearReturnRoute();
  const { pathname, search, hash } = window.location;
  if (hash) return;
  const orderId = pathname === '/fulfillment' && search.startsWith('?orderId=') ? search.slice(9) : undefined;
  if (search && (orderId === undefined || !orderIdPattern.test(orderId))) return;
  const route = validatedRoute({ path: pathname, orderId });
  if (!route) return;
  try { window.sessionStorage.setItem(storageKey, JSON.stringify({ ...route, createdAt: Date.now() })); }
  catch { /* A blocked or full sessionStorage must not prevent authentication. */ }
}
export function prepareReturnRoute(): boolean {
  const { pathname, search, hash } = window.location;
  const parameters = new URLSearchParams(hash.slice(1));
  const callback = pathname === '/' && !search && parameters.has('state') &&
    (parameters.has('code') || parameters.has('error'));
  if (!callback) rememberReturnRoute();
  return callback;
}
export function finishReturnRoute(callback: boolean): void {
  try {
    const stored = window.sessionStorage.getItem(storageKey);
    // Consume before parsing or navigation, including invalid or stale metadata.
    window.sessionStorage.removeItem(storageKey);
    if (!callback || window.location.pathname !== '/' || window.location.search || window.location.hash || !stored) return;
    const value: unknown = JSON.parse(stored);
    const route = validatedRoute(value);
    if (!route) return;
    const createdAt = (value as Record<string, unknown>).createdAt;
    if (typeof createdAt !== 'number' || !Number.isFinite(createdAt) ||
        createdAt > Date.now() || Date.now() - createdAt > maximumAge) return;
    window.history.replaceState(null, '', route.path + (route.orderId ? `?orderId=${route.orderId}` : ''));
  } catch { /* Invalid metadata or unavailable storage leaves the root route. */ }
}
