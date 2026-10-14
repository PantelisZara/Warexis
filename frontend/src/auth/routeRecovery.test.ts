import { beforeEach, expect, it, vi } from 'vitest';
import { clearReturnRoute, finishReturnRoute, prepareReturnRoute, rememberReturnRoute } from './routeRecovery';

const key = 'warexis.auth.return-route';
const orderId = '00000000-0000-0000-0000-000000000abc';
function callback() {
  window.history.replaceState(null, '', '/#state=private-state&code=private-code&session_state=private-session');
  expect(prepareReturnRoute()).toBe(true);
  window.history.replaceState(null, '', '/');
}
beforeEach(() => {
  window.history.replaceState(null, '', '/');
  window.sessionStorage.clear(); window.localStorage.clear();
});
it.each(['/orders', '/inventory', '/fulfillment', `/fulfillment?orderId=${orderId}`])('recovers only an allowlisted application route once: %s', (route) => {
  window.history.replaceState(null, '', route); expect(prepareReturnRoute()).toBe(false);
  const stored = window.sessionStorage.getItem(key)!;
  expect(stored).not.toContain('?'); expect(stored).not.toContain('http');
  callback(); finishReturnRoute(true);
  expect(window.location.pathname + window.location.search).toBe(route);
  expect(window.sessionStorage.length).toBe(0); expect(window.localStorage.length).toBe(0);
  callback(); finishReturnRoute(true); expect(window.location.pathname).toBe('/');
});
it.each([
  '/', '/unknown', '/orders?orderId=anything', '/inventory?code=private-code',
  '/fulfillment?orderId=not-a-uuid', `/fulfillment?orderId=${orderId.toUpperCase()}`,
  `/fulfillment?orderId=${orderId}&orderId=${orderId}`, `/fulfillment?orderId=${orderId}&code=private-code`,
  `/fulfillment?orderId=%30${orderId.slice(1)}`, '/fulfillment#access_token=private-token',
  '/orders#private-fragment', '/%6frders',
])('does not persist invalid, root, fragmented or noncanonical input: %s', (route) => {
  window.sessionStorage.setItem(key, 'obsolete'); window.history.replaceState(null, '', route);
  rememberReturnRoute(); expect(window.sessionStorage.length).toBe(0); expect(window.localStorage.length).toBe(0);
});
it.each([
  { path: '//external.example/orders' }, { path: 'https://external.example/orders' },
  { path: 'http://user:password@localhost/orders' }, { path: '/orders#access_token=private-token' },
  { path: '/orders?code=private-code' }, { path: '/unknown' }, { path: '/' },
  { path: '/fulfillment', orderId: `${orderId}&code=private-code` },
  { path: '/fulfillment', orderId: orderId.toUpperCase() }, { path: '/inventory', orderId },
  { path: '/fulfillment', orderId: null }, { path: '/orders', createdAt: 0 },
  { path: '/orders', createdAt: Date.now() + 60_000 }, { path: '/orders', createdAt: 'today' },
])('consumes invalid, external or stale stored metadata without navigating: %j', (metadata) => {
  window.sessionStorage.setItem(key, JSON.stringify({ createdAt: Date.now(), ...metadata }));
  callback(); finishReturnRoute(true);
  expect(window.location.pathname + window.location.search + window.location.hash).toBe('/');
  expect(window.sessionStorage.length).toBe(0);
});
it('expires route metadata after five minutes and rejects malformed stored JSON', () => {
  for (const stored of [JSON.stringify({ path: '/orders', createdAt: Date.now() - 300_001 }), 'broken-json', 'null', '[]']) {
    window.sessionStorage.setItem(key, stored); callback(); finishReturnRoute(true);
    expect(window.location.pathname).toBe('/'); expect(window.sessionStorage.length).toBe(0);
  }
});
it('clears root visits, logout metadata, and callbacks that are not at the clean root', () => {
  window.history.replaceState(null, '', '/orders'); rememberReturnRoute(); clearReturnRoute();
  callback(); finishReturnRoute(true); expect(window.location.pathname).toBe('/');
  window.history.replaceState(null, '', '/orders'); rememberReturnRoute();
  window.history.replaceState(null, '', '/'); expect(prepareReturnRoute()).toBe(false);
  callback(); finishReturnRoute(true); expect(window.location.pathname).toBe('/');
  for (const location of ['/inventory', '/?code=private-code', '/#state=private-state&code=private-code']) {
    window.history.replaceState(null, '', '/orders'); rememberReturnRoute();
    window.history.replaceState(null, '', location); finishReturnRoute(true);
    expect(window.location.pathname + window.location.search + window.location.hash).toBe(location);
    expect(window.sessionStorage.length).toBe(0);
  }
});
it.each(['getItem', 'removeItem', 'setItem'] as const)('tolerates unavailable storage operation %s without navigating', (operation) => {
  window.history.replaceState(null, '', '/orders'); rememberReturnRoute();
  vi.spyOn(Storage.prototype, operation).mockImplementation(() => { throw new Error('blocked storage'); });
  expect(() => rememberReturnRoute()).not.toThrow(); callback();
  expect(() => finishReturnRoute(true)).not.toThrow(); expect(window.location.pathname).toBe('/');
});
it('never persists fragment, OAuth, credential or token values', () => {
  const writes = vi.spyOn(Storage.prototype, 'setItem');
  for (const location of ['/orders#access_token=private-token&refresh_token=private-refresh',
    '/fulfillment?orderId=private-token', '/#state=private-state&code=private-code&session_state=private-session']) {
    window.history.replaceState(null, '', location); prepareReturnRoute();
  }
  expect(writes).not.toHaveBeenCalled();
  window.history.replaceState(null, '', `/fulfillment?orderId=${orderId}`); rememberReturnRoute();
  expect(JSON.parse(writes.mock.calls[0]![1])).toEqual({ path: '/fulfillment', orderId, createdAt: expect.any(Number) });
});
