// Isolated adapter unit tests. They do not establish real redirects, JWT validation,
// browser storage behavior inside keycloak-js, or the mandatory browser E2E gate.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type Keycloak from 'keycloak-js';

const factory = vi.hoisted(() => ({ create: vi.fn() }));
vi.mock('keycloak-js', () => ({ default: class { constructor(config: unknown) { return factory.create(config); } } }));
const rootRedirect = 'http://127.0.0.1:5173/';
const stock = { id: '00000000-0000-0000-0000-000000000001', sku: 'SKU-1', onHand: 10, reserved: 3, available: 7 };
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function makeAdapter() {
  const init = vi.fn().mockResolvedValue(false);
  const updateToken = vi.fn().mockResolvedValue(false);
  const login = vi.fn().mockResolvedValue(undefined);
  const logout = vi.fn().mockResolvedValue(undefined);
  const clearToken = vi.fn(() => {
    instance.authenticated = false;
    delete instance.token;
    delete instance.tokenParsed;
    delete instance.realmAccess;
    instance.onAuthLogout?.();
  });
  const instance = { authenticated: false, init, updateToken, login, logout, clearToken } as unknown as Keycloak;
  factory.create.mockReturnValue(instance);
  return { instance, init, updateToken, login, logout, clearToken };
}
function signedIn(instance: Keycloak, roles = ['WAREXIS_VIEWER'], token = 'unit-token-one', sub = 'unit-principal') {
  instance.authenticated = true;
  instance.token = token;
  instance.tokenParsed = { sub, preferred_username: 'unit-user' };
  instance.realmAccess = { roles };
}
async function authenticated(roles = ['WAREXIS_VIEWER']) {
  const mock = makeAdapter(); signedIn(mock.instance, roles); mock.init.mockResolvedValue(true);
  const session = await import('./session');
  const { api } = await import('../shared/api/client');
  await session.initializeAuth();
  return { ...mock, session, api };
}
beforeEach(() => {
  vi.resetModules();
  factory.create.mockReset();
  window.history.replaceState(null, '', '/');
  window.sessionStorage.clear();
  window.localStorage.clear();
  vi.stubEnv('VITE_KEYCLOAK_URL', 'http://127.0.0.1:18080');
  vi.stubEnv('VITE_KEYCLOAK_REALM', 'warexis');
  vi.stubEnv('VITE_KEYCLOAK_CLIENT_ID', 'warexis-operations');
});
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });

describe('official adapter session (mocked adapter only)', () => {
  it('initializes once outside React with standard S256 top-level check-sso and exact root', async () => {
    const mock = makeAdapter(); const pending = deferred<boolean>(); mock.init.mockReturnValue(pending.promise);
    const session = await import('./session');
    const first = session.initializeAuth(); const second = session.initializeAuth();
    expect(first).toBe(second); expect(factory.create).toHaveBeenCalledOnce(); expect(mock.init).toHaveBeenCalledOnce();
    expect(factory.create).toHaveBeenCalledWith({ url: 'http://127.0.0.1:18080', realm: 'warexis', clientId: 'warexis-operations' });
    expect(mock.init).toHaveBeenCalledWith({ onLoad: 'check-sso', flow: 'standard', pkceMethod: 'S256',
      checkLoginIframe: false, redirectUri: rootRedirect, responseMode: 'fragment' });
    expect(mock.init.mock.calls[0]?.[0]).not.toHaveProperty('silentCheckSsoRedirectUri');
    expect(session.getAuthSnapshot().status).toBe('initializing'); pending.resolve(false); await first;
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'anonymous', canRead: false, canOperate: false });
  });
  it('publishes recoverable errors without rejecting init or exposing provider error details', async () => {
    const mock = makeAdapter(); mock.init.mockRejectedValue(new Error('private provider data'));
    const session = await import('./session'); await expect(session.initializeAuth()).resolves.toBeUndefined();
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'error', canRead: false, principalId: null, error: expect.stringContaining('reload') });
    expect(session.getAuthSnapshot().error).not.toContain('private'); expect(mock.clearToken).toHaveBeenCalled();
    await session.login(); expect(mock.login).not.toHaveBeenCalled();
    await session.initializeAuth(); expect(mock.init).toHaveBeenCalledOnce();
  });
  it('fails closed for missing public configuration', async () => {
    makeAdapter(); vi.stubEnv('VITE_KEYCLOAK_CLIENT_ID', ''); const session = await import('./session');
    await session.initializeAuth(); expect(session.getAuthSnapshot().status).toBe('error'); expect(factory.create).not.toHaveBeenCalled();
  });
  it.each([
    [['WAREXIS_VIEWER'], true, false, 'viewer'],
    [['WAREXIS_OPERATOR'], true, true, 'operator'],
    [['WAREXIS_OPERATOR', 'WAREXIS_VIEWER'], true, true, 'operator'],
    [[], false, false, 'none'],
    [['warexis_viewer', 'SCOPE_WAREXIS_OPERATOR', 'ROLE_WAREXIS_VIEWER'], false, false, 'none'],
  ] as const)('grants capabilities only for exact recognized realm roles %s', async (roles, canRead, canOperate, role) => {
    const { session, instance } = await authenticated([...roles]);
    instance.resourceAccess = { 'warexis-operations': { roles: ['WAREXIS_OPERATOR'] } };
    instance.onAuthRefreshSuccess?.();
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'authenticated', canRead, canOperate, role, principalId: 'unit-principal', username: 'unit-user' });
  });
  it('rejects malformed realm-role containers rather than matching string substrings', async () => {
    const { session, instance } = await authenticated();
    instance.realmAccess = { roles: 'WAREXIS_OPERATOR' } as unknown as NonNullable<Keycloak['realmAccess']>;
    instance.onAuthRefreshSuccess?.();
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'authenticated', canRead: false, canOperate: false, role: 'none' });
  });
  it('refreshes before each request and attaches only the current adapter token', async () => {
    const { api, instance, updateToken } = await authenticated();
    const fetcher = vi.fn().mockImplementation(() => Promise.resolve(Response.json(stock))); vi.stubGlobal('fetch', fetcher);
    updateToken.mockImplementation(async () => { instance.token = 'unit-token-refreshed'; instance.onAuthRefreshSuccess?.(); return true; });
    await api.getStock('SKU-1'); await api.getStock('SKU-1');
    expect(updateToken).toHaveBeenNthCalledWith(1, 30); expect(updateToken).toHaveBeenNthCalledWith(2, 30);
    const options = fetcher.mock.calls[0]?.[1] as RequestInit;
    expect(new Headers(options.headers).get('Authorization')).toBe('Bearer unit-token-refreshed');
  });
  it('clears credentials and expires without sending business requests on refresh failure', async () => {
    const { api, session, instance, updateToken, clearToken } = await authenticated();
    updateToken.mockRejectedValue(new Error('provider private failure')); const fetcher = vi.fn(); vi.stubGlobal('fetch', fetcher);
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 });
    expect(fetcher).not.toHaveBeenCalled(); expect(clearToken).toHaveBeenCalled(); expect(instance.token).toBeUndefined();
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'expired', canRead: false, principalId: null });
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 }); expect(updateToken).toHaveBeenCalledOnce();
  });
  it('does not clear a current session on 403 but invalidates a relevant 401', async () => {
    const { api, session } = await authenticated(); const fetcher = vi.fn().mockResolvedValue(new Response('', { status: 403 })); vi.stubGlobal('fetch', fetcher);
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 403 }); expect(session.getAuthSnapshot().status).toBe('authenticated');
    fetcher.mockResolvedValue(new Response('', { status: 401 })); await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 });
    expect(session.getAuthSnapshot().status).toBe('expired');
  });
  it('ignores a late old-token 401 after refresh rotation', async () => {
    const { api, session, instance } = await authenticated(); const response = deferred<Response>();
    const fetcher = vi.fn().mockReturnValueOnce(response.promise).mockResolvedValue(Response.json(stock)); vi.stubGlobal('fetch', fetcher);
    const old = api.getStock('SKU-1'); const expectedFailure = expect(old).rejects.toMatchObject({ status: 401 });
    await vi.waitFor(() => expect(fetcher).toHaveBeenCalledOnce());
    instance.token = 'unit-token-two'; instance.onAuthRefreshSuccess?.(); await api.getStock('SKU-1');
    response.resolve(new Response('', { status: 401 })); await expectedFailure;
    expect(session.getAuthSnapshot().status).toBe('authenticated');
  });
  it('ignores a late 401 from a different principal session even if a token string repeats', async () => {
    const { api, session, instance } = await authenticated(); const response = deferred<Response>();
    const fetcher = vi.fn().mockReturnValue(response.promise); vi.stubGlobal('fetch', fetcher);
    const old = api.getStock('SKU-1'); const expectedFailure = expect(old).rejects.toMatchObject({ status: 401 });
    await vi.waitFor(() => expect(fetcher).toHaveBeenCalledOnce());
    signedIn(instance, ['WAREXIS_VIEWER'], 'unit-token-one', 'other-principal'); instance.onAuthSuccess?.();
    response.resolve(new Response('', { status: 401 })); await expectedFailure;
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'authenticated', principalId: 'other-principal' });
  });
  it('changes sessionEpoch for permission/principal/auth changes, not ordinary token rotation', async () => {
    const { session, instance } = await authenticated(); const initial = session.getAuthSnapshot().sessionEpoch;
    instance.token = 'unit-token-two'; instance.onAuthRefreshSuccess?.(); expect(session.getAuthSnapshot().sessionEpoch).toBe(initial);
    instance.realmAccess = { roles: ['WAREXIS_OPERATOR'] }; instance.onAuthRefreshSuccess?.(); expect(session.getAuthSnapshot().sessionEpoch).toBe(initial + 1);
    instance.tokenParsed = { sub: 'other-principal', preferred_username: 'other' }; instance.onAuthSuccess?.(); expect(session.getAuthSnapshot().sessionEpoch).toBe(initial + 2);
    await session.logout(); expect(session.getAuthSnapshot().sessionEpoch).toBe(initial + 3);
  });
  it('uses the root redirect for login and surfaces a generic recoverable login error', async () => {
    const { session, login } = await authenticated(); login.mockRejectedValue(new Error('sensitive login detail'));
    await expect(session.login()).resolves.toBeUndefined(); expect(login).toHaveBeenCalledWith({ redirectUri: rootRedirect });
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'error', error: expect.stringContaining('try signing in again') });
    expect(session.getAuthSnapshot().error).not.toContain('sensitive');
  });
  it.each([false, true])('blocks requests during logout, preserves hint before adapter logout, and clears on completion (failure %s)', async (fails) => {
    const { session, api, instance, logout, clearToken } = await authenticated(); const pending = deferred<void>();
    logout.mockImplementation(() => { expect(instance.token).toBe('unit-token-one'); expect(clearToken).not.toHaveBeenCalled(); return pending.promise; });
    const finishing = session.logout(); const fetcher = vi.fn(); vi.stubGlobal('fetch', fetcher);
    expect(logout).toHaveBeenCalledWith({ redirectUri: rootRedirect }); expect(session.getAuthSnapshot().canRead).toBe(false);
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 }); expect(fetcher).not.toHaveBeenCalled();
    if (fails) pending.reject(new Error('private logout data')); else pending.resolve(); await finishing;
    expect(instance.token).toBeUndefined(); expect(clearToken).toHaveBeenCalled();
    expect(session.getAuthSnapshot().status).toBe(fails ? 'error' : 'anonymous');
    if (fails) expect(session.getAuthSnapshot().error).toContain('could not be confirmed');
  });
  it('keeps failed logout explicit and business access blocked until a successful sign-out retry', async () => {
    const { session, api, instance, logout, login } = await authenticated();
    logout.mockRejectedValueOnce(new Error('Provider logout unavailable')).mockResolvedValueOnce(undefined);
    const fetcher = vi.fn(); vi.stubGlobal('fetch', fetcher);
    await session.logout();
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'error', canRetryLogout: true, canRead: false, canOperate: false, principalId: null });
    expect(instance.token).toBeUndefined();
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 });
    await session.login(); expect(login).not.toHaveBeenCalled();
    await session.logout();
    expect(logout).toHaveBeenCalledTimes(2);
    expect(logout).toHaveBeenNthCalledWith(2, { redirectUri: rootRedirect });
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'anonymous', canRetryLogout: false, error: null, canRead: false, canOperate: false });
    expect(instance.token).toBeUndefined(); expect(login).not.toHaveBeenCalled(); expect(fetcher).not.toHaveBeenCalled();
  });
  it('does not resurrect a revoked session from an in-flight refresh after logout', async () => {
    const { session, api, instance, updateToken } = await authenticated(); const pending = deferred<boolean>(); updateToken.mockReturnValue(pending.promise);
    const request = api.getStock('SKU-1'); const expectedFailure = expect(request).rejects.toMatchObject({ status: 401 });
    await session.logout(); signedIn(instance); instance.onAuthRefreshSuccess?.(); pending.resolve(true); await expectedFailure;
    expect(session.getAuthSnapshot().status).toBe('anonymous'); expect(instance.token).toBeUndefined();
  });
  it('refreshes on expiration and fails closed when expiry recovery fails', async () => {
    const { session, instance, updateToken } = await authenticated(); updateToken.mockRejectedValue(new Error('expired'));
    instance.onTokenExpired?.(); await vi.waitFor(() => expect(session.getAuthSnapshot().status).toBe('expired'));
    expect(updateToken).toHaveBeenCalledWith(30);
  });
  it('subscribes to state and unsubscribes without storing or logging credentials in application code', async () => {
    const local = vi.spyOn(Storage.prototype, 'setItem'); const log = vi.spyOn(console, 'log');
    const { session } = await authenticated(); const listener = vi.fn(); const remove = session.subscribeAuth(listener);
    await session.logout(); expect(listener).toHaveBeenCalled(); listener.mockClear(); remove(); await session.logout();
    expect(listener).not.toHaveBeenCalled(); expect(local).not.toHaveBeenCalled(); expect(log).not.toHaveBeenCalled();
  });
});


describe('route recovery around official adapter initialization', () => {
  const orderId = '00000000-0000-0000-0000-000000000001';
  const route = `/fulfillment?orderId=${orderId}`;
  const callback = '/#state=unit-state&session_state=unit-session&code=unit-private-code';
  async function redirectedDocument() {
    vi.resetModules();
    window.history.replaceState(null, '', route);
    const mock = makeAdapter();
    // A real top-level redirect leaves this document before init resolves.
    mock.init.mockReturnValue(new Promise<boolean>(() => {}));
    const session = await import('./session');
    void session.initializeAuth();
    expect(window.sessionStorage.length).toBe(1);
    vi.resetModules();
    window.history.replaceState(null, '', callback);
  }
  it('restores fulfillment and its single UUID after a separate callback document finishes SDK initialization', async () => {
    await redirectedDocument();
    const mock = makeAdapter(); const pending = deferred<boolean>(); mock.init.mockReturnValue(pending.promise);
    const session = await import('./session'); const initializing = session.initializeAuth();
    expect(window.location.pathname).toBe('/');
    signedIn(mock.instance, ['WAREXIS_OPERATOR']);
    // The SDK removes the OAuth fragment before init resolves.
    window.history.replaceState(null, '', '/'); pending.resolve(true); await initializing;
    expect(window.location.pathname + window.location.search).toBe(route);
    expect(window.sessionStorage.length).toBe(0);
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'authenticated', canOperate: true });
  });
  it('preserves the current route for explicit login, with only navigation metadata in sessionStorage', async () => {
    window.history.replaceState(null, '', route);
    const mock = makeAdapter(); const session = await import('./session'); await session.initializeAuth();
    expect(window.sessionStorage.length).toBe(0);
    await session.login();
    expect(mock.login).toHaveBeenCalledWith({ redirectUri: rootRedirect });
    const stored = window.sessionStorage.getItem('warexis.auth.return-route');
    expect(JSON.parse(stored!)).toEqual({ path: '/fulfillment', orderId, createdAt: expect.any(Number) });
    expect(window.localStorage.length).toBe(0);
    vi.resetModules(); window.history.replaceState(null, '', callback);
    const next = makeAdapter(); signedIn(next.instance);
    next.init.mockImplementation(async () => { window.history.replaceState(null, '', '/'); return true; });
    const returned = await import('./session'); await returned.initializeAuth();
    expect(window.location.pathname + window.location.search).toBe(route);
  });
  it('can restore an anonymous check-sso return without granting any permission', async () => {
    await redirectedDocument(); window.history.replaceState(null, '', '/#state=unit-state&error=login_required');
    const mock = makeAdapter();
    mock.init.mockImplementation(async () => { window.history.replaceState(null, '', '/'); return false; });
    const session = await import('./session'); await session.initializeAuth();
    expect(window.location.pathname + window.location.search).toBe(route);
    expect(session.getAuthSnapshot()).toMatchObject({ status: 'anonymous', canRead: false, canOperate: false });
  });
  it('does not restore when SDK initialization fails or leaves an unprocessed fragment', async () => {
    await redirectedDocument(); const mock = makeAdapter(); mock.init.mockRejectedValue(new Error('invalid OAuth response'));
    const session = await import('./session'); await session.initializeAuth();
    expect(window.location.pathname).toBe('/'); expect(window.sessionStorage.length).toBe(0);
    expect(session.getAuthSnapshot().status).toBe('error');
    await redirectedDocument(); const next = makeAdapter(); signedIn(next.instance); next.init.mockResolvedValue(true);
    const returned = await import('./session'); await returned.initializeAuth();
    expect(window.location.pathname).toBe('/'); expect(window.location.hash).not.toBe('');
    expect(window.sessionStorage.length).toBe(0);
  });
  it('discards stale navigation on an ordinary root visit and keeps logout at root', async () => {
    await redirectedDocument(); window.history.replaceState(null, '', '/');
    const mock = makeAdapter(); signedIn(mock.instance);
    const session = await import('./session'); await session.initializeAuth();
    expect(window.location.pathname).toBe('/'); expect(window.sessionStorage.length).toBe(0);
    window.history.replaceState(null, '', route); await session.login();
    expect(window.sessionStorage.length).toBe(1); await session.logout();
    expect(window.sessionStorage.length).toBe(0);
    vi.resetModules(); window.history.replaceState(null, '', callback);
    const next = makeAdapter(); signedIn(next.instance);
    next.init.mockImplementation(async () => { window.history.replaceState(null, '', '/'); return true; });
    const returned = await import('./session'); await returned.initializeAuth();
    expect(window.location.pathname).toBe('/');
  });
  it('clears pending navigation if explicit login fails', async () => {
    window.history.replaceState(null, '', route);
    const mock = makeAdapter(); const session = await import('./session'); await session.initializeAuth();
    mock.login.mockRejectedValue(new Error('identity unavailable')); await session.login();
    expect(window.sessionStorage.length).toBe(0); expect(session.getAuthSnapshot().status).toBe('error');
  });
  it('does not let blocked storage prevent SDK authentication', async () => {
    window.history.replaceState(null, '', route);
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('storage blocked'); });
    vi.spyOn(Storage.prototype, 'removeItem').mockImplementation(() => { throw new Error('storage blocked'); });
    const { session } = await authenticated();
    expect(session.getAuthSnapshot().status).toBe('authenticated');
    await expect(session.login()).resolves.toBeUndefined(); await expect(session.logout()).resolves.toBeUndefined();
  });
});
