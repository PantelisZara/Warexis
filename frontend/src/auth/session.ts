import Keycloak from 'keycloak-js';
import { ApiError, configureAuthentication } from '../shared/api/client';
import type { AuthenticationCredential } from '../shared/api/client';
import type { AuthSnapshot } from './contracts';
import { clearReturnRoute, finishReturnRoute, prepareReturnRoute, rememberReturnRoute } from './routeRecovery';

const rootRedirect = 'http://127.0.0.1:5173/';
const listeners = new Set<() => void>();
let snapshot: AuthSnapshot = {
  status: 'initializing', canRead: false, canOperate: false, canRetryLogout: false, role: 'none',
  principalId: null, username: null, error: null, sessionEpoch: 0,
};
let adapter: Keycloak | undefined;
let initialization: Promise<void> | undefined;
let adapterReady = false;
let acceptsCredentials = true;
let loggingOut = false;
let loggingIn = false;
let revision = 0;
let refreshing: Promise<boolean> | undefined;

export function getAuthSnapshot(): AuthSnapshot { return snapshot; }
export function subscribeAuth(listener: () => void): () => void {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}
function publish(next: Omit<AuthSnapshot, 'sessionEpoch'>): void {
  const boundary = next.status !== snapshot.status || next.principalId !== snapshot.principalId ||
    next.role !== snapshot.role || next.canRead !== snapshot.canRead || next.canOperate !== snapshot.canOperate;
  snapshot = { ...next, sessionEpoch: snapshot.sessionEpoch + Number(boundary) };
  for (const listener of listeners) listener();
}
function empty(status: AuthSnapshot['status'], error: string | null = null, canRetryLogout = false): void {
  publish({ status, error, canRetryLogout, canRead: false, canOperate: false, role: 'none', principalId: null, username: null });
}
function clearCredentials(): void {
  clearReturnRoute();
  // Disable callbacks before clearToken, which invokes onAuthLogout synchronously.
  acceptsCredentials = false;
  adapter?.clearToken();
}
function expire(): void {
  clearCredentials();
  if (!loggingOut) empty('expired', 'Your session has expired. Please sign in again.');
}
function synchronize(): void {
  if (!acceptsCredentials || loggingOut) { adapter?.clearToken(); return; }
  if (!adapter?.authenticated || !adapter.token) { empty('anonymous'); return; }
  const roles = Array.isArray(adapter.realmAccess?.roles) ? adapter.realmAccess.roles : [];
  const operator = roles.includes('WAREXIS_OPERATOR');
  const viewer = roles.includes('WAREXIS_VIEWER');
  publish({ status: 'authenticated', canRead: operator || viewer, canOperate: operator, canRetryLogout: false,
    role: operator ? 'operator' : viewer ? 'viewer' : 'none',
    principalId: typeof adapter.tokenParsed?.sub === 'string' ? adapter.tokenParsed.sub : null,
    username: typeof adapter.tokenParsed?.preferred_username === 'string' ? adapter.tokenParsed.preferred_username : null,
    error: null });
}
async function tokenForRequest(): Promise<AuthenticationCredential> {
  if (!adapterReady || !adapter || loggingOut || !acceptsCredentials || snapshot.status !== 'authenticated')
    throw new ApiError(401, 'Please sign in to continue.');
  const startedRevision = revision;
  try {
    // The adapter coordinates its refresh queue. Share the promise here as well,
    // so navigation actions can wait for an already started refresh to settle.
    refreshing ??= adapter.updateToken(30);
    const currentRefresh = refreshing;
    try { await currentRefresh; }
    finally { if (refreshing === currentRefresh) refreshing = undefined; }
    if (loggingOut || startedRevision !== revision || !acceptsCredentials || !adapter.authenticated || !adapter.token)
      throw new ApiError(401, 'Your session has expired. Please sign in again.');
    synchronize();
    return { token: adapter.token, sessionEpoch: snapshot.sessionEpoch };
  } catch {
    if (startedRevision === revision && !loggingOut) expire();
    throw new ApiError(401, 'Your session has expired. Please sign in again.');
  }
}
function unauthorized(credential: AuthenticationCredential): void {
  if (!loggingOut && snapshot.status === 'authenticated' && credential.token === adapter?.token &&
      credential.sessionEpoch === snapshot.sessionEpoch) expire();
}

export function initializeAuth(): Promise<void> {
  // This singleton is called before Router creation, outside React effects.
  initialization ??= initialize();
  return initialization;
}
async function initialize(): Promise<void> {
  configureAuthentication(tokenForRequest, unauthorized);
  const callback = prepareReturnRoute();
  try {
    const url = import.meta.env.VITE_KEYCLOAK_URL;
    const realm = import.meta.env.VITE_KEYCLOAK_REALM;
    const clientId = import.meta.env.VITE_KEYCLOAK_CLIENT_ID;
    if (typeof url !== 'string' || !url.trim() || typeof realm !== 'string' || !realm.trim() ||
        typeof clientId !== 'string' || !clientId.trim()) throw new Error('Missing public identity configuration');
    adapter = new Keycloak({ url, realm, clientId });
    adapter.onAuthSuccess = synchronize;
    adapter.onAuthRefreshSuccess = synchronize;
    adapter.onAuthRefreshError = () => { if (acceptsCredentials && !loggingOut) expire(); };
    adapter.onAuthError = () => {
      clearCredentials();
      if (!loggingOut) empty('error', 'Sign-in failed. Please try signing in again.');
    };
    adapter.onAuthLogout = () => {
      clearReturnRoute();
      if (acceptsCredentials && !loggingOut) { acceptsCredentials = false; empty('expired', 'Your session has ended. Please sign in again.'); }
    };
    adapter.onTokenExpired = () => { if (acceptsCredentials && !loggingOut) void tokenForRequest().catch(() => {}); };
    await adapter.init({ onLoad: 'check-sso', flow: 'standard', pkceMethod: 'S256',
      checkLoginIframe: false, redirectUri: rootRedirect, responseMode: 'fragment' });
    adapterReady = true;
    synchronize();
    finishReturnRoute(callback && acceptsCredentials && !loggingOut);
  } catch {
    clearCredentials();
    empty('error', 'Authentication could not start. Check the identity service and public configuration, then reload this page.');
  }
}
export async function login(): Promise<void> {
  await initializeAuth();
  if (!adapterReady || !adapter || loggingOut || loggingIn || snapshot.canRetryLogout) return;
  loggingIn = true;
  const startedRevision = ++revision;
  // A late refresh callback from a revoked session must finish before a new login.
  await refreshing?.catch(() => {});
  if (revision !== startedRevision || loggingOut) { loggingIn = false; return; }
  acceptsCredentials = true;
  rememberReturnRoute();
  try { await adapter.login({ redirectUri: rootRedirect }); }
  catch {
    clearCredentials();
    if (revision === startedRevision && !loggingOut) empty('error', 'Sign-in could not be opened. Please try signing in again.');
  } finally { loggingIn = false; }
}
export async function logout(): Promise<void> {
  clearReturnRoute();
  if (loggingOut) return;
  ++revision;
  loggingOut = true;
  acceptsCredentials = false;
  empty('anonymous');
  let failed = false;
  try {
    // Preserve the adapter credentials until it has formed its supported logout
    // request (including the ID-token hint); block all business requests now.
    await adapter?.logout({ redirectUri: rootRedirect });
  } catch { failed = true; }
  finally {
    clearCredentials();
    loggingOut = false;
    empty(failed ? 'error' : 'anonymous', failed ? 'Local sign-out is complete, but identity-provider sign-out could not be confirmed. Try signing out again.' : null, failed);
  }
}
