import { decodeOrder, decodeStock, decodeFulfillment, decodeHealth } from './contracts';
import type { OrderLineInput } from './contracts';
export type Service = 'order' | 'inventory' | 'fulfillment';
export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) { super(message); this.name = 'ApiError'; this.status = status; }
}
export interface AuthenticationCredential { token: string; sessionEpoch?: number }
let getToken: () => Promise<string | AuthenticationCredential> = () => Promise.reject(new ApiError(401, 'Please sign in to continue.'));
let onUnauthorized: (credential: AuthenticationCredential) => void = () => {};
export function configureAuthentication(tokenProvider: () => Promise<string | AuthenticationCredential>, unauthorized: (credential: AuthenticationCredential) => void): void {
  getToken = tokenProvider; onUnauthorized = unauthorized;
}
function aborted(signal?: AbortSignal): void { signal?.throwIfAborted(); }
async function request<T>(service: Service, path: string, decode: (value: unknown) => T,
  method = 'GET', body?: unknown, signal?: AbortSignal, authenticated = true): Promise<T> {
  aborted(signal);
  const headers = new Headers({ Accept: 'application/json' });
  let credential: AuthenticationCredential | undefined;
  if (authenticated) {
    const provided = await getToken(); aborted(signal);
    credential = typeof provided === 'string' ? { token: provided } : provided;
    const { token } = credential;
    if (!token) { onUnauthorized(credential); throw new ApiError(401, 'Your session has expired. Please sign in again.'); }
    headers.set('Authorization', `Bearer ${token}`);
  }
  if (body !== undefined) headers.set('Content-Type', 'application/json');
  let response: Response;
  try {
    response = await fetch(`/api/${service}${path}`, { method, headers, ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      ...(signal ? { signal } : {}), credentials: 'omit', cache: 'no-store' });
  } catch (error) {
    if (signal?.aborted || (error instanceof DOMException && error.name === 'AbortError')) throw error;
    throw new ApiError(0, method === 'GET' ? 'Could not reach the service. Check your connection and try again.' :
      'The request could not be confirmed. Its outcome may be unknown; check the resource before retrying.');
  }
  aborted(signal);
  if (!response.ok) {
    if (response.status === 401 && credential) onUnauthorized(credential);
    let detail = response.status === 401 ? 'Your session has expired. Please sign in again.' :
      response.status === 403 ? 'You do not have permission to perform this action.' :
      response.status === 404 ? 'The requested resource was not found.' :
      response.status === 409 ? 'The operation conflicts with the current state. Refresh the resource and check your input.' :
      response.status === 400 ? 'The service could not accept this request. Check the input.' : 'The service could not complete the request.';
    // Authentication failures retain their clear actionable messages. Business Problem Details are plain text in React.
    if (response.status !== 401 && response.status !== 403) {
      try {
        const problem: unknown = await response.json();
        aborted(signal);
        if (problem && typeof problem === 'object' && 'detail' in problem && typeof problem.detail === 'string' && problem.detail.trim())
          detail = problem.detail.slice(0, 500);
      } catch (error) {
        if (signal?.aborted || (error instanceof DOMException && error.name === 'AbortError')) throw error;
        // A non-JSON error response still has a useful status-specific message.
      }
    }
    throw new ApiError(response.status, detail);
  }
  try {
    const value: unknown = await response.json();
    aborted(signal);
    return decode(value);
  }
  catch (error) {
    if (signal?.aborted || (error instanceof DOMException && error.name === 'AbortError')) throw error;
    throw new ApiError(502, method === 'GET' ? 'The service returned an unexpected response. Please refresh and try again.' :
      'The service accepted the request, but its result could not be read. Check the resource before submitting another mutation.');
  }
}
export const api = {
  getOrder: (id: string, signal?: AbortSignal) => request('order', `/api/orders/${encodeURIComponent(id)}`, decodeOrder, 'GET', undefined, signal),
  createOrder: (lines: OrderLineInput[], signal?: AbortSignal) => request('order', '/api/orders', decodeOrder, 'POST', { lines }, signal),
  getStock: (sku: string, signal?: AbortSignal) => request('inventory', `/api/stock-items/${encodeURIComponent(sku)}`, decodeStock, 'GET', undefined, signal),
  createStock: (sku: string, onHand: number, signal?: AbortSignal) => request('inventory', '/api/stock-items', decodeStock, 'POST', { sku, onHand }, signal),
  updateStock: (sku: string, onHand: number, signal?: AbortSignal) => request('inventory', `/api/stock-items/${encodeURIComponent(sku)}`, decodeStock, 'PUT', { onHand }, signal),
  getFulfillment: (id: string, signal?: AbortSignal) => request('fulfillment', `/api/fulfillments/${encodeURIComponent(id)}`, decodeFulfillment, 'GET', undefined, signal),
  getFulfillmentByOrder: (orderId: string, signal?: AbortSignal) => request('fulfillment', `/api/fulfillments/orders/${encodeURIComponent(orderId)}`, decodeFulfillment, 'GET', undefined, signal),
  shipFulfillment: (id: string, shipmentReference: string, signal?: AbortSignal) => request('fulfillment', `/api/fulfillments/${encodeURIComponent(id)}/ship`, decodeFulfillment, 'POST', { shipmentReference }, signal),
  health: (service: Service, signal?: AbortSignal) => request(service, '/actuator/health', decodeHealth, 'GET', undefined, signal, false),
};
