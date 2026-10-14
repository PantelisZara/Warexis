import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, ApiError, configureAuthentication } from './client';
import { decodeOrder, decodeStock, decodeFulfillment, isQuantity } from './contracts';
const id = '00000000-0000-0000-0000-000000000001';
const stock = { id, sku: 'SKU-1', onHand: 10, reserved: 3, available: 7 };
afterEach(() => vi.unstubAllGlobals());
describe('authenticated API client', () => {
  it('refreshes before attaching one Bearer header and uses the real proxy route', async () => {
    const provider = vi.fn().mockResolvedValue('unit-test-token'); const fetcher = vi.fn().mockResolvedValue(Response.json(stock));
    configureAuthentication(provider, vi.fn()); vi.stubGlobal('fetch', fetcher);
    expect(await api.getStock('SKU-1')).toEqual(stock); expect(provider).toHaveBeenCalledTimes(1);
    const [url, options] = fetcher.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('/api/inventory/api/stock-items/SKU-1'); expect(new Headers(options.headers).get('Authorization')).toBe('Bearer unit-test-token');
    expect(options.credentials).toBe('omit'); expect(options.cache).toBe('no-store');
  });
  it('does not send a request when refresh fails', async () => {
    const fetcher = vi.fn(); vi.stubGlobal('fetch', fetcher); configureAuthentication(() => Promise.reject(new ApiError(401, 'Session expired')), vi.fn());
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 }); expect(fetcher).not.toHaveBeenCalled();
  });
  it('clears unauthorized authentication but keeps forbidden distinct', async () => {
    const expired = vi.fn(); configureAuthentication(() => Promise.resolve('unit-test-token'), expired);
    const fetcher = vi.fn().mockResolvedValue(new Response('', { status: 401 })); vi.stubGlobal('fetch', fetcher);
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 }); expect(expired).toHaveBeenCalledOnce();
    expired.mockClear(); fetcher.mockResolvedValue(new Response('', { status: 403 }));
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 403 }); expect(expired).not.toHaveBeenCalled();
  });
  it('passes the exact requesting credential to 401 handling, including its session epoch', async () => {
    const credential = { token: 'unit-token-old', sessionEpoch: 2 }; const unauthorized = vi.fn();
    configureAuthentication(() => Promise.resolve(credential), unauthorized);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 401 })));
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 401 });
    expect(unauthorized).toHaveBeenCalledExactlyOnceWith(credential);
  });
  it('checks cancellation before invalidating a late unauthorized response', async () => {
    const controller = new AbortController(); const unauthorized = vi.fn();
    configureAuthentication(() => Promise.resolve('unit-test-token'), unauthorized);
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => { controller.abort(); return new Response('', { status: 401 }); }));
    await expect(api.getStock('SKU-1', controller.signal)).rejects.toMatchObject({ name: 'AbortError' });
    expect(unauthorized).not.toHaveBeenCalled();
  });
  it('does not expire authentication when an anonymous health request returns 401', async () => {
    const unauthorized = vi.fn(); const token = vi.fn(); configureAuthentication(token, unauthorized);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 401 })));
    await expect(api.health('order')).rejects.toMatchObject({ status: 401 });
    expect(token).not.toHaveBeenCalled(); expect(unauthorized).not.toHaveBeenCalled();
  });
  it('parses business Problem Details and does not retry an uncertain mutation', async () => {
    configureAuthentication(() => Promise.resolve('unit-test-token'), vi.fn());
    const fetcher = vi.fn().mockResolvedValue(Response.json({ detail: 'On-hand quantity cannot be less than reserved stock.' }, { status: 409 })); vi.stubGlobal('fetch', fetcher);
    await expect(api.updateStock('SKU-1', 1)).rejects.toMatchObject({ status: 409, message: 'On-hand quantity cannot be less than reserved stock.' });
    fetcher.mockRejectedValue(new TypeError('Network error'));
    await expect(api.createStock('SKU-1', 10)).rejects.toMatchObject({ status: 0, message: expect.stringContaining('outcome may be unknown') });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });
  it('honors cancellation before token refresh and after it without sending', async () => {
    const controller = new AbortController(); const fetcher = vi.fn(); const token = vi.fn().mockResolvedValue('unit-test-token');
    vi.stubGlobal('fetch', fetcher); configureAuthentication(token, vi.fn()); controller.abort();
    await expect(api.getOrder(id, controller.signal)).rejects.toMatchObject({ name: 'AbortError' }); expect(token).not.toHaveBeenCalled(); expect(fetcher).not.toHaveBeenCalled();
    const next = new AbortController(); configureAuthentication(async () => { next.abort(); return 'unit-test-token'; }, vi.fn());
    await expect(api.getOrder(id, next.signal)).rejects.toMatchObject({ name: 'AbortError' }); expect(fetcher).not.toHaveBeenCalled();
  });
  it('preserves cancellation while reading a response body', async () => {
    const controller = new AbortController(); configureAuthentication(() => Promise.resolve('unit-test-token'), vi.fn());
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: () => {
      controller.abort(); return Promise.reject(new DOMException('Cancelled', 'AbortError'));
    } }));
    await expect(api.getOrder(id, controller.signal)).rejects.toMatchObject({ name: 'AbortError' });
  });
  it('honors cancellation even if a response body resolves after abort instead of rejecting', async () => {
    const controller = new AbortController(); configureAuthentication(() => Promise.resolve('unit-test-token'), vi.fn());
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => { controller.abort(); return stock; } }));
    await expect(api.getStock('SKU-1', controller.signal)).rejects.toMatchObject({ name: 'AbortError' });
    const next = new AbortController();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 409, json: async () => { next.abort(); return { detail: 'Conflict' }; } }));
    await expect(api.updateStock('SKU-1', 10, next.signal)).rejects.toMatchObject({ name: 'AbortError' });
  });
  it('preserves cancellation while reading an error response body', async () => {
    const controller = new AbortController(); configureAuthentication(() => Promise.resolve('unit-test-token'), vi.fn());
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 409, json: () => {
      controller.abort(); return Promise.reject(new DOMException('Cancelled', 'AbortError'));
    } }));
    await expect(api.updateStock('SKU-1', 10, controller.signal)).rejects.toMatchObject({ name: 'AbortError' });
  });
  it('does not suggest resubmitting a committed mutation when its response cannot be decoded', async () => {
    configureAuthentication(() => Promise.resolve('unit-test-token'), vi.fn());
    const fetcher = vi.fn().mockResolvedValue(Response.json({ unexpected: true }, { status: 201 })); vi.stubGlobal('fetch', fetcher);
    await expect(api.createOrder([{ sku: 'SKU-1', quantity: 1 }])).rejects.toMatchObject({ status: 502, message: expect.stringContaining('accepted the request') });
    expect(fetcher).toHaveBeenCalledOnce();
    fetcher.mockResolvedValue({ ok: true, json: () => Promise.reject(new SyntaxError('Unreadable JSON')) });
    await expect(api.createStock('SKU-1', 10)).rejects.toMatchObject({ message: expect.stringContaining('before submitting another mutation') });
    expect(fetcher).toHaveBeenCalledTimes(2);
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ message: expect.stringContaining('refresh and try again') });
  });
  it('does not authenticate anonymous health queries and rejects malformed business JSON', async () => {
    const token = vi.fn(); configureAuthentication(token, vi.fn()); vi.stubGlobal('fetch', vi.fn().mockResolvedValue(Response.json({ status: 'UP' })));
    expect(await api.health('order')).toEqual({ status: 'UP' }); expect(token).not.toHaveBeenCalled();
    configureAuthentication(() => Promise.resolve('unit-test-token'), vi.fn()); vi.stubGlobal('fetch', vi.fn().mockResolvedValue(Response.json({ ...stock, available: 100 })));
    await expect(api.getStock('SKU-1')).rejects.toMatchObject({ status: 502 });
  });
});
describe('runtime DTO validation', () => {
  it('accepts the actual Order shape and rejects unsupported states/quantities', () => {
    const order = { id, status: 'CONFIRMED', createdAt: '2026-10-13T12:00:00Z', updatedAt: '2026-10-13T12:00:01Z', lines: [{ id, sku: 'SKU-1', quantity: 1 }] };
    expect(decodeOrder(order)).toEqual(order); expect(() => decodeOrder({ ...order, status: 'SHIPPED' })).toThrow();
    expect(() => decodeOrder({ ...order, lines: [{ id, sku: 'SKU-1', quantity: '1' }] })).toThrow();
  });
  it('validates Inventory arithmetic and nullable fulfillment shipment facts', () => {
    expect(decodeStock(stock)).toEqual(stock); expect(() => decodeStock({ ...stock, reserved: 11, available: -1 })).toThrow();
    const pending = { id, orderId: id, reservationId: id, confirmationEventId: id, status: 'PENDING', createdAt: '2026-10-13T12:00:00Z', updatedAt: '2026-10-13T12:00:00Z', shippedAt: null, shipmentReference: null, lines: [{ orderLineId: id, sku: 'SKU-1', quantity: 1 }] };
    expect(decodeFulfillment(pending)).toEqual(pending); expect(() => decodeFulfillment({ ...pending, status: 'SHIPPED' })).toThrow();
  });
  it('keeps input quantities inside backend integer constraints', () => {
    expect(isQuantity('1')).toBe(true); expect(isQuantity('0', 0)).toBe(true);
    for (const value of ['0', '-1', '1.5', '1e3', '2147483648', '']) expect(isQuantity(value)).toBe(false);
  });
});
