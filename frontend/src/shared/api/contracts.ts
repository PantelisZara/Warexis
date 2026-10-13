export interface OrderLineInput { sku: string; quantity: number }
export interface OrderView {
  id: string; status: 'PENDING_INVENTORY' | 'CONFIRMED' | 'REJECTED';
  createdAt: string; updatedAt: string; lines: (OrderLineInput & { id: string })[];
}
export interface StockItem { id: string; sku: string; onHand: number; reserved: number; available: number }
export interface FulfillmentView {
  id: string; orderId: string; reservationId: string; confirmationEventId: string;
  status: 'PENDING' | 'SHIPPED'; createdAt: string; updatedAt: string;
  shippedAt: string | null; shipmentReference: string | null;
  lines: (OrderLineInput & { orderLineId: string })[];
}
export const skuPattern = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
export const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const maximumInteger = 2_147_483_647;
export function isQuantity(value: string, minimum = 1): boolean {
  return /^\d+$/.test(value) && Number.isSafeInteger(Number(value)) && Number(value) >= minimum && Number(value) <= maximumInteger;
}
function invalid(): never { throw new Error('The service returned an unexpected response.'); }
function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return invalid();
  return value as Record<string, unknown>;
}
function text(value: unknown): string { return typeof value === 'string' ? value : invalid(); }
function identifier(value: unknown): string { const s = text(value); return uuidPattern.test(s) ? s : invalid(); }
function sku(value: unknown): string { const s = text(value); return skuPattern.test(s) ? s : invalid(); }
function integer(value: unknown, minimum = 0): number {
  return typeof value === 'number' && Number.isInteger(value) && value >= minimum && value <= maximumInteger ? value : invalid();
}
function instant(value: unknown): string {
  const s = text(value);
  return /^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:\d{2})$/.test(s) && Number.isFinite(Date.parse(s)) ? s : invalid();
}
function lines(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) && value.length > 0 && value.length <= 100 ? value.map(record) : invalid();
}
export function decodeOrder(value: unknown): OrderView {
  const d = record(value);
  if (d.status !== 'PENDING_INVENTORY' && d.status !== 'CONFIRMED' && d.status !== 'REJECTED') return invalid();
  return { id: identifier(d.id), status: d.status, createdAt: instant(d.createdAt), updatedAt: instant(d.updatedAt),
    lines: lines(d.lines).map(l => ({ id: identifier(l.id), sku: sku(l.sku), quantity: integer(l.quantity, 1) })) };
}
export function decodeStock(value: unknown): StockItem {
  const d = record(value); const onHand = integer(d.onHand); const reserved = integer(d.reserved); const available = integer(d.available);
  if (onHand - reserved !== available) return invalid();
  return { id: identifier(d.id), sku: sku(d.sku), onHand, reserved, available };
}
export function decodeFulfillment(value: unknown): FulfillmentView {
  const d = record(value);
  if (d.status !== 'PENDING' && d.status !== 'SHIPPED') return invalid();
  const shippedAt = d.shippedAt === null ? null : instant(d.shippedAt);
  const shipmentReference = d.shipmentReference === null ? null : sku(d.shipmentReference);
  if ((d.status === 'PENDING' && (shippedAt !== null || shipmentReference !== null)) ||
      (d.status === 'SHIPPED' && (shippedAt === null || shipmentReference === null))) return invalid();
  return { id: identifier(d.id), orderId: identifier(d.orderId), reservationId: identifier(d.reservationId),
    confirmationEventId: identifier(d.confirmationEventId), status: d.status, createdAt: instant(d.createdAt), updatedAt: instant(d.updatedAt),
    shippedAt, shipmentReference,
    lines: lines(d.lines).map(l => ({ orderLineId: identifier(l.orderLineId), sku: sku(l.sku), quantity: integer(l.quantity, 1) })) };
}
export function decodeHealth(value: unknown): { status: string } {
  const d = record(value); return { status: text(d.status) };
}
