import { StrictMode } from 'react'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, useNavigate } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { api, ApiError } from '../../shared/api/client'
import type { FulfillmentView } from '../../shared/api/contracts'
import FulfillmentPage from './FulfillmentPage'

const fulfillmentId = 'aaaaaaaa-1111-2222-3333-444444444444'
const orderId = 'bbbbbbbb-1111-2222-3333-444444444444'
const secondId = 'cccccccc-1111-2222-3333-444444444444'
const secondOrderId = 'dddddddd-1111-2222-3333-444444444444'
const pending: FulfillmentView = {
  id: fulfillmentId, orderId, reservationId: 'eeeeeeee-1111-2222-3333-444444444444',
  confirmationEventId: 'ffffffff-1111-2222-3333-444444444444', status: 'PENDING',
  createdAt: '2026-10-14T10:00:00Z', updatedAt: '2026-10-14T10:00:01Z', shippedAt: null, shipmentReference: null,
  lines: [{ orderLineId: '11111111-1111-2222-3333-444444444444', sku: 'SKU-A', quantity: 3 }],
}
const shipped: FulfillmentView = { ...pending, status: 'SHIPPED', shipmentReference: 'REF-1', shippedAt: '2026-10-14T11:00:00Z', updatedAt: '2026-10-14T11:00:00Z' }
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
function Navigation() {
  const navigate = useNavigate()
  return <><button onClick={() => void navigate(`/fulfillment?orderId=${secondOrderId}`)}>Other order link</button><button onClick={() => void navigate('/fulfillment')}>Clear query</button></>
}
function page(canOperate = true, path = '/fulfillment') {
  return <MemoryRouter initialEntries={[path]}><Navigation /><FulfillmentPage canOperate={canOperate} /></MemoryRouter>
}
async function load(canOperate = true) {
  const view = render(page(canOperate))
  const user = userEvent.setup()
  await user.type(screen.getByRole('textbox', { name: 'Fulfillment ID' }), fulfillmentId)
  await user.click(screen.getByRole('button', { name: 'Look up fulfillment' }))
  await screen.findByRole('heading', { name: 'Fulfillment details' })
  return { user, ...view }
}
async function enterReference(value = 'REF-1') {
  await userEvent.setup().type(screen.getByRole('textbox', { name: 'Shipment reference' }), value)
}
beforeEach(() => {
  vi.restoreAllMocks()
  vi.spyOn(api, 'getFulfillment').mockResolvedValue(pending)
  vi.spyOn(api, 'getFulfillmentByOrder').mockResolvedValue(pending)
  vi.spyOn(api, 'shipFulfillment').mockResolvedValue(shipped)
})

describe('fulfillment lookup and actual record presentation', () => {
  it('starts with an honest empty state and no requests', () => {
    render(page())
    expect(screen.getByText('Enter a fulfillment ID or order ID to read its current state.')).toBeInTheDocument()
    expect(api.getFulfillment).not.toHaveBeenCalled()
    expect(api.getFulfillmentByOrder).not.toHaveBeenCalled()
    expect(screen.queryByRole('form', { name: 'Manual dispatch' })).not.toBeInTheDocument()
  })
  it.each(['', 'aaaaaaaa111122223333444444444444', fulfillmentId.toUpperCase(), ` ${fulfillmentId}`, `${fulfillmentId} `])('rejects noncanonical lookup input %j before any request', async value => {
    const user = userEvent.setup()
    render(page())
    if (value) await user.type(screen.getByRole('textbox', { name: 'Fulfillment ID' }), value)
    await user.click(screen.getByRole('button', { name: 'Look up fulfillment' }))
    const input = screen.getByRole('textbox', { name: 'Fulfillment ID' })
    expect(input).toHaveAttribute('aria-invalid', 'true')
    expect(input).toHaveAccessibleDescription(expect.stringContaining('canonical UUID'))
    expect(input).toHaveFocus()
    expect(api.getFulfillment).not.toHaveBeenCalled()
  })
  it('looks up by order ID and presents all returned identifiers, lines and timestamps', async () => {
    const user = userEvent.setup()
    render(page(false))
    await user.selectOptions(screen.getByRole('combobox', { name: 'Look up by' }), 'order')
    await user.type(screen.getByRole('textbox', { name: 'Order ID' }), orderId)
    await user.click(screen.getByRole('button', { name: 'Look up fulfillment' }))
    await screen.findByRole('heading', { name: 'Fulfillment details' })
    expect(api.getFulfillmentByOrder).toHaveBeenCalledWith(orderId, expect.any(AbortSignal))
    for (const value of [fulfillmentId, orderId, pending.reservationId, pending.confirmationEventId, pending.createdAt, pending.updatedAt, 'PENDING', 'Not shipped', 'Not assigned']) expect(screen.getByText(value)).toBeInTheDocument()
    const table = screen.getByRole('table', { name: 'Fulfillment lines' })
    expect(within(table).getByText(pending.lines[0]!.orderLineId)).toBeInTheDocument()
    expect(within(table).getByText('SKU-A')).toBeInTheDocument()
    expect(within(table).getByText('3')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Fulfillment details' })).toHaveFocus()
  })
  it('loads a validated order query automatically', async () => {
    render(page(false, `/fulfillment?orderId=${orderId}`))
    await screen.findByRole('heading', { name: 'Fulfillment details' })
    expect(screen.getByRole('textbox', { name: 'Order ID' })).toHaveValue(orderId)
    expect(api.getFulfillmentByOrder).toHaveBeenCalledTimes(1)
    expect(api.getFulfillmentByOrder).toHaveBeenCalledWith(orderId, expect.any(AbortSignal))
  })
  it.each([`${orderId.toUpperCase()}`, 'bad', '', `${orderId}&orderId=${secondOrderId}`])('rejects invalid or ambiguous order query %j', async query => {
    render(page(false, `/fulfillment?orderId=${query}`))
    expect(await screen.findByText('Enter a complete canonical UUID using lowercase letters.')).toBeInTheDocument()
    expect(api.getFulfillmentByOrder).not.toHaveBeenCalled()
    expect(screen.getByRole('textbox', { name: 'Order ID' })).toHaveFocus()
  })
  it.each([[401, 'Your session has expired. Please sign in again.'], [403, 'You do not have permission to perform this action.'], [404, 'The requested resource was not found.'], [409, 'The operation conflicts with the current state.'], [0, 'Could not reach the service.']])('shows read error %s without manufacturing a record', async (status, detail) => {
    vi.mocked(api.getFulfillmentByOrder).mockRejectedValue(new ApiError(status as number, detail as string))
    render(page(false, `/fulfillment?orderId=${orderId}`))
    expect(await screen.findByRole('alert')).toHaveTextContent(detail as string)
    expect(screen.queryByRole('heading', { name: 'Fulfillment details' })).not.toBeInTheDocument()
    expect(screen.getByText(/temporary missing result/)).toBeInTheDocument()
    await waitFor(() => expect(screen.getByRole('alert').parentElement).toHaveFocus())
  })
  it('shows loading, aborts superseded reads and ignores late responses', async () => {
    const first = deferred<FulfillmentView>()
    const second = deferred<FulfillmentView>()
    vi.mocked(api.getFulfillmentByOrder).mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise)
    const user = userEvent.setup()
    render(page(false, `/fulfillment?orderId=${orderId}`))
    expect(await screen.findByRole('status')).toHaveTextContent('Loading fulfillment…')
    const signal = vi.mocked(api.getFulfillmentByOrder).mock.calls[0]![1]!
    await user.click(screen.getByRole('button', { name: 'Other order link' }))
    expect(signal.aborted).toBe(true)
    await act(async () => second.resolve({ ...pending, id: secondId, orderId: secondOrderId }))
    expect(screen.getByText(secondId)).toBeInTheDocument()
    await act(async () => first.resolve(pending))
    expect(screen.queryByText(fulfillmentId)).not.toBeInTheDocument()
  })
  it('aborts unmounted reads and suppresses only cancellation', async () => {
    const request = deferred<FulfillmentView>()
    vi.mocked(api.getFulfillmentByOrder).mockReturnValue(request.promise)
    const view = render(page(false, `/fulfillment?orderId=${orderId}`))
    await waitFor(() => expect(api.getFulfillmentByOrder).toHaveBeenCalledTimes(1))
    const signal = vi.mocked(api.getFulfillmentByOrder).mock.calls[0]![1]!
    view.unmount()
    expect(signal.aborted).toBe(true)
    await act(async () => request.reject(new DOMException('Cancelled', 'AbortError')))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })
  it('clears a pending query lookup when the query is removed', async () => {
    const request = deferred<FulfillmentView>()
    vi.mocked(api.getFulfillmentByOrder).mockReturnValue(request.promise)
    render(page(false, `/fulfillment?orderId=${orderId}`))
    await waitFor(() => expect(api.getFulfillmentByOrder).toHaveBeenCalledTimes(1))
    await userEvent.setup().click(screen.getByRole('button', { name: 'Clear query' }))
    expect(vi.mocked(api.getFulfillmentByOrder).mock.calls[0]![1]!.aborted).toBe(true)
    await act(async () => request.resolve(pending))
    expect(screen.queryByRole('heading', { name: 'Fulfillment details' })).not.toBeInTheDocument()
    expect(screen.queryByText('Loading fulfillment…')).not.toBeInTheDocument()
  })
  it('loads a query once under StrictMode and keeps later query changes usable', async () => {
    const user = userEvent.setup()
    render(<StrictMode>{page(false, `/fulfillment?orderId=${orderId}`)}</StrictMode>)
    await screen.findByRole('heading', { name: 'Fulfillment details' })
    expect(api.getFulfillmentByOrder).toHaveBeenCalledTimes(1)
    vi.mocked(api.getFulfillmentByOrder).mockResolvedValue({ ...pending, id: secondId, orderId: secondOrderId })
    await user.click(screen.getByRole('button', { name: 'Other order link' }))
    expect(await screen.findByText(secondId)).toBeInTheDocument()
    expect(api.getFulfillmentByOrder).toHaveBeenCalledTimes(2)
  })
  it('suppresses an actual active AbortError but surfaces other failures', async () => {
    vi.mocked(api.getFulfillmentByOrder).mockRejectedValueOnce(new DOMException('Cancelled', 'AbortError')).mockRejectedValueOnce(new Error('Unexpected lookup failure.'))
    render(page(false, `/fulfillment?orderId=${orderId}`))
    await waitFor(() => expect(api.getFulfillmentByOrder).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(screen.queryByText('Loading fulfillment…')).not.toBeInTheDocument())
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    await userEvent.setup().click(screen.getByRole('button', { name: 'Look up fulfillment' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('Unexpected lookup failure.')
  })
  it('invalidates pending reads when lookup fields change', async () => {
    const request = deferred<FulfillmentView>()
    vi.mocked(api.getFulfillment).mockReturnValue(request.promise)
    const user = userEvent.setup()
    render(page())
    await user.type(screen.getByRole('textbox', { name: 'Fulfillment ID' }), fulfillmentId)
    await user.click(screen.getByRole('button', { name: 'Look up fulfillment' }))
    await user.clear(screen.getByRole('textbox', { name: 'Fulfillment ID' }))
    expect(vi.mocked(api.getFulfillment).mock.calls[0]![1]!.aborted).toBe(true)
    await act(async () => request.resolve(pending))
    expect(screen.queryByRole('heading', { name: 'Fulfillment details' })).not.toBeInTheDocument()
  })
})

describe('operator manual dispatch', () => {
  it('provides viewer reads without dispatch controls or requests', async () => {
    await load(false)
    expect(screen.getByText('Viewer access: dispatch requires an operator.')).toBeInTheDocument()
    expect(screen.queryByRole('form', { name: 'Manual dispatch' })).not.toBeInTheDocument()
    expect(api.shipFulfillment).not.toHaveBeenCalled()
  })
  it('removes dispatch controls when permission changes', async () => {
    const view = await load()
    await enterReference()
    view.rerender(page(false))
    expect(screen.queryByRole('button', { name: 'Confirm dispatch' })).not.toBeInTheDocument()
    expect(api.shipFulfillment).not.toHaveBeenCalled()
  })
  it.each(['', '-ref', '_ref', 'réf', 'bad ref', 'a'.repeat(65), 'ref/1'])('rejects invalid reference %j in the handler', async reference => {
    await load()
    if (reference) await enterReference(reference)
    fireEvent.submit(screen.getByRole('form', { name: 'Manual dispatch' }))
    const input = screen.getByRole('textbox', { name: 'Shipment reference' })
    expect(input).toHaveAttribute('aria-invalid', 'true')
    expect(input).toHaveAccessibleDescription(expect.stringContaining('Enter a valid shipment reference.'))
    expect(input).toHaveFocus()
    expect(api.shipFulfillment).not.toHaveBeenCalled()
  })
  it.each(['A', 'a'.repeat(64), 'Ref._-123'])('accepts exact reference boundary %s and shows confirmed returned shipment', async reference => {
    vi.mocked(api.shipFulfillment).mockResolvedValue({ ...shipped, shipmentReference: reference })
    await load()
    await enterReference(reference)
    await userEvent.setup().click(screen.getByRole('button', { name: 'Confirm dispatch' }))
    expect(api.shipFulfillment).toHaveBeenCalledExactlyOnceWith(fulfillmentId, reference)
    expect(await screen.findByText('SHIPPED')).toBeInTheDocument()
    expect(screen.getByText(reference)).toBeInTheDocument()
    expect(screen.getAllByText(shipped.shippedAt!)).toHaveLength(2)
    expect(screen.queryByRole('form', { name: 'Manual dispatch' })).not.toBeInTheDocument()
    expect(screen.getByRole('status')).toHaveTextContent('Dispatch confirmed by the service.')
  })
  it('renders an already shipped record without another dispatch form', async () => {
    vi.mocked(api.getFulfillment).mockResolvedValue(shipped)
    await load()
    expect(screen.getByText('SHIPPED')).toBeInTheDocument()
    expect(screen.getByText('REF-1')).toBeInTheDocument()
    expect(screen.queryByRole('form', { name: 'Manual dispatch' })).not.toBeInTheDocument()
  })
  it('uses a synchronous duplicate guard and captures the target while blocking conflicting edits and lookups', async () => {
    const request = deferred<FulfillmentView>()
    vi.mocked(api.shipFulfillment).mockReturnValue(request.promise)
    const { user } = await load()
    await enterReference()
    const form = screen.getByRole('form', { name: 'Manual dispatch' })
    act(() => { fireEvent.submit(form); fireEvent.submit(form) })
    expect(api.shipFulfillment).toHaveBeenCalledExactlyOnceWith(fulfillmentId, 'REF-1')
    expect(screen.getByRole('textbox', { name: 'Fulfillment ID' })).toBeDisabled()
    expect(screen.getByRole('textbox', { name: 'Shipment reference' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Refresh fulfillment' })).toBeDisabled()
    fireEvent.change(screen.getByRole('textbox', { name: 'Shipment reference' }), { target: { value: 'OTHER' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'Fulfillment ID' }), { target: { value: secondId } })
    fireEvent.submit(screen.getByRole('form', { name: 'Fulfillment lookup' }))
    expect(screen.getByRole('textbox', { name: 'Shipment reference' })).toHaveValue('REF-1')
    expect(screen.getByRole('textbox', { name: 'Fulfillment ID' })).toHaveValue(fulfillmentId)
    await user.click(screen.getByRole('button', { name: 'Other order link' }))
    expect(api.getFulfillmentByOrder).not.toHaveBeenCalled()
    expect(api.getFulfillment).toHaveBeenCalledTimes(1)
    vi.mocked(api.getFulfillmentByOrder).mockResolvedValue({ ...pending, id: secondId, orderId: secondOrderId })
    await act(async () => request.resolve(shipped))
    await waitFor(() => expect(api.getFulfillmentByOrder).toHaveBeenCalledWith(secondOrderId, expect.any(AbortSignal)))
    expect(await screen.findByText(secondId)).toBeInTheDocument()
  })
  it('prevents a stale form submitting after lookup input changes in the same event batch', async () => {
    await load()
    await enterReference()
    const form = screen.getByRole('form', { name: 'Manual dispatch' })
    act(() => {
      fireEvent.change(screen.getByRole('textbox', { name: 'Fulfillment ID' }), { target: { value: secondId } })
      fireEvent.submit(form)
    })
    expect(api.shipFulfillment).not.toHaveBeenCalled()
    expect(screen.queryByRole('heading', { name: 'Fulfillment details' })).not.toBeInTheDocument()
  })
  it('guards a programmatic dispatch while refresh is loading', async () => {
    await load()
    await enterReference()
    const request = deferred<FulfillmentView>()
    vi.mocked(api.getFulfillment).mockReturnValue(request.promise)
    act(() => {
      fireEvent.click(screen.getByRole('button', { name: 'Refresh fulfillment' }))
      fireEvent.submit(screen.getByRole('form', { name: 'Manual dispatch' }))
    })
    expect(api.shipFulfillment).not.toHaveBeenCalled()
    await act(async () => request.resolve(pending))
  })
  it.each([401, 403, 400])('shows rejected dispatch %s without retries', async status => {
    vi.mocked(api.shipFulfillment).mockRejectedValue(new ApiError(status, `Dispatch rejected (${status}).`))
    await load()
    await enterReference()
    await userEvent.setup().click(screen.getByRole('button', { name: 'Confirm dispatch' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(`Dispatch rejected (${status}).`)
    expect(api.shipFulfillment).toHaveBeenCalledTimes(1)
    expect(screen.getByText('PENDING')).toBeInTheDocument()
  })
  it.each([0, 502, 503, 409])('requires same-record reconciliation after uncertain or conflicting outcome %s', async status => {
    const detail = status === 409 ? 'A different reference is already committed.' : 'The request could not be confirmed. Its outcome may be unknown; check the resource before retrying.'
    vi.mocked(api.shipFulfillment).mockRejectedValueOnce(new ApiError(status, detail))
    const { user } = await load()
    await enterReference()
    await user.click(screen.getByRole('button', { name: 'Confirm dispatch' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(detail)
    expect(screen.getByRole('button', { name: 'Confirm dispatch' })).toBeDisabled()
    fireEvent.submit(screen.getByRole('form', { name: 'Manual dispatch' }))
    fireEvent.submit(screen.getByRole('form', { name: 'Fulfillment lookup' }))
    await user.click(screen.getByRole('button', { name: 'Other order link' }))
    expect(api.getFulfillmentByOrder).not.toHaveBeenCalled()
    expect(api.shipFulfillment).toHaveBeenCalledTimes(1)
    vi.mocked(api.getFulfillment).mockRejectedValueOnce(new ApiError(0, 'Reconciliation unavailable.'))
    await user.click(screen.getByRole('button', { name: 'Refresh fulfillment' }))
    expect(await screen.findByText('Reconciliation unavailable.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Confirm dispatch' })).toBeDisabled()
    // Reconciliation always reads the captured fulfillment ID, even after an attempted route change.
    vi.mocked(api.getFulfillment).mockResolvedValueOnce(shipped)
    await user.click(screen.getByRole('button', { name: 'Refresh fulfillment' }))
    expect(api.getFulfillment).toHaveBeenLastCalledWith(fulfillmentId, expect.any(AbortSignal))
    expect(api.shipFulfillment).toHaveBeenCalledTimes(1)
  })
  it('allows a deliberate same-reference retry only after reading the same pending record', async () => {
    vi.mocked(api.shipFulfillment).mockRejectedValueOnce(new ApiError(0, 'Outcome unknown.'))
    const { user } = await load()
    await enterReference()
    await user.click(screen.getByRole('button', { name: 'Confirm dispatch' }))
    await screen.findByRole('alert')
    await user.click(screen.getByRole('button', { name: 'Refresh fulfillment' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Confirm dispatch' })).toBeEnabled())
    expect(screen.getByRole('textbox', { name: 'Shipment reference' })).toHaveValue('REF-1')
    expect(api.shipFulfillment).toHaveBeenCalledTimes(1)
    await user.click(screen.getByRole('button', { name: 'Confirm dispatch' }))
    expect(api.shipFulfillment).toHaveBeenNthCalledWith(2, fulfillmentId, 'REF-1')
    expect(await screen.findByText('SHIPPED')).toBeInTheDocument()
  })
  it('ignores a mutation response after unmount without retrying', async () => {
    const request = deferred<FulfillmentView>()
    vi.mocked(api.shipFulfillment).mockReturnValue(request.promise)
    const view = await load()
    await enterReference()
    fireEvent.submit(screen.getByRole('form', { name: 'Manual dispatch' }))
    view.unmount()
    await act(async () => request.resolve(shipped))
    expect(api.shipFulfillment).toHaveBeenCalledTimes(1)
    expect(screen.queryByText('Dispatch confirmed by the service.')).not.toBeInTheDocument()
  })
})
