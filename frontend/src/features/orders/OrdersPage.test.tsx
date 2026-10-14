import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError, api } from '../../shared/api/client'
import type { OrderView } from '../../shared/api/contracts'
import OrdersPage from './OrdersPage'

vi.mock('../../shared/api/client', async importOriginal => ({
  ...await importOriginal<typeof import('../../shared/api/client')>(),
  api: { getOrder: vi.fn(), createOrder: vi.fn() },
}))
const id = '12345678-1234-1234-1234-123456789abc'
const otherId = '22345678-1234-1234-1234-123456789abc'
const order: OrderView = {
  id, status: 'PENDING_INVENTORY', createdAt: '2026-10-14T08:00:00Z', updatedAt: '2026-10-14T08:01:00Z',
  lines: [{ id: otherId, sku: 'SKU-1', quantity: 2 }],
}
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
function mount(canOperate = true) {
  return render(<MemoryRouter><OrdersPage canOperate={canOperate} /></MemoryRouter>)
}
function fillLine(sku = 'SKU-1', quantity = '2', line = 1) {
  fireEvent.change(screen.getByLabelText(`SKU for line ${line}`), { target: { value: sku } })
  fireEvent.change(screen.getByLabelText(`Quantity for line ${line}`), { target: { value: quantity } })
}
function lookup(value = id) {
  fireEvent.change(screen.getByLabelText('Order ID'), { target: { value } })
  fireEvent.click(screen.getByRole('button', { name: 'Look up order' }))
}

beforeEach(() => vi.resetAllMocks())

describe('Orders operations', () => {
  it('starts empty, makes no unsolicited requests and hides mutations for viewers', () => {
    mount(false)
    expect(screen.getByText('Look up an order to see its current state.')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Create order' })).not.toBeInTheDocument()
    expect(api.getOrder).not.toHaveBeenCalled()
    expect(api.createOrder).not.toHaveBeenCalled()
  })

  it('validates UUID input and focuses its associated error', () => {
    mount(false); lookup('not-an-id')
    expect(screen.getByLabelText('Order ID')).toHaveFocus()
    expect(screen.getByLabelText('Order ID')).toHaveAccessibleDescription(/complete order UUID/)
    expect(api.getOrder).not.toHaveBeenCalled()
  })

  it.each(['', ' SKU', 'SKU ', '_SKU', 'éclair', 'A'.repeat(65)])('rejects invalid ASCII SKU %j without submitting', sku => {
    mount(); fillLine(sku)
    fireEvent.click(screen.getByRole('button', { name: 'Create order' }))
    expect(screen.getByLabelText('SKU for line 1')).toHaveFocus()
    expect(screen.getByLabelText('SKU for line 1')).toHaveAttribute('aria-invalid', 'true')
    expect(screen.getByLabelText('SKU for line 1')).toHaveAccessibleDescription(/ASCII/)
    expect(api.createOrder).not.toHaveBeenCalled()
  })

  it.each(['', '0', '-1', '1.5', '1e2', '2147483648', ' 2 '])('rejects out-of-contract quantity %j', quantity => {
    mount(); fillLine('SKU', quantity)
    fireEvent.click(screen.getByRole('button', { name: 'Create order' }))
    expect(screen.getByLabelText('Quantity for line 1')).toHaveFocus()
    expect(screen.getByLabelText('Quantity for line 1')).toHaveAccessibleDescription(/2147483647/)
    expect(api.createOrder).not.toHaveBeenCalled()
  })

  it('creates duplicate SKU lines with int32 boundary quantities and displays the actual response', async () => {
    vi.mocked(api.createOrder).mockResolvedValue(order)
    mount(); fillLine('A._-9', '2147483647')
    fireEvent.click(screen.getByRole('button', { name: 'Add line' })); fillLine('A._-9', '1', 2)
    fireEvent.click(screen.getByRole('button', { name: 'Create order' }))
    expect(await screen.findByText('PENDING_INVENTORY')).toBeInTheDocument()
    expect(api.createOrder).toHaveBeenCalledWith([{ sku: 'A._-9', quantity: 2147483647 }, { sku: 'A._-9', quantity: 1 }], expect.any(AbortSignal))
    expect(screen.getByText(id)).toBeInTheDocument()
    expect(screen.getByText(order.createdAt)).toHaveAttribute('datetime', order.createdAt)
    expect(screen.getByText(order.updatedAt)).toBeInTheDocument()
    expect(within(screen.getByRole('table')).getByText('SKU-1')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: 'Look up fulfillment' })).not.toBeInTheDocument()
  })

  it('keeps line count between 1 and 100 and preserves remaining values when removing', () => {
    mount(); fillLine('KEEP')
    expect(screen.getByRole('button', { name: 'Remove line 1' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Add line' })); fillLine('REMOVE', '2', 2)
    fireEvent.click(screen.getByRole('button', { name: 'Remove line 2' }))
    expect(screen.getByLabelText('SKU for line 1')).toHaveValue('KEEP')
    expect(screen.getByRole('button', { name: 'Add line' })).toHaveFocus()
    const add = screen.getByRole('button', { name: 'Add line' })
    act(() => { for (let n = 0; n < 110; n++) fireEvent.click(add) })
    expect(screen.getAllByRole('group')).toHaveLength(100)
    expect(screen.getByRole('button', { name: 'Add line' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Add line' }))
    expect(screen.getAllByRole('group')).toHaveLength(100)
    fireEvent.click(screen.getByRole('button', { name: 'Remove line 100' }))
    expect(screen.getByRole('button', { name: 'Add line' })).toBeEnabled()
  })

  it.each(['CONFIRMED', 'REJECTED'] as const)('reads actual %s state for viewers and explicitly refreshes', async status => {
    vi.mocked(api.getOrder).mockResolvedValue({ ...order, status })
    mount(false); lookup()
    expect(await screen.findByText(status)).toBeInTheDocument()
    expect(api.getOrder).toHaveBeenCalledTimes(1)
    if (status === 'CONFIRMED') expect(screen.getByRole('link', { name: 'Look up fulfillment' })).toHaveAttribute('href', `/fulfillment?orderId=${id}`)
    fireEvent.click(screen.getByRole('button', { name: 'Refresh order' }))
    await screen.findByText(status)
    expect(api.getOrder).toHaveBeenCalledTimes(2)
  })

  it.each([401, 403, 404, 409])('keeps backend %i error visible', async status => {
    vi.mocked(api.getOrder).mockRejectedValue(new ApiError(status, `Actual service error ${status}`))
    mount(false); lookup()
    expect(await screen.findByRole('alert')).toHaveTextContent(`Actual service error ${status}`)
    expect(screen.queryByRole('table')).not.toBeInTheDocument()
  })

  it('cancels changing lookups and prevents a stale result overwriting the new response', async () => {
    const old = deferred<OrderView>()
    vi.mocked(api.getOrder).mockReturnValueOnce(old.promise).mockResolvedValueOnce({ ...order, id: otherId, status: 'REJECTED' })
    mount(false); lookup()
    const signal = vi.mocked(api.getOrder).mock.calls[0]?.[1]
    expect(screen.getByText('Loading order…')).toBeInTheDocument()
    lookup(otherId)
    expect(signal?.aborted).toBe(true)
    expect(await screen.findByText('REJECTED')).toBeInTheDocument()
    await act(async () => { old.resolve(order); await old.promise })
    expect(screen.queryByText('PENDING_INVENTORY')).not.toBeInTheDocument()
    expect(screen.getByText(otherId, { selector: 'dd' })).toBeInTheDocument()
  })

  it('ignores a cancelled stale error and aborts pending lookups on unmount', async () => {
    const old = deferred<OrderView>(); const next = deferred<OrderView>()
    vi.mocked(api.getOrder).mockReturnValueOnce(old.promise).mockReturnValueOnce(next.promise)
    const view = mount(false); lookup()
    lookup(otherId)
    await act(async () => { old.reject(new ApiError(403, 'Old permission error')); await old.promise.catch(() => {}) })
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    const signal = vi.mocked(api.getOrder).mock.calls[1]?.[1]
    view.unmount()
    expect(signal?.aborted).toBe(true)
    await act(async () => { next.resolve(order); await next.promise })
  })

  it('synchronously blocks duplicate create submissions and competing lookup requests', async () => {
    const result = deferred<OrderView>(); vi.mocked(api.createOrder).mockReturnValue(result.promise)
    mount(); fillLine()
    const form = screen.getByRole('form', { name: 'Create order' })
    act(() => { fireEvent.submit(form); fireEvent.submit(form) })
    expect(api.createOrder).toHaveBeenCalledTimes(1)
    expect(screen.getByLabelText('Order ID')).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Look up order' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Add line' })).toBeDisabled()
    expect(screen.getByLabelText('SKU for line 1')).toBeDisabled()
    fireEvent.submit(screen.getByRole('button', { name: 'Look up order' }).closest('form')!)
    expect(api.getOrder).not.toHaveBeenCalled()
    await act(async () => { result.resolve(order); await result.promise })
    expect(screen.getByText('PENDING_INVENTORY')).toBeInTheDocument()
  })

  it('aborts an unmounted creation and ignores its late result', async () => {
    const result = deferred<OrderView>(); vi.mocked(api.createOrder).mockReturnValue(result.promise)
    const view = mount(); fillLine()
    fireEvent.click(screen.getByRole('button', { name: 'Create order' }))
    const signal = vi.mocked(api.createOrder).mock.calls[0]?.[1]
    view.unmount()
    expect(signal?.aborted).toBe(true)
    await act(async () => { result.resolve(order); await result.promise })
    expect(api.createOrder).toHaveBeenCalledTimes(1)
  })

  it('blocks creation during lookups, including direct form submissions', async () => {
    const result = deferred<OrderView>(); vi.mocked(api.getOrder).mockReturnValue(result.promise)
    mount(); fillLine(); lookup()
    fireEvent.submit(screen.getByRole('form', { name: 'Create order' }))
    expect(api.createOrder).not.toHaveBeenCalled()
    await act(async () => { result.resolve(order); await result.promise })
  })

  it('guards an old create form handler after operator permission is removed', () => {
    const view = mount(); fillLine()
    const oldForm = screen.getByRole('form', { name: 'Create order' })
    view.rerender(<MemoryRouter><OrdersPage canOperate={false} /></MemoryRouter>)
    fireEvent.submit(oldForm)
    expect(api.createOrder).not.toHaveBeenCalled()
    expect(screen.queryByRole('form', { name: 'Create order' })).not.toBeInTheDocument()
  })

  it.each([0, 502])('requires acknowledgement and a fresh form after unresolved create status %i', async status => {
    const message = 'The original creation outcome could not be confirmed.'
    vi.mocked(api.createOrder).mockRejectedValueOnce(new ApiError(status, message)).mockResolvedValueOnce(order)
    vi.mocked(api.getOrder).mockResolvedValue({ ...order, status: 'CONFIRMED' })
    mount(); fillLine()
    const form = screen.getByRole('form', { name: 'Create order' })
    fireEvent.submit(form)
    expect(await screen.findByRole('alert')).toHaveTextContent(message)
    expect(screen.getByRole('button', { name: 'Create order' })).toBeDisabled()
    expect(screen.getByLabelText('SKU for line 1')).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Add line' })).toBeDisabled()
    fireEvent.submit(form)
    fireEvent.change(screen.getByLabelText('SKU for line 1'), { target: { value: 'UNSAFE-EDIT' } })
    expect(screen.getByLabelText('SKU for line 1')).toHaveValue('SKU-1')
    expect(api.createOrder).toHaveBeenCalledTimes(1)
    // Looking up a known resource must not dismiss an unrelated unresolved creation.
    lookup(otherId)
    expect(await screen.findByText('CONFIRMED')).toBeInTheDocument()
    expect(screen.getByRole('alert')).toHaveTextContent(message)
    fireEvent.submit(form)
    expect(api.createOrder).toHaveBeenCalledTimes(1)
    await userEvent.setup().click(screen.getByRole('button', { name: 'I understand; start another order' }))
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getByLabelText('SKU for line 1')).toHaveValue('')
    expect(screen.getByLabelText('SKU for line 1')).toHaveFocus()
    expect(screen.getByLabelText('Quantity for line 1')).toHaveValue('1')
    // Acknowledgement does not replay the old payload or submit an empty new order.
    fireEvent.submit(form)
    expect(api.createOrder).toHaveBeenCalledTimes(1)
    fillLine('NEW-ORDER', '3')
    fireEvent.submit(form)
    expect(await screen.findByText('PENDING_INVENTORY')).toBeInTheDocument()
    expect(api.createOrder).toHaveBeenCalledTimes(2)
    expect(api.createOrder).toHaveBeenLastCalledWith([{ sku: 'NEW-ORDER', quantity: 3 }], expect.any(AbortSignal))
  })

  it.each([
    'The request could not be confirmed. Its outcome may be unknown; check the resource before retrying.',
    'The service accepted the request, but its result could not be read. Check the resource before submitting another mutation.',
  ])('preserves mutation uncertainty without retrying: %s', async message => {
    vi.mocked(api.createOrder).mockRejectedValue(new ApiError(0, message))
    mount(); fillLine(); await userEvent.setup().click(screen.getByRole('button', { name: 'Create order' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(message)
    await waitFor(() => expect(api.createOrder).toHaveBeenCalledTimes(1))
    expect(screen.getByLabelText('SKU for line 1')).toHaveValue('SKU-1')
  })
})
