import { useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { Link } from 'react-router'
import { ApiError, api } from '../../shared/api/client'
import { isQuantity, maximumInteger, skuPattern, uuidPattern } from '../../shared/api/contracts'
import type { OrderLineInput, OrderView } from '../../shared/api/contracts'
import { ErrorMessage, Field, LoadingState, PageHeader, StatusBadge } from '../../shared/ui'

type Line = { key: number; sku: string; quantity: string }
type RequestState = { controller: AbortController; kind: 'lookup' | 'create' }
const newLine = (key: number): Line => ({ key, sku: '', quantity: '1' })
const errorMessage = (error: unknown) => error instanceof Error ? error.message : 'The request could not be completed.'

export default function OrdersPage({ canOperate }: { canOperate: boolean }) {
  const [lookupId, setLookupId] = useState('')
  const [lookupError, setLookupError] = useState('')
  const [lines, setLines] = useState<Line[]>([newLine(0)])
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [order, setOrder] = useState<OrderView | null>(null)
  const [error, setError] = useState('')
  const [unresolvedCreate, setUnresolvedCreate] = useState('')
  const unresolvedCreateRef = useRef(false)
  const focusNewOrder = useRef(false)
  const [pending, setPending] = useState<RequestState['kind'] | null>(null)
  const request = useRef<RequestState | null>(null)
  const nextKey = useRef(1)
  const lookupInput = useRef<HTMLInputElement>(null)
  const createForm = useRef<HTMLFormElement>(null)
  const addButton = useRef<HTMLButtonElement>(null)

  useEffect(() => () => { request.current?.controller.abort(); request.current = null }, [])

  useEffect(() => {
    if (focusNewOrder.current) {
      focusNewOrder.current = false
      createForm.current?.querySelector<HTMLInputElement>('input')?.focus()
    }
  }, [lines])

  function startAnotherOrder() {
    if (!canOperate || request.current || !unresolvedCreateRef.current) return
    unresolvedCreateRef.current = false
    setUnresolvedCreate(''); setErrors({}); setError('')
    focusNewOrder.current = true
    setLines([newLine(nextKey.current++)])
  }

  function cancelLookup() {
    if (request.current?.kind === 'lookup') {
      request.current.controller.abort()
      request.current = null
      setPending(null)
    }
  }

  async function lookup(id: string) {
    if (request.current) return
    if (!uuidPattern.test(id)) {
      setLookupError('Enter a complete order UUID, including hyphens.')
      lookupInput.current?.focus()
      return
    }
    const active: RequestState = { controller: new AbortController(), kind: 'lookup' }
    request.current = active
    setPending('lookup'); setError(''); setLookupError(''); setOrder(null)
    try {
      const result = await api.getOrder(id, active.controller.signal)
      if (request.current === active && !active.controller.signal.aborted) setOrder(result)
    } catch (failure) {
      if (request.current === active && !active.controller.signal.aborted) setError(errorMessage(failure))
    } finally {
      if (request.current === active) { request.current = null; setPending(null) }
    }
  }

  function addLine() {
    if (!canOperate || request.current || unresolvedCreateRef.current || lines.length >= 100) return
    const line = newLine(nextKey.current++)
    setLines(current => current.length < 100 ? [...current, line] : current)
  }

  function editLine(key: number, field: 'sku' | 'quantity', value: string) {
    if (!canOperate || request.current || unresolvedCreateRef.current) return
    setLines(current => current.map(line => line.key === key ? { ...line, [field]: value } : line))
    setErrors(current => ({ ...current, [`${field}-${key}`]: '' }))
  }

  async function create(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!canOperate || request.current || unresolvedCreateRef.current) return
    const validation: Record<string, string> = {}
    for (const line of lines) {
      if (!skuPattern.test(line.sku)) validation[`sku-${line.key}`] = 'Use 1–64 ASCII letters, digits, dots, underscores or hyphens, starting with a letter or digit.'
      if (!isQuantity(line.quantity)) validation[`quantity-${line.key}`] = `Enter a whole number from 1 to ${maximumInteger}.`
    }
    setErrors(validation)
    if (Object.keys(validation).length) {
      const first = Object.keys(validation)[0]
      createForm.current?.querySelector<HTMLInputElement>(`[id="${first}"]`)?.focus()
      return
    }
    const active: RequestState = { controller: new AbortController(), kind: 'create' }
    request.current = active
    setPending('create'); setError(''); setOrder(null)
    const payload: OrderLineInput[] = lines.map(({ sku, quantity }) => ({ sku, quantity: Number(quantity) }))
    try {
      const result = await api.createOrder(payload, active.controller.signal)
      if (request.current === active && !active.controller.signal.aborted) {
        setOrder(result); setLookupId(result.id); setLookupError('')
        setLines([newLine(nextKey.current++)])
      }
    } catch (failure) {
      if (request.current === active && !active.controller.signal.aborted) {
        if (failure instanceof ApiError && (failure.status === 0 || failure.status === 502)) {
          unresolvedCreateRef.current = true
          setUnresolvedCreate(errorMessage(failure))
        } else setError(errorMessage(failure))
      }
    } finally {
      if (request.current === active) { request.current = null; setPending(null) }
    }
  }

  return <>
    <PageHeader title="Orders" description="Create an order or look up its current inventory decision by ID." />
    <section className="panel" aria-labelledby="lookup-heading">
      <h2 id="lookup-heading">Look up an order</h2>
      <form noValidate onSubmit={event => { event.preventDefault(); void lookup(lookupId) }}>
        <Field id="order-id" label="Order ID" hint="Enter the UUID returned when the order was created." error={lookupError}>
          <input ref={lookupInput} id="order-id" value={lookupId} disabled={pending === 'create'} spellCheck={false}
            aria-invalid={Boolean(lookupError)} aria-describedby={`order-id-hint${lookupError ? ' order-id-error' : ''}`}
            onChange={event => { cancelLookup(); setLookupId(event.target.value); setLookupError(''); setOrder(null); setError('') }} />
        </Field>
        <button className="button" disabled={pending !== null}>Look up order</button>
      </form>
    </section>

    {canOperate ? <section className="panel" aria-labelledby="create-heading">
      <h2 id="create-heading">Create an order</h2>
      <p>Include 1–100 lines. Inventory is checked asynchronously after creation.</p>
      {unresolvedCreate && <>
        <ErrorMessage title="Order creation outcome is unresolved" message={unresolvedCreate} />
        <p>The original order may already exist. Starting another order does not cancel or retry the original request and may create a duplicate.</p>
        <button type="button" className="button button--secondary" disabled={pending !== null} onClick={startAnotherOrder}>I understand; start another order</button>
      </>}
      <form ref={createForm} noValidate onSubmit={event => { void create(event) }} aria-label="Create order">
        {lines.map((line, index) => <fieldset key={line.key} disabled={pending !== null || Boolean(unresolvedCreate)}>
          <legend>Line {index + 1}</legend>
          <div className="form-grid">
            <Field id={`sku-${line.key}`} label={`SKU for line ${index + 1}`} error={errors[`sku-${line.key}`] ?? ''}>
              <input id={`sku-${line.key}`} value={line.sku} spellCheck={false} aria-invalid={Boolean(errors[`sku-${line.key}`])}
                aria-describedby={errors[`sku-${line.key}`] ? `sku-${line.key}-error` : undefined}
                onChange={event => editLine(line.key, 'sku', event.target.value)} />
            </Field>
            <Field id={`quantity-${line.key}`} label={`Quantity for line ${index + 1}`} error={errors[`quantity-${line.key}`] ?? ''}>
              <input id={`quantity-${line.key}`} inputMode="numeric" value={line.quantity} aria-invalid={Boolean(errors[`quantity-${line.key}`])}
                aria-describedby={errors[`quantity-${line.key}`] ? `quantity-${line.key}-error` : undefined}
                onChange={event => editLine(line.key, 'quantity', event.target.value)} />
            </Field>
          </div>
          <button type="button" className="button button--secondary" disabled={lines.length === 1 || pending !== null || Boolean(unresolvedCreate)}
            aria-label={`Remove line ${index + 1}`} onClick={() => {
              if (!canOperate || request.current || unresolvedCreateRef.current || lines.length <= 1) return
              setLines(current => current.length > 1 ? current.filter(candidate => candidate.key !== line.key) : current); addButton.current?.focus()
            }}>Remove line</button>
        </fieldset>)}
        <div className="button-group">
          <button ref={addButton} type="button" className="button button--secondary" disabled={lines.length >= 100 || pending !== null || Boolean(unresolvedCreate)}
            onClick={addLine}>Add line</button>
          <button className="button" disabled={pending !== null || Boolean(unresolvedCreate)}>Create order</button>
        </div>
      </form>
    </section> : <p className="scope-note">Viewer access: look up and refresh orders. Creating orders requires an operator role.</p>}

    <section className="panel" aria-labelledby="result-heading" aria-busy={pending !== null}>
      <h2 id="result-heading">Order details</h2>
      {pending && <LoadingState label={pending === 'create' ? 'Creating order…' : 'Loading order…'} />}
      {error && <ErrorMessage message={error} />}
      {!pending && !order && !error && <p className="empty-state">Look up an order to see its current state.</p>}
      {order && <>
        <div role="status"><StatusBadge tone={order.status === 'CONFIRMED' ? 'success' : order.status === 'REJECTED' ? 'danger' : 'warning'}>{order.status}</StatusBadge></div>
        <p>{order.status === 'PENDING_INVENTORY' ? 'Waiting for the inventory decision. Refresh to read the latest state.' :
          order.status === 'CONFIRMED' ? 'Inventory confirmed this order.' : 'Inventory rejected this order.'}</p>
        <dl className="record-details">
          <div><dt>Order ID</dt><dd className="inline-code">{order.id}</dd></div>
          <div><dt>Created</dt><dd><time dateTime={order.createdAt}>{order.createdAt}</time></dd></div>
          <div><dt>Updated</dt><dd><time dateTime={order.updatedAt}>{order.updatedAt}</time></dd></div>
        </dl>
        <div className="table-scroll"><table><caption>Order lines</caption><thead><tr><th scope="col">Line ID</th><th scope="col">SKU</th><th scope="col">Quantity</th></tr></thead>
          <tbody>{order.lines.map(line => <tr key={line.id}><td className="inline-code">{line.id}</td><td>{line.sku}</td><td>{line.quantity}</td></tr>)}</tbody>
        </table></div>
        <div className="button-group">
          <button type="button" className="button button--secondary" disabled={pending !== null} onClick={() => { void lookup(order.id) }}>Refresh order</button>
          {order.status === 'CONFIRMED' && <Link to={`/fulfillment?orderId=${encodeURIComponent(order.id)}`}>Look up fulfillment</Link>}
        </div>
      </>}
    </section>
  </>
}
