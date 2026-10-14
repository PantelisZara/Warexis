import { useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { api, ApiError } from '../../shared/api/client'
import { isQuantity, maximumInteger, skuPattern } from '../../shared/api/contracts'
import type { StockItem } from '../../shared/api/contracts'
import { ErrorMessage, Field, LoadingState, PageHeader } from '../../shared/ui'

const skuHint = '1–64 ASCII letters, digits, dots, underscores or hyphens; start with a letter or digit.'
const quantityHint = `Whole number from 0 to ${maximumInteger.toLocaleString('en-US')}.`
function message(error: unknown): string {
  return error instanceof Error ? error.message : 'The service could not complete the request.'
}
function aborted(error: unknown, signal: AbortSignal): boolean {
  return signal.aborted || (error instanceof DOMException && error.name === 'AbortError')
}

export default function InventoryPage({ canOperate }: { canOperate: boolean }) {
  const [lookupSku, setLookupSku] = useState('')
  const [lookupInvalid, setLookupInvalid] = useState(false)
  const [stock, setStock] = useState<StockItem | null>(null)
  const [loading, setLoading] = useState(false)
  const [readError, setReadError] = useState<string | null>(null)
  const [readComplete, setReadComplete] = useState(false)
  const [createSku, setCreateSku] = useState('')
  const [createOnHand, setCreateOnHand] = useState('')
  const [createErrors, setCreateErrors] = useState({ sku: '', quantity: '' })
  const [updateOnHand, setUpdateOnHand] = useState('')
  const [updateError, setUpdateError] = useState('')
  const [mutationError, setMutationError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [pending, setPending] = useState(false)
  const [reconcileSku, setReconcileSku] = useState<string | null>(null)
  const readController = useRef<AbortController | null>(null)
  const mutationController = useRef<AbortController | null>(null)
  const readVersion = useRef(0)
  const mutationInFlight = useRef(false)
  const lookupInput = useRef<HTMLInputElement>(null)
  const createSkuInput = useRef<HTMLInputElement>(null)
  const createQuantityInput = useRef<HTMLInputElement>(null)
  const updateInput = useRef<HTMLInputElement>(null)

  useEffect(() => () => {
    readVersion.current += 1
    readController.current?.abort()
    mutationController.current?.abort()
  }, [])

  function cancelRead() {
    readVersion.current += 1
    readController.current?.abort()
    setLoading(false)
  }

  function editLookup(value: string) {
    if (mutationInFlight.current) return
    cancelRead()
    setLookupSku(value)
    setStock(null)
    setReadError(null)
    setReadComplete(false)
    setLookupInvalid(false)
    setNotice(null)
    setUpdateError('')
  }

  async function lookup(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (mutationInFlight.current) return
    if (!skuPattern.test(lookupSku)) {
      setLookupInvalid(true)
      lookupInput.current?.focus()
      return
    }
    cancelRead()
    const version = readVersion.current
    const controller = new AbortController()
    const targetSku = lookupSku
    readController.current = controller
    setLookupInvalid(false)
    setStock(null)
    setReadError(null)
    setReadComplete(false)
    setNotice(null)
    setLoading(true)
    try {
      const result = await api.getStock(targetSku, controller.signal)
      if (controller.signal.aborted || version !== readVersion.current) return
      setStock(result)
      setUpdateOnHand(String(result.onHand))
      setUpdateError('')
      if (targetSku === reconcileSku) {
        setReconcileSku(null)
        setMutationError(null)
      }
    } catch (error) {
      if (aborted(error, controller.signal) || version !== readVersion.current) return
      setReadError(message(error))
      if (error instanceof ApiError && error.status === 404 && targetSku === reconcileSku) {
        setReconcileSku(null)
        setMutationError(null)
      }
    } finally {
      if (!controller.signal.aborted && version === readVersion.current) {
        setLoading(false)
        setReadComplete(true)
      }
    }
  }

  async function mutate(kind: 'create' | 'update', event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!canOperate || mutationInFlight.current || reconcileSku) return
    const targetSku = kind === 'create' ? createSku : stock?.sku
    const quantity = kind === 'create' ? createOnHand : updateOnHand
    if (kind === 'create') {
      const errors = { sku: skuPattern.test(createSku) ? '' : 'Enter a valid SKU.',
        quantity: isQuantity(quantity, 0) ? '' : `Enter a whole number from 0 to ${maximumInteger.toLocaleString('en-US')}.` }
      setCreateErrors(errors)
      if (errors.sku || errors.quantity) {
        (errors.sku ? createSkuInput : createQuantityInput).current?.focus()
        return
      }
    } else {
      if (!stock || !targetSku) return
      const error = !isQuantity(quantity, 0) ? `Enter a whole number from 0 to ${maximumInteger.toLocaleString('en-US')}.` :
        Number(quantity) < stock.reserved ? `On-hand stock cannot be less than the last retrieved reserved quantity (${stock.reserved}).` : ''
      setUpdateError(error)
      if (error) { updateInput.current?.focus(); return }
    }
    if (!targetSku) return
    mutationInFlight.current = true
    cancelRead()
    setPending(true)
    setMutationError(null)
    setNotice(null)
    const controller = new AbortController()
    mutationController.current = controller
    try {
      const result = kind === 'create' ? await api.createStock(targetSku, Number(quantity), controller.signal) :
        await api.updateStock(targetSku, Number(quantity), controller.signal)
      if (controller.signal.aborted) return
      setLookupSku(targetSku)
      setLookupInvalid(false)
      setStock(result)
      setUpdateOnHand(String(result.onHand))
      setReadError(null)
      setReadComplete(true)
      setNotice(`${kind === 'create' ? 'Created' : 'Updated'} stock for ${targetSku}.`)
    } catch (error) {
      if (aborted(error, controller.signal)) return
      setMutationError(message(error))
      if (error instanceof ApiError && [0, 409, 502].includes(error.status)) setReconcileSku(targetSku)
      if (kind === 'update' && error instanceof ApiError && error.status === 404) setStock(null)
    } finally {
      mutationInFlight.current = false
      if (!controller.signal.aborted) setPending(false)
    }
  }

  return <>
    <PageHeader title="Inventory" description="Look up stock by SKU and manage on-hand quantities. Reservations are managed by the inventory workflow." />
    <section className="panel" aria-labelledby="stock-lookup-heading">
      <h2 id="stock-lookup-heading">Look up stock</h2>
      <form onSubmit={lookup} noValidate aria-label="Stock lookup">
        <Field id="inventory-lookup-sku" label="Lookup SKU" hint={skuHint} error={lookupInvalid ? 'Enter a valid SKU.' : ''}>
          <input ref={lookupInput} id="inventory-lookup-sku" value={lookupSku} disabled={pending} onChange={event => editLookup(event.target.value)}
            aria-invalid={lookupInvalid} aria-describedby={`inventory-lookup-sku-hint${lookupInvalid ? ' inventory-lookup-sku-error' : ''}`} />
        </Field>
        <button className="button button--secondary" disabled={pending} type="submit">Look up stock</button>
      </form>
      {loading && <LoadingState label="Loading stock…" />}
      {readError && <ErrorMessage title="Stock lookup failed" message={readError} />}
      {!stock && !loading && !readError && <p className="empty-state">{readComplete ? 'No stock item is displayed.' : 'Enter a SKU to retrieve its current quantities.'}</p>}
      {stock && <section aria-labelledby="stock-details-heading">
        <h3 id="stock-details-heading">Stock for {stock.sku}</h3>
        <dl className="record-details">
          <div><dt>Stock ID</dt><dd className="inline-code">{stock.id}</dd></div>
          <div><dt>SKU</dt><dd>{stock.sku}</dd></div>
          <div><dt>On hand</dt><dd>{stock.onHand}</dd></div>
          <div><dt>Reserved</dt><dd>{stock.reserved}</dd></div>
          <div><dt>Available</dt><dd>{stock.available}</dd></div>
        </dl>
        <p className="scope-note">These are the last retrieved quantities. Look up this SKU again to refresh them.</p>
        {canOperate && <form onSubmit={event => mutate('update', event)} noValidate aria-label="Update stock">
          <Field id="inventory-update-quantity" label="New on-hand quantity" hint={`Must be at least the last retrieved reserved quantity (${stock.reserved}). ${quantityHint}`} error={updateError}>
            <input ref={updateInput} id="inventory-update-quantity" inputMode="numeric" value={updateOnHand} disabled={pending || !!reconcileSku}
              onChange={event => { setUpdateOnHand(event.target.value); setUpdateError('') }} aria-invalid={!!updateError}
              aria-describedby={`inventory-update-quantity-hint${updateError ? ' inventory-update-quantity-error' : ''}`} />
          </Field>
          <button className="button" disabled={pending || !!reconcileSku} type="submit">Update on-hand stock</button>
        </form>}
      </section>}
    </section>
    {canOperate ? <section className="panel" aria-labelledby="stock-create-heading">
      <h2 id="stock-create-heading">Create stock item</h2>
      <form onSubmit={event => mutate('create', event)} noValidate aria-label="Create stock">
        <div className="form-grid">
          <Field id="inventory-create-sku" label="New SKU" hint={skuHint} error={createErrors.sku}>
            <input ref={createSkuInput} id="inventory-create-sku" value={createSku} disabled={pending || !!reconcileSku}
              onChange={event => { setCreateSku(event.target.value); setCreateErrors(old => ({ ...old, sku: '' })) }} aria-invalid={!!createErrors.sku}
              aria-describedby={`inventory-create-sku-hint${createErrors.sku ? ' inventory-create-sku-error' : ''}`} />
          </Field>
          <Field id="inventory-create-quantity" label="Initial on-hand quantity" hint={quantityHint} error={createErrors.quantity}>
            <input ref={createQuantityInput} id="inventory-create-quantity" inputMode="numeric" value={createOnHand} disabled={pending || !!reconcileSku}
              onChange={event => { setCreateOnHand(event.target.value); setCreateErrors(old => ({ ...old, quantity: '' })) }} aria-invalid={!!createErrors.quantity}
              aria-describedby={`inventory-create-quantity-hint${createErrors.quantity ? ' inventory-create-quantity-error' : ''}`} />
          </Field>
        </div>
        <button className="button" disabled={pending || !!reconcileSku} type="submit">Create stock item</button>
      </form>
    </section> : <p className="scope-note">Read-only access. An Operator can create stock items and change on-hand quantities.</p>}
    {pending && <LoadingState label="Saving stock…" />}
    {mutationError && <ErrorMessage title="Stock change failed" message={mutationError} />}
    {reconcileSku && <p role="status">Look up {reconcileSku} before another stock change. Do not resubmit without checking the current state.</p>}
    {notice && <p role="status">{notice}</p>}
  </>
}
