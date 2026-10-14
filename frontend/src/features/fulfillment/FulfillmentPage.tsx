import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { useLocation } from 'react-router'
import { api, ApiError } from '../../shared/api/client'
import { skuPattern, uuidPattern } from '../../shared/api/contracts'
import type { FulfillmentView } from '../../shared/api/contracts'
import { ErrorMessage, Field, LoadingState, PageHeader, StatusBadge } from '../../shared/ui'

type LookupKind = 'fulfillment' | 'order'
const uuidError = 'Enter a complete canonical UUID using lowercase letters.'
const referenceHint = '1–64 ASCII letters, digits, dots, underscores or hyphens; start with a letter or digit.'
function isIdentifier(value: string) { return uuidPattern.test(value) && value === value.toLowerCase() }
function isAbort(error: unknown) { return error instanceof DOMException && error.name === 'AbortError' }
function message(error: unknown) { return error instanceof Error ? error.message : 'The service could not complete the request.' }

export default function FulfillmentPage({ canOperate }: { canOperate: boolean }) {
  const { search } = useLocation()
  const [kind, setKind] = useState<LookupKind>('fulfillment')
  const [identifier, setIdentifier] = useState('')
  const [identifierError, setIdentifierError] = useState('')
  const [record, setRecord] = useState<FulfillmentView | null>(null)
  const [loading, setLoading] = useState(false)
  const [readError, setReadError] = useState('')
  const [reference, setReference] = useState('')
  const [referenceError, setReferenceError] = useState('')
  const [dispatchError, setDispatchError] = useState('')
  const [dispatching, setDispatching] = useState(false)
  const [reconcileId, setReconcileId] = useState<string | null>(null)
  const [notice, setNotice] = useState('')
  const identifierInput = useRef<HTMLInputElement>(null)
  const referenceInput = useRef<HTMLInputElement>(null)
  const resultHeading = useRef<HTMLHeadingElement>(null)
  const errorRegion = useRef<HTMLDivElement>(null)
  const mounted = useRef(false)
  const readController = useRef<AbortController | null>(null)
  const readSequence = useRef(0)
  const mutationLock = useRef(false)
  const reading = useRef(false)
  const currentRecord = useRef<FulfillmentView | null>(null)
  const reconciliationTarget = useRef<string | null>(null)
  // A route change while dispatch is pending must not replace the captured target.
  const deferredQuery = useRef<string | null>(null)
  const appliedSearch = useRef<string | null>(null)
  const [queryRevision, setQueryRevision] = useState(0)

  const cancelRead = useCallback(() => {
    readController.current?.abort()
    reading.current = false
    readSequence.current += 1
  }, [])

  const lookup = useCallback(async (lookupKind: LookupKind, id: string, refresh = false) => {
    if (mutationLock.current || (reconciliationTarget.current !== null && (!refresh || lookupKind !== 'fulfillment' || id !== reconciliationTarget.current))) return
    cancelRead()
    setReadError('')
    setNotice('')
    if (!refresh) {
      currentRecord.current = null
      setRecord(null)
      setReference('')
      setDispatchError('')
      setReferenceError('')
    }
    const sequence = readSequence.current
    if (!isIdentifier(id)) {
      setLoading(false)
      setIdentifierError(uuidError)
      currentRecord.current = null
      setRecord(null)
      identifierInput.current?.focus()
      return
    }
    const controller = new AbortController()
    readController.current = controller
    reading.current = true
    setIdentifierError('')
    setLoading(true)
    try {
      const result = lookupKind === 'order' ? await api.getFulfillmentByOrder(id, controller.signal) : await api.getFulfillment(id, controller.signal)
      if (!mounted.current || controller.signal.aborted || sequence !== readSequence.current) return
      currentRecord.current = result
      setRecord(result)
      if (refresh && result.id === id) {
        reconciliationTarget.current = null
        setReconcileId(null)
        setDispatchError('')
        setNotice('Fulfillment refreshed. Review its current status before dispatching.')
      }
      resultHeading.current?.focus()
    } catch (error) {
      if (!mounted.current || controller.signal.aborted || sequence !== readSequence.current || isAbort(error)) return
      setReadError(message(error))
      errorRegion.current?.focus()
    } finally {
      if (mounted.current && !controller.signal.aborted && sequence === readSequence.current) {
        reading.current = false
        setLoading(false)
      }
    }
  }, [cancelRead])

  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false; cancelRead() }
  }, [cancelRead])

  useEffect(() => {
    let active = true
    // Schedule URL-driven work so cleanup can cancel an obsolete navigation before it starts.
    queueMicrotask(() => {
      if (!active || appliedSearch.current === search) return
      const params = new URLSearchParams(search)
      if (mutationLock.current || reconciliationTarget.current !== null) { deferredQuery.current = search; return }
      deferredQuery.current = null
      const previousSearch = appliedSearch.current
      appliedSearch.current = search
      if (!params.has('orderId')) {
        if (previousSearch !== null) {
          cancelRead()
          setLoading(false)
          currentRecord.current = null
          setRecord(null)
          setReadError('')
          setIdentifierError('')
          setIdentifier('')
        }
        return
      }
      const values = params.getAll('orderId')
      const id = values.length === 1 ? values[0] ?? '' : ''
      setKind('order')
      setIdentifier(id)
      void lookup('order', id)
    })
    return () => { active = false }
  }, [search, queryRevision, lookup, cancelRead, reconcileId])

  useEffect(() => {
    if (record && !loading) resultHeading.current?.focus()
  }, [record, loading])
  useEffect(() => {
    if (readError || dispatchError) errorRegion.current?.focus()
  }, [readError, dispatchError])

  function changeLookup(nextKind: LookupKind, nextIdentifier: string) {
    if (mutationLock.current || reconciliationTarget.current !== null) return
    cancelRead()
    setLoading(false)
    setKind(nextKind)
    setIdentifier(nextIdentifier)
    setIdentifierError('')
    setReadError('')
    setDispatchError('')
    setReferenceError('')
    setNotice('')
    currentRecord.current = null
    setRecord(null)
    setReference('')
  }
  function submitLookup(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (mutationLock.current || reconciliationTarget.current !== null) return
    void lookup(kind, identifier)
  }
  async function dispatch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!canOperate || mutationLock.current || reading.current || reconciliationTarget.current !== null || !record || currentRecord.current !== record || record.status !== 'PENDING') return
    if (!skuPattern.test(reference)) {
      setReferenceError('Enter a valid shipment reference. ' + referenceHint)
      referenceInput.current?.focus()
      return
    }
    const targetId = record.id
    const submittedReference = reference
    mutationLock.current = true
    cancelRead()
    setDispatching(true)
    setReferenceError('')
    setDispatchError('')
    setReadError('')
    setNotice('')
    try {
      const result = await api.shipFulfillment(targetId, submittedReference)
      if (!mounted.current) return
      currentRecord.current = result
      setRecord(result)
      setNotice('Dispatch confirmed by the service.')
    } catch (error) {
      if (!mounted.current) return
      setDispatchError(message(error))
      if (!(error instanceof ApiError) || error.status === 0 || error.status === 409 || error.status >= 500) {
        reconciliationTarget.current = targetId
        setReconcileId(targetId)
      }
    } finally {
      mutationLock.current = false
      if (mounted.current) {
        setDispatching(false)
        if (deferredQuery.current !== null) setQueryRevision(value => value + 1)
      }
    }
  }
  const locked = dispatching || reconcileId !== null
  const description = [identifierError ? 'fulfillment-id-error' : '', 'fulfillment-id-hint'].filter(Boolean).join(' ')
  return (
    <>
      <PageHeader title="Fulfillment" description="Look up a fulfillment and review its actual dispatch status." eyebrow="Operations" />
      <section className="panel" aria-labelledby="lookup-heading">
        <div className="panel-heading"><div><h2 id="lookup-heading">Find a fulfillment</h2><p>Fulfillments are created after order confirmation. A temporary missing result may mean processing is still underway.</p></div></div>
        <form onSubmit={submitLookup} noValidate aria-label="Fulfillment lookup">
          <div className="form-grid">
            <Field id="fulfillment-kind" label="Look up by"><select id="fulfillment-kind" value={kind} disabled={locked} onChange={event => changeLookup(event.target.value as LookupKind, identifier)}><option value="fulfillment">Fulfillment ID</option><option value="order">Order ID</option></select></Field>
            <Field id="fulfillment-id" label={kind === 'order' ? 'Order ID' : 'Fulfillment ID'} hint="Use the full lowercase UUID, including hyphens." error={identifierError}>
              <input ref={identifierInput} id="fulfillment-id" value={identifier} disabled={locked} onChange={event => changeLookup(kind, event.target.value)} aria-invalid={!!identifierError} aria-describedby={description} autoComplete="off" spellCheck={false} />
            </Field>
          </div>
          <button className="button" type="submit" disabled={locked}>Look up fulfillment</button>
        </form>
      </section>
      <div ref={errorRegion} tabIndex={-1}>
        {readError && <ErrorMessage title="Lookup failed" message={readError} />}
        {dispatchError && <ErrorMessage title="Dispatch not confirmed" message={dispatchError} />}
      </div>
      {loading && <LoadingState label="Loading fulfillment…" />}
      {notice && <p role="status" className="message">{notice}</p>}
      {!record && !loading && !readError && <p className="empty-state">Enter a fulfillment ID or order ID to read its current state.</p>}
      {record && <section className="panel" aria-labelledby="fulfillment-record-heading" aria-busy={loading || dispatching}>
        <div className="panel-heading"><div><h2 ref={resultHeading} tabIndex={-1} id="fulfillment-record-heading">Fulfillment details</h2><StatusBadge tone={record.status === 'SHIPPED' ? 'success' : 'warning'}>{record.status}</StatusBadge></div>
          <button className="button button--secondary" type="button" disabled={dispatching || loading} onClick={() => { if (!mutationLock.current) void lookup('fulfillment', record.id, true) }}>Refresh fulfillment</button>
        </div>
        {reconcileId !== null && <p role="status" className="message">Refresh this fulfillment before another dispatch or lookup. Its outcome must be reconciled. If it remains pending, a deliberate retry with the same reference is idempotent.</p>}
        <dl className="record-details">
          <div><dt>Fulfillment ID</dt><dd className="inline-code">{record.id}</dd></div>
          <div><dt>Order ID</dt><dd className="inline-code">{record.orderId}</dd></div>
          <div><dt>Reservation ID</dt><dd className="inline-code">{record.reservationId}</dd></div>
          <div><dt>Confirmation event ID</dt><dd className="inline-code">{record.confirmationEventId}</dd></div>
          <div><dt>Created at</dt><dd><time dateTime={record.createdAt}>{record.createdAt}</time></dd></div>
          <div><dt>Updated at</dt><dd><time dateTime={record.updatedAt}>{record.updatedAt}</time></dd></div>
          <div><dt>Shipped at</dt><dd>{record.shippedAt ? <time dateTime={record.shippedAt}>{record.shippedAt}</time> : 'Not shipped'}</dd></div>
          <div><dt>Shipment reference</dt><dd>{record.shipmentReference ?? 'Not assigned'}</dd></div>
        </dl>
        <div className="table-scroll"><table><caption>Fulfillment lines</caption><thead><tr><th scope="col">Order line ID</th><th scope="col">SKU</th><th scope="col">Quantity</th></tr></thead><tbody>{record.lines.map(line => <tr key={line.orderLineId}><td className="inline-code">{line.orderLineId}</td><td>{line.sku}</td><td>{line.quantity}</td></tr>)}</tbody></table></div>
        {!canOperate && <p className="scope-note">Viewer access: dispatch requires an operator.</p>}
        {canOperate && record.status === 'PENDING' && <form onSubmit={dispatch} noValidate aria-label="Manual dispatch">
          <h3>Manual dispatch</h3>
          <p>Record the shipment reference when dispatching this fulfillment. Reusing the same reference is idempotent; a different committed reference conflicts.</p>
          <Field id="shipment-reference" label="Shipment reference" hint={referenceHint} error={referenceError}>
            <input ref={referenceInput} id="shipment-reference" value={reference} disabled={locked || loading} onChange={event => { if (!mutationLock.current && reconciliationTarget.current === null && !reading.current) { setReference(event.target.value); setReferenceError('') } }} aria-invalid={!!referenceError} aria-describedby={`shipment-reference-hint${referenceError ? ' shipment-reference-error' : ''}`} autoComplete="off" spellCheck={false} />
          </Field>
          <button className="button" type="submit" disabled={locked || loading}>{dispatching ? 'Dispatching…' : 'Confirm dispatch'}</button>
        </form>}
        {dispatching && <LoadingState label="Confirming dispatch…" />}
      </section>}
    </>
  )
}
