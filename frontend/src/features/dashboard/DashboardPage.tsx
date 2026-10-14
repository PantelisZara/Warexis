import { Link } from 'react-router'
import { PageHeader, StatusBadge } from '../../shared/ui'

export type ServiceHealth = { service: 'Order' | 'Inventory' | 'Fulfillment'; status: 'checking' | 'up' | 'unavailable' | 'not-checked' }
const areas = [
  { title: 'Orders', to: '/orders', step: '01', description: 'Create an order, look up its current status, and follow its fulfillment.' },
  { title: 'Inventory', to: '/inventory', step: '02', description: 'Look up stock by SKU and manage available quantities.' },
  { title: 'Fulfillment', to: '/fulfillment', step: '03', description: 'Find an order’s fulfillment and record its shipment reference.' },
] as const

export default function DashboardPage({ health, onRefresh }: { health?: ServiceHealth[]; onRefresh?: () => void }) {
  return (
    <>
      <PageHeader title="Operations overview" description="Keep orders, stock, and fulfillment moving through one focused workspace." eyebrow="Your workspace" />
      <section aria-labelledby="workspace-heading">
        <h2 className="section-heading" id="workspace-heading">Choose a workspace</h2>
        <div className="workspace-grid">
          {areas.map(({ title, to, step, description }) => (
            <Link className="workspace-card" to={to} key={to}>
              <span className="workspace-card-number" aria-hidden="true">{step}</span>
              <h3>{title}</h3><p>{description}</p><span className="workspace-card-action">Open {title.toLowerCase()} <span aria-hidden="true">↗</span></span>
            </Link>
          ))}
        </div>
      </section>
      <section className="panel service-panel" aria-labelledby="health-heading">
        <div className="panel-heading"><div><h2 id="health-heading">Service availability</h2><p>Availability reports connectivity, not order or reservation progress.</p></div>{onRefresh && <button className="button button--secondary" type="button" onClick={onRefresh}>Refresh health</button>}</div>
        {health ? <ul className="service-list">{health.map(({ service, status }) => <li key={service}><span>{service}</span><StatusBadge tone={status === 'up' ? 'success' : status === 'unavailable' ? 'danger' : 'neutral'}>{status === 'up' ? 'Available' : status === 'unavailable' ? 'Unavailable' : status === 'checking' ? 'Checking…' : 'Not checked'}</StatusBadge></li>)}</ul> : <p className="empty-state">Service health has not been checked.</p>}
      </section>
      <p className="scope-note">Look up records by order ID, fulfillment ID, or SKU.</p>
    </>
  )
}
