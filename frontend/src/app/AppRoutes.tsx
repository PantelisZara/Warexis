import { Link, Route, Routes } from 'react-router'
import { AppShell } from './AppShell'
import DashboardPage from '../features/dashboard/DashboardPage'

function PendingPage({ name }: { name: string }) {
  return <section className="panel"><h1>{name}</h1><p>This screen is awaiting its Phase 8 feature integration.</p><Link to="/">Return to overview</Link></section>
}
function NotFoundPage() {
  return <section className="panel"><p className="eyebrow">Page not found</p><h1>That workspace page does not exist.</h1><Link to="/">Return to overview</Link></section>
}

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<DashboardPage />} />
        <Route path="orders" element={<PendingPage name="Orders" />} />
        <Route path="inventory" element={<PendingPage name="Inventory" />} />
        <Route path="fulfillment" element={<PendingPage name="Fulfillment" />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  )
}
