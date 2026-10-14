import { Link, Route, Routes } from 'react-router'
import { useAuth } from '../auth/useAuth'
import { AppShell } from './AppShell'
import { SessionControls } from './SessionControls'
import { ReadAccess } from './ReadAccess'
import { Overview } from './Overview'
import OrdersPage from '../features/orders/OrdersPage'
import InventoryPage from '../features/inventory/InventoryPage'
import FulfillmentPage from '../features/fulfillment/FulfillmentPage'

function NotFoundPage() {
  return <section className="panel"><p className="eyebrow">Page not found</p><h1>That workspace page does not exist.</h1><Link to="/">Return to overview</Link></section>
}
export function AppRoutes() {
  const auth = useAuth()
  return <Routes>
    <Route element={<AppShell session={<SessionControls />} />}>
      <Route index element={<Overview />} />
      <Route path="orders" element={<ReadAccess><OrdersPage key={auth.sessionEpoch} canOperate={auth.canOperate} /></ReadAccess>} />
      <Route path="inventory" element={<ReadAccess><InventoryPage key={auth.sessionEpoch} canOperate={auth.canOperate} /></ReadAccess>} />
      <Route path="fulfillment" element={<ReadAccess><FulfillmentPage key={auth.sessionEpoch} canOperate={auth.canOperate} /></ReadAccess>} />
      <Route path="*" element={<NotFoundPage />} />
    </Route>
  </Routes>
}
