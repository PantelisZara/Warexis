import type { ReactNode } from 'react'
import { NavLink, Outlet } from 'react-router'

const navigation = [
  { to: '/', label: 'Overview', symbol: 'overview' },
  { to: '/orders', label: 'Orders', symbol: 'orders' },
  { to: '/inventory', label: 'Inventory', symbol: 'inventory' },
  { to: '/fulfillment', label: 'Fulfillment', symbol: 'fulfillment' },
] as const

function NavigationIcon({ symbol }: { symbol: string }) {
  const paths: Record<string, string> = {
    overview: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
    orders: 'M6 3h12v18H6z M9 8h6 M9 12h6 M9 16h4',
    inventory: 'M3 7l9-4 9 4v10l-9 4-9-4z M3 7l9 4 9-4 M12 11v10',
    fulfillment: 'M3 5h12v12H3z M15 9h3l3 4v4h-6 M5 19a2 2 0 1 0 4 0a2 2 0 1 0-4 0 M16 19a2 2 0 1 0 4 0a2 2 0 1 0-4 0',
  }
  return <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d={paths[symbol]} /></svg>
}

export function AppShell({ session }: { session?: ReactNode }) {
  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">Skip to content</a>
      <aside className="sidebar">
        <NavLink className="brand-link" to="/" aria-label="Warexis overview">
          <img src="/brand/warexis-logo-dark.svg" alt="Warexis" className="wordmark" />
          <span className="brand-caption">OPERATIONS</span>
        </NavLink>
        <div className="nav-caption" aria-hidden="true">Workspace</div>
        <nav aria-label="Primary navigation" className="primary-nav">
          {navigation.map(({ to, label, symbol }) => (
            <NavLink key={to} to={to} end={to === '/'} className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}>
              <NavigationIcon symbol={symbol} /><span>{label}</span>
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer"><img src="/brand/warexis-icon-red.svg" alt="" /><span>Connected operations.<br />Reliable outcomes.</span></div>
      </aside>
      <div className="workspace">
        <header className="topbar"><span className="workspace-label">Operations workspace</span><div className="session-controls">{session}</div></header>
        <main id="main-content" className="main-content" tabIndex={-1}><Outlet /></main>
        <footer className="workspace-footer">Warexis · Orders, inventory &amp; fulfillment</footer>
      </div>
    </div>
  )
}
