import type { ReactNode } from 'react'
import { useAuth } from '../auth/useAuth'
import { LoadingState, PageHeader } from '../shared/ui'

export function ReadAccess({ children }: { children: ReactNode }) {
  const auth = useAuth()
  if (auth.status === 'initializing') return <LoadingState label="Checking your session…" />
  if (auth.status !== 'authenticated') return <section className="panel">
    <PageHeader title={auth.status === 'expired' ? 'Your session has expired' : auth.status === 'error' ? 'Authentication is unavailable' : 'Sign in to Warexis'} description={auth.canRetryLogout ? "Your local session is cleared. Use Retry sign out to finish ending the identity-provider session." : "Sign in using the control at the top of this page to access operational records."} />
    {auth.status === 'error' && !auth.canRetryLogout && <button type="button" className="button button--secondary" onClick={() => window.location.reload()}>Reload authentication</button>}
  </section>
  if (!auth.canRead) return <section className="panel"><PageHeader title="No Warexis access" description="Your account is signed in but has no permitted Warexis role. Ask your administrator for the access you need." /></section>
  return children
}
