import { useRef, useState } from 'react'
import { useAuth } from '../auth/useAuth'
import { StatusBadge } from '../shared/ui'

export function SessionControls() {
  const auth = useAuth()
  const inFlight = useRef(false)
  const [busy, setBusy] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)
  const signedIn = auth.status === 'authenticated'
  const signingOut = signedIn || auth.canRetryLogout
  async function act() {
    if (inFlight.current) return
    inFlight.current = true
    setBusy(true)
    setActionError(null)
    try { await (signingOut ? auth.logout() : auth.login()) }
    catch { setActionError(signingOut ? 'Sign out could not be completed. Your local session has been cleared.' : 'Sign in could not be started. Please try again.') }
    finally { inFlight.current = false; setBusy(false) }
  }
  return <>
    <span className="session-description" aria-live="polite">{signedIn ? auth.username || 'Signed in' : auth.status === 'expired' ? 'Session expired' : auth.status === 'error' ? 'Authentication unavailable' : 'Not signed in'}</span>
    {signedIn && <StatusBadge tone={auth.canOperate ? 'info' : 'neutral'}>{auth.role === 'operator' ? 'Operator' : auth.role === 'viewer' ? 'Viewer' : 'No business access'}</StatusBadge>}
    <button className="button button--secondary" type="button" disabled={busy || auth.status === 'initializing'} onClick={() => { void act() }}>{busy ? 'Please wait…' : auth.canRetryLogout ? 'Retry sign out' : signedIn ? 'Sign out' : 'Sign in'}</button>
    {(actionError || auth.error) && <span className="field-error session-error" role="alert">{actionError || auth.error}</span>}
  </>
}
