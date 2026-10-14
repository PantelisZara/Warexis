import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router'
import { AuthProvider } from './auth/AuthProvider'
import { initializeAuth } from './auth/session'
import { AppRoutes } from './app/AppRoutes'
import { ErrorMessage, LoadingState } from './shared/ui'
import './styles/global.css'

const element = document.getElementById('root')
if (!element) throw new Error('Application mount point is missing.')
const root = createRoot(element)
root.render(<div className="startup-screen"><LoadingState label="Starting Warexis…" /></div>)
async function start() {
  try {
    // The official adapter processes redirects before a router reads the URL.
    await initializeAuth()
    root.render(<StrictMode><AuthProvider><BrowserRouter><AppRoutes /></BrowserRouter></AuthProvider></StrictMode>)
  } catch {
    root.render(<div className="startup-screen"><h1>Warexis could not start</h1><ErrorMessage message="Reload the application to try again." /><button type="button" className="button" onClick={() => window.location.reload()}>Reload Warexis</button></div>)
  }
}
void start()
