import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router'
import { AppRoutes } from './app/AppRoutes'
import './styles/global.css'

const element = document.getElementById('root')
if (!element) throw new Error('Application mount point is missing.')
createRoot(element).render(
  <StrictMode>
    <BrowserRouter>
      <AppRoutes />
    </BrowserRouter>
  </StrictMode>,
)
