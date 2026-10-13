import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router'
import { describe, expect, it } from 'vitest'
import { AppRoutes } from './AppRoutes'

function renderRoute(path = '/') {
  return render(<MemoryRouter initialEntries={[path]}><AppRoutes /></MemoryRouter>)
}
describe('operations shell', () => {
  it('exposes real work areas without invented health or metrics', () => {
    renderRoute()
    expect(screen.getByRole('heading', { name: 'Operations overview' })).toBeInTheDocument()
    expect(screen.getByText('Service health has not been checked.')).toBeInTheDocument()
    expect(screen.getByRole('img', { name: 'Warexis' })).toHaveAttribute('src', '/brand/warexis-logo-dark.svg')
    expect(screen.getByRole('link', { name: 'Overview' })).toHaveAttribute('aria-current', 'page')
    expect(screen.getByRole('link', { name: 'Skip to content' })).toHaveAttribute('href', '#main-content')
  })
  it('supports keyboard navigation and marks the active area', async () => {
    const user = userEvent.setup()
    renderRoute()
    screen.getByRole('link', { name: 'Orders' }).focus()
    await user.keyboard('{Enter}')
    expect(screen.getByRole('heading', { name: 'Orders' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Orders' })).toHaveAttribute('aria-current', 'page')
    expect(screen.getByRole('link', { name: 'Overview' })).not.toHaveAttribute('aria-current')
  })
  it('offers recovery from an unknown route', () => {
    renderRoute('/missing')
    expect(screen.getByRole('heading', { name: 'That workspace page does not exist.' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Return to overview' })).toHaveAttribute('href', '/')
  })
})
