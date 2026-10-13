import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import DashboardPage from './DashboardPage'

describe('overview service health', () => {
  it('shows only supplied connectivity states and refreshes explicitly', async () => {
    const refresh = vi.fn()
    render(<MemoryRouter><DashboardPage health={[{ service: 'Order', status: 'up' }, { service: 'Inventory', status: 'unavailable' }, { service: 'Fulfillment', status: 'checking' }]} onRefresh={refresh} /></MemoryRouter>)
    expect(screen.getByText('Available', { exact: true })).toBeInTheDocument()
    expect(screen.getByText('Unavailable', { exact: true })).toBeInTheDocument()
    expect(screen.getByText('Checking…')).toBeInTheDocument()
    await userEvent.setup().click(screen.getByRole('button', { name: 'Refresh health' }))
    expect(refresh).toHaveBeenCalledOnce()
  })
})
