import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { AuthView } from '../auth/contracts'
import { AppRoutes } from './AppRoutes'
const { authMock, healthMock } = vi.hoisted(() => ({ authMock: vi.fn(), healthMock: vi.fn() }))
vi.mock('../auth/useAuth', () => ({ useAuth: authMock }))
vi.mock('../shared/api/client', () => ({ api: { health: healthMock } }))
vi.mock('../features/orders/OrdersPage', () => ({ default: ({ canOperate }: { canOperate: boolean }) => <section><h1>Orders</h1><input aria-label="Unit record" defaultValue="" /><button disabled={!canOperate}>Create order</button></section> }))
vi.mock('../features/inventory/InventoryPage', () => ({ default: () => <h1>Inventory</h1> }))
vi.mock('../features/fulfillment/FulfillmentPage', () => ({ default: () => <h1>Fulfillment</h1> }))
const baseline = (): AuthView => ({ status: 'authenticated', role: 'viewer', canRead: true, canOperate: false, principalId: 'unit-principal', username: 'Unit viewer', error: null, sessionEpoch: 1, canRetryLogout: false, login: vi.fn().mockResolvedValue(undefined), logout: vi.fn().mockResolvedValue(undefined) })
beforeEach(() => { authMock.mockReturnValue(baseline()); healthMock.mockResolvedValue({ status: 'UP' }) })
function renderRoute(path = '/') { return render(<MemoryRouter initialEntries={[path]}><AppRoutes /></MemoryRouter>) }
describe('operations route integration', () => {
  it('preserves branded navigation and displays health from the real API interface', async () => {
    renderRoute()
    expect(screen.getByRole('heading', { name: 'Operations overview' })).toBeInTheDocument()
    expect(await screen.findAllByText('Available', { exact: true })).toHaveLength(3)
    expect(healthMock).toHaveBeenCalledTimes(3)
    expect(screen.getByRole('img', { name: 'Warexis' })).toHaveAttribute('src', '/brand/warexis-logo-dark.svg')
    expect(screen.getByRole('link', { name: 'Overview' })).toHaveAttribute('aria-current', 'page')
    expect(screen.getByRole('link', { name: 'Skip to content' })).toHaveAttribute('href', '#main-content')
  })
  it('supports keyboard route navigation and passes viewer permissions', async () => {
    renderRoute('/inventory')
    screen.getByRole('link', { name: 'Orders' }).focus()
    await userEvent.setup().keyboard('{Enter}')
    expect(screen.getByRole('heading', { name: 'Orders' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Orders' })).toHaveAttribute('aria-current', 'page')
    expect(screen.getByRole('link', { name: 'Overview' })).not.toHaveAttribute('aria-current')
    expect(screen.getByRole('button', { name: 'Create order' })).toBeDisabled()
  })
  it('offers recovery from an unknown route', () => {
    renderRoute('/missing')
    expect(screen.getByRole('heading', { name: 'That workspace page does not exist.' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Return to overview' })).toHaveAttribute('href', '/')
  })
  it.each(['anonymous', 'expired', 'initializing', 'error'] as const)('does not mount business pages while %s', status => {
    authMock.mockReturnValue({ ...baseline(), status, canRead: false, canOperate: false, error: status === 'error' ? 'Identity connection failed.' : null })
    renderRoute('/orders')
    expect(screen.queryByRole('heading', { name: 'Orders' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Create order' })).not.toBeInTheDocument()
    expect(healthMock).not.toHaveBeenCalled()
  })
  it('denies business UI for a signed-in principal with no recognized role', () => {
    authMock.mockReturnValue({ ...baseline(), role: 'none', canRead: false })
    renderRoute('/orders')
    expect(screen.getByRole('heading', { name: 'No Warexis access' })).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Orders' })).not.toBeInTheDocument()
  })
  it('passes actual operator permission and clears prior-view records at an identity boundary', async () => {
    const auth = { ...baseline(), role: 'operator' as const, canOperate: true }
    authMock.mockReturnValue(auth)
    const { rerender } = renderRoute('/orders')
    expect(screen.getByRole('button', { name: 'Create order' })).toBeEnabled()
    await userEvent.setup().type(screen.getByRole('textbox', { name: 'Unit record' }), 'prior principal record')
    authMock.mockReturnValue({ ...baseline(), principalId: 'other-unit-principal', sessionEpoch: 2 })
    rerender(<MemoryRouter initialEntries={['/orders']}><AppRoutes /></MemoryRouter>)
    expect(screen.getByRole('textbox', { name: 'Unit record' })).toHaveValue('')
    expect(screen.getByRole('button', { name: 'Create order' })).toBeDisabled()
  })
  it('retries provider logout instead of logging in after an unconfirmed sign-out', async () => {
    const login = vi.fn(); const logout = vi.fn().mockResolvedValue(undefined)
    authMock.mockReturnValue({ ...baseline(), status: 'error', canRead: false, canOperate: false, canRetryLogout: true,
      error: 'Local sign-out is complete, but identity-provider sign-out could not be confirmed.', login, logout })
    renderRoute('/orders')
    await userEvent.setup().click(screen.getByRole('button', { name: 'Retry sign out' }))
    expect(logout).toHaveBeenCalledOnce()
    expect(login).not.toHaveBeenCalled()
    expect(screen.queryByRole('button', { name: 'Sign in' })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Reload authentication' })).not.toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Orders' })).not.toBeInTheDocument()
  })
  it('keeps sign-out pending controls mounted while the business identity view is cleared', async () => {
    let complete: () => void = () => {}
    const pending = new Promise<void>(resolve => { complete = resolve })
    const auth = { ...baseline(), logout: vi.fn().mockReturnValue(pending) }
    authMock.mockReturnValue(auth)
    const { rerender } = renderRoute('/orders')
    await userEvent.setup().click(screen.getByRole('button', { name: 'Sign out' }))
    authMock.mockReturnValue({ ...auth, status: 'anonymous', canRead: false, canOperate: false, role: 'none', principalId: null, username: null, sessionEpoch: 2 })
    rerender(<MemoryRouter initialEntries={['/orders']}><AppRoutes /></MemoryRouter>)
    expect(screen.getByRole('button', { name: 'Please wait…' })).toBeDisabled()
    expect(screen.queryByRole('heading', { name: 'Orders' })).not.toBeInTheDocument()
    await act(async () => { complete(); await pending })
    expect(screen.getByRole('button', { name: 'Sign in' })).toBeEnabled()
  })
  it('provides explicit login and reports action failure without exposing the thrown value', async () => {
    const login = vi.fn().mockRejectedValue(new Error('sensitive exception text'))
    authMock.mockReturnValue({ ...baseline(), status: 'anonymous', canRead: false, login })
    renderRoute('/orders')
    await userEvent.setup().click(screen.getByRole('button', { name: 'Sign in' }))
    expect(login).toHaveBeenCalledOnce()
    expect(screen.getByRole('alert')).toHaveTextContent('Sign in could not be started.')
    expect(screen.queryByText('sensitive exception text')).not.toBeInTheDocument()
  })
})
