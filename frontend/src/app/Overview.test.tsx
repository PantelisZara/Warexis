import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { Overview } from './Overview'
const { healthMock } = vi.hoisted(() => ({ healthMock: vi.fn() }))
vi.mock('../shared/api/client', () => ({ api: { health: healthMock } }))
beforeEach(() => { healthMock.mockReset() })
describe('actual service-health requests', () => {
  it('maps only UP to available and makes failures explicit', async () => {
    healthMock.mockImplementation((service: string) => service === 'order' ? Promise.resolve({ status: 'UP' }) : service === 'inventory' ? Promise.resolve({ status: 'DOWN' }) : Promise.reject(new Error('unavailable')))
    render(<MemoryRouter><Overview /></MemoryRouter>)
    const list = screen.getByRole('list')
    expect(await within(list).findAllByText('Unavailable', { exact: true })).toHaveLength(2)
    expect(within(list).getAllByText('Available', { exact: true })).toHaveLength(1)
    expect(healthMock).toHaveBeenCalledTimes(3)
  })
  it('aborts earlier requests on refresh and discards their later results', async () => {
    let release: (value: { status: string }) => void = () => {}
    const old = new Promise<{status: string}>(resolve => { release = resolve })
    healthMock.mockReturnValue(old)
    render(<MemoryRouter><Overview /></MemoryRouter>)
    const firstSignal = healthMock.mock.calls[0]![1] as AbortSignal
    healthMock.mockResolvedValue({ status: 'DOWN' })
    await userEvent.setup().click(screen.getByRole('button', { name: 'Refresh health' }))
    expect(firstSignal.aborted).toBe(true)
    expect(await screen.findAllByText('Unavailable', { exact: true })).toHaveLength(3)
    release({ status: 'UP' })
    await Promise.resolve()
    expect(screen.queryByText('Available', { exact: true })).not.toBeInTheDocument()
  })
  it('cancels in-flight requests when unmounted', () => {
    healthMock.mockReturnValue(new Promise(() => {}))
    const { unmount } = render(<MemoryRouter><Overview /></MemoryRouter>)
    const signal = healthMock.mock.calls[0]![1] as AbortSignal
    unmount()
    expect(signal.aborted).toBe(true)
  })
})
