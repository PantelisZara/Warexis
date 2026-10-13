import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { ErrorMessage, Field, LoadingState, StatusBadge } from './index'

describe('shared UI semantics', () => {
  it('associates field labels and supplied descriptions with a control', () => {
    render(<Field id="sku" label="SKU" hint="Use the stock SKU." error="SKU is required."><input id="sku" aria-invalid="true" aria-describedby="sku-hint sku-error" /></Field>)
    const input = screen.getByRole('textbox', { name: 'SKU' })
    expect(input).toHaveAccessibleDescription('Use the stock SKU. SKU is required.')
    expect(input).toHaveAttribute('aria-invalid', 'true')
  })
  it('announces errors and loading without relying on color', () => {
    render(<><ErrorMessage message="Please try again." /><LoadingState label="Loading order…" /><StatusBadge tone="warning">Pending inventory</StatusBadge></>)
    expect(screen.getByRole('alert')).toHaveTextContent('Please try again.')
    expect(screen.getByRole('status')).toHaveTextContent('Loading order…')
    expect(screen.getByText('Pending inventory')).toBeInTheDocument()
  })
})
