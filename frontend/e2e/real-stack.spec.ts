import { randomUUID } from 'node:crypto'
import { expect, test, type Locator, type Page } from '@playwright/test'
import { assertNoStoredTokens, loadRuntime, login, logout } from './support/fixture'

const runtime = loadRuntime()
const unique = (kind: string) => `E2E-${kind}-${randomUUID()}`
const details = (page: Page, name: string) => page.getByRole('region', { name, exact: true })
const value = (region: Locator, label: string) => region.locator('dl > div').filter({ has: region.page().getByRole('term').filter({ hasText: new RegExp(`^${label}$`) }) }).getByRole('definition')

async function navigate(page: Page, name: string) {
  await page.getByRole('navigation', { name: 'Primary navigation' }).getByRole('link', { name, exact: true }).click()
  await expect(page.getByRole('heading', { level: 1, name, exact: true })).toBeVisible()
}
async function createStock(page: Page, sku: string, onHand: number) {
  await navigate(page, 'Inventory')
  await page.getByLabel('New SKU', { exact: true }).fill(sku)
  await page.getByLabel('Initial on-hand quantity', { exact: true }).fill(String(onHand))
  await page.getByRole('button', { name: 'Create stock item', exact: true }).click()
  await expect(page.getByText(`Created stock for ${sku}.`, { exact: true })).toBeVisible()
}
async function lookupStock(page: Page, sku: string) {
  await page.getByLabel('Lookup SKU', { exact: true }).fill(sku)
  await page.getByRole('button', { name: 'Look up stock', exact: true }).click()
  await expect(page.getByRole('heading', { name: `Stock for ${sku}`, exact: true })).toBeVisible()
}
async function fillOrder(page: Page, sku: string, quantity: number) {
  await page.getByLabel('SKU for line 1', { exact: true }).fill(sku)
  await page.getByLabel('Quantity for line 1', { exact: true }).fill(String(quantity))
}
async function waitOrder(page: Page, status: 'CONFIRMED' | 'REJECTED') {
  const region = details(page, 'Order details')
  await expect(async () => {
    await page.getByRole('button', { name: 'Refresh order', exact: true }).click()
    await expect(region.getByText(status, { exact: true })).toBeVisible({ timeout: 2_000 })
  }).toPass({ timeout: 45_000, intervals: [250, 500, 1_000] })
}
async function createOrder(page: Page, sku: string, quantity: number) {
  await navigate(page, 'Orders')
  await fillOrder(page, sku, quantity)
  await page.getByRole('button', { name: 'Create order', exact: true }).click()
  const id = value(details(page, 'Order details'), 'Order ID')
  await expect(id).toHaveText(/^[0-9a-f-]{36}$/)
  return (await id.innerText()).trim()
}
async function lookupOrder(page: Page, orderId: string) {
  await navigate(page, 'Orders')
  await page.getByLabel('Order ID', { exact: true }).fill(orderId)
  await page.getByRole('button', { name: 'Look up order', exact: true }).click()
  await expect(value(details(page, 'Order details'), 'Order ID')).toHaveText(orderId)
}
async function lookupFulfillment(page: Page, orderId: string) {
  await navigate(page, 'Fulfillment')
  await page.getByLabel('Look up by', { exact: true }).selectOption('order')
  await page.getByLabel('Order ID', { exact: true }).fill(orderId)
  await expect(async () => {
    await page.getByRole('button', { name: 'Look up fulfillment', exact: true }).click()
    await expect(value(details(page, 'Fulfillment details'), 'Order ID')).toHaveText(orderId, { timeout: 2_000 })
  }).toPass({ timeout: 45_000, intervals: [250, 500, 1_000] })
}

test('real PKCE operator workflow persists; viewer reads and server denies mutations; logout removes access', async ({ page, context }) => {
  const failures: string[] = []
  page.on('pageerror', () => failures.push('Uncaught browser error'))
  const sku = unique('stock')
  const shipment = unique('shipment')
  await page.goto('/')
  await page.getByRole('button', { name: 'Sign in', exact: true }).waitFor()
  await page.getByRole('navigation', { name: 'Primary navigation' }).getByRole('link', { name: 'Orders', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Sign in to Warexis', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Create order', exact: true })).toHaveCount(0)
  await login(page, runtime, 'operator')
  await assertNoStoredTokens(page)

  await createStock(page, sku, 12)
  await page.getByLabel('New on-hand quantity', { exact: true }).fill('15')
  await page.getByRole('button', { name: 'Update on-hand stock', exact: true }).click()
  await expect(page.getByText(`Updated stock for ${sku}.`, { exact: true })).toBeVisible()
  const stock = details(page, `Stock for ${sku}`)
  await expect(value(stock, 'On hand')).toHaveText('15')
  const orderId = await createOrder(page, sku, 3)
  await waitOrder(page, 'CONFIRMED')
  await page.getByRole('link', { name: 'Look up fulfillment', exact: true }).click()
  await expect(async () => {
    if (await page.getByRole('button', { name: 'Refresh fulfillment', exact: true }).count()) {
      await page.getByRole('button', { name: 'Refresh fulfillment', exact: true }).click()
    } else {
      await page.getByRole('button', { name: 'Look up fulfillment', exact: true }).click()
    }
    await expect(details(page, 'Fulfillment details').getByText('PENDING', { exact: true })).toBeVisible({ timeout: 2_000 })
  }).toPass({ timeout: 45_000, intervals: [250, 500, 1_000] })
  const fulfillmentId = (await value(details(page, 'Fulfillment details'), 'Fulfillment ID').innerText()).trim()
  await page.getByLabel('Shipment reference', { exact: true }).fill(shipment)
  await page.getByRole('button', { name: 'Confirm dispatch', exact: true }).click()
  await expect(details(page, 'Fulfillment details').getByText('SHIPPED', { exact: true })).toBeVisible()
  await expect(value(details(page, 'Fulfillment details'), 'Shipment reference')).toHaveText(shipment)
  await page.reload()
  await expect(page.getByRole('button', { name: 'Sign out', exact: true })).toBeVisible()
  await expect(value(details(page, 'Fulfillment details'), 'Fulfillment ID')).toHaveText(fulfillmentId)
  await expect(details(page, 'Fulfillment details').getByText('SHIPPED', { exact: true })).toBeVisible()
  await expect(value(details(page, 'Fulfillment details'), 'Shipment reference')).toHaveText(shipment)
  await navigate(page, 'Inventory')
  await lookupStock(page, sku)
  await expect(value(stock, 'Reserved')).toHaveText('3')
  await expect(value(stock, 'Available')).toHaveText('12')
  await assertNoStoredTokens(page)
  await logout(page)

  await login(page, runtime, 'viewer')
  await lookupOrder(page, orderId)
  await expect(details(page, 'Order details').getByText('CONFIRMED', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Create order', exact: true })).toHaveCount(0)
  await navigate(page, 'Inventory')
  const readResponse = page.waitForResponse(response => response.url().endsWith(`/api/stock-items/${sku}`) && response.request().method() === 'GET')
  await lookupStock(page, sku)
  await expect(page.getByRole('button', { name: 'Create stock item', exact: true })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Update on-hand stock', exact: true })).toHaveCount(0)
  const authorization = await (await readResponse).request().headerValue('authorization')
  if (!authorization) throw new Error('Authenticated viewer read did not carry an authorization credential.')
  // A real forbidden request proves server enforcement in addition to hidden controls.
  const forbiddenSku = unique('forbidden')
  let deniedStatus: number
  try {
    const denied = await context.request.post('/api/inventory/api/stock-items', {
      headers: { Authorization: authorization }, data: { sku: forbiddenSku, onHand: 1 },
    })
    deniedStatus = denied.status()
  } catch { throw new Error('Viewer authorization probe failed; credential diagnostics suppressed.') }
  expect(deniedStatus).toBe(403)
  await page.getByLabel('Lookup SKU', { exact: true }).fill(forbiddenSku)
  await page.getByRole('button', { name: 'Look up stock', exact: true }).click()
  await expect(page.getByRole('alert')).toBeVisible()
  await expect(page.getByRole('heading', { name: `Stock for ${forbiddenSku}`, exact: true })).toHaveCount(0)
  await lookupStock(page, sku)
  await expect(value(stock, 'On hand')).toHaveText('15')
  await lookupFulfillment(page, orderId)
  await expect(details(page, 'Fulfillment details').getByText('SHIPPED', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Confirm dispatch', exact: true })).toHaveCount(0)
  await assertNoStoredTokens(page)
  await logout(page)
  await page.getByRole('navigation', { name: 'Primary navigation' }).getByRole('link', { name: 'Orders', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Sign in to Warexis', exact: true })).toBeVisible()
  const anonymous = await context.request.get(`/api/order/api/orders/${orderId}`)
  expect(anonymous.status()).toBe(401)
  expect(failures).toEqual([])
})

test('real validation, absent stock, inventory conflict, rejection, and offline read recovery', async ({ page, context }) => {
  await login(page, runtime, 'operator')
  await navigate(page, 'Inventory')
  await page.getByRole('button', { name: 'Create stock item', exact: true }).click()
  await expect(page.getByLabel('New SKU', { exact: true })).toBeFocused()
  await expect(page.getByLabel('New SKU', { exact: true })).toHaveAttribute('aria-invalid', 'true')
  await page.getByLabel('Lookup SKU', { exact: true }).fill(unique('absent'))
  await page.getByRole('button', { name: 'Look up stock', exact: true }).click()
  await expect(page.getByRole('alert')).toBeVisible()
  await expect(page.getByRole('heading', { name: /^Stock for / })).toHaveCount(0)
  const sku = unique('empty')
  await createStock(page, sku, 0)
  await page.getByRole('button', { name: 'Create stock item', exact: true }).click()
  await expect(page.getByRole('alert')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Create stock item', exact: true })).toBeDisabled()
  await lookupStock(page, sku)
  await expect(page.getByRole('button', { name: 'Create stock item', exact: true })).toBeEnabled()
  await createOrder(page, sku, 1)
  await waitOrder(page, 'REJECTED')
  await expect(page.getByRole('link', { name: 'Look up fulfillment', exact: true })).toHaveCount(0)
  await navigate(page, 'Orders')
  await page.getByLabel('Order ID', { exact: true }).fill('invalid')
  await page.getByRole('button', { name: 'Look up order', exact: true }).click()
  await expect(page.getByLabel('Order ID', { exact: true })).toBeFocused()
  await expect(page.getByLabel('Order ID', { exact: true })).toHaveAttribute('aria-invalid', 'true')
  await navigate(page, 'Inventory')
  await page.getByLabel('Lookup SKU', { exact: true }).fill(sku)
  await context.setOffline(true)
  try {
    await page.getByRole('button', { name: 'Look up stock', exact: true }).click()
    await expect(page.getByRole('alert')).toContainText('Could not reach the service')
  } finally { await context.setOffline(false) }
  await lookupStock(page, sku)
  await expect(value(details(page, `Stock for ${sku}`), 'Available')).toHaveText('0')
})

test('a committed order with a dropped response stays unresolved and duplicate submit does not retry', async ({ page }) => {
  await login(page, runtime, 'operator')
  const sku = unique('uncertain')
  await createStock(page, sku, 5)
  await navigate(page, 'Orders')
  await fillOrder(page, sku, 1)
  let posts = 0
  let committedId = ''
  let release: () => void = () => {}
  const released = new Promise<void>(resolve => { release = resolve })
  let committed: () => void = () => {}
  const upstreamCommitted = new Promise<void>(resolve => { committed = resolve })
  await page.route('**/api/order/api/orders', async route => {
    if (route.request().method() !== 'POST') { await route.continue(); return }
    posts++
    try {
      const response = await route.fetch({ maxRetries: 0 })
      if (response.status() !== 201) throw new Error('Real order service did not commit the uncertain request.')
      const body: unknown = await response.json()
      if (!body || typeof body !== 'object' || !('id' in body) || typeof body.id !== 'string') throw new Error('Real order response lacked its identifier.')
      committedId = body.id
      committed()
      await released
      // The upstream201 is real; only delivery back to the SPA is deliberately lost.
      await route.abort('connectionfailed')
    } catch { committed(); await route.abort('connectionfailed') }
  })
  try {
    await page.getByRole('button', { name: 'Create order', exact: true }).dblclick()
    await upstreamCommitted
    await expect(page.getByRole('button', { name: 'Create order', exact: true })).toBeDisabled()
    expect(posts).toBe(1)
  } finally { release() }
  await expect(page.getByRole('alert').filter({ hasText: 'Order creation outcome is unresolved' })).toBeVisible()
  expect(committedId).toMatch(/^[0-9a-f-]{36}$/)
  await expect(page.getByRole('button', { name: 'Create order', exact: true })).toBeDisabled()
  await page.getByRole('button', { name: 'I understand; start another order', exact: true }).click()
  await expect(page.getByLabel('SKU for line 1', { exact: true })).toHaveValue('')
  await expect(page.getByLabel('Quantity for line 1', { exact: true })).toHaveValue('1')
  expect(posts).toBe(1)
  await page.unroute('**/api/order/api/orders')
  await lookupOrder(page, committedId)
  await waitOrder(page, 'CONFIRMED')
  await navigate(page, 'Inventory')
  await lookupStock(page, sku)
  await expect(value(details(page, `Stock for ${sku}`), 'Reserved')).toHaveText('1')
})

function bearerExpiry(authorization: string | null): number {
  try {
    if (!authorization?.startsWith('Bearer ')) throw new Error()
    const encoded = authorization.slice(7).split('.')[1]
    if (!encoded) throw new Error()
    const claims: unknown = JSON.parse(Buffer.from(encoded, 'base64url').toString('utf8'))
    if (!claims || typeof claims !== 'object' || !('exp' in claims) || typeof claims.exp !== 'number' ||
        !Number.isFinite(claims.exp) || claims.exp * 1_000 <= Date.now()) throw new Error()
    return claims.exp * 1_000
  } catch { throw new Error('The real protected request did not contain a valid future token expiry; credential diagnostics suppressed.') }
}

async function waitForRefreshWindow(expiry: number) {
  // The production adapter requests 30 seconds of validity. Five seconds inside
  // that boundary tolerates rounding without manipulating clocks or token TTLs.
  const boundary = expiry - 25_000
  if (boundary - Date.now() > 330_000) throw new Error('Fixture token lifespan exceeds the bounded lifecycle test contract.')
  await expect.poll(() => Date.now() >= boundary, {
    message: 'Wait for the actual bearer expiry to enter the adapter refresh window',
    timeout: Math.max(1_000, boundary - Date.now() + 10_000),
    intervals: [1_000],
  }).toBe(true)
}

test('real token refresh, revoked SSO expiry and normal login recovery clear stale business access', async ({ page, context }) => {
  test.setTimeout(12 * 60_000)
  const refreshes: { status: number; at: number }[] = []
  const tokenEndpoint = `${runtime.issuer}/protocol/openid-connect/token`
  page.on('response', response => {
    if (response.url() !== tokenEndpoint || response.request().method() !== 'POST') return
    // Inspect the grant type in memory; never retain request bodies or token responses.
    const grant = new URLSearchParams(response.request().postData() ?? '').get('grant_type')
    if (grant === 'refresh_token') refreshes.push({ status: response.status(), at: Date.now() })
  })
  await login(page, runtime, 'operator')
  const sku = unique('lifecycle')
  const created = page.waitForResponse(response => response.url().endsWith('/api/inventory/api/stock-items') && response.request().method() === 'POST')
  await createStock(page, sku, 2)
  const initialExpiry = bearerExpiry(await (await created).request().headerValue('authorization'))
  await assertNoStoredTokens(page)
  await waitForRefreshWindow(initialExpiry)
  const refreshStarted = Date.now()
  const read = page.waitForResponse(response => response.url().endsWith(`/api/inventory/api/stock-items/${sku}`) && response.request().method() === 'GET')
  await lookupStock(page, sku)
  const refreshedRead = await read
  expect(refreshedRead.status()).toBe(200)
  await expect.poll(() => refreshes.some(event => event.at >= refreshStarted && event.status === 200)).toBe(true)
  const renewedExpiry = bearerExpiry(await refreshedRead.request().headerValue('authorization'))
  expect(renewedExpiry > initialExpiry, 'Successful real refresh must renew the bearer lifetime').toBe(true)
  await assertNoStoredTokens(page)

  const otherPage = await context.newPage()
  try {
    await otherPage.goto('/')
    await expect(otherPage.getByRole('button', { name: 'Sign out', exact: true })).toBeVisible()
    // Ordinary provider logout revokes the shared session without admin APIs,
    // realm changes, synthetic 401s or browser clock manipulation.
    await logout(otherPage)
    await expect(page.getByRole('button', { name: 'Sign out', exact: true })).toBeVisible()
    await waitForRefreshWindow(renewedExpiry)
    const revokedAttempt = Date.now()
    await page.getByRole('button', { name: 'Look up stock', exact: true }).click()
    await expect(page.getByRole('heading', { name: 'Your session has expired', exact: true })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible()
    await expect(page.getByRole('heading', { name: `Stock for ${sku}`, exact: true })).toHaveCount(0)
    await expect(page.getByRole('button', { name: 'Create stock item', exact: true })).toHaveCount(0)
    await expect.poll(() => refreshes.some(event => event.at >= revokedAttempt && [400, 401].includes(event.status))).toBe(true)
    await assertNoStoredTokens(page)
  } finally { await otherPage.close() }

  await login(page, runtime, 'operator')
  await navigate(page, 'Inventory')
  await lookupStock(page, sku)
  await expect(value(details(page, `Stock for ${sku}`), 'On hand')).toHaveText('2')
  await assertNoStoredTokens(page)
  await logout(page)
})
