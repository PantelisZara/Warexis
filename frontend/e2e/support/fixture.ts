import { lstatSync, readFileSync } from 'node:fs'
import { dirname, isAbsolute, join, normalize, parse } from 'node:path'
import { expect, type Page } from '@playwright/test'

type Credentials = { username: string; password: string }
export type Runtime = { issuer: string; users: { viewer: Credentials; operator: Credentials } }
const origin = 'http://127.0.0.1:5173'

function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}
function credentials(value: unknown): value is Credentials {
  return object(value) && typeof value.username === 'string' && !!value.username &&
    typeof value.password === 'string' && !!value.password
}

export function loadRuntime(): Runtime {
  try {
    const path = process.env.WAREXIS_E2E_RUNTIME_FILE
    if (!path || !isAbsolute(path) || path !== normalize(path)) throw new Error()
    const stat = lstatSync(path)
    if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o777) !== 0o600) throw new Error()
    if ((lstatSync(dirname(path)).mode & 0o777) !== 0o700) throw new Error()
    for (let directory = dirname(path); ; directory = dirname(directory)) {
      if (lstatSync(directory).isSymbolicLink()) throw new Error()
      try { lstatSync(join(directory, '.git')); throw new Error('git') }
      catch (error) { if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error }
      if (directory === parse(directory).root) break
    }
    const value: unknown = JSON.parse(readFileSync(path, 'utf8'))
    if (!object(value) || value.status !== 'READY' || value.browserOrigin !== origin || typeof value.issuer !== 'string' ||
        !object(value.users) || !credentials(value.users.viewer) || !credentials(value.users.operator)) throw new Error()
    const issuer = new URL(value.issuer)
    if (issuer.protocol !== 'http:' || issuer.hostname !== '127.0.0.1' || !issuer.port || issuer.pathname !== '/realms/warexis' ||
        issuer.username || issuer.password || issuer.search || issuer.hash) throw new Error()
    return { issuer: value.issuer, users: { viewer: value.users.viewer, operator: value.users.operator } }
  } catch {
    throw new Error('Provide a READY disposable fixture using WAREXIS_E2E_RUNTIME_FILE and its private runtime file.')
  }
}

export async function login(page: Page, runtime: Runtime, role: 'viewer' | 'operator') {
  await page.goto('/')
  await page.getByRole('button', { name: 'Sign in', exact: true }).click()
  await expect.poll(() => {
    const url = new URL(page.url())
    return url.origin === new URL(runtime.issuer).origin && url.pathname.endsWith('/protocol/openid-connect/auth') &&
      url.searchParams.get('response_type') === 'code' && url.searchParams.get('code_challenge_method') === 'S256' &&
      !!url.searchParams.get('code_challenge')
  }).toBe(true)
  // Do not let locator diagnostics include an ephemeral password on failure.
  try {
    await page.getByRole('textbox', { name: /username/i }).fill(runtime.users[role].username)
    await page.getByLabel('Password', { exact: true }).fill(runtime.users[role].password)
    await page.getByRole('button', { name: /sign in/i }).click()
  } catch { throw new Error('Disposable identity login interaction failed; credential diagnostics suppressed.') }
  await expect(page.getByRole('button', { name: 'Sign out', exact: true })).toBeVisible()
  await expect(page.getByText(role === 'operator' ? 'Operator' : 'Viewer', { exact: true })).toBeVisible()
  await expect.poll(() => page.url() === origin + '/').toBe(true)
}

export async function logout(page: Page) {
  await page.getByRole('button', { name: 'Sign out', exact: true }).click()
  await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible()
}

export async function assertNoStoredTokens(page: Page) {
  const containsToken = await page.evaluate(async () => {
    const token = /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/
    const sensitiveKey = /^(access[_-]?token|refresh[_-]?token|id[_-]?token|authorization)$/i
    for (const storage of [localStorage, sessionStorage]) {
      for (let index = 0; index < storage.length; index++) {
        const key = storage.key(index)
        if (key && (sensitiveKey.test(key) || token.test(storage.getItem(key) ?? ''))) return true
      }
    }
    if (token.test(document.cookie)) return true
    // The application has no IndexedDB persistence. Report only a boolean.
    return (await indexedDB.databases()).length > 0
  })
  const cookieContainsToken = (await page.context().cookies('http://127.0.0.1:5173')).some(cookie =>
    /\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/.test(cookie.value))
  expect(containsToken || cookieContainsToken, 'No tokens or application databases should persist on the SPA origin').toBe(false)
}
