import { defineConfig, devices } from '@playwright/test'

// The pinned runner otherwise persists a failure-time ARIA snapshot of the login form.
process.env.PLAYWRIGHT_NO_COPY_PROMPT = '1'

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  forbidOnly: true,
  timeout: 120_000,
  expect: { timeout: 15_000 },
  reporter: 'line',
  use: {
    baseURL: 'http://127.0.0.1:5173',
    trace: 'off',
    screenshot: 'off',
    video: 'off',
    serviceWorkers: 'block',
    launchOptions: {
      chromiumSandbox: true,
      ...(process.env.WAREXIS_E2E_BROWSER_EXECUTABLE ? { executablePath: process.env.WAREXIS_E2E_BROWSER_EXECUTABLE } : {}),
    },
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
})
