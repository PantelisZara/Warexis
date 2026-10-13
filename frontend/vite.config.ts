import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const proxy = (prefix: string, target: string) => ({
    target,
    changeOrigin: false,
    rewrite: (path: string) => path.replace(new RegExp(`^${prefix}`), ''),
  })
  return {
    plugins: [react()],
    server: {
      host: '127.0.0.1',
      port: 5173,
      strictPort: true,
      proxy: {
        '/api/order': proxy('/api/order', env.WAREXIS_ORDER_PROXY_TARGET || 'http://127.0.0.1:8081'),
        '/api/inventory': proxy('/api/inventory', env.WAREXIS_INVENTORY_PROXY_TARGET || 'http://127.0.0.1:8082'),
        '/api/fulfillment': proxy('/api/fulfillment', env.WAREXIS_FULFILLMENT_PROXY_TARGET || 'http://127.0.0.1:8083'),
      },
    },
  }
})
