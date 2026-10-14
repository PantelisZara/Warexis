import { useEffect, useRef, useState } from 'react'
import DashboardPage from '../features/dashboard/DashboardPage'
import type { ServiceHealth } from '../features/dashboard/DashboardPage'
import { api } from '../shared/api/client'

const services = [{ key: 'order', label: 'Order' }, { key: 'inventory', label: 'Inventory' }, { key: 'fulfillment', label: 'Fulfillment' }] as const
const checking = (): ServiceHealth[] => services.map(s => ({ service: s.label, status: 'checking' }))
export function Overview() {
  const [health, setHealth] = useState<ServiceHealth[]>(checking)
  const [revision, setRevision] = useState(0)
  const active = useRef<AbortController | null>(null)
  useEffect(() => {
    const controller = new AbortController()
    active.current = controller
    void Promise.allSettled(services.map(s => api.health(s.key, controller.signal))).then(results => {
      if (controller.signal.aborted) return
      setHealth(results.map((result, i) => ({ service: services[i]!.label, status: result.status === 'fulfilled' && result.value.status === 'UP' ? 'up' : 'unavailable' })))
    })
    return () => { controller.abort() }
  }, [revision])
  function refresh() {
    active.current?.abort()
    setHealth(checking())
    setRevision(r => r + 1)
  }
  return <DashboardPage health={health} onRefresh={refresh} />
}
