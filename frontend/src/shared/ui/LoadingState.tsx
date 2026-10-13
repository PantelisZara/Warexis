export function LoadingState({ label = 'Loading…' }: { label?: string }) {
  return <p className="loading-state" role="status"><span className="loading-indicator" aria-hidden="true" />{label}</p>
}
