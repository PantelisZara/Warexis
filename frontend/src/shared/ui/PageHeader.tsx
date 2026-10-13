import type { ReactNode } from 'react'

export function PageHeader({ title, description, eyebrow = 'Operations', actions }: { title: string; description: string; eyebrow?: string; actions?: ReactNode }) {
  return <header className="page-header"><div><p className="eyebrow">{eyebrow}</p><h1>{title}</h1><p className="page-description">{description}</p></div>{actions && <div className="page-actions">{actions}</div>}</header>
}
