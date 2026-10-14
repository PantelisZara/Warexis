// Adapter behavior and real login redirects are outside these context unit tests.
import { StrictMode } from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { AuthSnapshot } from './contracts';
const state = vi.hoisted(() => ({
  snapshot: { status: 'anonymous', canRead: false, canOperate: false, canRetryLogout: false, role: 'none', principalId: null, username: null, error: null, sessionEpoch: 1 } as AuthSnapshot,
  listeners: new Set<() => void>(), login: vi.fn().mockResolvedValue(undefined), logout: vi.fn().mockResolvedValue(undefined),
}));
vi.mock('./session', () => ({
  getAuthSnapshot: () => state.snapshot,
  subscribeAuth: (listener: () => void) => { state.listeners.add(listener); return () => { state.listeners.delete(listener); }; },
  login: state.login, logout: state.logout,
}));
import { AuthProvider } from './AuthProvider';
import { useAuth } from './useAuth';
function Consumer() {
  const auth = useAuth();
  return <><p>{auth.status} {auth.role} {auth.sessionEpoch}</p><button onClick={() => { void auth.login(); }}>Sign in</button><button onClick={() => { void auth.logout(); }}>Sign out</button></>;
}
afterEach(() => { state.listeners.clear(); });
describe('authentication React context', () => {
  it('exposes actions and snapshot through a StrictMode-safe subscription without initializing from effects', () => {
    const view = render(<StrictMode><AuthProvider><Consumer /></AuthProvider></StrictMode>);
    expect(screen.getByText('anonymous none 1')).toBeInTheDocument(); expect(state.listeners.size).toBe(1);
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' })); expect(state.login).toHaveBeenCalledOnce();
    fireEvent.click(screen.getByRole('button', { name: 'Sign out' })); expect(state.logout).toHaveBeenCalledOnce();
    act(() => {
      state.snapshot = { ...state.snapshot, status: 'authenticated', role: 'viewer', canRead: true, sessionEpoch: 2 };
      for (const listener of state.listeners) listener();
    });
    expect(screen.getByText('authenticated viewer 2')).toBeInTheDocument();
    view.unmount(); expect(state.listeners.size).toBe(0);
  });
  it('requires a provider instead of returning a fake authenticated context', () => {
    expect(() => render(<Consumer />)).toThrow('useAuth requires AuthProvider.');
  });
});
