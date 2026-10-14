import { useSyncExternalStore } from 'react';
import type { ReactNode } from 'react';
import { AuthContext } from './context';
import { getAuthSnapshot, login, logout, subscribeAuth } from './session';

export function AuthProvider({ children }: { children: ReactNode }) {
  const snapshot = useSyncExternalStore(subscribeAuth, getAuthSnapshot, getAuthSnapshot);
  return <AuthContext value={{ ...snapshot, login, logout }}>{children}</AuthContext>;
}
