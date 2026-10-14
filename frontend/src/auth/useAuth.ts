import { useContext } from 'react';
import { AuthContext } from './context';

export function useAuth() {
  const auth = useContext(AuthContext);
  if (!auth) throw new Error('useAuth requires AuthProvider.');
  return auth;
}
