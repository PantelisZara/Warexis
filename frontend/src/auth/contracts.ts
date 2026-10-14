export type AuthStatus = 'initializing' | 'authenticated' | 'anonymous' | 'expired' | 'error';
export interface AuthSnapshot {
  status: AuthStatus;
  canRead: boolean;
  canOperate: boolean;
  canRetryLogout: boolean;
  role: 'operator' | 'viewer' | 'none';
  principalId: string | null;
  username: string | null;
  error: string | null;
  sessionEpoch: number;
}
export interface AuthView extends AuthSnapshot {
  login(): Promise<void>;
  logout(): Promise<void>;
}
