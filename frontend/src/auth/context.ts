import { createContext } from 'react';
import type { AuthView } from './contracts';

export const AuthContext = createContext<AuthView | null>(null);
