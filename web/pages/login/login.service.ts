import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { useEffect } from 'react';
import { HttpRequestError } from '@/lib/http';
import { useAuthStore } from '@/store/auth_store';
import { fetchCurrentUser, login, observeCurrentUser, logout, refreshToken } from './login.api';

const loginKeys = {
    currentUser: ['auth', 'currentUser'] as const,
};

export function useLogin() {
    const setAuth = useAuthStore((state) => state.setAuth);
    return useMutation({
        mutationFn: login,
        onSuccess: ({ token, refresh_token, user }) => setAuth(token, refresh_token, user),
    });
}

export function useCurrentUser() {
    const token = useAuthStore((s) => s.token);
    const user = useAuthStore((s) => s.user);
    const setUser = useAuthStore((s) => s.setUser);
    const queryClient = useQueryClient();
    const userId = user?.id;
    useEffect(() => {
        if (!token) return;
        return observeCurrentUser().subscribe({
            next: (freshUser) => {
                if (useAuthStore.getState().user?.id !== userId) return;
                const queryKey = [...loginKeys.currentUser, userId];
                void queryClient.cancelQueries({ queryKey, exact: true });
                queryClient.setQueryData(queryKey, freshUser);
                setUser(freshUser);
            },
            error: () => {},
        });
    }, [token, userId, queryClient, setUser]);
    return useQuery({
        queryKey: [...loginKeys.currentUser, userId],
        queryFn: async ({ signal }) => {
            const freshUser = await fetchCurrentUser(signal);
            if (!signal.aborted && useAuthStore.getState().user?.id === userId) setUser(freshUser);
            return freshUser;
        },
        enabled: !!token,
        initialData: user ?? undefined,
        initialDataUpdatedAt: 0,
        staleTime: Infinity,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
        refetchOnMount: 'always',
    });
}
export function useLogout() {
    const navigate = useNavigate();
    const clearAuth = useAuthStore((s) => s.clearAuth);
    return useMutation({
        mutationFn: () => logout(),
        onSettled: () => {
            clearAuth();
            navigate('/login', { replace: true });
        },
    });
}

let pendingRefresh: Promise<boolean> | undefined;
export function refreshAccessToken(): Promise<boolean> {
    if (pendingRefresh) return pendingRefresh;
    const currentRefreshToken = useAuthStore.getState().refresh_token;
    if (!currentRefreshToken) return Promise.resolve(false);
    pendingRefresh = refreshToken(currentRefreshToken, { _silent: true })
        .then(({ token, refresh_token, user }) => {
            if (useAuthStore.getState().refresh_token !== currentRefreshToken) return false;
            useAuthStore.getState().setAuth(token, refresh_token, user);
            return true;
        })
        .catch((error) => {
            if (
                error instanceof HttpRequestError &&
                (error.status === 401 || error.status === 403) &&
                useAuthStore.getState().refresh_token === currentRefreshToken
            )
                useAuthStore.getState().clearAuth();
            return false;
        })
        .finally(() => {
            pendingRefresh = undefined;
        });
    return pendingRefresh;
}
