import {
    hashKey,
    type QueryObserverLoadingErrorResult,
    type UseQueryOptions,
    type UseQueryResult,
    useQuery,
    useQueryClient,
} from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';
import type { SnapshotStream } from '@/lib/snapshot_stream';
import { useAuthStore } from '@/store/auth_store';

/** 实时查询的缓存只随推送更新，刷新与重试策略由本 Hook 固定，调用方不可覆盖。 */
type Options<T, Selected> = Omit<
    UseQueryOptions<T, Error, Selected>,
    | 'queryFn'
    | 'staleTime'
    | 'retry'
    | 'refetchInterval'
    | 'refetchOnWindowFocus'
    | 'refetchOnReconnect'
> & {
    queryFn: () => SnapshotStream<T>;
    streamKey?: unknown;
};

interface StreamFailure {
    subscription: string;
    error: Error;
}

export function useSnapshotQuery<T, Selected = T>(
    options: Options<T, Selected>
): UseQueryResult<Selected, Error> {
    const client = useQueryClient();
    const token = useAuthStore((state) => state.token);
    const factory = useRef(options.queryFn);
    factory.current = options.queryFn;
    const queryKey = useRef(options.queryKey);
    queryKey.current = options.queryKey;
    const subscription = hashKey([options.queryKey, options.streamKey ?? null]);
    const [failure, setFailure] = useState<StreamFailure | null>(null);
    const result = useQuery({
        ...options,
        queryFn: ({ signal }) => factory.current().first(signal, { fresh: true }),
        staleTime: Infinity,
        retry: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
    const enabled = options.enabled !== false;
    useEffect(() => {
        if (!enabled || !token) return;
        const key = queryKey.current;
        return factory.current().subscribe({
            next: (value) => {
                setFailure((current) => (current?.subscription === subscription ? null : current));
                client.setQueryData(key, value);
            },
            error: (error) => setFailure({ subscription, error }),
        });
    }, [client, subscription, enabled, token]);
    const error = failure?.subscription === subscription ? failure.error : null;
    if (!error) return result;
    // A failed stream no longer authorizes or refreshes the cached snapshot.
    const failed: QueryObserverLoadingErrorResult<Selected, Error> = {
        ...result,
        data: undefined,
        error,
        isError: true,
        isPending: false,
        isLoading: false,
        isLoadingError: true,
        isRefetchError: false,
        isSuccess: false,
        isPlaceholderData: false,
        status: 'error',
    };
    return failed;
}
