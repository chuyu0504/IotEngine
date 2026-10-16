import {
    hashKey,
    type QueryClient,
    type QueryKey,
    type QueryObserverLoadingErrorResult,
    type UseQueryOptions,
    type UseQueryResult,
    useQuery,
    useQueryClient,
} from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';
import {
    applySnapshotDelta,
    type SnapshotObserver,
    type SnapshotStream,
} from '@/lib/snapshot_stream';
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

/**
 * Writes a live stream into the query cache. A delta replaces only the rows it names, so untouched
 * rows keep their object identity and their memoized row components skip rendering.
 */
export function snapshotCacheObserver<T>(
    client: QueryClient,
    queryKey: QueryKey,
    status: { received: () => void; error: (error: Error) => void }
): SnapshotObserver<T> {
    return {
        next: (value) => {
            status.received();
            client.setQueryData(queryKey, value);
        },
        delta: (delta, value) => {
            status.received();
            client.setQueryData<T>(queryKey, (cached) =>
                cached === undefined ? value : applySnapshotDelta(cached, delta)
            );
        },
        error: status.error,
    };
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
        return factory.current().subscribe(
            snapshotCacheObserver<T>(client, queryKey.current, {
                received: () =>
                    setFailure((current) =>
                        current?.subscription === subscription ? null : current
                    ),
                error: (error) => setFailure({ subscription, error }),
            })
        );
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
