import { useAuthStore } from '@/store/auth_store';
import { queryUrl, refreshSession } from './http';
import { LiveSubscriptions, type LiveTransport } from './live_subscriptions';
import { SseTransport } from './sse_transport';

/** A registry following the signed-in account: the single session wiring of each transport. */
export function createAuthLiveSubscriptions<Target>(transport: LiveTransport<Target>) {
    const subscriptions = new LiveSubscriptions({
        session: () => {
            const { token, user } = useAuthStore.getState();
            return { token, userId: user?.id ?? null };
        },
        refresh: refreshSession,
        transport,
    });
    useAuthStore.subscribe(() => subscriptions.sessionChanged());
    return subscriptions;
}

const sseSubscriptions = createAuthLiveSubscriptions(
    new SseTransport({ fetch: (input, init) => fetch(input, init) })
);

export function createSseSnapshotStream<T>(
    path: string,
    params?: Record<string, unknown>,
    eventName = 'snapshot'
) {
    return sseSubscriptions.create<T>(queryUrl(path, params), eventName);
}

export function createSharedSseSnapshotStream<T>(eventName: string) {
    return sseSubscriptions.createShared<T>(eventName);
}
