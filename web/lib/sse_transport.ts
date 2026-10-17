import { HttpRequestError } from './http';
import type { LiveContext, LiveSink, LiveTransport } from './live_subscriptions';
import { consumeServerSentEvents } from './sse';
import type { SnapshotDelta } from './snapshot_stream';

/** One fetch-based SSE connection per URL; named events are the channels of that source. */
export class SseTransport implements LiveTransport<string> {
    private readonly receivers = new Map<AbortController, Promise<void>>();

    constructor(
        private readonly options: {
            fetch: typeof fetch;
            retryDelay?: (attempt: number, signal: AbortSignal) => Promise<void>;
        }
    ) {}

    key(url: string) {
        return url.startsWith('/v1/') || url.startsWith('/api/') ? url : undefined;
    }

    open(url: string, context: LiveContext, sink: LiveSink) {
        const controller = new AbortController();
        // A replacement connects only after cancelled readers released their connection.
        const closing = [...this.receivers]
            .filter(([previous]) => previous.signal.aborted)
            .map(([, finished]) => finished);
        const finished = Promise.all(closing)
            .then(() => {
                if (!controller.signal.aborted)
                    return this.receive(url, context, sink, controller.signal);
            })
            .finally(() => this.receivers.delete(controller));
        this.receivers.set(controller, finished);
        return () => controller.abort();
    }

    private async receive(url: string, context: LiveContext, sink: LiveSink, signal: AbortSignal) {
        let failures = 0;
        while (!signal.aborted) {
            const connectedAt = Date.now();
            try {
                const response = await this.options.fetch(url, {
                    headers: {
                        Accept: 'text/event-stream',
                        Authorization: `Bearer ${context.token}`,
                        ...(context.shared ? { 'X-SSE-User': '1' } : {}),
                    },
                    cache: 'no-store',
                    signal,
                });
                if (signal.aborted) {
                    await response.body?.cancel();
                    return;
                }
                if (!response.ok) {
                    const error = await response.json().catch(() => ({}));
                    throw new HttpRequestError(
                        error.message ?? '实时订阅连接失败',
                        response.status,
                        error.code
                    );
                }
                if (
                    !response.body ||
                    !response.headers.get('content-type')?.includes('text/event-stream')
                )
                    throw new HttpRequestError('服务器未返回 SSE 数据流', 406);
                await consumeServerSentEvents(
                    response.body,
                    (event) => {
                        if (signal.aborted || event.event === 'heartbeat') return;
                        const result = JSON.parse(event.data);
                        if (result.code !== 0 || event.event === 'error') {
                            const error = new HttpRequestError(
                                result.message ?? '实时订阅已中断',
                                result.code === 11005 ? 401 : result.code === 10004 ? 500 : 403,
                                result.code
                            );
                            if (event.event === 'error' || event.event === 'snapshot') throw error;
                            sink.reject(event.event, error);
                            return;
                        }
                        if (result.delta !== undefined)
                            sink.delta(event.event, readSnapshotDelta(result.delta));
                        else sink.next(event.event, result.data);
                    },
                    signal
                );
                if (signal.aborted) return;
                throw new Error('实时连接已断开');
            } catch (failure) {
                if (signal.aborted) return;
                const error = failure instanceof Error ? failure : new Error(String(failure));
                if (error instanceof HttpRequestError && error.status === 401) {
                    sink.fail(error, true);
                    return;
                }
                if (
                    (error instanceof HttpRequestError && error.status < 500) ||
                    error instanceof SyntaxError
                ) {
                    sink.fail(error);
                    return;
                }
                // Reconnect only after a failed connection, never on a healthy-stream timer.
                if (Date.now() - connectedAt >= 30000) failures = 0;
                if (++failures > 8) {
                    sink.fail(error);
                    return;
                }
                try {
                    await (this.options.retryDelay ?? waitBeforeReconnect)(failures, signal);
                } catch {
                    if (!signal.aborted) sink.fail(error);
                    return;
                }
            }
        }
    }
}

function waitBeforeReconnect(attempt: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
        const abort = () => {
            clearTimeout(timer);
            signal.removeEventListener('abort', abort);
            reject(signal.reason);
        };
        const timer = setTimeout(
            () => {
                signal.removeEventListener('abort', abort);
                resolve();
            },
            Math.min(1000 * 2 ** (attempt - 1), 30000)
        );
        signal.addEventListener('abort', abort, { once: true });
        if (signal.aborted) abort();
    });
}

/** Validates a `delta` frame body; a malformed frame is a protocol error that ends the source. */
function readSnapshotDelta(value: unknown): SnapshotDelta {
    if (typeof value === 'object' && value !== null) {
        const { upsert, remove } = value as Record<string, unknown>;
        if (
            Array.isArray(upsert) &&
            Array.isArray(remove) &&
            remove.every((id) => typeof id === 'string')
        )
            return { upsert, remove };
    }
    throw new SyntaxError('实时增量格式无效');
}
