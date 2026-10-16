import { ServerSentEventDecoder, type ServerSentEvent } from '@/utils/sse';

/** A stream that delivered no bytes, not even keepalive comments, within the idle limit. */
export class ServerSentEventIdleError extends Error {
    constructor() {
        super('实时连接无响应');
    }
}

/**
 * `idleTimeoutMs` ends a stream that stays silent longer than the server's keepalive
 * guarantee: a proxy or network path can keep a dead connection open indefinitely.
 */
export async function consumeServerSentEvents(
    stream: ReadableStream<Uint8Array>,
    onEvent: (event: ServerSentEvent) => void,
    signal?: AbortSignal,
    idleTimeoutMs?: number
) {
    const reader = stream.getReader();
    const text = new TextDecoder();
    const decoder = new ServerSentEventDecoder();
    let cancellation: Promise<void> | undefined;
    const cancel = () => (cancellation ??= reader.cancel().catch(() => undefined));
    const abort = () => void cancel();
    const read = async () => {
        if (idleTimeoutMs === undefined) return reader.read();
        let timer: ReturnType<typeof setTimeout> | undefined;
        const idle = new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new ServerSentEventIdleError()), idleTimeoutMs);
        });
        try {
            return await Promise.race([reader.read(), idle]);
        } finally {
            clearTimeout(timer);
        }
    };
    signal?.addEventListener('abort', abort, { once: true });
    try {
        while (!signal?.aborted) {
            const result = await read();
            if (result.done) break;
            for (const event of decoder.push(text.decode(result.value, { stream: true }))) {
                onEvent(event);
            }
        }
    } finally {
        signal?.removeEventListener('abort', abort);
        await cancel();
        reader.releaseLock();
    }
}
