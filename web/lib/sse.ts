import { ServerSentEventDecoder, type ServerSentEvent } from '@/utils/sse';

export async function consumeServerSentEvents(
    stream: ReadableStream<Uint8Array>,
    onEvent: (event: ServerSentEvent) => void,
    signal?: AbortSignal
) {
    const reader = stream.getReader();
    const text = new TextDecoder();
    const decoder = new ServerSentEventDecoder();
    let cancellation: Promise<void> | undefined;
    const cancel = () => (cancellation ??= reader.cancel().catch(() => undefined));
    const abort = () => void cancel();
    signal?.addEventListener('abort', abort, { once: true });
    try {
        while (!signal?.aborted) {
            const result = await reader.read();
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
