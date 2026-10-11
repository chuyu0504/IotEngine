import assert from 'node:assert/strict';
import { apiBase } from './architecture-fixture';
import { consumeServerSentEvents } from '../web/lib/sse';
import type { ServerSentEvent } from '../web/utils/sse';

export interface SnapshotSubscription {
    readonly commentCount: number;
    next(timeoutMs?: number): Promise<ServerSentEvent>;
    unread(event: ServerSentEvent): void;
    expectQuiet(durationMs?: number): Promise<void>;
    close(): Promise<void>;
}

export async function openSnapshotSubscription(path: string, bearer: string, options?: { includeUser?: boolean }): Promise<SnapshotSubscription> {
    const controller = new AbortController();
    const response = await fetch(apiBase + path, {
        headers: { Accept: 'text/event-stream', Authorization: `Bearer ${bearer}`, ...(options?.includeUser ? { 'X-SSE-User': '1' } : {}) }, signal: controller.signal,
    });
    assert.equal(response.status, 200);
    assert.match(response.headers.get('content-type') ?? '', /text\/event-stream/);
    assert(response.body);
    const events: ServerSentEvent[] = [];
    let commentCount = 0;
    let lineBuffer = '';
    const wireText = new TextDecoder();
    const observedBody = response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, output) {
            lineBuffer += wireText.decode(chunk, { stream: true });
            let newline: number;
            while ((newline = lineBuffer.indexOf('\n')) >= 0) {
                if (lineBuffer.slice(0, newline).startsWith(':')) commentCount++;
                lineBuffer = lineBuffer.slice(newline + 1);
            }
            output.enqueue(chunk);
        },
    }));
    let wake: (() => void) | undefined;
    let failure: unknown;
    let ended = false;
    const reading = consumeServerSentEvents(observedBody, event => { events.push(event); wake?.(); }, controller.signal)
        .catch(error => { if (!controller.signal.aborted) failure = error; })
        .finally(() => { ended = true; wake?.(); });
    return {
        get commentCount() { return commentCount; },
        async next(timeoutMs = 20000): Promise<ServerSentEvent> {
            if (!events.length && !ended) {
                const timeoutError = new Error('SSE event timeout');
                await new Promise<void>((resolve, reject) => {
                    const timeout = setTimeout(() => { wake = undefined; reject(timeoutError); }, timeoutMs);
                    wake = () => { clearTimeout(timeout); wake = undefined; resolve(); };
                });
            }
            if (failure) throw failure;
            const event = events.shift();
            assert(event, 'SSE ended before the expected event');
            return event;
        },
        unread(event: ServerSentEvent) {
            events.unshift(event);
        },
        async expectQuiet(durationMs = 16000) {
            assert.equal(events.length, 0, 'unexpected queued SSE business event');
            await new Promise<void>((resolve, reject) => {
                const timeout = setTimeout(() => { wake = undefined; resolve(); }, durationMs);
                wake = () => {
                    clearTimeout(timeout);
                    wake = undefined;
                    reject(failure ?? new Error(ended ? 'SSE ended during idle observation' : `unexpected SSE business event: ${events[0]?.event}`));
                };
            });
            if (failure) throw failure;
            assert.equal(ended, false, 'SSE must remain connected while idle');
            assert.equal(events.length, 0, 'unexpected SSE business event');
        },
        async close() { controller.abort(); await reading; },
    };
}
