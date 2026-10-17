import { expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import { IncomingMessage, ServerResponse } from 'node:http';
import { Socket } from 'node:net';
import { configureSseProxy } from '../vite.config';

function responsePair(contentType: string) {
    const proxy = new EventEmitter();
    configureSseProxy(proxy as Parameters<typeof configureSseProxy>[0], {} as Parameters<typeof configureSseProxy>[1]);
    const request = new IncomingMessage(new Socket());
    const upstream = new IncomingMessage(new Socket());
    upstream.headers['content-type'] = contentType;
    const downstream = new ServerResponse(request);
    // The proxy owns its general upstream error handler independently of SSE teardown.
    upstream.on('error', () => {});
    proxy.emit('proxyRes', upstream, request, downstream);
    return { upstream, downstream };
}

for (const event of ['close', 'error'] as const) {
    test(`SSE proxy ends the browser response immediately on upstream ${event}`, () => {
        const { upstream, downstream } = responsePair('text/event-stream; charset=utf-8');
        expect(downstream.writableEnded).toBe(false);
        upstream.emit(event, ...(event === 'error' ? [new Error('upstream lost')] : []));
        expect(downstream.writableEnded).toBe(true);
        // Repeated teardown events must remain harmless.
        upstream.emit('close');
        expect(downstream.writableEnded).toBe(true);
    });
}

test('proxy leaves non-SSE response completion to normal HTTP piping', () => {
    const { upstream, downstream } = responsePair('application/json');
    downstream.write('{"items":');
    upstream.emit('close');
    expect(downstream.writableEnded).toBe(false);
    upstream.emit('error', new Error('ordinary response error'));
    expect(downstream.writableEnded).toBe(false);
    downstream.end('[]}');
    expect(downstream.writableEnded).toBe(true);
});

test('closing the browser response releases SSE teardown listeners', () => {
    const { upstream, downstream } = responsePair('text/event-stream');
    expect(upstream.listenerCount('close')).toBe(1);
    expect(upstream.listenerCount('error')).toBe(2);
    downstream.emit('close');
    expect(upstream.listenerCount('close')).toBe(0);
    expect(upstream.listenerCount('error')).toBe(1);
});
