import { expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import type { ClientRequest, IncomingMessage, Server, ServerResponse } from 'node:http';
import { createServer, get } from 'node:http';
import { configureSseProxy } from '../vite.config';

async function listen(server: Server) {
    await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', resolve);
    });
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('HTTP fixture has no address');
    return `http://127.0.0.1:${address.port}`;
}

async function responsePair(contentType: string) {
    const proxy = new EventEmitter();
    configureSseProxy(proxy as Parameters<typeof configureSseProxy>[0], {} as Parameters<typeof configureSseProxy>[1]);
    const source = createServer((_request, response) => {
        response.writeHead(200, { 'Content-Type': contentType });
        response.write('ready\n');
    });
    const sourceUrl = await listen(source);
    let sourceRequest: ClientRequest | undefined;
    const upstream = await new Promise<IncomingMessage>((resolve, reject) => {
        sourceRequest = get(sourceUrl, resolve);
        sourceRequest.once('error', reject);
    });
    // The proxy's general upstream error handler is independent of SSE teardown.
    upstream.on('error', () => {});
    let responseReady: (() => void) | undefined;
    const ready = new Promise<void>(resolve => { responseReady = resolve; });
    let pair: { request: IncomingMessage; response: ServerResponse } | undefined;
    const destination = createServer((request, response) => {
        pair = { request, response };
        responseReady?.();
    });
    const destinationUrl = await listen(destination);
    const destinationRequest = get(destinationUrl, response => response.resume());
    destinationRequest.on('error', () => {});
    await ready;
    if (!pair) throw new Error('HTTP fixture did not receive the downstream request');
    const downstream = pair.response;
    proxy.emit('proxyRes', upstream, pair.request, downstream);
    return {
        upstream, downstream,
        async close() {
            sourceRequest?.destroy();
            destinationRequest.destroy();
            upstream.destroy();
            downstream.destroy();
            source.closeAllConnections();
            destination.closeAllConnections();
            await Promise.all([source, destination].map(server => new Promise<void>(resolve => server.close(() => resolve()))));
        },
    };
}

for (const event of ['close', 'error'] as const) {
    test(`SSE proxy ends the browser response immediately on upstream ${event}`, async () => {
        const pair = await responsePair('text/event-stream; charset=utf-8');
        try {
            expect(pair.downstream.writableEnded).toBe(false);
            pair.upstream.emit(event, ...(event === 'error' ? [new Error('upstream lost')] : []));
            expect(pair.downstream.writableEnded).toBe(true);
            pair.upstream.emit('close');
            expect(pair.downstream.writableEnded).toBe(true);
        } finally { await pair.close(); }
    });
}

test('proxy leaves non-SSE response completion to normal HTTP piping', async () => {
    const pair = await responsePair('application/json');
    try {
        pair.downstream.write('{"items":');
        pair.upstream.emit('close');
        expect(pair.downstream.writableEnded).toBe(false);
        pair.upstream.emit('error', new Error('ordinary response error'));
        expect(pair.downstream.writableEnded).toBe(false);
        pair.downstream.end('[]}');
        expect(pair.downstream.writableEnded).toBe(true);
    } finally { await pair.close(); }
});

test('closing the browser response releases SSE teardown listeners', async () => {
    const pair = await responsePair('text/event-stream');
    try {
        const closeListeners = pair.upstream.listenerCount('close');
        const errorListeners = pair.upstream.listenerCount('error');
        pair.downstream.emit('close');
        expect(pair.upstream.listenerCount('close')).toBe(closeListeners - 1);
        expect(pair.upstream.listenerCount('error')).toBe(errorListeners - 1);
    } finally { await pair.close(); }
});
