import { expect, test } from 'bun:test';
import { acknowledgeTerminalOutput, closeTerminal, edgeDebugConnection, openTerminal, writeTerminal } from '../web/pages/edge_node/edge_node.api';

test('terminal control and output use the dedicated debug connection', async () => {
    const previousWindow = Object.getOwnPropertyDescriptor(globalThis, 'window');
    const node = '00000000-0000-7000-8000-000000000001';
    const sessionId = '00000000-0000-7000-8000-000000000002';
    const bytes = Buffer.from([0, 255, 27, 91, 51, 49, 109]);
    let connections = 0;
    const received: { id: string; event: string; data: Record<string, unknown> }[] = [];
    const server = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        fetch(request, server) {
            const url = new URL(request.url);
            expect(url.pathname).toBe('/v1/edge/debug');
            if (server.upgrade(request)) return;
            return new Response('upgrade required', { status: 400 });
        },
        websocket: {
            open() { connections++; },
            message(socket, wire) {
                const request = JSON.parse(String(wire));
                expect(Object.keys(request).sort()).toEqual(['data', 'event', 'id']);
                expect(request.id).toMatch(/^[0-9a-f]{8}-[0-9a-f-]{27}$/);
                received.push(request);
                const data = request.event === 'edge.terminal.open' ? { id: sessionId }
                    : request.event === 'edge.terminal.events.subscribe' ? { events: [
                        { kind: 'ready' }, { kind: 'data', content: bytes.toString('base64'), sequence: 1 },
                    ] } : null;
                socket.send(JSON.stringify({ id: request.id, data }));
            },
        },
    });
    let release: (() => void) | undefined;
    try {
        Object.defineProperty(globalThis, 'window', { configurable: true, value: {
            location: { protocol: 'http:', host: `127.0.0.1:${server.port}` },
        } });
        expect(await openTerminal(node, 120, 30)).toEqual({ id: sessionId });
        const output = await new Promise<{ events: { kind: string; content?: string }[] }>((resolve, reject) => {
            release = edgeDebugConnection.subscribe('edge.terminal.events.subscribe', { id: node, sessionId }, {
                next: value => resolve(value as { events: { kind: string; content?: string }[] }), error: reject,
            });
        });
        expect(output.events[0].kind).toBe('ready');
        expect(Buffer.from(output.events[1].content!, 'base64')).toEqual(bytes);
        await acknowledgeTerminalOutput(node, sessionId, 1);
        await writeTerminal(node, sessionId, bytes);
        const input = received.find(request => request.event === 'edge.terminal.write');
        expect(input?.data.sessionId).toBe(sessionId);
        expect(Buffer.from(input?.data.content as string, 'base64')).toEqual(bytes);
        release(); release = undefined;
        await closeTerminal(node, sessionId);
        expect(connections).toBe(1);
        expect(new Set(received.map(request => request.id)).size).toBe(received.length);
    } finally {
        release?.();
        edgeDebugConnection.reset();
        server.stop(true);
        if (previousWindow) Object.defineProperty(globalThis, 'window', previousWindow);
        else Reflect.deleteProperty(globalThis, 'window');
    }
});
