import { expect, spyOn, test } from 'bun:test';
import * as React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import type { DebugSubscriptionObserver } from '../web/lib/edge_debug_connection';
import { acknowledgeTerminalOutput, closeTerminal, edgeDebugConnection, openTerminal, writeTerminal } from '../web/pages/edge_node/edge_node.api';
import { useTerminalSession } from '../web/pages/edge_node/edge_node.service';
import type { Edge } from '../web/pages/edge_node/edge_node.types';
import { useAuthStore } from '../web/store/auth_store';

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

const node = '00000000-0000-7000-8000-000000000001';
const sessionId = '00000000-0000-7000-8000-000000000002';
const settle = () => Bun.sleep(0);
const output = (sequence: number, content: string) => ({
    kind: 'data' as const, sequence, content: Buffer.from(content).toString('base64'),
});

function mountTerminal(screen: Edge.TerminalScreen) {
    const effects: React.EffectCallback[] = [];
    const effect = spyOn(React, 'useEffect').mockImplementation(callback => {
        effects.push(callback);
    });
    function Capture() {
        useTerminalSession(node, screen);
        return null;
    }
    try {
        renderToStaticMarkup(React.createElement(Capture));
    } finally {
        effect.mockRestore();
    }
    // Run the actual session lifecycle without needing an xterm or a DOM renderer.
    expect(effects).toHaveLength(1);
    const dispose = effects[0]();
    if (typeof dispose !== 'function') throw new Error('Terminal session did not start');
    return dispose;
}

async function terminalFixture() {
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
    const originalSession = useAuthStore.getState();
    Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
        getItem: () => null, setItem: () => {}, removeItem: () => {},
    } });
    const user = {
        id: 'terminal-user', username: 'tester', status: 'enabled', roles: [], permissions: [],
    };
    const requests: string[] = [];
    const acknowledgements: {
        sequence: number; resolve: () => void; reject: (error: Error) => void;
    }[] = [];
    const subscriptions: { observer: DebugSubscriptionObserver; active: boolean }[] = [];
    const send = spyOn(edgeDebugConnection, 'request').mockImplementation(async (event, data) => {
        requests.push(event);
        if (event === 'edge.terminal.open') return { id: sessionId };
        if (event === 'edge.terminal.output.ack') {
            if (!data || typeof data !== 'object' || !('sequence' in data) ||
                typeof data.sequence !== 'number')
                throw new Error('Terminal ACK is missing its sequence');
            const { promise, resolve, reject } = Promise.withResolvers<void>();
            acknowledgements.push({ sequence: data.sequence, resolve, reject });
            return promise;
        }
        return null;
    });
    const subscribe = spyOn(edgeDebugConnection, 'subscribe').mockImplementation((event, data, observer) => {
        expect(event).toBe('edge.terminal.events.subscribe');
        expect(data).toEqual({ id: node, sessionId });
        const subscription = { observer, active: true };
        subscriptions.push(subscription);
        return () => { subscription.active = false; };
    });
    const writes: string[] = [];
    const completions: (() => void)[] = [];
    let resets = 0;
    let closed = 0;
    let revision = 0;
    let dispose: (() => void) | undefined;
    const screen: Edge.TerminalScreen = {
        size: () => ({ columns: 80, rows: 24 }),
        reset: () => { resets++; },
        write: (bytes, written) => {
            writes.push(Buffer.from(bytes).toString('utf8'));
            completions.push(written);
        },
        ready: () => {},
        closed: () => { closed++; },
    };
    async function cleanup() {
        dispose?.();
        useAuthStore.setState(originalSession);
        await settle();
        send.mockRestore();
        subscribe.mockRestore();
        if (previousStorage) Object.defineProperty(globalThis, 'sessionStorage', previousStorage);
        else Reflect.deleteProperty(globalThis, 'sessionStorage');
    }
    try {
        useAuthStore.getState().setAuth('terminal-token', 'terminal-refresh', user);
        dispose = mountTerminal(screen);
        await settle();
        expect(subscriptions).toHaveLength(1);
    } catch (error) {
        await cleanup();
        throw error;
    }
    return {
        writes, acknowledgements, subscriptions, requests, cleanup,
        get resets() { return resets; },
        get closed() { return closed; },
        receive(events: unknown[]) {
            subscriptions.at(-1)!.observer.next({ events });
        },
        rendered() {
            const written = completions.shift();
            if (!written) throw new Error('No pending terminal render');
            written();
        },
        async refresh() {
            useAuthStore.getState().setAuth(`terminal-token-${++revision}`, 'terminal-refresh', user);
            await settle();
            expect(subscriptions.filter(subscription => subscription.active)).toHaveLength(1);
        },
    };
}

test('token refresh preserves pending renders and ignores replay without an early ACK', async () => {
    const terminal = await terminalFixture();
    try {
        terminal.receive([{ kind: 'ready' }, output(1, 'first'), output(2, 'second')]);
        expect(terminal.writes).toEqual(['first']);
        expect(terminal.acknowledgements).toHaveLength(0);

        await terminal.refresh();
        expect(terminal.subscriptions).toHaveLength(2);
        terminal.receive([
            { kind: 'ready' }, output(1, 'first'), output(2, 'second'), output(3, 'third'),
        ]);
        expect(terminal.writes).toEqual(['first']);
        expect(terminal.acknowledgements).toHaveLength(0);
        expect(terminal.resets).toBe(1);
        expect(terminal.closed).toBe(0);

        for (let index = 0; index < 3; index++) {
            terminal.rendered();
            expect(terminal.acknowledgements.map(ack => ack.sequence)).toEqual(
                Array.from({ length: index + 1 }, (_, offset) => offset + 1)
            );
            // The next frame cannot render before the preceding ACK succeeds.
            expect(terminal.writes).toHaveLength(index + 1);
            terminal.acknowledgements[index].resolve();
            await settle();
        }
        expect(terminal.writes).toEqual(['first', 'second', 'third']);
        expect(terminal.closed).toBe(0);
    } finally {
        await terminal.cleanup();
    }
});

test('rendered replay recovers a lost ACK and stale replies cannot release a newer render', async () => {
    const terminal = await terminalFixture();
    try {
        terminal.receive([{ kind: 'ready' }, output(1, 'first'), output(2, 'second')]);
        terminal.rendered();
        const lost = terminal.acknowledgements[0];
        expect(lost.sequence).toBe(1);

        await terminal.refresh();
        terminal.receive([{ kind: 'ready' }, output(1, 'first'), output(2, 'second')]);
        expect(terminal.writes).toEqual(['first']);
        expect(terminal.acknowledgements.map(ack => ack.sequence)).toEqual([1, 1]);
        // Only the replacement ACK reaches the server; its reply must resume output.
        terminal.acknowledgements[1].resolve();
        await settle();
        expect(terminal.writes).toEqual(['first', 'second']);

        terminal.receive([output(1, 'first'), output(2, 'second'), output(3, 'third')]);
        expect(terminal.acknowledgements.map(ack => ack.sequence)).toEqual([1, 1, 1]);
        terminal.acknowledgements[2].resolve();
        lost.reject(new Error('Original ACK reply was lost'));
        await settle();
        expect(terminal.writes).toEqual(['first', 'second']);
        expect(terminal.closed).toBe(0);

        terminal.rendered();
        expect(terminal.acknowledgements.map(ack => ack.sequence)).toEqual([1, 1, 1, 2]);
        terminal.acknowledgements[3].resolve();
        await settle();
        expect(terminal.writes).toEqual(['first', 'second', 'third']);
        terminal.rendered();
        terminal.acknowledgements[4].resolve();
        await settle();

        // Older retained frames repeat the latest rendered cumulative ACK, never an old one.
        terminal.receive([output(1, 'first')]);
        expect(terminal.acknowledgements.map(ack => ack.sequence)).toEqual([1, 1, 1, 2, 3, 3]);
        terminal.acknowledgements[5].resolve();
        await settle();
        expect(terminal.writes).toEqual(['first', 'second', 'third']);
        expect(terminal.closed).toBe(0);
    } finally {
        await terminal.cleanup();
    }
});

test('an unconfirmed output ACK failure still closes the session', async () => {
    const terminal = await terminalFixture();
    try {
        terminal.receive([{ kind: 'ready' }, output(1, 'first'), output(2, 'second')]);
        terminal.rendered();
        terminal.acknowledgements[0].reject(new Error('Output ACK rejected'));
        await settle();
        expect(terminal.writes).toEqual(['first']);
        expect(terminal.closed).toBe(1);
        expect(terminal.requests.filter(event => event === 'edge.terminal.close')).toHaveLength(1);
    } finally {
        await terminal.cleanup();
    }
});

test('replay tolerance still fails a genuine output gap and cancels pending ACK eligibility', async () => {
    const terminal = await terminalFixture();
    try {
        terminal.receive([{ kind: 'ready' }, output(1, 'first')]);
        await terminal.refresh();
        terminal.receive([output(1, 'first'), output(3, 'gap')]);
        expect(terminal.writes).toEqual(['first']);
        expect(terminal.closed).toBe(1);
        expect(terminal.requests.filter(event => event === 'edge.terminal.close')).toHaveLength(1);
        terminal.rendered();
        await settle();
        expect(terminal.acknowledgements).toHaveLength(0);
    } finally {
        await terminal.cleanup();
    }
});

test('legacy sequence-zero frames retain repeated output without sequence ACKs', async () => {
    const terminal = await terminalFixture();
    try {
        terminal.receive([{ kind: 'ready' }, output(0, 'legacy'), output(0, 'legacy')]);
        terminal.rendered();
        await settle();
        terminal.rendered();
        await settle();
        expect(terminal.writes).toEqual(['legacy', 'legacy']);
        expect(terminal.acknowledgements).toHaveLength(0);
        expect(terminal.closed).toBe(0);
    } finally {
        await terminal.cleanup();
    }
});
