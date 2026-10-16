import { expect, test } from 'bun:test';
import { EdgeDebugConnection, DebugConnectionError, DebugOperationError } from '../web/lib/edge_debug_connection';

class Socket {
    readyState = 0;
    sent: { id: string; event: string; data: unknown }[] = [];
    onopen?: () => void;
    onclose?: () => void;
    onerror?: () => void;
    onmessage?: (event: { data: string }) => void;
    send(data: string) { this.sent.push(JSON.parse(data)); }
    open() { this.readyState = 1; this.onopen?.(); }
    close() { this.readyState = 2; }
    closed() { this.readyState = 3; this.onclose?.(); }
    reply(id: string, data: unknown) { this.onmessage?.({ data: JSON.stringify({ id, data }) }); }
}
const settle = () => new Promise(resolve => setTimeout(resolve, 0));
function fixture() {
    const sockets: Socket[] = [];
    const channel = new EdgeDebugConnection(() => {
        const socket = new Socket(); sockets.push(socket); return socket as unknown as WebSocket;
    });
    return { channel, sockets };
}

test('debug operations share one socket, correlate replies and cancel with a fresh UUID', async () => {
    const { channel, sockets } = fixture();
    const values: unknown[] = [];
    const release = channel.subscribe('edge.serial.events.subscribe', {}, { next: value => values.push(value), error: () => {} });
    const first = channel.request('edge.serial.open', { name: 'a' });
    const second = channel.request('edge.serial.close', { id: 'b' });
    expect(sockets).toHaveLength(1);
    const socket = sockets[0]; socket.open(); await settle();
    expect(socket.sent.map(frame => Object.keys(frame))).toEqual(Array(3).fill(['id', 'event', 'data']));
    for (const frame of socket.sent) expect(frame.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    socket.reply(socket.sent[1].id, 'second'); socket.reply(socket.sent[0].id, 'first');
    expect(await first).toBe('first'); expect(await second).toBe('second');
    const subscriptionId = socket.sent[2].id;
    socket.reply(subscriptionId, [1]); release(); socket.reply(subscriptionId, [2]);
    expect(values).toEqual([[1]]);
    expect(socket.sent[3].event).toBe('subscription.cancel');
    expect(socket.sent[3].id).not.toBe(subscriptionId);
    expect(socket.sent[3].data).toEqual({ subscriptionId });
    channel.reset(); socket.closed();
});

test('session restoration gates protected operations and reconnect never replays writes', async () => {
    const { channel, sockets } = fixture();
    channel.configureSessionRestore(async () => { await channel.request('edge.debug.authenticate', { token: 'fixture' }, { anonymous: true }); });
    const release = channel.subscribe('edge.serial.events.subscribe', {}, { next: () => {}, error: () => {} });
    const write = channel.request('edge.serial.open', {}).catch(error => error);
    const socket = sockets[0]; socket.open(); await settle();
    expect(socket.sent.map(frame => frame.event)).toEqual(['edge.debug.authenticate']);
    socket.reply(socket.sent[0].id, null); await settle();
    expect(socket.sent.map(frame => frame.event)).toEqual(['edge.debug.authenticate', 'edge.serial.open', 'edge.serial.events.subscribe']);
    const oldId = socket.sent[2].id;
    socket.closed();
    const error = await write;
    expect(error).toBeInstanceOf(DebugConnectionError); expect(error.mayHaveExecuted).toBe(true);
    await new Promise(resolve => setTimeout(resolve, 1050));
    expect(sockets).toHaveLength(2);
    const next = sockets[1]; next.open(); await settle();
    next.reply(next.sent[0].id, null); await settle();
    expect(next.sent.map(frame => frame.event)).toEqual(['edge.debug.authenticate', 'edge.serial.events.subscribe']);
    expect(next.sent[1].id).not.toBe(oldId);
    release(); channel.reset(); next.closed();
});

test('reset queues authentication until the old socket has actually closed', async () => {
    const { channel, sockets } = fixture();
    const old = channel.request('edge.serial.open').catch(error => error);
    sockets[0].open(); await settle(); channel.reset();
    const login = channel.request('edge.debug.authenticate', {}, { anonymous: true });
    expect(sockets).toHaveLength(1);
    expect((await old).mayHaveExecuted).toBe(true);
    sockets[0].closed(); expect(sockets).toHaveLength(2);
    sockets[1].open(); await settle();
    sockets[1].reply(sockets[1].sent[0].id, 'ok'); expect(await login).toBe('ok');
    channel.reset(); sockets[1].closed();
});

test('debug errors and observer failures stay isolated from other operations', async () => {
    const { channel, sockets } = fixture();
    let failed = false;
    channel.subscribe('edge.serial.events.subscribe', {}, { next: () => { throw new Error('consumer'); }, error: () => { failed = true; throw new Error('consumer'); } });
    const pending = channel.request('edge.serial.close').catch(error => error);
    const socket = sockets[0]; socket.open(); await settle();
    socket.reply(socket.sent[1].id, []); expect(failed).toBe(true); expect(socket.readyState).toBe(1);
    socket.onmessage?.({ data: JSON.stringify({ id: socket.sent[0].id, error: { code: 11007, message: 'denied' } }) });
    expect(await pending).toBeInstanceOf(DebugOperationError);
    channel.reset(); socket.closed();
});

test('aborting a dispatched write reports uncertainty and ignores its late reply', async () => {
    const { channel, sockets } = fixture();
    const abort = new AbortController();
    const write = channel.request('edge.serial.open', {}, { signal: abort.signal }).catch(error => error);
    sockets[0].open(); await settle(); abort.abort();
    const error = await write;
    expect(error).toBeInstanceOf(DebugConnectionError); expect(error.mayHaveExecuted).toBe(true);
    expect(sockets[0].sent).toHaveLength(1);
    sockets[0].reply(sockets[0].sent[0].id, null);
    expect(sockets[0].readyState).toBe(1);
    channel.reset(); sockets[0].closed();
});

test('legacy HTTP-shaped responses fail the connection instead of being interpreted', async () => {
    const { channel, sockets } = fixture();
    const write = channel.request('edge.serial.open').catch(error => error);
    sockets[0].open(); await settle();
    sockets[0].onmessage?.({ data: JSON.stringify({ id: sockets[0].sent[0].id, status: 200, data: null }) });
    expect(await write).toBeInstanceOf(DebugConnectionError);
    expect(sockets[0].readyState).toBe(2);
    sockets[0].closed(); channel.reset();
});


test('brief successful reconnects exhaust nine retries and a manual reopen starts a new session', async () => {
    const nativeSetTimeout = globalThis.setTimeout;
    const nativeClearTimeout = globalThis.clearTimeout;
    const timers = new Map<number, () => void>();
    let sequence = 0;
    globalThis.setTimeout = ((callback: () => void) => {
        const id = ++sequence;
        timers.set(id, callback);
        return id;
    }) as typeof setTimeout;
    globalThis.clearTimeout = ((id: number) => { timers.delete(id); }) as typeof clearTimeout;
    const { channel, sockets } = fixture();
    const errors: Error[] = [];
    try {
        channel.subscribe('edge.serial.events.subscribe', {}, { next: () => {}, error: error => errors.push(error) });
        for (let attempt = 0; attempt <= 9; ++attempt) {
            sockets[attempt].open();
            await Promise.resolve();
            expect(timers.size).toBe(0);
            sockets[attempt].closed();
            if (attempt < 9) {
                expect(timers.size).toBe(1);
                const [id, callback] = [...timers][0];
                timers.delete(id);
                callback();
                expect(sockets).toHaveLength(attempt + 2);
            }
        }
        expect(timers.size).toBe(0);
        expect(sockets).toHaveLength(10);
        expect(errors).toHaveLength(1);
        expect(errors[0].message).toContain('重试已达上限');
        const release = channel.subscribe('edge.serial.events.subscribe', {}, { next: () => {}, error: () => {} });
        expect(sockets).toHaveLength(11);
        sockets[10].open();
        await Promise.resolve();
        release();
        channel.reset();
        sockets[10].closed();
        expect(timers.size).toBe(0);
    } finally {
        channel.reset();
        globalThis.setTimeout = nativeSetTimeout;
        globalThis.clearTimeout = nativeClearTimeout;
    }
});
