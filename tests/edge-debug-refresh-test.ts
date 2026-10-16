import { expect, test } from 'bun:test';
import { DebugTransport, EdgeDebugConnection, debugTarget } from '../web/lib/edge_debug_connection';
import { LiveSubscriptions } from '../web/lib/live_subscriptions';
import { SnapshotStream } from '../web/lib/snapshot_stream';

test('fresh readers of a failed subscription coalesce into one resubscription and ignore retired replies', async () => {
    const { channel, sockets, stream } = fixture();
    const source = stream<number>('edge.terminal.events.subscribe', { page: 1 });
    const values: number[] = [];
    const errors: Error[] = [];
    const release = source.subscribe({ next: (value) => values.push(value), error: (error) => errors.push(error) });
    await settle();
    const socket = sockets[0]; socket.open(); await settle();
    const oldId = String(subscriptions(socket)[0].id);
    socket.reply(oldId, 1);
    socket.deny(oldId);
    expect(errors).toHaveLength(1);
    let resolved = false;
    const fresh = source.map((value) => value * 2).filter((value) => value > 0)
        .first(undefined, { fresh: true }).then((value) => { resolved = true; return value; });
    const concurrent = SnapshotStream.combine([source] as const).first(undefined, { fresh: true });
    await settle();
    expect(resolved).toBe(false);
    expect(sockets).toHaveLength(1);
    const frames = subscriptions(socket);
    expect(frames).toHaveLength(2);
    socket.reply(oldId, 99);
    expect(values).toEqual([1]);
    socket.reply(String(frames[1].id), 2);
    expect(await fresh).toBe(4);
    expect(await concurrent).toEqual([2]);
    expect(values).toEqual([1, 2]);
    release(); channel.reset(); await settle();
});

test('query conditions have independent subscriptions and cancelled reads release them', async () => {
    const { channel, sockets, stream } = fixture();
    const abort = new AbortController();
    const first = stream('edge.terminal.events.subscribe', { page: 1 }).first(abort.signal).catch((error) => error);
    const second = stream('edge.terminal.events.subscribe', { page: 2 }).first();
    await settle();
    const socket = sockets[0]; socket.open(); await settle();
    const frames = subscriptions(socket);
    abort.abort();
    expect((await first).name).toBe('AbortError');
    socket.reply(String(frames[0].id), 'old page');
    socket.reply(String(frames[1].id), 'new page');
    expect(await second).toBe('new page');
    expect(socket.sent.filter((frame) => frame.event === 'subscription.cancel')).toHaveLength(2);
    channel.reset(); await settle();
});

test('invalid subscription events are rejected without touching the socket', async () => {
    const { sockets, stream } = fixture();
    await expect(stream('Edge.Invalid').first()).rejects.toThrow('订阅目标无效');
    expect(sockets).toHaveLength(0);
});

class Socket {
    readyState = 0;
    onopen?: () => void;
    onclose?: () => void;
    onerror?: () => void;
    onmessage?: (event: { data: string }) => void;
    sent: Array<Record<string, unknown>> = [];
    send(value: string) { this.sent.push(JSON.parse(value)); }
    open() { this.readyState = 1; this.onopen?.(); }
    close() { this.readyState = 3; queueMicrotask(() => this.onclose?.()); }
    reply(id: string, data: unknown) {
        this.onmessage?.({ data: JSON.stringify({ id, data }) });
    }
    deny(id: string) {
        this.onmessage?.({ data: JSON.stringify({ id, error: { code: 17018, message: 'session closed' } }) });
    }
}
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));
const subscriptions = (socket: Socket) => socket.sent.filter((frame) => frame.event === 'edge.terminal.events.subscribe');
function fixture() {
    const sockets: Socket[] = [];
    const channel = new EdgeDebugConnection(() => {
        const socket = new Socket(); sockets.push(socket); return socket as unknown as WebSocket;
    });
    const registry = new LiveSubscriptions({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        transport: new DebugTransport(channel),
    });
    const stream = <T,>(event: string, data: unknown = {}) => registry.create<T>(debugTarget(event, data), event);
    return { channel, sockets, stream };
}
