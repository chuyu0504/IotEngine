import { expect, test } from 'bun:test';
import { DebugOperationError, DebugTransport, EdgeDebugConnection, debugTarget } from '../web/lib/edge_debug_connection';
import { LiveSubscriptions } from '../web/lib/live_subscriptions';

class Socket {
    readyState = 0;
    sent: { id: string; event: string; data: unknown }[] = [];
    onopen?: () => void;
    onclose?: () => void;
    onmessage?: (event: { data: string }) => void;
    send(data: string) { this.sent.push(JSON.parse(data)); }
    open() { this.readyState = 1; this.onopen?.(); }
    close() { this.readyState = 3; this.onclose?.(); }
    reply(id: string, data: unknown) { this.onmessage?.({ data: JSON.stringify({ id, data }) }); }
    deny(id: string, code: number) { this.onmessage?.({ data: JSON.stringify({ id, error: { code, message: 'denied' } }) }); }
}
const settle = () => new Promise(resolve => setTimeout(resolve, 0));
function fixture(refresh: () => Promise<boolean> = async () => false) {
    const socket = new Socket();
    const channel = new EdgeDebugConnection(() => socket as unknown as WebSocket);
    const session = { token: 'fixture', userId: 'user-a' };
    const subscriptions = new LiveSubscriptions({ session: () => ({ ...session }), refresh, transport: new DebugTransport(channel) });
    const stream = <T,>(event: string, data: unknown = {}) => subscriptions.create<T>(debugTarget(event, data), event);
    return { socket, channel, subscriptions, session, stream };
}
const subscribeFrames = (socket: Socket) => socket.sent.filter(frame => frame.event.endsWith('.subscribe'));

test('equivalent JSON parameters share one wire subscription and captured parameters ignore later mutation', async () => {
    const { socket, channel, stream } = fixture();
    const parameters = { page: 1, id: 'device' };
    const values: unknown[] = [];
    const release = stream('edge.terminal.events.subscribe', parameters).subscribe({ next: v => values.push(v), error: () => {} });
    parameters.page = 2;
    const first = stream('edge.terminal.events.subscribe', { id: 'device', page: 1 }).first();
    await settle(); socket.open(); await settle();
    expect(socket.sent).toHaveLength(1);
    expect(socket.sent[0].data).toEqual({ id: 'device', page: 1 });
    socket.reply(socket.sent[0].id, ['first']);
    expect(await first).toEqual(['first']);
    expect(values).toEqual([['first']]);
    release(); channel.reset();
});

test('a fresh read reuses a healthy subscription and reconnects only after a failure', async () => {
    const { socket, channel, stream } = fixture();
    const source = stream<number[]>('edge.terminal.events.subscribe', { page: 1 });
    const values: unknown[] = [], errors: Error[] = [];
    const release = source.subscribe({ next: v => values.push(v), error: e => errors.push(e) });
    await settle(); socket.open(); await settle();
    const firstId = socket.sent[0].id;
    socket.reply(firstId, [1]);
    expect(await source.first(undefined, { fresh: true })).toEqual([1]);
    expect(subscribeFrames(socket)).toHaveLength(1);
    socket.deny(firstId, 17018);
    expect(errors[0]).toBeInstanceOf(DebugOperationError);
    await expect(source.first()).rejects.toBeInstanceOf(DebugOperationError);
    const restored = source.first(undefined, { fresh: true });
    await settle();
    const frames = subscribeFrames(socket);
    expect(frames).toHaveLength(2);
    expect(frames[1].id).not.toBe(firstId);
    socket.reply(firstId, ['retired']);
    socket.reply(frames[1].id, [2]);
    expect(await restored).toEqual([2]);
    expect(values).toEqual([[1], [2]]);
    release(); channel.reset();
});

test('one failing consumer cannot stop another consumer of the same snapshot', async () => {
    const { socket, channel, stream } = fixture();
    stream('edge.serial.events.subscribe').subscribe({ next: () => { throw new Error('consumer'); }, error: () => { throw new Error('consumer'); } });
    const good = stream('edge.serial.events.subscribe').first();
    await settle(); socket.open(); await settle(); socket.reply(socket.sent[0].id, [1]);
    expect(await good).toEqual([1]); expect(socket.readyState).toBe(1);
    channel.reset();
});

test('account replacement drops cached values, terminates old observers and resets the socket', async () => {
    const { socket, channel, subscriptions, session, stream } = fixture();
    const values: unknown[] = []; let failures = 0;
    const release = stream('edge.serial.events.subscribe').subscribe({ next: v => values.push(v), error: () => { failures++; } });
    await settle(); socket.open(); await settle(); const id = socket.sent[0].id; socket.reply(id, ['private']);
    session.userId = 'user-b'; session.token = 'other'; subscriptions.sessionChanged();
    socket.reply(id, ['stale']); expect(values).toEqual([['private']]); expect(failures).toBe(1);
    expect(socket.readyState).toBe(3);
    release(); channel.reset();
});

test('an expired session refreshes once, resubscribes, and fails closed when the refresh is rejected', async () => {
    let refreshes = 0;
    const { socket, channel, session, stream } = fixture(async () => { refreshes++; session.token = `fresh-${refreshes}`; return refreshes === 1; });
    const values: unknown[] = [], errors: Error[] = [];
    const release = stream('edge.serial.events.subscribe').subscribe({ next: v => values.push(v), error: e => errors.push(e) });
    await settle(); socket.open(); await settle();
    socket.deny(subscribeFrames(socket)[0].id, 11005);
    await settle(); await settle();
    expect(refreshes).toBe(1);
    expect(subscribeFrames(socket)).toHaveLength(2);
    socket.deny(subscribeFrames(socket)[1].id, 11005);
    await settle(); await settle();
    expect(refreshes).toBe(1);
    expect(subscribeFrames(socket)).toHaveLength(2);
    expect(values).toEqual([]);
    expect((errors[0] as DebugOperationError).code).toBe(11005);
    release(); channel.reset();
});
