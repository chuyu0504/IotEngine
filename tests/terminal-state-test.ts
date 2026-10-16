import { afterAll, beforeAll, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:net';

// Executes the production Lua of the edge terminal and serial debug state
// machines against a disposable redis-server. Scripts are taken by constant
// name; assertions are on the resulting Redis state only.
const terminalService = readFileSync(new URL('../service/features/edge/terminal/terminal.service.h', import.meta.url), 'utf8');
const serialService = readFileSync(new URL('../service/features/edge/serial_debug/serial_debug.service.h', import.meta.url), 'utf8');
function lua(file: string, name: string) {
    const match = file.match(new RegExp(`\\b${name} = R"lua\\(([\\s\\S]*?)\\)lua"`));
    if (!match) throw new Error(`Missing production Lua constant ${name}`);
    return match[1];
}
const refresh = lua(terminalService, 'kRefreshScript');
const failTerminal = lua(terminalService, 'kFailScript');
const inputAck = lua(terminalService, 'kSaveTerminalAckScript');
const output = lua(terminalService, 'kSaveTerminalDataScript');
const frame = lua(terminalService, 'kSaveTerminalFrameScript');
const enqueueInput = lua(terminalService, 'kEnqueueInputScript');
const openTerminal = lua(terminalService, 'kOpenTerminalScript');
const release = lua(terminalService, 'kReleaseTerminalSessionScript');
const serialEvent = lua(serialService, 'kSaveSerialEventScript');
// service::message::live: broadcast stream, directed stream capacity and lifetime.
const changes = 'iot:live:changes';
const capacity = '10000';
const streamLifetime = '600000';
let server: ReturnType<typeof Bun.spawn> | undefined;
let redis: InstanceType<typeof Bun.RedisClient>;

beforeAll(async () => {
    const executable = Bun.which('redis-server');
    if (!executable)
        throw new Error('redis-server is required for the isolated terminal state test');
    const listener = createServer();
    await new Promise<void>((resolve) => listener.listen(0, '127.0.0.1', resolve));
    const address = listener.address();
    if (!address || typeof address === 'string') throw new Error('No test port');
    await new Promise<void>((resolve) => listener.close(() => resolve()));
    server = Bun.spawn(
        [
            executable,
            '--bind',
            '127.0.0.1',
            '--port',
            String(address.port),
            '--save',
            '',
            '--appendonly',
            'no',
        ],
        { stdout: 'ignore', stderr: 'pipe' }
    );
    for (let attempt = 0; attempt < 50; attempt++) {
        redis = new Bun.RedisClient(`redis://127.0.0.1:${address.port}`, { autoReconnect: false });
        try {
            await redis.send('PING', []);
            return;
        } catch {
            redis.close();
            await Bun.sleep(50);
        }
    }
    throw new Error('Disposable Redis did not start');
});

afterAll(async () => {
    redis?.close();
    server?.kill();
    if (server) await server.exited;
});

// One terminal: node session, owner, output, input ACK, output sequence, its
// directed topics and their routes, and the browser worker's stream.
interface Terminal {
    node: string; owner: string; out: string; ack: string; sequence: string;
    ready: string; remoteClosed: string;
    eventTopic: string; ackTopic: string; eventRoute: string; ackRoute: string;
    worker: string;
}
function terminal(): Terminal {
    const prefix = `test:terminal:${crypto.randomUUID()}`;
    const [node, owner, out, ack, sequence] = ['node', 'owner', 'out', 'ack', 'sequence'].map((part) => `${prefix}:${part}`);
    const eventTopic = `edge-terminal:${crypto.randomUUID()}:${crypto.randomUUID()}`;
    const ackTopic = `${eventTopic}:input-ack`;
    return {
        node, owner, out, ack, sequence, eventTopic, ackTopic,
        ready: `${prefix}:ready`, remoteClosed: `${prefix}:remote-closed`,
        eventRoute: `iot:live:route:${eventTopic}`, ackRoute: `iot:live:route:${ackTopic}`,
        worker: `iot:live:worker:${crypto.randomUUID()}:0`,
    };
}
const evalLua = (script: string, keys: string[], args: string[]) =>
    redis.send('EVAL', [script, String(keys.length), ...keys, ...args]);
const saveAck = (t: Terminal, owner: string, sequence: number, ttl = '2') =>
    evalLua(inputAck, [t.owner, t.ack, t.ackRoute], [owner, String(sequence), ttl, t.ackTopic, capacity, streamLifetime]);
const saveOutput = (t: Terminal, owner: string, data: string, sequence: number, ttl = '2') =>
    evalLua(output, [t.owner, t.out, t.sequence, t.eventRoute], [owner, data, String(sequence), ttl, t.eventTopic, capacity, streamLifetime]);
const saveFrame = (t: Terminal, owner: string, data: string, kind: 'ready' | 'close' | 'data') =>
    evalLua(frame, [t.owner, t.out, t.ready, t.remoteClosed, t.eventRoute, t.ackRoute], [owner, data, '120', kind, t.eventTopic, t.ackTopic, capacity, streamLifetime]);
const fail = (t: Terminal, owner: string, data: string) =>
    evalLua(failTerminal, [t.owner, t.out, t.ack, t.sequence, t.eventRoute, t.ackRoute], [owner, data, t.eventTopic, t.ackTopic, capacity, streamLifetime]);
const keepalive = (t: Terminal, owner: string, ttl: string) =>
    evalLua(refresh, [t.node, t.owner, t.out, t.ack, t.sequence], [owner, ttl]);
async function route(t: Terminal) {
    await redis.send('SET', [t.eventRoute, t.worker, 'EX', '120']);
    await redis.send('SET', [t.ackRoute, t.worker, 'EX', '120']);
}
async function seed(t: Terminal) {
    await redis.send('SET', [t.node, 'epoch', 'EX', '30']);
    await redis.send('SET', [t.owner, 'epoch', 'EX', '2']);
    expect(await saveAck(t, 'epoch', 1)).toBe(1);
    expect(await saveOutput(t, 'epoch', 'first', 1)).toBe(1);
}
async function topics(stream: string) {
    const entries = (await redis.send('XRANGE', [stream, '-', '+'])) as [string, string[]][];
    return entries.map(([, fields]) => {
        expect(fields.length).toBe(2);
        expect(fields[0]).toBe('topic');
        return fields[1];
    });
}

test('terminal input overflow preserves the accepted prefix and its original expiry', async () => {
    const input = terminal().out;
    expect(await evalLua(enqueueInput, [input], ['open', '2', '120'])).toBe(1);
    await redis.send('EXPIRE', [input, '30']);
    expect(await evalLua(enqueueInput, [input], ['data', '2', '120'])).toBe(2);
    expect(await evalLua(enqueueInput, [input], ['overflow', '2', '120'])).toBe(0);
    expect(await redis.send('LRANGE', [input, '0', '-1'])).toEqual(['open', 'data']);
    expect(Number(await redis.send('TTL', [input]))).toBeLessThanOrEqual(30);
});

test('terminal open registers ownership once for the current node session', async () => {
    const t = terminal();
    const binding = `${t.node}:binding`, closed = `${t.node}:closed`;
    await redis.send('SET', [t.node, 'epoch']);
    expect(await evalLua(openTerminal, [t.node, t.owner, binding, closed], ['stale', 'browser'])).toBe(0);
    expect(await redis.send('EXISTS', [t.owner, binding])).toBe(0);
    expect(await evalLua(openTerminal, [t.node, t.owner, binding, closed], ['epoch', 'browser'])).toBe(1);
    expect(await redis.send('GET', [t.owner])).toBe('epoch');
    expect(await redis.send('GET', [binding])).toBe('browser');
    expect(Number(await redis.send('TTL', [binding]))).toBeGreaterThan(100);
    expect(await evalLua(openTerminal, [t.node, t.owner, binding, closed], ['epoch', 'other'])).toBe(0);
    const reopened = terminal();
    await redis.send('SET', [reopened.node, 'epoch']);
    await redis.send('SET', [closed, '1']);
    expect(await evalLua(openTerminal, [reopened.node, reopened.owner, `${reopened.node}:binding`, closed], ['epoch', 'browser'])).toBe(0);
});

test('terminal frames reach only the routed browser worker stream', async () => {
    const t = terminal();
    await route(t);
    await redis.send('SET', [t.node, 'epoch']);
    await redis.send('SET', [t.owner, 'epoch']);
    expect(await saveFrame(t, 'epoch', 'ready', 'ready')).toBe(1);
    expect(await saveOutput(t, 'epoch', 'data', 1)).toBe(1);
    expect(await saveOutput(t, 'epoch', 'data', 1)).toBe(2);
    expect(await saveAck(t, 'epoch', 1)).toBe(1);
    expect(await saveFrame(t, 'epoch', 'closed', 'close')).toBe(1);
    expect(await topics(t.worker)).toEqual([t.eventTopic, t.eventTopic, t.ackTopic, t.ackTopic, t.eventTopic]);
    expect(Number(await redis.send('PTTL', [t.worker]))).toBeGreaterThan(590000);
    expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'data', 'closed']);
    expect(await redis.send('GET', [t.ready])).toBe('1');
    expect(await redis.send('GET', [t.remoteClosed])).toBe('1');
    expect(await redis.send('EXISTS', [changes])).toBe(0);
});

test('terminal frames without a browser route are stored but notify nobody', async () => {
    const t = terminal();
    await redis.send('SET', [t.owner, 'epoch']);
    expect(await saveFrame(t, 'epoch', 'ready', 'ready')).toBe(1);
    expect(await saveOutput(t, 'epoch', 'data', 1)).toBe(1);
    expect(await saveAck(t, 'epoch', 1)).toBe(1);
    expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'data']);
    expect(await redis.send('EXISTS', [t.worker, changes])).toBe(0);
    // A replaced owner neither stores nor notifies.
    await route(t);
    expect(await saveOutput(t, 'other', 'stale', 2)).toBe(0);
    expect(await saveAck(t, 'other', 2)).toBe(0);
    expect(await redis.send('EXISTS', [t.worker])).toBe(0);
});

test('idle keepalive preserves ACK and output sequence beyond their original expiry', async () => {
    const t = terminal();
    await seed(t);
    // Scale the production 120-second TTL to two seconds, crossing its deadline.
    for (let index = 0; index < 5; index++) {
        await Bun.sleep(550);
        expect(await keepalive(t, 'epoch', '2')).toBe(1);
    }
    expect(await saveAck(t, 'epoch', 2)).toBe(1);
    expect(await saveOutput(t, 'epoch', 'second', 2)).toBe(1);
    expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['first', 'second']);
    // Keep strict ordering: genuine gaps must still fail.
    expect(await saveAck(t, 'epoch', 4)).toBe(-1);
    expect(await saveOutput(t, 'epoch', 'gap', 4)).toBe(-1);
}, 10000);

test('old owner-only keepalive reproduces failure on the next input and output', async () => {
    const t = terminal();
    await seed(t);
    await redis.send('EXPIRE', [t.owner, '30']);
    await Bun.sleep(2100);
    expect(await saveAck(t, 'epoch', 2)).toBe(-1);
    expect(await saveOutput(t, 'epoch', 'second', 2)).toBe(-1);
});

test('keepalive cannot renew a replaced node or terminal owner', async () => {
    for (const replaced of ['node', 'owner'] as const) {
        const t = terminal();
        await seed(t);
        await redis.send('SET', [t[replaced], 'replacement', 'EX', '30']);
        expect(await keepalive(t, 'epoch', '120')).toBe(replaced === 'node' ? -1 : 0);
        expect(Number(await redis.send('TTL', [t.ack]))).toBeLessThanOrEqual(2);
    }
});

test('keepalive does not create absent sequence state for unused or legacy terminals', async () => {
    const t = terminal();
    await redis.send('SET', [t.node, 'epoch']);
    await redis.send('SET', [t.owner, 'epoch']);
    expect(await keepalive(t, 'epoch', '120')).toBe(1);
    expect(await redis.send('EXISTS', [t.out, t.ack, t.sequence])).toBe(0);
});

test('terminal failure preserves node and other terminals and notifies its browser once', async () => {
    const broken = terminal(), other = terminal();
    await seed(broken);
    await seed(other);
    await route(broken);
    expect(await fail(broken, 'wrong-owner', 'close')).toBe(0);
    expect(await fail(broken, 'epoch', 'close')).toBe(1);
    expect(await redis.send('GET', [broken.node])).toBe('epoch');
    expect(await redis.send('GET', [other.owner])).toBe('epoch');
    expect(await redis.send('GET', [other.ack])).toBe('1');
    expect(await redis.send('LRANGE', [broken.out, '0', '-1'])).toEqual(['close']);
    expect(await redis.send('EXISTS', [broken.owner, broken.ack, broken.sequence])).toBe(0);
    expect(await fail(broken, 'epoch', 'close')).toBe(0);
    // Output and input waiters both wake once; nothing is broadcast.
    expect(await topics(broken.worker)).toEqual([broken.eventTopic, broken.ackTopic]);
    expect(await redis.send('EXISTS', [changes])).toBe(0);
});

test('terminal release deletes state only for its own node session', async () => {
    const t = terminal();
    await seed(t);
    expect(await evalLua(release, [t.owner, t.out, t.ack, t.sequence], ['other'])).toBe(0);
    expect(await redis.send('EXISTS', [t.owner, t.out, t.ack, t.sequence])).toBe(4);
    expect(await evalLua(release, [t.owner, t.out, t.ack, t.sequence], ['epoch'])).toBe(1);
    expect(await redis.send('EXISTS', [t.owner, t.out, t.ack, t.sequence])).toBe(0);
});

test('serial events are stored for the owning session and reach only the routed worker', async () => {
    const prefix = `test:serial:${crypto.randomUUID()}`;
    const [owner, out, node] = ['owner', 'out', 'node'].map((part) => `${prefix}:${part}`);
    const topic = `edge-serial:${crypto.randomUUID()}:${crypto.randomUUID()}`;
    const routeKey = `iot:live:route:${topic}`, worker = `iot:live:worker:${crypto.randomUUID()}:1`;
    const save = (session: string, event: string) =>
        evalLua(serialEvent, [owner, out, node, routeKey], [session, event, topic, capacity, streamLifetime]);
    await redis.send('SET', [owner, 'epoch']);
    await redis.send('SET', [node, 'epoch']);
    expect(await save('epoch', 'unrouted')).toBe(1);
    expect(await redis.send('EXISTS', [worker, changes])).toBe(0);
    await redis.send('SET', [routeKey, worker, 'EX', '60']);
    expect(await save('epoch', 'routed')).toBe(1);
    expect(await save('stale', 'ignored')).toBe(0);
    expect(await redis.send('LRANGE', [out, '0', '-1'])).toEqual(['unrouted', 'routed']);
    expect(await topics(worker)).toEqual([topic]);
    expect(Number(await redis.send('PTTL', [worker]))).toBeGreaterThan(590000);
    expect(await redis.send('EXISTS', [changes])).toBe(0);
});
