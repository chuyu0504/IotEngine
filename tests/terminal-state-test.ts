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
const browserInput = lua(terminalService, 'kEnqueueBrowserInputScript');
const release = lua(terminalService, 'kReleaseTerminalSessionScript');
const serialEvent = lua(serialService, 'kSaveSerialEventScript');
let server: ReturnType<typeof Bun.spawn> | undefined;
let redis: InstanceType<typeof Bun.RedisClient>;
let redisAddress: string;

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
        redisAddress = `redis://127.0.0.1:${address.port}`;
        redis = new Bun.RedisClient(redisAddress, { autoReconnect: false });
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

// Session data is reliable independently of the directed Pub/Sub receivers.
interface Terminal {
    node: string; owner: string; out: string; ack: string; sequence: string;
    ready: string; remoteClosed: string;
    eventTopic: string; ackTopic: string;
}
function terminal(): Terminal {
    const prefix = `test:terminal:${crypto.randomUUID()}`;
    const [node, owner, out, ack, sequence] = ['node', 'owner', 'out', 'ack', 'sequence'].map((part) => `${prefix}:${part}`);
    const eventTopic = `edge-terminal:${crypto.randomUUID()}:${crypto.randomUUID()}`;
    const ackTopic = `${eventTopic}:input-ack`;
    return {
        node, owner, out, ack, sequence, eventTopic, ackTopic,
        ready: `${prefix}:ready`, remoteClosed: `${prefix}:remote-closed`,
    };
}
const evalLua = (script: string, keys: string[], args: string[]) =>
    redis.send('EVAL', [script, String(keys.length), ...keys, ...args]);
const saveAck = (t: Terminal, owner: string, sequence: number, ttl = '2') =>
    evalLua(inputAck, [t.owner, t.ack], [owner, String(sequence), ttl, t.ackTopic]);
const saveOutput = (t: Terminal, owner: string, data: string, sequence: number, ttl = '2') =>
    evalLua(output, [t.owner, t.out, t.sequence], [owner, data, String(sequence), ttl, t.eventTopic]);
const saveFrame = (t: Terminal, owner: string, data: string, kind: 'ready' | 'close' | 'data') =>
    evalLua(frame, [t.owner, t.out, t.ready, t.remoteClosed], [owner, data, '120', kind, t.eventTopic, t.ackTopic]);
const fail = (t: Terminal, owner: string, data: string) =>
    evalLua(failTerminal, [t.owner, t.out, t.ack, t.sequence], [owner, data, t.eventTopic, t.ackTopic]);
const keepalive = (t: Terminal, owner: string, ttl: string) =>
    evalLua(refresh, [t.node, t.owner, t.out, t.ack, t.sequence], [owner, ttl]);
async function seed(t: Terminal) {
    await redis.send('SET', [t.node, 'epoch', 'EX', '30']);
    await redis.send('SET', [t.owner, 'epoch', 'EX', '2']);
    expect(await saveAck(t, 'epoch', 1)).toBe(1);
    expect(await saveOutput(t, 'epoch', 'first', 1)).toBe(1);
}
async function notifications(channels: string[]) {
    const client = new Bun.RedisClient(redisAddress, { autoReconnect: false });
    const received: { topic: string; payload: string }[] = [];
    let signal: (() => void) | undefined;
    for (const topic of channels) {
        await client.subscribe(topic, (payload, channel) => {
            received.push({ topic: channel, payload });
            signal?.();
        });
    }
    return {
        received,
        close: () => client.close(),
        async wait(count: number) {
            if (received.length >= count) return;
            await new Promise<void>((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error('Directed notification not received')), 3000);
                signal = () => {
                    if (received.length >= count) { clearTimeout(timer); resolve(); }
                };
            });
        },
    };
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


test('browser output ACK removes only its retained frame atomically with downstream enqueue', async () => {
    const t = terminal();
    const browser = `${t.node}:browser`, closed = `${t.node}:closed`, queue = `${t.node}:input`;
    const inputSequence = `${t.node}:input-sequence`, outputAck = `${t.node}:output-ack`;
    const keys = [t.node, t.owner, browser, closed, queue, inputSequence, t.ack, outputAck, t.sequence, t.ready, t.out];
    await redis.send('MSET', [t.node, 'epoch', t.owner, 'epoch', browser, 'browser', t.sequence, '2', t.ready, '1']);
    await redis.send('RPUSH', [t.out, 'ready', 'data-1', 'data-2']);
    expect(await evalLua(browserInput, keys, ['epoch', 'browser', 'ack-2', 'ack', '2', 'data-2'])).toBe(0);
    expect(await evalLua(browserInput, keys, ['epoch', 'browser', 'ack-1', 'ack', '1', 'missing'])).toBe(0);
    expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'data-1', 'data-2']);
    expect(await evalLua(browserInput, keys, ['epoch', 'browser', 'ack-1', 'ack', '1', 'data-1'])).toBe(1);
    expect(await redis.send('GET', [outputAck])).toBe('1');
    expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'data-2']);
    expect(await evalLua(browserInput, keys, ['epoch', 'browser', 'ack-1', 'ack', '1', ''])).toBe(1);
    expect(await redis.send('LRANGE', [queue, '0', '-1'])).toEqual(['ack-1']);
    await redis.send('RPUSH', [queue, ...Array<string>(4095).fill('blocked')]);
    expect(await evalLua(browserInput, keys, ['epoch', 'browser', 'ack-2', 'ack', '2', 'data-2'])).toBe(0);
    expect(await redis.send('GET', [outputAck])).toBe('1');
    expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'data-2']);
    await redis.send('DEL', [queue]);
    await redis.send('SET', [t.node, 'replacement']);
    expect(await evalLua(browserInput, keys, ['epoch', 'browser', 'ack-2', 'ack', '2', 'data-2'])).toBe(0);
    expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'data-2']);
});

test('terminal frames atomically persist and wake each direct topic subscriber', async () => {
    const t = terminal();
    const observer = await notifications([t.eventTopic, t.ackTopic]);
    const second = await notifications([t.eventTopic]);
    try {
        await redis.send('SET', [t.owner, 'epoch']);
        expect(await saveFrame(t, 'epoch', 'ready', 'ready')).toBe(1);
        expect(await saveOutput(t, 'epoch', 'data', 1)).toBe(1);
        expect(await saveOutput(t, 'epoch', 'data', 1)).toBe(2);
        expect(await saveAck(t, 'epoch', 1)).toBe(1);
        expect(await saveFrame(t, 'epoch', 'closed', 'close')).toBe(1);
        await observer.wait(5);
        await second.wait(3);
        expect(observer.received).toEqual([t.eventTopic, t.eventTopic, t.ackTopic, t.ackTopic, t.eventTopic].map(topic => ({ topic, payload: '' })));
        expect(second.received.map(event => event.topic)).toEqual([t.eventTopic, t.eventTopic, t.eventTopic]);
        expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'data', 'closed']);
        expect(await redis.send('GET', [t.ready])).toBe('1');
        expect(await redis.send('GET', [t.remoteClosed])).toBe('1');
    } finally { observer.close(); second.close(); }
});

test('terminal frames survive subscriber loss and replay before the next live wake', async () => {
    const t = terminal();
    await redis.send('SET', [t.owner, 'epoch']);
    expect(await saveFrame(t, 'epoch', 'ready', 'ready')).toBe(1);
    expect(await saveOutput(t, 'epoch', 'first', 1)).toBe(1);
    const observer = await notifications([t.eventTopic, t.ackTopic]);
    try {
        expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'first']);
        expect(await saveOutput(t, 'other', 'stale', 2)).toBe(0);
        expect(await saveAck(t, 'other', 1)).toBe(0);
        expect(await saveOutput(t, 'epoch', 'second', 2)).toBe(1);
        await observer.wait(1);
        expect(observer.received).toEqual([{ topic: t.eventTopic, payload: '' }]);
        expect(await redis.send('GET', [t.sequence])).toBe('2');
        expect(await redis.send('LRANGE', [t.out, '0', '-1'])).toEqual(['ready', 'first', 'second']);
    } finally { observer.close(); }
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
    const observer = await notifications([broken.eventTopic, broken.ackTopic]);
    try {
        expect(await fail(broken, 'wrong-owner', 'close')).toBe(0);
        expect(await fail(broken, 'epoch', 'close')).toBe(1);
        expect(await redis.send('GET', [broken.node])).toBe('epoch');
        expect(await redis.send('GET', [other.owner])).toBe('epoch');
        expect(await redis.send('GET', [other.ack])).toBe('1');
        expect(await redis.send('LRANGE', [broken.out, '0', '-1'])).toEqual(['close']);
        expect(await redis.send('EXISTS', [broken.owner, broken.ack, broken.sequence])).toBe(0);
        expect(await fail(broken, 'epoch', 'close')).toBe(0);
        await observer.wait(2);
        expect(observer.received).toEqual([broken.eventTopic, broken.ackTopic].map(topic => ({ topic, payload: '' })));
    } finally { observer.close(); }
});

test('terminal release deletes state only for its own node session', async () => {
    const t = terminal();
    await seed(t);
    expect(await evalLua(release, [t.owner, t.out, t.ack, t.sequence], ['other'])).toBe(0);
    expect(await redis.send('EXISTS', [t.owner, t.out, t.ack, t.sequence])).toBe(4);
    expect(await evalLua(release, [t.owner, t.out, t.ack, t.sequence], ['epoch'])).toBe(1);
    expect(await redis.send('EXISTS', [t.owner, t.out, t.ack, t.sequence])).toBe(0);
});

test('serial output persists without subscribers and wakes only the session topic', async () => {
    const prefix = `test:serial:${crypto.randomUUID()}`;
    const [owner, out, node] = ['owner', 'out', 'node'].map((part) => `${prefix}:${part}`);
    const topic = `edge-serial:${crypto.randomUUID()}:${crypto.randomUUID()}`;
    const save = (session: string, event: string) =>
        evalLua(serialEvent, [owner, out, node], [session, event, topic]);
    await redis.send('SET', [owner, 'epoch']);
    await redis.send('SET', [node, 'epoch']);
    expect(await save('epoch', 'offline')).toBe(1);
    const observer = await notifications([topic, `${topic}:other`]);
    try {
        expect(await redis.send('LRANGE', [out, '0', '-1'])).toEqual(['offline']);
        expect(await save('epoch', 'online')).toBe(1);
        expect(await save('stale', 'ignored')).toBe(0);
        await observer.wait(1);
        expect(await redis.send('LRANGE', [out, '0', '-1'])).toEqual(['offline', 'online']);
        expect(observer.received).toEqual([{ topic, payload: '' }]);
    } finally { observer.close(); }
});
