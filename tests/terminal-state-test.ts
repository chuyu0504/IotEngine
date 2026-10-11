import { afterAll, beforeAll, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:net';

const terminalService = readFileSync(
    new URL('../service/features/edge/terminal/terminal.service.h', import.meta.url),
    'utf8'
);
function lua(source: string, marker: string) {
    const match = source.slice(source.indexOf(marker)).match(/R"lua\(([\s\S]*?)\)lua"/);
    if (!match) throw new Error(`Missing production Lua: ${marker}`);
    return match[1];
}
const refresh = lua(terminalService, 'kRefreshScript');
const failTerminal = lua(terminalService, 'kFailScript');
const inputAck = lua(terminalService, 'static ruvia::task<void> saveTerminalDataAck(');
const output = lua(terminalService, 'static ruvia::task<void> saveTerminalData(');
const enqueueInput = lua(terminalService, 'static ruvia::task<void> enqueueInput(');
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

function keys() {
    const prefix = `test:terminal:${crypto.randomUUID()}`;
    return ['node', 'owner', 'out', 'ack', 'sequence'].map((part) => `${prefix}:${part}`);
}
async function evalLua(script: string, scriptKeys: string[], args: string[]) {
    if (script === inputAck || script === output || script === failTerminal) {
        scriptKeys = [...scriptKeys, `${scriptKeys[0]}:changes`];
        args = [...args, 'terminal-events'];
        if (script === failTerminal) args.push('terminal-input-ack');
    }
    return redis.send('EVAL', [script, String(scriptKeys.length), ...scriptKeys, ...args]);
}
async function seed(k: string[]) {
    await redis.send('SET', [k[0], 'epoch', 'EX', '30']);
    await redis.send('SET', [k[1], 'epoch', 'EX', '2']);
    expect(await evalLua(inputAck, [k[1], k[3]], ['epoch', '1', '2'])).toBe(1);
    expect(await evalLua(output, [k[1], k[2], k[4]], ['epoch', 'first', '1', '2'])).toBe(1);
}

test('terminal input overflow preserves the accepted prefix and its original expiry', async () => {
    const input = keys()[2];
    expect(await evalLua(enqueueInput, [input], ['open', '2', '120'])).toBe(1);
    await redis.send('EXPIRE', [input, '30']);
    expect(await evalLua(enqueueInput, [input], ['data', '2', '120'])).toBe(2);
    expect(await evalLua(enqueueInput, [input], ['overflow', '2', '120'])).toBe(0);
    expect(await redis.send('LRANGE', [input, '0', '-1'])).toEqual(['open', 'data']);
    expect(Number(await redis.send('TTL', [input]))).toBeLessThanOrEqual(30);
});

test('idle keepalive preserves ACK and output sequence beyond their original expiry', async () => {
    const k = keys();
    await seed(k);
    // Scale the production 120-second TTL to two seconds, crossing its deadline.
    for (let index = 0; index < 5; index++) {
        await Bun.sleep(550);
        expect(await evalLua(refresh, k, ['epoch', '2'])).toBe(1);
    }
    expect(await evalLua(inputAck, [k[1], k[3]], ['epoch', '2', '2'])).toBe(1);
    expect(await evalLua(output, [k[1], k[2], k[4]], ['epoch', 'second', '2', '2'])).toBe(1);
    expect(await redis.send('LRANGE', [k[2], '0', '-1'])).toEqual(['first', 'second']);
    // Keep strict ordering: genuine gaps must still fail.
    expect(await evalLua(inputAck, [k[1], k[3]], ['epoch', '4', '2'])).toBe(-1);
    expect(await evalLua(output, [k[1], k[2], k[4]], ['epoch', 'gap', '4', '2'])).toBe(-1);
}, 10000);

test('old owner-only keepalive reproduces failure on the next input and output', async () => {
    const k = keys();
    await seed(k);
    await redis.send('EXPIRE', [k[1], '30']);
    await Bun.sleep(2100);
    expect(await evalLua(inputAck, [k[1], k[3]], ['epoch', '2', '2'])).toBe(-1);
    expect(await evalLua(output, [k[1], k[2], k[4]], ['epoch', 'second', '2', '2'])).toBe(-1);
});

test('keepalive cannot renew a replaced node or terminal owner', async () => {
    for (const replaced of [0, 1]) {
        const k = keys();
        await seed(k);
        await redis.send('SET', [k[replaced], 'replacement', 'EX', '30']);
        expect(await evalLua(refresh, k, ['epoch', '120'])).toBe(replaced === 0 ? -1 : 0);
        expect(Number(await redis.send('TTL', [k[3]]))).toBeLessThanOrEqual(2);
    }
});

test('keepalive does not create absent sequence state for unused or legacy terminals', async () => {
    const k = keys();
    await redis.send('SET', [k[0], 'epoch']);
    await redis.send('SET', [k[1], 'epoch']);
    expect(await evalLua(refresh, k, ['epoch', '120'])).toBe(1);
    expect(await redis.send('EXISTS', [k[2], k[3], k[4]])).toBe(0);
});

test('terminal failure preserves node and other terminals and publishes one close', async () => {
    const broken = keys(),
        other = keys();
    await seed(broken);
    await seed(other);
    const failureKeys = [broken[1], broken[2], broken[3], broken[4]];
    expect(await evalLua(failTerminal, failureKeys, ['wrong-owner', 'close'])).toBe(0);
    expect(await evalLua(failTerminal, failureKeys, ['epoch', 'close'])).toBe(1);
    expect(await redis.send('GET', [broken[0]])).toBe('epoch');
    expect(await redis.send('GET', [other[1]])).toBe('epoch');
    expect(await redis.send('GET', [other[3]])).toBe('1');
    expect(await redis.send('LRANGE', [broken[2], '0', '-1'])).toEqual(['close']);
    expect(await redis.send('EXISTS', [broken[1], broken[3], broken[4]])).toBe(0);
    expect(await evalLua(failTerminal, failureKeys, ['epoch', 'close'])).toBe(0);
    // One initial input ACK, one output frame, then two failure notifications.
    expect(Number(await redis.send('XLEN', [`${broken[1]}:changes`]))).toBe(4);
});
