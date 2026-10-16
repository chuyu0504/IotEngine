import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
// This test is intentionally limited to the disposable local fixture.
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { Agent, request as httpRequest } from 'node:http';
import { createConnection, type Socket } from 'node:net';
import { consumeServerSentEvents } from '../web/lib/sse';
import type { ServerSentEvent } from '../web/utils/sse';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const id = () => crypto.randomUUID();
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
    iss: 'iot-engine',
    aud: 'iot-engine-web',
    sub: admin,
    user_id: admin,
    username: 'admin',
    token_type: 'access',
    iat: now,
    exp: now + 3600,
})}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000')
    .update(unsigned)
    .digest('base64url')}`;

const link = id();
const device = id();
const protocol = id();
const point = id();
const instance = id();
const operationIds: string[] = [];
const idempotencyKeys: string[] = [];
const serviceWorkerCount = Number(Bun.env.RPC_TEST_WORKERS ?? '2');
assert(
    Number.isInteger(serviceWorkerCount) && serviceWorkerCount >= 2,
    'RPC_TEST_WORKERS must be at least two'
);
let rpcStreams: string[] = [];
let rpcStream = '';
let redisMonitor: ReturnType<typeof openRedisMonitor> | undefined;
let liveStreams: Array<Awaited<ReturnType<typeof openLiveStream>>> = [];
let releaseShared: (() => void) | undefined;
let moduleTransaction: Promise<unknown> | undefined;
let updateTransaction: Promise<unknown> | undefined;
const keepAliveAgent = new Agent({ keepAlive: true, maxSockets: 1, maxFreeSockets: 1 });
const socketNumbers = new WeakMap<Socket, number>();
const localPortNumbers = new Map<number, number>();
let nextSocketNumber = 1;
let readinessBackups: Array<{ key: string; value: string[]; pttl: number }> = [];

async function until(
    check: () => Promise<boolean>,
    message: string,
    timeout = 15000,
    interval = 50
) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(interval);
    }
    throw new Error(message);
}

async function waitFor<T>(promise: Promise<T>, message: string, timeout = 10000): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            promise,
            new Promise<T>((_, reject) => {
                timer = setTimeout(() => reject(new Error(message)), timeout);
            }),
        ]);
    } finally {
        if (timer) clearTimeout(timer);
    }
}

function socketNumber(socket: Socket | null | undefined) {
    if (!socket) return undefined;
    if (typeof socket.localPort === 'number') {
        const existing = localPortNumbers.get(socket.localPort);
        if (existing !== undefined) return existing;
        const number = nextSocketNumber++;
        localPortNumbers.set(socket.localPort, number);
        return number;
    }
    const existing = socketNumbers.get(socket);
    if (existing !== undefined) return existing;
    const number = nextSocketNumber++;
    socketNumbers.set(socket, number);
    return number;
}

async function postCommand(
    idempotencyKey: string,
    timeout = 20000,
    value = '1',
    closeConnection = false,
    agent?: Agent
) {
    const requestBody = JSON.stringify({
        idempotency_key: idempotencyKey,
        elements: [{ elementId: point, value }],
    });
    if (agent) {
        const target = new URL(`${apiBase}/v1/device/${device}/commands`);
        return await new Promise<{ status: number; body: any; socketNumber?: number }>(
            (resolve, reject) => {
                const request = httpRequest(
                    target,
                    {
                        method: 'POST',
                        agent,
                        headers: {
                            Authorization: `Bearer ${token}`,
                            'Content-Type': 'application/json',
                            Accept: 'application/json',
                            Connection: 'keep-alive',
                            'Content-Length': Buffer.byteLength(requestBody),
                        },
                    },
                    (response) => {
                        let text = '';
                        response.setEncoding('utf8');
                        response.on('data', (chunk) => {
                            text += chunk;
                        });
                        response.once('error', reject);
                        response.once('end', () => {
                            let parsed: any;
                            try {
                                parsed = JSON.parse(text);
                            } catch {
                                parsed = text;
                            }
                            resolve({
                                status: response.statusCode ?? 0,
                                body: parsed,
                                socketNumber: socketNumber(response.socket),
                            });
                        });
                    }
                );
                request.once('error', reject);
                request.setTimeout(timeout, () =>
                    request.destroy(new Error('HTTP command timed out'))
                );
                request.end(requestBody);
            }
        );
    }
    const response = await fetch(`${apiBase}/v1/device/${device}/commands`, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
            Accept: 'application/json',
            ...(closeConnection ? { Connection: 'close' } : {}),
        },
        body: requestBody,
        signal: AbortSignal.timeout(timeout),
    });
    const text = await response.text();
    let parsedBody: any;
    try {
        parsedBody = JSON.parse(text);
    } catch {
        parsedBody = text;
    }
    return { status: response.status, body: parsedBody, socketNumber: undefined };
}

function rpcWorkerIndex(stream: string) {
    const match = stream.match(/^iot:rpc:requests:[^:]+:worker:(\d+)$/);
    if (!match) throw new Error(`RPC stream has no worker index: ${stream}`);
    return Number(match[1]);
}

function rpcInstanceId(stream: string) {
    const match = stream.match(/^iot:rpc:requests:([^:]+):worker:\d+$/);
    if (!match) throw new Error(`RPC stream has no instance id: ${stream}`);
    return match[1];
}

function rpcStreamFromMonitor(line: string) {
    const match = line.match(/"(iot:rpc:requests:[^\"]+:worker:\d+)"/);
    if (!match) throw new Error(`MONITOR line has no RPC request stream: ${line}`);
    return match[1];
}

async function internalResponse(path: string) {
    const response = await fetch(`${apiBase}${path}`, {
        signal: AbortSignal.timeout(5000),
        headers: { Accept: 'application/json' },
    });
    return { status: response.status, text: await response.text() };
}

async function restoreReadinessBackups() {
    for (const backup of readinessBackups) {
        const ttl = Math.max(1000, Math.min(15000, backup.pttl));
        await redis.send('EVAL', [
            "redis.call('HSET',KEYS[1],unpack(ARGV,2)); return redis.call('PEXPIRE',KEYS[1],ARGV[1])",
            '1', backup.key, String(ttl), ...backup.value,
        ]);
    }
}

async function findRpcStreams(expected: number) {
    let streams: string[] = [];
    await until(async () => {
        const result = await redis.send('KEYS', ['iot:rpc:requests:*:worker:*']);
        streams = (Array.isArray(result) ? result.map(String) : []).filter((key) =>
            /^iot:rpc:requests:[^:]+:worker:\d+$/.test(key)
        );
        return new Set(streams.map(rpcWorkerIndex)).size >= expected;
    }, `expected ${expected} worker-local RPC request streams`);
    const byWorker = new Map(streams.map((stream) => [rpcWorkerIndex(stream), stream]));
    const indexes = [...byWorker.keys()].sort((left, right) => left - right);
    assert.deepEqual(
        indexes.slice(0, expected),
        Array.from({ length: expected }, (_, index) => index),
        'RPC request stream worker indexes are not contiguous from zero'
    );
    return indexes.slice(0, expected).map((index) => byWorker.get(index)!);
}

async function hasControlGroup(stream: string) {
    try {
        const result = await redis.send('XINFO', ['GROUPS', stream]);
        return JSON.stringify(result).includes('control');
    } catch {
        return false;
    }
}

async function liveGroupNames() {
    const result = await redis.send('XINFO', ['GROUPS', 'iot:live:changes']);
    const names: string[] = [];
    const visit = (value: unknown) => {
        if (Array.isArray(value)) {
            for (let index = 0; index + 1 < value.length; index++) {
                if (value[index] === 'name' && typeof value[index + 1] === 'string')
                    names.push(value[index + 1]);
            }
            for (const child of value) visit(child);
            return;
        }
        if (value && typeof value === 'object') {
            const record = value as Record<string, unknown>;
            if (typeof record.name === 'string') names.push(record.name);
            for (const child of Object.values(record)) visit(child);
        }
    };
    visit(result);
    return [...new Set(names)];
}

async function liveConsumerNames(group: string) {
    const result = await redis.send('XINFO', ['CONSUMERS', 'iot:live:changes', group]);
    const names: string[] = [];
    const visit = (value: unknown) => {
        if (Array.isArray(value)) {
            for (let index = 0; index + 1 < value.length; index++) {
                if (value[index] === 'name' && typeof value[index + 1] === 'string')
                    names.push(value[index + 1]);
            }
            for (const child of value) visit(child);
            return;
        }
        if (value && typeof value === 'object') {
            const record = value as Record<string, unknown>;
            if (typeof record.name === 'string') names.push(record.name);
            for (const child of Object.values(record)) visit(child);
        }
    };
    visit(result);
    return [...new Set(names)];
}

async function findConfigStreams(expected: number) {
    let streams: string[] = [];
    await until(async () => {
        const result = await redis.send('KEYS', ['iot:channel:config:worker:*']);
        const byInstance = new Map<string, Map<number, string>>();
        for (const key of Array.isArray(result) ? result.map(String) : []) {
            const match = key.match(/^iot:channel:config:worker:([^:]+):(\d+)$/);
            if (!match) continue;
            const indexes = byInstance.get(match[1]) ?? new Map<number, string>();
            indexes.set(Number(match[2]), key);
            byInstance.set(match[1], indexes);
        }
        const candidate = [...byInstance.values()].find((indexes) =>
            Array.from({ length: expected }, (_, index) => index).every((index) =>
                indexes.has(index)
            )
        );
        if (!candidate) return false;
        streams = [...candidate.entries()]
            .sort(([left], [right]) => left - right)
            .slice(0, expected)
            .map(([, stream]) => stream);
        return true;
    }, `expected ${expected} collector-local configuration streams`);
    return streams;
}

function openRedisMonitor() {
    const url = new URL(redisUrl);
    const socket = createConnection({
        host: url.hostname,
        port: Number(url.port || 6379),
    });
    const lines: string[] = [];
    const waiters: Array<() => void> = [];
    let pending = '';
    let monitorError: unknown;
    let readyResolve!: () => void;
    let readyReject!: (error: unknown) => void;
    const ready = new Promise<void>((resolve, reject) => {
        readyResolve = resolve;
        readyReject = reject;
    });
    socket.on('connect', () => socket.write('*1\r\n$7\r\nMONITOR\r\n'));
    socket.on('data', (chunk) => {
        pending += chunk.toString('utf8');
        for (;;) {
            const end = pending.indexOf('\r\n');
            if (end < 0) break;
            const line = pending.slice(0, end);
            pending = pending.slice(end + 2);
            if (line === '+OK') {
                readyResolve();
                continue;
            }
            lines.push(line);
            for (const wake of waiters.splice(0)) wake();
        }
    });
    socket.on('error', (error) => {
        monitorError = error;
        readyReject(error);
        for (const wake of waiters.splice(0)) wake();
    });
    return {
        ready,
        async next(stream: string) {
            const deadline = Date.now() + 10000;
            for (;;) {
                if (monitorError) throw monitorError;
                const needle = `\"${stream}\"`;
                const index = lines.findIndex(
                    (line) => line.includes('"XADD"') && line.includes(needle)
                );
                if (index >= 0) return lines.splice(index, 1)[0];
                const remaining = deadline - Date.now();
                if (remaining <= 0) throw new Error(`timed out monitoring RPC XADD ${stream}`);
                await waitFor(
                    new Promise<void>((resolve) => waiters.push(resolve)),
                    `timed out monitoring RPC XADD ${stream}`,
                    remaining
                );
            }
        },
        async nextRpc() {
            const deadline = Date.now() + 10000;
            for (;;) {
                if (monitorError) throw monitorError;
                const index = lines.findIndex(
                    (line) =>
                        line.includes('"XADD"') && /"iot:rpc:requests:[^\"]+:worker:\d+"/.test(line)
                );
                if (index >= 0) return lines.splice(index, 1)[0];
                const remaining = deadline - Date.now();
                if (remaining <= 0) throw new Error('timed out monitoring any RPC XADD');
                await waitFor(
                    new Promise<void>((resolve) => waiters.push(resolve)),
                    'timed out monitoring any RPC XADD',
                    remaining
                );
            }
        },
        close() {
            monitorError ??= new Error('Redis monitor closed');
            socket.destroy();
            for (const wake of waiters.splice(0)) wake();
        },
    };
}

async function callPrepareRpc(stream: string) {
    const requestId = id();
    const replyKey = `iot:rpc:reply:${requestId}`;
    const payload = JSON.stringify({ deviceId: device, elements: [[point, '1']] });
    const deadline = String(Date.now() + 30000);
    const entry = await redis.send('XADD', [
        stream,
        '*',
        'version',
        '1',
        'id',
        requestId,
        'component',
        'command',
        'operation',
        'prepare',
        'payload',
        payload,
        'deadline',
        deadline,
    ]);
    assert.equal(typeof entry, 'string', 'RPC request was not appended to its stream');
    try {
        let response = '';
        await until(
            async () => {
                const value = await redis.send('GET', [replyKey]);
                if (typeof value !== 'string') return false;
                response = value;
                return true;
            },
            'RPC prepare did not publish a reply',
            10000,
            25
        );
        assert(response.startsWith('OK\n'), `RPC prepare failed: ${response}`);
        const prepared = JSON.parse(response.slice(3));
        assert.equal(prepared.kind, 'stream');
        assert.equal(prepared.commands.length, 1);
        assert.equal(prepared.commands[0].deviceId, device);
        return prepared;
    } finally {
        await redis.send('DEL', [
            replyKey,
            `iot:rpc:claim:${requestId}`,
            `iot:rpc:cancelled:${requestId}`,
        ]);
    }
}

async function openLiveStream(path: string, eventName: string) {
    const controller = new AbortController();
    const response = await fetch(`${apiBase}${path}`, {
        headers: { Authorization: `Bearer ${token}`, Accept: 'text/event-stream' },
        signal: controller.signal,
    });
    assert.equal(response.status, 200, `SSE ${path} returned ${response.status}`);
    assert(response.body);
    assert(response.headers.get('content-type')?.includes('text/event-stream'));
    const events: ServerSentEvent[] = [];
    const waiters: Array<() => void> = [];
    let streamError: unknown;
    const consumed = consumeServerSentEvents(
        response.body,
        (event) => {
            events.push(event);
            for (const wake of waiters.splice(0)) wake();
        },
        controller.signal
    ).catch((error) => {
        if (!controller.signal.aborted) streamError = error;
        for (const wake of waiters.splice(0)) wake();
    });
    return {
        async next(predicate: (value: any) => boolean = () => true) {
            const deadline = Date.now() + 10000;
            for (;;) {
                if (streamError) throw streamError;
                const index = events.findIndex((event) => {
                    if (event.event !== eventName) return false;
                    try {
                        return predicate(JSON.parse(event.data));
                    } catch {
                        return false;
                    }
                });
                if (index >= 0) return JSON.parse(events.splice(index, 1)[0].data);
                const remaining = deadline - Date.now();
                if (remaining <= 0) throw new Error(`timed out waiting for SSE snapshot ${path}`);
                await waitFor(
                    new Promise<void>((resolve) => waiters.push(resolve)),
                    `timed out waiting for SSE snapshot ${path}`,
                    remaining
                );
            }
        },
        async close() {
            controller.abort();
            try {
                await consumed;
            } catch {
                /* stream cancellation is expected */
            }
        },
    };
}

async function verifyCollectorLeaseFencing() {
    const source = await Bun.file(
        new URL('../service/features/collector/collector.service.h', import.meta.url)
    ).text();
    const script = (name: string) => {
        const marker = source.indexOf(`${name}(const Redis&`);
        assert(marker >= 0, `missing production ownership operation ${name}`);
        const body = source.slice(marker).match(/R"lua\(([\s\S]*?)\)lua"/)?.[1];
        assert(body, `missing production lease Lua for ${name}`);
        return body;
    };
    const claim = script('retainTarget');
    const renew = script('renewTarget');
    const release = script('releaseTarget');
    const key = `iot:v2:owner:target:${id()}:${id()}`;
    const first = `worker:0:${id()}`;
    const replacement = `worker:0:${id()}`;
    const other = `worker:1:${id()}`;
    const run = async (body: string, owner: string) =>
        Number(await redis.send('EVAL', [body, '1', key, owner]));
    try {
        assert.equal(await run(claim, first), 1);
        assert.equal(await run(claim, other), 0, 'another worker stole an active target');
        assert.equal(await run(renew, first), 1);
        await redis.send('PEXPIRE', [key, '1']);
        await until(
            async () => Number(await redis.send('EXISTS', [key])) === 0,
            'target lease did not expire',
            1000,
            10
        );
        assert.equal(await run(renew, first), 0, 'renewal recreated an expired target');
        assert.equal(await run(claim, replacement), 1);
        assert.equal(await run(release, first), 0, 'old connection released its replacement');
        assert.equal(await run(renew, first), 0, 'old connection renewed its replacement');
        assert.equal(await redis.send('GET', [key]), replacement);
        assert.equal(await run(release, replacement), 1);
        assert.equal(
            await run(claim, other),
            1,
            'another worker could not claim a released target'
        );
        console.log('PASS real Redis target leases fence stale release and expired renewal');
    } finally {
        await redis.send('DEL', [key]);
    }
}

try {
    await verifyCollectorLeaseFencing();
    const config = {
        registers: [
            {
                id: point,
                name: 'switch',
                registerType: 'COIL',
                dataType: 'BOOL',
                address: 0,
                quantity: 1,
                writable: true,
            },
        ],
        readInterval: 60,
        storagePolicy: 'report',
    };
    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by)
        VALUES(${protocol},${protocol},'Modbus',${config}::jsonb,${admin})`;
    await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution)
        VALUES(${link},${link},'Modbus',
            '{"transport":"tcp","mode":"TCP Server","ip":"0.0.0.0","port":55199,"targets":[]}'::jsonb,
            ${admin},'collector')`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
        VALUES(${device},${device},${link},${protocol},
            '{"device_code":"0000000001","modbus_mode":"TCP","remote_control":true}'::jsonb,
            ${admin})`;
    // Seed the minimum latest metadata and the synthetic collector connection
    // lease that the command route lookup validates against the link owner.
    await redis.send('HSET', [
        `iot:v2:device:${device}:latest`,
        '_device_id',
        device,
        '_element_ids',
        JSON.stringify({ [point]: true }),
    ]);
    await redis.send('SET', [`iot:v2:owner:link:${link}`, instance, 'PX', '120000']);
    await redis.send('HSET', [
        `iot:v3:device:connection:${device}`,
        'device_id',
        device,
        'link_id',
        link,
        'instance_id',
        instance,
        'worker_id',
        '0',
        'worker_instance_id',
        id(),
        'connection_id',
        'rpc-integration-connection',
        'session_epoch',
        '1',
    ]);
    await redis.send('PEXPIRE', [`iot:v3:device:connection:${device}`, '120000']);

    const firstKey = id();
    idempotencyKeys.push(firstKey);
    const first = await postCommand(firstKey);
    assert.equal(first.status, 200, JSON.stringify(first.body));
    const firstIds = first.body?.data?.command_ids;
    assert(
        Array.isArray(firstIds) && firstIds.length === 1,
        'prepare command response has no command id'
    );
    operationIds.push(...firstIds);

    const requestRows = await db`SELECT id::text AS id, actor, device_id::text AS device_id, payload
        FROM command_request WHERE idempotency_key=${firstKey}`;
    assert.equal(requestRows.length, 1, 'command request was not committed');
    const requestId = String(requestRows[0].id);
    assert.equal(requestRows[0].actor, admin);
    assert.equal(requestRows[0].device_id, device);
    assert.deepEqual(requestRows[0].payload, [[point, '1']]);
    const operationRows = await db`SELECT id::text AS id, request_id::text AS request_id,
        device_id::text AS device_id, protocol, elements
        FROM command_operation WHERE request_id=${requestId}`;
    assert.equal(operationRows.length, 1, 'prepare did not atomically persist one operation');
    assert.equal(operationRows[0].id, firstIds[0]);
    assert.equal(operationRows[0].device_id, device);
    assert.equal(operationRows[0].protocol, 'Modbus');
    assert.deepEqual(operationRows[0].elements, [{ elementId: point, value: '1' }]);
    const attemptRows = await db`SELECT operation_id::text AS operation_id, queue_kind,
        queue_key, submitted_by, max_length, payload
        FROM command_attempt WHERE operation_id=${firstIds[0]}`;
    assert.equal(attemptRows.length, 1, 'prepare did not atomically persist one attempt');
    assert.equal(attemptRows[0].operation_id, firstIds[0]);
    assert.equal(attemptRows[0].queue_kind, 'stream');
    assert.equal(attemptRows[0].submitted_by, admin);
    assert.equal(Number(attemptRows[0].max_length), 10000);
    assert(Array.isArray(attemptRows[0].payload) && attemptRows[0].payload.length > 0);
    const acceptedEvents = await db`SELECT id FROM outbox_event
        WHERE event_type='device.command.accepted'
          AND payload->'data'->>'commandId'=${firstIds[0]}`;
    assert.equal(acceptedEvents.length, 1, 'prepare did not atomically persist its accepted event');
    console.log('PASS command prepare RPC persists request/operation/attempt/event records');

    rpcStreams = await findRpcStreams(serviceWorkerCount);
    rpcStream = rpcStreams[0];
    assert.deepEqual(
        rpcStreams.map(rpcWorkerIndex),
        Array.from({ length: serviceWorkerCount }, (_, index) => index),
        'RPC request streams must identify every service worker'
    );
    for (const stream of rpcStreams)
        assert(await hasControlGroup(stream), `RPC consumer group was not ready for ${stream}`);
    const perWorkerPrepared = await Promise.all(rpcStreams.map((stream) => callPrepareRpc(stream)));
    assert(
        perWorkerPrepared.every((prepared) => prepared.commands[0].deviceId === device),
        'a request sent to each worker stream did not receive its local RPC reply'
    );
    console.log('PASS each worker-local RPC stream is consumed and replies independently');

    // A keep-alive HTTP connection is owned by the Service Worker that accepts
    // its socket. Every request on that connection must therefore use the same
    // worker-local RPC stream. MONITOR observes XADD before the RPC runtime
    // ACK/XDELs the request and makes the routing decision externally visible.
    redisMonitor = openRedisMonitor();
    await redisMonitor.ready;
    const routeKeys = Array.from({ length: Math.max(8, serviceWorkerCount * 4) }, id);
    idempotencyKeys.push(...routeKeys);
    const observedRoutes: string[] = [];
    const socketIds: number[] = [];
    for (const [index, key] of routeKeys.entries()) {
        const response = await postCommand(
            key,
            20000,
            index % 2 ? '1' : '0',
            false,
            keepAliveAgent
        );
        assert.equal(response.status, 200, JSON.stringify(response.body));
        const commandIds = response.body?.data?.command_ids;
        assert(
            Array.isArray(commandIds) && commandIds.length === 1,
            'worker-routed command response has no command id'
        );
        operationIds.push(...commandIds);
        assert(
            response.socketNumber !== undefined,
            'keep-alive request exposed no socket identity'
        );
        socketIds.push(response.socketNumber);
        observedRoutes.push(rpcStreamFromMonitor(await redisMonitor.nextRpc()));
    }
    assert.equal(
        new Set(socketIds).size,
        1,
        `keep-alive command requests used multiple sockets: ${socketIds.join(', ')}`
    );
    assert.equal(
        new Set(observedRoutes).size,
        1,
        `one keep-alive socket changed RPC worker streams: ${observedRoutes.join(', ')}`
    );
    assert(
        rpcStreams.includes(observedRoutes[0]),
        `keep-alive request used an unknown RPC worker stream: ${observedRoutes[0]}`
    );
    console.log(`PASS one keep-alive HTTP socket stayed on ${observedRoutes[0]}`);

    const rpcInstance = rpcInstanceId(rpcStreams[0]);
    const entityPrefix = `ruvia:orm:${Buffer.from('iot_worker_snapshot').toString('hex')}:`;
    const metricKeys = Array.from(
        { length: serviceWorkerCount },
        (_, index) => `${entityPrefix}${rpcInstance}:worker:${index}`
    );
    await until(async () => {
        const values = await Promise.all(
            metricKeys.map((key) => redis.send('HGET', [key, 'metrics']))
        );
        return values.every((value) => typeof value === 'string' && value.length > 0);
    }, 'multi-worker observability snapshots were not published');
    const metricSnapshots = await Promise.all(
        metricKeys.map(async (key) => {
            const value = await redis.send('HGET', [key, 'metrics']);
            assert.equal(typeof value, 'string', `missing metrics snapshot ${key}`);
            return value as string;
        })
    );
    const readinessValues = await Promise.all(
        metricKeys.map(async (key) => {
            const [value, pttl] = await Promise.all([
                redis.send('HGETALL', [key]),
                redis.send('PTTL', [key]),
            ]);
            assert(value !== null && typeof value === 'object' && !Array.isArray(value),
                `missing readiness entity ${key}`);
            const record = value as Record<string, string>;
            const fields = Object.entries(record).flat();
            assert.equal(record.id, key.slice(entityPrefix.length));
            assert(record.metrics && record.health && record.ready !== undefined,
                `worker snapshot ${key} is missing atomic entity fields`);
            const remaining = Number(pttl);
            assert(remaining > 0 && remaining <= 15000, `readiness snapshot ${key} has no bounded expiry`);
            return { key, value: fields, pttl: remaining };
        })
    );
    readinessBackups = readinessValues;
    for (const [index, snapshot] of metricSnapshots.entries()) {
        assert(
            snapshot.includes(`worker="${index}"`),
            `metrics snapshot ${metricKeys[index]} has no worker label`
        );
    }
    const metricsFirst = await internalResponse('/internal/metrics');
    assert.equal(metricsFirst.status, 200, metricsFirst.text);
    const metricsWorkerIndexes = (text: string) =>
        [...text.matchAll(/^iot_engine_ready\{worker="(\d+)"\}\s+\d+$/gm)]
            .map((match) => Number(match[1]))
            .sort((left, right) => left - right);
    assert.deepEqual(
        metricsWorkerIndexes(metricsFirst.text),
        Array.from({ length: serviceWorkerCount }, (_, index) => index),
        'metrics endpoint did not aggregate every worker snapshot'
    );
    const metricTypes = [...metricsFirst.text.matchAll(/^# TYPE (.+)$/gm)].map((match) => match[1]);
    assert.equal(
        new Set(metricTypes).size,
        metricTypes.length,
        'metrics endpoint emitted duplicate TYPE declarations'
    );
    await Bun.sleep(250);
    const metricsSecond = await internalResponse('/internal/metrics');
    assert.equal(metricsSecond.status, 200, metricsSecond.text);
    assert.deepEqual(
        metricsWorkerIndexes(metricsSecond.text),
        metricsWorkerIndexes(metricsFirst.text),
        'metrics worker snapshot set changed between stable reads'
    );
    console.log('PASS metrics endpoint aggregates stable snapshots from every Service Worker');

    await until(
        async () => (await internalResponse('/internal/health/ready')).status === 200,
        'all Service Worker readiness snapshots did not become healthy',
        15000,
        100
    );
    const readyBefore = await internalResponse('/internal/health/ready');
    assert.equal(readyBefore.status, 200, readyBefore.text);
    const expiredKey = readinessBackups[0].key;
    await redis.send('PEXPIRE', [expiredKey, '0']);
    try {
        await until(
            async () => (await internalResponse('/internal/health/ready')).status === 503,
            'expired readiness snapshot did not make the aggregate endpoint unavailable'
        );
    } finally {
        await restoreReadinessBackups();
    }
    await until(
        async () => (await internalResponse('/internal/health/ready')).status === 200,
        'readiness endpoint did not recover after restoring the worker snapshot'
    );
    readinessBackups = [];
    console.log('PASS expired readiness snapshot is detected and restored cleanly');

    const replayKey = id();
    idempotencyKeys.push(replayKey);
    const replays = await Promise.all([postCommand(replayKey), postCommand(replayKey)]);
    for (const replay of replays) assert.equal(replay.status, 200, JSON.stringify(replay.body));
    assert.deepEqual(replays[0].body.data.command_ids, replays[1].body.data.command_ids);
    operationIds.push(...replays[0].body.data.command_ids);
    const replayRequests =
        await db`SELECT id FROM command_request WHERE idempotency_key=${replayKey}`;
    assert.equal(replayRequests.length, 1, 'concurrent replay created duplicate requests');
    const replayOperations =
        await db`SELECT id FROM command_operation WHERE request_id=${replayRequests[0].id}`;
    assert.equal(replayOperations.length, 1, 'concurrent replay created duplicate operations');
    console.log('PASS concurrent idempotency replay returns one durable command');

    const failureKey = id();
    idempotencyKeys.push(failureKey);
    const beforeFailure =
        await db`SELECT count(*)::int AS count FROM command_operation WHERE device_id=${device}`;
    await db.unsafe(
        'ALTER TABLE command_attempt ADD CONSTRAINT rpc_test_reject_attempt CHECK (false) NOT VALID'
    );
    try {
        const failed = await postCommand(failureKey);
        assert.equal(
            failed.status,
            500,
            'injected attempt persistence failure did not fail the command'
        );
        const failedRequests =
            await db`SELECT id FROM command_request WHERE idempotency_key=${failureKey}`;
        assert.equal(failedRequests.length, 0, 'failed attempt left a committed request');
        const afterFailure =
            await db`SELECT count(*)::int AS count FROM command_operation WHERE device_id=${device}`;
        assert.equal(
            afterFailure[0].count,
            beforeFailure[0].count,
            'failed attempt left a committed operation'
        );
    } finally {
        await db.unsafe('ALTER TABLE command_attempt DROP CONSTRAINT rpc_test_reject_attempt');
    }
    console.log('PASS attempt persistence failure rolls back the request and operation');

    for (const stream of rpcStreams) {
        await redis.send('DEL', [stream]);
        const recovered = await callPrepareRpc(stream);
        assert.equal(recovered.commands[0].deviceId, device);
        await until(
            () => hasControlGroup(stream),
            `RPC consumer group was not recreated after deleting ${stream}`
        );
    }
    console.log('PASS online DEL of every worker-local RPC stream recovers its consumer');

    liveStreams.push(await openLiveStream('/v1/device/events', 'devices'));
    liveStreams.push(await openLiveStream('/v1/device/events', 'devices'));
    await Promise.all(liveStreams.map((stream) => stream.next()));
    const liveGroups = await liveGroupNames();
    const apiGroups = liveGroups.filter((name) => name.startsWith('api-live:'));
    assert(
        apiGroups.length >= serviceWorkerCount,
        `expected one API live consumer group per worker, got ${apiGroups.join(', ')}`
    );
    const featureGroups = liveGroups.filter((name) => /^live:[^:]+$/.test(name));
    assert.equal(
        featureGroups.length,
        1,
        `configuration fanout must use one shared live consumer group, got ${featureGroups.join(', ')}`
    );
    const featureGroup = featureGroups[0];
    assert(featureGroup, 'shared live configuration group was not created');
    const featureConsumers = await liveConsumerNames(featureGroup);
    const expectedFeatureConsumers = Array.from(
        { length: serviceWorkerCount },
        (_, index) => `fanout:${featureGroup.slice('live:'.length)}:${index}`
    );
    for (const consumer of expectedFeatureConsumers)
        assert(
            featureConsumers.includes(consumer),
            `shared live group is missing independent Service Worker consumer ${consumer}`
        );
    const liveMarker = `rpc-live-worker-${id()}`;
    await db`UPDATE device SET protocol_params=jsonb_set(
        protocol_params, '{device_code}', to_jsonb(${liveMarker}::text)) WHERE id=${device}`;
    await redis.send('XADD', ['iot:live:changes', '*', 'schema_version', '1', 'topic', 'device']);
    const liveSnapshots = await Promise.all(
        liveStreams.map((stream) =>
            stream.next((value) => JSON.stringify(value).includes(liveMarker))
        )
    );
    assert(
        liveSnapshots.every((snapshot) => JSON.stringify(snapshot).includes(liveMarker)),
        'one committed device change did not reach every worker-local live subscription'
    );
    console.log('PASS two simultaneous SSE subscriptions receive one committed live change');
    await Promise.all(liveStreams.map((stream) => stream.close()));
    liveStreams = [];

    const configStreams = await findConfigStreams(1);
    const configLengths = await Promise.all(
        configStreams.map(async (stream) => Number(await redis.send('XLEN', [stream])))
    );
    await redis.send('XADD', [
        'iot:live:changes',
        '*',
        'schema_version',
        '1',
        'topic',
        'runtime-config',
    ]);
    await until(async () => {
        const lengths = await Promise.all(
            configStreams.map(async (stream) => Number(await redis.send('XLEN', [stream])))
        );
        return lengths.every((length, index) => length > configLengths[index]);
    }, 'shared live configuration notification was not relayed to every Collector Worker');
    console.log('PASS one shared live notification independently relays to every Collector Worker');

    const sharedReleased = new Promise<void>((resolve) => {
        releaseShared = resolve;
    });
    let sharedPid = 0;
    let sharedReady!: () => void;
    const sharedHeld = new Promise<void>((resolve) => {
        sharedReady = resolve;
    });
    moduleTransaction = db.begin(async (tx) => {
        const locked = await tx`SELECT pg_backend_pid() AS pid, id::text AS id
            FROM device WHERE id=${device} FOR SHARE`;
        assert.equal(locked.length, 1, 'module device shared-lock query found no device');
        sharedPid = Number(locked[0].pid);
        sharedReady();
        await sharedReleased;
    });
    await waitFor(sharedHeld, 'module transaction did not acquire the device shared lock');

    let updatePid = 0;
    let updateDone = false;
    let updateReady!: () => void;
    const updateStarted = new Promise<void>((resolve) => {
        updateReady = resolve;
    });
    const updateMarker = `rpc-lock-${id()}`;
    updateTransaction = db.begin(async (tx) => {
        const backend = await tx`SELECT pg_backend_pid() AS pid`;
        updatePid = Number(backend[0].pid);
        updateReady();
        await tx`UPDATE device SET remark=${updateMarker} WHERE id=${device}`;
        updateDone = true;
    });
    await waitFor(updateStarted, 'device UPDATE transaction did not start');
    await until(
        async () => {
            const rows = await db`SELECT wait_event_type::text AS wait_type
            FROM pg_stat_activity WHERE pid=${updatePid}`;
            return rows[0]?.wait_type === 'Lock';
        },
        'device UPDATE did not queue behind the module shared lock',
        5000,
        25
    );
    assert.notEqual(
        sharedPid,
        updatePid,
        'module lock and UPDATE reused one PostgreSQL connection'
    );
    assert.equal(updateDone, false, 'device UPDATE completed while the shared lock was held');

    const rpcStarted = Date.now();
    const preparedWhileLocked = await callPrepareRpc(rpcStream);
    const rpcElapsed = Date.now() - rpcStarted;
    assert.equal(preparedWhileLocked.commands[0].deviceId, device);
    assert(rpcElapsed < 5000, `background MVCC prepare took ${rpcElapsed}ms behind a device lock`);
    assert.equal(updateDone, false, 'device UPDATE was not still queued during MVCC prepare');
    releaseShared();
    await moduleTransaction;
    await updateTransaction;
    assert.equal(updateDone, true, 'queued device UPDATE did not complete after lock release');
    console.log(
        'PASS three-connection lock isolation: module FOR SHARE queues UPDATE while RPC MVCC read completes'
    );
} finally {
    try {
        await Promise.all(liveStreams.map((stream) => stream.close().catch(() => {})));
        liveStreams = [];
        redisMonitor?.close();
        redisMonitor = undefined;
        await restoreReadinessBackups().catch((error) =>
            console.error('readiness snapshot cleanup failed', error)
        );
        keepAliveAgent.destroy();
        releaseShared?.();
        await moduleTransaction?.catch((error) =>
            console.error('module lock cleanup failed', error)
        );
        await updateTransaction?.catch((error) =>
            console.error('queued UPDATE cleanup failed', error)
        );
        if (operationIds.length) {
            await db`DELETE FROM outbox_event
                WHERE payload->'data'->>'commandId' IN ${db(operationIds)}`;
            await db`DELETE FROM command_attempt WHERE operation_id IN ${db(operationIds)}`;
            await db`DELETE FROM command_operation WHERE id IN ${db(operationIds)}`;
        }
        if (idempotencyKeys.length)
            await db`DELETE FROM command_request WHERE idempotency_key IN ${db(idempotencyKeys)}`;
        await redis.send('DEL', [
            `iot:v2:owner:link:${link}`,
            `iot:v3:device:connection:${device}`,
            `iot:v2:device:${device}:latest`,
            `iot:channel:command:worker:${instance}:0:high`,
            ...rpcStreams,
        ]);
        await db`DELETE FROM device WHERE id=${device}`;
        await db`DELETE FROM link WHERE id=${link}`;
        // 已发布协议版本不可删除，随独立测试数据库保留用于失败排查。
    } finally {
        await db.close();
        redis.close();
    }
}
