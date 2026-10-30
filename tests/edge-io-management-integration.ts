import { sharedChannel } from './redis_channel.fixture';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const streams: Awaited<ReturnType<typeof openSnapshotSubscription>>[] = [];
const admin = '00000000-0000-7000-8000-000000000002';
const platform = process.env.TEST_EDGE_PLATFORM_ID ?? '00000000-0000-7000-8000-000000000001';
const node = crypto.randomUUID(),
    pending = crypto.randomUUID();
const user = crypto.randomUUID(),
    role = crypto.randomUUID();
const tag = `edge_event_${crypto.randomUUID()}`;
const groups: string[] = [];
let device: WebSocket | undefined;
function token(id: string) {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: id, user_id: id, username: id, token_type: 'access', iat: now, exp: now + 3600 })}`;
    return `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
async function request(method: string, path: string, data?: unknown, code = 0, actor = admin) {
    const response = await fetch(apiBase + path, {
        method,
        headers: {
            'Content-Type': 'application/json',
            ...(actor ? { Authorization: `Bearer ${token(actor)}` } : {}),
        },
        body: data === undefined ? undefined : JSON.stringify(data),
        signal: AbortSignal.timeout(15000),
    });
    const reply = (await response.json()) as { code: number; message: string; data: any };
    assert.equal(reply.code, code, `${method} ${path}: ${JSON.stringify(reply)}`);
    assert.equal(response.ok, code === 0);
    return reply.data;
}
async function subscribe(path: string, eventName: string) {
    const stream = await openSnapshotSubscription(path, token(admin));
    streams.push(stream);
    async function next() {
        for (;;) {
            const event = await stream.next();
            if (event.event !== eventName) {
                assert.notEqual(event.event, 'error', event.data);
                continue;
            }
            return JSON.parse(event.data);
        }
    }
    return { next, close: () => stream.close() };
}
function imei() {
    const base = '99' + String(Math.floor(Math.random() * 1e12)).padStart(12, '0');
    let sum = 0;
    for (let i = 0; i < 14; i++) {
        let n = Number(base[i]) * (i % 2 ? 2 : 1);
        if (n > 9) n -= 9;
        sum += n;
    }
    return base + String((10 - (sum % 10)) % 10);
}
const identity = imei(),
    pendingIdentity = imei();
function integer(input: bigint | number) {
    let n = BigInt(input);
    const result: number[] = [];
    do {
        let byte = Number(n & 127n);
        n >>= 7n;
        if (n) byte |= 128;
        result.push(byte);
    } while (n);
    return Buffer.from(result);
}
function field(tag: number, value: bigint | number | string | Buffer) {
    if (typeof value === 'number' || typeof value === 'bigint')
        return Buffer.concat([integer(tag * 8), integer(value)]);
    const bytes = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(bytes.length), bytes]);
}
function decode(data: Buffer) {
    const result = new Map<number, bigint | Buffer>();
    let position = 0;
    const read = () => {
        let n = 0n,
            shift = 0n;
        for (;;) {
            const b = data[position++];
            assert(b !== undefined);
            n |= BigInt(b & 127) << shift;
            if (!(b & 128)) return n;
            shift += 7n;
        }
    };
    while (position < data.length) {
        const key = Number(read()),
            wire = key & 7;
        if (!wire) result.set(key >> 3, read());
        else if (wire === 2) {
            const n = Number(read());
            result.set(key >> 3, data.subarray(position, position + n));
            position += n;
        } else if (wire === 1 || wire === 5) {
            const n = wire === 1 ? 8 : 4;
            position += n;
        } else throw Error('Invalid device frame');
    }
    return result;
}
async function until(check: () => Promise<boolean>, reason: string) {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(25);
    }
    throw Error(reason);
}
const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
const link = crypto.randomUUID(),
    scheme = crypto.randomUUID(),
    primary = crypto.randomUUID(),
    owner = crypto.randomUUID(),
    point = crypto.randomUUID();
const channels = [
    { channel: 'di0', kind: 'DI', available: true, controllable: false },
    { channel: 'do0', kind: 'DO', available: true, controllable: true },
    { channel: 'do1', kind: 'DO', available: true, controllable: true },
];
const incoming: Buffer[] = [];
let epoch = 0n,
    sequence = 1n;
const envelope = (tag: number, payload: Buffer) =>
    Buffer.concat([
        field(1, 6),
        field(2, bytes(crypto.randomUUID())),
        field(3, bytes(node)),
        field(4, bytes(platform)),
        field(5, epoch),
        field(6, Date.now()),
        field(8, sequence++),
        field(tag, payload),
    ]);
const ioPath = `/v1/edge/${node}/io`;
const doControl = (channel = 'do1', idempotencyKey = crypto.randomUUID()) => ({
    channel,
    value: true,
    idempotencyKey,
});
try {
    const models = await request('GET', '/v1/protocol/configs/external-io-models');
    assert(
        models.some(
            (value: any) => value.model === 'Ebyte T113-I ECM30' && value.channels.length === 15
        )
    );
    const invalidScheme = {
        name: scheme,
        protocol: 'ExternalIO',
        config: {
            storagePolicy: 'report',
            edgeNodeModel: 'unknown',
            points: [{ id: point, name: 'DO', kind: 'DO', channel: 'do0' }],
        },
    };
    await request('POST', '/v1/protocol/configs', invalidScheme, 16004);
    await request(
        'POST',
        '/v1/protocol/configs',
        {
            ...invalidScheme,
            config: {
                ...invalidScheme.config,
                edgeNodeModel: 'Ebyte T113-I ECM30',
                points: [{ id: point, name: 'DO', kind: 'DO', channel: 'do7' }],
            },
        },
        16004
    );
    await db`INSERT INTO edge_node(id,platform_id,imei,model,name,enrollment_status,capability) VALUES(${node},${platform},${identity},'Ebyte T113-I ECM30',${tag},'approved',${{ externalIo: { supported: true, channels } }}::jsonb)`;
    await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);
    await request('GET', ioPath, undefined, 11004, '');
    await request('POST', ioPath, doControl(), 17004);
    device = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect');
    device.binaryType = 'arraybuffer';
    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(Error('Hello timed out')), 10000);
        device!.onopen = () =>
            device!.send(
                envelope(
                    20,
                    Buffer.concat([
                        field(1, identity),
                        field(2, 'Ebyte T113-I ECM30'),
                        field(3, '0.3.78'),
                        field(23, 1),
                        field(36, 1),
                        field(37, 1),
                        field(38, 1),
                        field(39, 1),
                        field(40, 1),
                        field(41, 1),
                    ])
                )
            );
        device!.onerror = () => reject(Error('device websocket failed'));
        device!.onmessage = (event) => {
            const message = decode(Buffer.from(event.data as ArrayBuffer));
            if (message.has(21)) {
                epoch = message.get(5) as bigint;
                assert.equal(decode(message.get(21) as Buffer).get(10), 1n);
                clearTimeout(timeout);
                resolve();
            }
            if (message.has(80)) device!.send(envelope(81, message.get(80) as Buffer));
            if (message.has(50)) incoming.push(message.get(50) as Buffer);
        };
    });
    const wireChannel = (channel: (typeof channels)[number], value = false) =>
        Buffer.concat([
            field(1, channel.channel),
            field(2, channel.kind === 'DI' ? 1 : 2),
            field(3, 1),
            field(4, Number(channel.controllable)),
            field(6, Number(value)),
        ]);
    device.send(
        envelope(
            26,
            Buffer.concat([
                field(7, Buffer.from([1, 2, 3, 4, 5, 6])),
                field(10, 1),
                field(12, 1),
                ...channels.map((channel) => field(11, wireChannel(channel))),
            ])
        )
    );
    await until(
        async () =>
            (
                await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'externalIo'->>'supported'='true'`
            ).length === 1,
        'capability missing'
    );
    await request('POST', ioPath, doControl(), 17004);
    const sendState = (value = false, observedAt = Date.now()) =>
        device!.send(
            envelope(
                89,
                Buffer.concat([
                    field(1, observedAt),
                    ...channels.map((channel) => field(2, wireChannel(channel, value))),
                ])
            )
        );
    sendState();
    await until(
        async () => (await request('GET', ioPath)).current,
        'initial node IO status missing'
    );
    const io = await subscribe(`/v1/edge/events?nodeId=${node}&io=true`, 'io');
    assert.equal((await io.next()).data.channels.length, 3);
    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by) VALUES(${primary},${primary},'Modbus',${{ storagePolicy: 'change', registers: [] }}::jsonb,${admin}),(${scheme},${scheme},'ExternalIO',${{ storagePolicy: 'report', edgeNodeModel: 'Ebyte T113-I ECM30', points: [{ id: point, name: 'owned output', kind: 'DO', channel: 'do0' }] }}::jsonb,${admin})`;
    await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,edge_node_id,created_by) VALUES(${link},${link},'Modbus',${{ transport: 'tcp', interface: 'lo', mode: 'TCP Client', ip: '127.0.0.1', port: 502 }}::jsonb,'enabled','edge',${node},${admin})`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by) VALUES(${owner},${owner},${link},${primary},${{ device_code: '1', remote_control: true, modbus_mode: 'TCP', slave_id: 1 }}::jsonb,${admin})`;
    await db`INSERT INTO device_external_io(device_id,link_id,node_id,protocol_config_id) VALUES(${owner},${link},${node},${scheme})`;
    const state = await request('GET', ioPath);
    assert.equal(
        state.channels.find((row: any) => row.state.channel === 'do0').bindings[0].deviceId,
        owner
    );
    await request('POST', ioPath, doControl('do0'), 17003);
    await request('POST', ioPath, doControl('di0'), 17004);
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:edge:query"]'::jsonb)`;
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${user},${user},'unused')`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${user},${role})`;
    await request('GET', ioPath, undefined, 0, user);
    await request('POST', ioPath, doControl(), 11007, user);
    const input = doControl();
    const accepted = await request('POST', ioPath, input);
    await until(async () => incoming.length === 1, 'node IO control was not delivered');
    const command = decode(incoming[0]);
    assert.equal((command.get(10) as Buffer).toString(), 'do1');
    assert.equal(command.get(2), undefined);
    assert.equal(
        (command.get(1) as Buffer).toString('hex'),
        input.idempotencyKey.replaceAll('-', '')
    );
    assert.equal((await request('POST', ioPath, input)).id, accepted.id);
    await request('POST', ioPath, { ...input, value: false }, 17003);
    await request('POST', ioPath, doControl(), 17003);
    // A channel cannot become bound while an accepted control is awaiting its durable result.
    await assert.rejects(
        async () => {
            await db`UPDATE protocol_config SET config=jsonb_set(config,'{points,0,channel}','"do1"'::jsonb) WHERE id=${scheme}`;
        },
        (error: any) => error.errno === '23514'
    );
    device.send(
        envelope(
            52,
            Buffer.concat([
                field(1, bytes(accepted.id)),
                field(2, 3),
                field(3, 'output confirmed'),
                field(6, Date.now()),
                field(8, 'do1'),
            ])
        )
    );
    await until(
        async () =>
            (await db`SELECT status FROM edge_task WHERE id=${accepted.id}`)[0]?.status ===
            'succeeded',
        'node IO result was not durable'
    );
    sendState(true);
    await until(
        async () =>
            (await request('GET', ioPath)).channels.find((row: any) => row.state.channel === 'do1')
                .state.value === '1',
        'IO state did not update'
    );
    sendState(false, Date.now() - 60000);
    await until(
        async () =>
            (await request('GET', ioPath)).channels.find((row: any) => row.state.channel === 'do1')
                .state.value === '0',
        'clock correction discarded a later IO state'
    );
    const final = await request('GET', ioPath);
    assert.equal(
        final.channels.find((row: any) => row.state.channel === 'do1').lastControl.id,
        accepted.id
    );
    assert.equal(final.channels.find((row: any) => row.state.channel === 'do1').pending, undefined);
    await db`UPDATE protocol_config SET config=jsonb_set(config,'{points,0,channel}','"do1"'::jsonb) WHERE id=${scheme}`;
    await request('POST', ioPath, doControl(), 17003);
    const expired = crypto.randomUUID();
    const currentLease = await redis.get(`iot:edge:session:${node}`);
    assert(currentLease);
    await db`INSERT INTO edge_task(id,node_id,task_type,request,created_by) VALUES(${expired},${node},'io',${{ channel: 'do0', value: true, session: currentLease, startBeforeMs: Date.now() - 1000 }}::jsonb,${admin})`;
    await until(
        async () =>
            (await db`SELECT status FROM edge_task WHERE id=${expired}`)[0]?.status === 'failed',
        'undispatched expired IO task did not complete'
    );
    const uncertainInput = doControl('do0');
    const uncertain = await request('POST', ioPath, uncertainInput);
    await until(async () => incoming.length === 2, 'uncertain IO command not delivered');
    device.send(
        envelope(
            52,
            Buffer.concat([
                field(1, bytes(uncertain.id)),
                field(2, 8),
                field(3, 'write result is uncertain'),
                field(5, 1),
                field(6, Date.now()),
                field(8, 'do0'),
            ])
        )
    );
    await until(
        async () =>
            (await request('GET', ioPath)).channels.find((row: any) => row.state.channel === 'do0')
                .lastControl?.uncertain === true,
        'uncertain IO result was not retained'
    );
    await request('POST', ioPath, doControl('do0'), 17003);
    await assert.rejects(
        async () => {
            await db`UPDATE protocol_config SET config=jsonb_set(config,'{points,0,channel}','"do0"'::jsonb) WHERE id=${scheme}`;
        },
        (error: any) => error.errno === '23514'
    );
    // A changed session fence invalidates previous values even before the first new report.
    const lease = await redis.get(`iot:edge:session:${node}`);
    assert(lease);
    await redis.set(`iot:edge:session:${node}`, lease.replace(/^\d+/, String(epoch + 1n)));
    await request('POST', ioPath, doControl('do0'), 17004);
    assert.equal((await request('GET', ioPath)).current, false);
    console.log(
        'PASS node IO HTTP, model limits, status, bindings, authorization, idempotency, binding/control fence, durable result and session fence'
    );
} finally {
    for (const stream of streams) await stream.close();
    device?.close();
    await Bun.sleep(100);
    await db`DELETE FROM device_external_io WHERE device_id=${owner}`;
    await db`DELETE FROM device WHERE id=${owner}`;
    await db`DELETE FROM link WHERE id=${link}`;
    await db`DELETE FROM protocol_config WHERE id IN (${primary},${scheme})`;
    await db`DELETE FROM edge_node WHERE id=${node}`;
    await db`DELETE FROM sys_user_role WHERE user_id=${user}`;
    await db`DELETE FROM sys_user WHERE id=${user}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await redis.send('DEL', [
        `iot:edge:auth:${identity}`,
        `iot:edge:session:${node}`,
        `iot:edge:io:${node}`,
    ]);
    redis.close();
    await db.close();
}
