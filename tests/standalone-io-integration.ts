import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const platform = '00000000-0000-7000-8000-000000000001';
const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin, username: 'admin', token_type: 'access', iat: now, exp: now + 3600 })}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };

function integer(input: bigint | number) {
    let value = BigInt(input);
    const output: number[] = [];
    do {
        let byte = Number(value & 127n);
        value >>= 7n;
        if (value) byte |= 128;
        output.push(byte);
    } while (value);
    return Buffer.from(output);
}
function field(tag: number, value: bigint | number | string | Buffer) {
    if (typeof value === 'bigint' || typeof value === 'number')
        return Buffer.concat([integer(tag * 8), integer(value)]);
    const data = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(data.length), data]);
}
function doubleField(tag: number, value: number) {
    const bytes = Buffer.alloc(8);
    bytes.writeDoubleLE(value);
    return Buffer.concat([integer(tag * 8 + 1), bytes]);
}
function decode(data: Buffer) {
    const result = new Map<number, bigint | Buffer>();
    let position = 0;
    const read = () => {
        let value = 0n, shift = 0n;
        for (;;) {
            const byte = data[position++];
            assert(byte !== undefined);
            value |= BigInt(byte & 127) << shift;
            if (!(byte & 128)) return value;
            shift += 7n;
        }
    };
    while (position < data.length) {
        const key = Number(read()), wire = key & 7;
        if (!wire) result.set(key >> 3, read());
        else {
            const size = wire === 2 ? Number(read()) : wire === 1 ? 8 : wire === 5 ? 4 : -1;
            assert(size >= 0);
            result.set(key >> 3, data.subarray(position, position + size));
            position += size;
        }
    }
    return result;
}
async function until(check: () => Promise<boolean>, reason: string) {
    const deadline = Date.now() + 25000;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(25);
    }
    throw Error(reason);
}
async function request(path: string, body?: unknown, method = body === undefined ? 'GET' : 'POST', expected = 200) {
    const response = await fetch(apiBase + path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
    const text = await response.text();
    assert.equal(response.status, expected, `${path}: ${text}`);
    return JSON.parse(text);
}
function imei() {
    const base = '99' + String(Math.floor(Math.random() * 1e12)).padStart(12, '0');
    let sum = 0;
    for (let i = 0; i < 14; i++) {
        let digit = Number(base[i]) * (i % 2 ? 2 : 1);
        if (digit > 9) digit -= 9;
        sum += digit;
    }
    return base + String((10 - sum % 10) % 10);
}

const node = crypto.randomUUID(), identity = imei(), name = crypto.randomUUID();
const analog = crypto.randomUUID(), output = crypto.randomUUID(), derived = crypto.randomUUID();
let model = '', device = '', link = '', socket: WebSocket | undefined;
let epoch = 0n, sequence = 1n;
const received = new Map<number, Buffer[]>();
const envelope = (tag: number, payload: Buffer) => Buffer.concat([field(1, 6), field(2, bytes(crypto.randomUUID())), field(3, bytes(node)), field(4, bytes(platform)), field(5, epoch), field(6, Date.now()), field(8, sequence++), field(tag, payload)]);
const channel = (name: string, kind: number, controllable = false) => field(11, Buffer.concat([field(1, name), field(2, kind), field(3, 1), field(4, Number(controllable))]));
const capability = (standalone: boolean) => Buffer.concat([field(7, Buffer.from(standalone ? [1, 2, 3, 4, 5, 6, 7] : [1, 2, 3, 4, 5, 6])), field(10, 1), field(12, 1), channel('AI0', 3), channel('do0', 2, true)]);
try {
    await db`INSERT INTO edge_node(id,imei,name,platform_id,enrollment_status) VALUES(${node},${identity},${node},${platform},'approved')`;
    await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);
    socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect');
    socket.binaryType = 'arraybuffer';
    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => reject(Error('IO Hello timeout')), 10000);
        socket!.onopen = () => socket!.send(envelope(20, Buffer.concat([field(1, identity), field(2, 'Ebyte T113-I ECM30'), field(3, '0.3.78'), field(23, 1), field(36, 1), field(37, 1), field(38, 1), field(39, 1), field(40, 1), field(41, 1)])));
        socket!.onerror = () => reject(Error('IO WebSocket error'));
        socket!.onmessage = event => {
            const message = decode(Buffer.from(event.data as ArrayBuffer));
            if (message.has(21)) { epoch = message.get(5) as bigint; clearTimeout(timeout); resolve(); }
            if (message.has(80)) socket!.send(envelope(81, message.get(80) as Buffer));
            for (const tag of [31, 32, 50]) if (message.has(tag)) {
                const queue = received.get(tag) ?? [];
                queue.push(message.get(tag) as Buffer);
                received.set(tag, queue);
            }
        };
    });
    socket.send(envelope(26, capability(false)));
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'externalIo'->>'supported'='true'`).length > 0, 'IO capability missing');
    await request('/v1/protocol/configs', { name, protocol: 'ExternalIO', config: { edgeNodeModel: 'Ebyte T113-I ECM30', readInterval: 1, storagePolicy: 'report', points: [{ id: analog, name: 'current', kind: 'AI', channel: 'AI0', unit: 'mA' }, { id: output, name: 'switch', kind: 'DO', channel: 'do0' }], derivedPoints: [{ id: derived, name: 'double current', kind: 'expression', valueType: 'number', expression: 'x * 2', inputs: [{ alias: 'x', pointId: analog }], unit: 'mA', unitMode: 'fixed', maxAgeSeconds: 30, visible: true }] } });
    model = (await db`SELECT id FROM protocol_config WHERE name=${name}`)[0].id;
    const connection = { name, edge_node_id: node, protocol: 'ExternalIO', endpoint: { transport: 'io' }, status: 'enabled' };
    const body = { name, device_code: name, protocol_config_id: model, edge_connection: connection };
    const legacyDetail = await request(`/v1/edge/${node}`);
    assert.deepEqual([...legacyDetail.data.capability.protocols].sort(), ['Modbus', 'S7', 'SL651', 'MC', 'FINS', 'DLT645'].sort());
    await request('/v1/device', body, 'POST', 400);
    socket.send(envelope(26, capability(true)));
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'protocols' ? 'ExternalIO'`).length > 0, 'standalone capability missing');
    socket.send(envelope(89, Buffer.concat([
        field(1, Date.now()),
        field(2, Buffer.concat([field(1, 'AI0'), field(2, 3), field(3, 1), doubleField(7, 12.345)])),
        field(2, Buffer.concat([field(1, 'do0'), field(2, 2), field(3, 1), field(4, 1), field(6, 0)])),
    ])));
    await until(async () => {
        const io = await request(`/v1/edge/${node}/io`);
        return io.data.channels.some((row: any) => row.state.channel === 'AI0' && row.state.value === '12.345') &&
            io.data.channels.some((row: any) => row.state.channel === 'do0' && row.state.value === '0');
    }, 'node IO state did not preserve mA current and off output');
    const detail = await request(`/v1/edge/${node}`);
    assert(detail.data.capability.protocols.includes('ExternalIO'));
    const inventory = await request('/v1/edge');
    assert(inventory.data.list.find((item: any) => item.id === node).capability.protocols.includes('ExternalIO'));
    await request('/v1/device', { ...body, edge_connection: { ...connection, endpoint: { transport: 'io', ip: '127.0.0.1' } } }, 'POST', 400);
    await request('/v1/device', body);
    ({ id: device, link_id: link } = (await db`SELECT id,link_id FROM device WHERE name=${name} AND deleted_at IS NULL`)[0]);
    assert.equal(Number((await db`SELECT count(*) AS count FROM device_external_io_binding WHERE device_id=${device}`)[0].count), 2);
    const deviceDetail = await request(`/v1/device/${device}`);
    assert.equal(deviceDetail.data.protocol_type, 'ExternalIO');
    assert.equal(deviceDetail.data.edge_transport, 'io');
    assert.equal(deviceDetail.data.externalIo.protocolConfigId, model);
    await request('/v1/device', { ...body, name: crypto.randomUUID(), device_code: crypto.randomUUID() }, 'POST', 409);
    await request(`/v1/device/${device}`, { ...body, name: name + ' edited' }, 'PUT');
    assert.equal((await db`SELECT link_id FROM device WHERE id=${device}`)[0].link_id, link, 'editing duplicated the logical endpoint');
    await request(`/v1/protocol/configs/${model}`, { config: { points: [{ id: analog, name: 'current renamed', kind: 'AI', channel: 'AI0', unit: 'mA' }, { id: output, name: 'switch', kind: 'DO', channel: 'do0' }] } }, 'PUT');
    await request(`/v1/edge/${node}/sync`, {});
    const revision = BigInt((await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`)[0].revision);
    await until(async () => (received.get(32) ?? []).some(item => decode(item).get(1) === revision), 'IO config commit missing');
    const items = (received.get(31) ?? []).map(decode).filter(item => item.get(1) === revision);
    const endpoint = decode(items.find(item => item.has(10))!.get(10) as Buffer);
    assert.equal(endpoint.get(3), 3n);
    assert(!endpoint.has(4) && !endpoint.has(6) && !endpoint.has(7) && !endpoint.has(8) && !endpoint.has(10), 'IO endpoint contains network or serial settings');
    assert.equal(items.filter(item => item.has(20)).length, 2);
    const current = decode(items.find(item => item.has(20) && (decode(item.get(20) as Buffer).get(3) as Buffer).toString() === analog)!.get(20) as Buffer);
    assert.equal((current.get(7) as Buffer).toString(), 'mA', 'firmware was not configured to report mA');
    assert.equal(items.filter(item => item.has(19)).length, 1, 'IO derived definition duplicated');
    const commit = decode((received.get(32) ?? []).find(item => decode(item).get(1) === revision)!);
    const digest = commit.get(2) as Buffer;
    socket.send(envelope(33, Buffer.concat([field(1, revision), field(2, digest)])));
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND status->'config'->>'state'='applied' AND status->'config'->>'activeVersion'=status->'config'->>'desiredVersion'`).length > 0, 'IO config not applied');
    const sample = crypto.randomUUID();
    const value = Buffer.concat([field(1, analog), field(4, Buffer.concat([field(1, 4), doubleField(5, 5)]))]);
    socket.send(envelope(40, field(1, Buffer.concat([field(1, bytes(sample)), field(2, bytes(device)), field(3, bytes(link)), field(4, 7), field(8, Date.now()), field(9, value), field(12, bytes(model)), field(21, digest)]))));
    await until(async () => (await db`SELECT 1 FROM device_data WHERE device_id=${device} AND source='derived' AND data->'values'->${derived}->>'value'='10'`).length > 0, 'standalone derived history missing');
    await until(async () => JSON.parse(String(await redis.send('HGET', [`iot:v2:device:${device}:latest`, derived]) ?? '{}')).value === '10', 'standalone derived latest missing');
    const accepted = await request(`/v1/device/${device}/commands`, { idempotency_key: crypto.randomUUID(), elements: [{ elementId: output, value: '1' }] });
    const operation = accepted.data.command_ids[0];
    await until(async () => (received.get(50) ?? []).some(item => (decode(item).get(1) as Buffer).equals(bytes(operation))), 'standalone IO command missing');
    const command = decode((received.get(50) ?? []).find(item => (decode(item).get(1) as Buffer).equals(bytes(operation)))!);
    assert.equal(command.get(9), 1n);
    socket.send(envelope(52, Buffer.concat([field(1, bytes(operation)), field(2, 3), field(6, Date.now()), field(7, bytes(device))])));
    await until(async () => (await db`SELECT 1 FROM command_operation WHERE id=${operation} AND protocol='ExternalIO' AND model_id=${model} AND status='SUCCEEDED'`).length > 0, 'standalone IO command result missing');
    for (const enabled of [false, true]) {
        await request(`/v1/device/${device}`, { status: enabled ? 'enabled' : 'disabled' }, 'PUT');
        await request(`/v1/edge/${node}/sync`, {});
        const version = BigInt((await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`)[0].revision);
        await until(async () => (received.get(32) ?? []).some(item => decode(item).get(1) === version), 'device state config commit missing');
        const snapshot = (received.get(31) ?? []).map(decode).filter(item => item.get(1) === version);
        const projectedDevice = decode(snapshot.find(item => item.has(11))!.get(11) as Buffer);
        assert.equal(projectedDevice.get(24) === 1n, enabled, 'device enable state was not projected');
        assert.equal(snapshot.filter(item => item.has(20)).length, enabled ? 2 : 0, 'disabled IO bindings were projected');
        assert.equal(snapshot.filter(item => item.has(19)).length, enabled ? 1 : 0, 'disabled IO-derived point has no projected source');
        const stateCommit = decode((received.get(32) ?? []).find(item => decode(item).get(1) === version)!);
        socket.send(envelope(33, Buffer.concat([field(1, version), field(2, stateCommit.get(2) as Buffer)])));
        await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND status->'config'->>'state'='applied' AND status->'config'->>'activeVersion'=${String(version)}`).length > 0, 'device state config was not applied');
    }
    socket.send(envelope(26, capability(false)));
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND NOT (capability->'protocols' ? 'ExternalIO')`).length > 0, 'standalone capability revocation missing');
    await request(`/v1/edge/${node}/sync`, {}, 'POST', 409);
    const rejectedRevision = BigInt((await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`)[0].revision);
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND status->'config'->>'state'='rejected' AND status->'config'->>'message' LIKE '%ExternalIO%'`).length > 0, 'unsupported standalone protocol was not rejected before delivery');
    assert((received.get(31) ?? []).every(item => decode(item).get(1) !== rejectedRevision), 'node without protocol support received an IO endpoint');
    await request(`/v1/device/${device}`, undefined, 'DELETE');
    assert.equal((await db`SELECT 1 FROM device_external_io_binding WHERE device_id=${device}`).length, 0);
    assert.equal((await db`SELECT status FROM link WHERE id=${link}`)[0].status, 'disabled');
    console.log('PASS 独立外部 IO 创建、能力门控、配置、采样、派生值、DO 独占与控制、编辑和清理');
} finally {
    if (socket) { socket.onclose = null; socket.close(); }
    if (device) await db`UPDATE device SET deleted_at=NOW() WHERE id=${device}`;
    if (link) await db`UPDATE link SET status='disabled',deleted_at=NOW() WHERE id=${link}`;
    if (model) await db`UPDATE protocol_config SET deleted_at=NOW() WHERE id=${model}`;
    await redis.send('DEL', [`iot:edge:auth:${identity}`]);
    redis.close();
    await db.close();
}
