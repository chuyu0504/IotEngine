import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, publishFixtureEvent, redisUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const user = crypto.randomUUID();
const name = `open_mqtt_${crypto.randomUUID()}`;
const device = crypto.randomUUID(), link = crypto.randomUUID(), model = crypto.randomUUID();
const readPoint = crypto.randomUUID(), writePoint = crypto.randomUUID();
const port = Number(Bun.env.OPEN_MQTT_BROKER_PORT ?? 18884);
const brokerUrl = Bun.env.OPEN_MQTT_BROKER_URL ?? `mqtt://127.0.0.1:${port}`;
const messages: { topic: string; body: Record<string, any> }[] = [];
let key = '', destination = '', webhook = '';
const webhookBodies: Record<string, unknown>[] = [];
const receiver = Bun.serve({ hostname: '127.0.0.1', port: 0, async fetch(request) { webhookBodies.push(await request.json()); return new Response('ok'); } });
let subscriber: Bun.Socket | undefined;
let buffer = Buffer.alloc(0);
let subscribed = false;
function packet(header: number, body: Buffer) {
    const bytes: number[] = [];
    for (let n = body.length;;) {
        const next = n % 128;
        n = Math.floor(n / 128);
        bytes.push(next | (n ? 128 : 0));
        if (!n) break;
    }
    return Buffer.concat([Buffer.from([header, ...bytes]), body]);
}
function text(value: string) {
    const bytes = Buffer.from(value), size = Buffer.alloc(2);
    size.writeUInt16BE(bytes.length);
    return Buffer.concat([size, bytes]);
}
function frame(bytes: Buffer) {
    let length = 0, offset = 1, multiplier = 1;
    while (offset < bytes.length && offset <= 4) {
        const byte = bytes[offset++]; length += (byte & 127) * multiplier; multiplier *= 128;
        if (!(byte & 128)) return bytes.length < offset + length ? undefined : { header: bytes[0], body: bytes.subarray(offset, offset + length), size: offset + length };
    }
}
function connectCredentials(body: Buffer) {
    let offset = 0;
    const field = () => { const size = body.readUInt16BE(offset), value = body.subarray(offset + 2, offset + 2 + size).toString(); offset += 2 + size; return value; };
    field();
    const flags = body[offset + 1];
    offset += 4;
    field();
    if (flags & 4) { field(); field(); }
    return { username: flags & 0x80 ? field() : undefined, password: flags & 0x40 ? field() : undefined };
}
// CI 的匿名 Broker 接受任意凭据；明文 Broker 前置此网关，按配置凭据校验 CONNECT，错误时回 CONNACK=5，正确时透传到真实 Broker。
const gated = brokerUrl.startsWith('mqtt://');
let rejectedConnects = 0;
const gate = Bun.listen<{ pending: Buffer; connecting: boolean; upstream?: Bun.Socket }>({ hostname: '127.0.0.1', port: 0, socket: {
    open(client) { client.data = { pending: Buffer.alloc(0), connecting: false }; },
    data(client, chunk) {
        const state = client.data;
        if (state.upstream) { state.upstream.write(chunk); return; }
        state.pending = Buffer.concat([state.pending, Buffer.from(chunk)]);
        const connect = state.connecting ? undefined : frame(state.pending);
        if (!connect) return;
        const credentials = connect.header === 0x10 ? connectCredentials(connect.body) : undefined;
        if (credentials?.username !== 'integration' || credentials.password !== 'mqtt-fixture-password') {
            rejectedConnects++;
            client.end(packet(0x20, Buffer.from([0, 5])));
            return;
        }
        state.connecting = true;
        Bun.connect({ hostname: '127.0.0.1', port, socket: {
            open(upstream) { upstream.write(state.pending); state.upstream = upstream; },
            data(_, bytes) { client.write(bytes); },
            close() { client.end(); },
            error() { client.end(); },
        } }).catch(() => client.end());
    },
    close(client) { client.data.upstream?.end(); },
} });
function token(id: string) {
    const enc = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now() / 1000);
    const raw = `${enc({ alg: 'HS256', typ: 'JWT' })}.${enc({ iss: 'iot-engine', aud: 'iot-engine-web', sub: id, user_id: id, username: id, token_type: 'access', iat: now, exp: now + 3600 })}`;
    return `${raw}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(raw).digest('base64url')}`;
}
async function request(method: string, path: string, data?: unknown, status = 200, actor: string | null = admin) {
    const response = await fetch(apiBase + path, { method,
        headers: { 'Content-Type': 'application/json', ...(actor ? { Authorization: `Bearer ${token(actor)}` } : {}) },
        body: data === undefined ? undefined : JSON.stringify(data), signal: AbortSignal.timeout(15000) });
    const body = await response.json();
    assert.equal(response.status, status, `${path}: ${JSON.stringify(body)}`);
    return body.data;
}
async function until(test: () => boolean | Promise<boolean>, label: string) {
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
        if (await test()) return;
        await Bun.sleep(50);
    }
    throw new Error(label);
}
async function publish(event: string, eventId = crypto.randomUUID()) {
    await publishFixtureEvent(redis, 'iot:channel:open-access:event', [
        'event_id', eventId, 'message_id', eventId, 'event_type', event,
        'device_id', device, 'device_code', 'D001', 'occurred_at_ms', String(Date.now()),
        'data_json', JSON.stringify({ severity: 'critical', commandId: 'command-1', status: 'SUCCEEDED', elements: [], actualValues: [] }),
    ], 'webhook');
    return eventId;
}
try {
    subscriber = await Bun.connect({ hostname: '127.0.0.1', port, socket: {
        open(socket) {
            socket.write(packet(0x10, Buffer.concat([text('MQTT'), Buffer.from([4, 0xc2, 0, 0]), text(`test-${Date.now()}`), text('integration'), text('mqtt-fixture-password')])));
        },
        data(socket, data) {
            buffer = Buffer.concat([buffer, Buffer.from(data)]);
            for (let next = frame(buffer); next; next = frame(buffer)) {
                const { header, body } = next;
                buffer = buffer.subarray(next.size);
                if (header === 0x20) {
                    assert.equal(body[1], 0);
                    socket.write(packet(0x82, Buffer.concat([Buffer.from([0, 1]), text(`iot/${device}`), Buffer.from([1])])));
                } else if (header === 0x90) subscribed = true;
                else if ((header >> 4) === 3) {
                    const size = body.readUInt16BE(), topic = body.subarray(2, 2 + size).toString();
                    const qos = (header >> 1) & 3;
                    const payloadOffset = 2 + size + (qos ? 2 : 0);
                    messages.push({ topic, body: JSON.parse(body.subarray(payloadOffset).toString()) });
                    if (qos === 1) socket.write(packet(0x40, body.subarray(2 + size, 4 + size)));
                }
            }
        },
    } });
    await until(() => subscribed, 'MQTT subscriber did not connect');
    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by) VALUES(${model},${name},'Modbus','{"registers":[],"storagePolicy":"report"}'::jsonb,${admin})`;
    await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution,status) VALUES(${link},${name},'Modbus','{"transport":"tcp","mode":"TCP Server","ip":"0.0.0.0","port":55199,"targets":[]}'::jsonb,${admin},'collector','disabled')`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by) VALUES(${device},${name},${link},${model},'{"device_code":"D001","slave_id":1,"modbus_mode":"TCP"}'::jsonb,${admin})`;
    const modelConfig = { storagePolicy: 'report', readInterval: 10, byteOrder: 'BIG_ENDIAN', registers: [
        { id: readPoint, name: '温度', registerType: 'INPUT_REGISTER', dataType: 'UINT16', address: 0, quantity: 1, writable: false, unit: 'C' },
        { id: writePoint, name: '设定值', registerType: 'HOLDING_REGISTER', dataType: 'UINT16', address: 1, quantity: 1, writable: true, unit: 'C' },
    ] };
    await request('PUT', `/v1/protocol/configs/${model}`, { config: modelConfig });
    await until(async () => {
        const raw = await redis.send('HGET', [`iot:v2:device:${device}:latest`, writePoint]);
        return !!raw && JSON.parse(String(raw)).access === 'rw';
    }, 'model write capability was not projected');
    const latestKey = `iot:v2:device:${device}:latest`;
    for (const [id, value] of [[readPoint, 25.6], [writePoint, 30]] as const) {
        const metadata = JSON.parse(String(await redis.send('HGET', [latestKey, id])));
        await redis.send('HSET', [latestKey, id, JSON.stringify({ ...metadata, value, observedAt: Date.now() })]);
    }
    await request('GET', '/api/open-mqtt', undefined, 401, null);
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${user},${user},'test-unused')`;
    await request('POST', '/api/open-mqtt', {}, 403, user);
    key = (await request('POST', '/api/open-access-key', { name, scopes: ['device:realtime','device:command','alert:read'], deviceIds: [device] })).id;
    const config = { accessKeyId: key, name, brokerUrl: gated ? `mqtt://127.0.0.1:${gate.port}` : brokerUrl,
        status: 'enabled', timeoutSeconds: 2, username: 'integration', password: 'mqtt-fixture-password',
        eventTypes: ['device.data.reported','device.alert.triggered','device.command.updated'] };
    await request('POST', '/api/open-mqtt', { ...config, brokerUrl: 'http://example.com' }, 400);
    await request('POST', '/api/open-mqtt', config);
    webhook = (await request('POST', '/api/open-webhook', { accessKeyId: key, name, url: `http://127.0.0.1:${receiver.port}/events`, eventTypes: config.eventTypes })).id;
    const listed = await request('GET', '/api/open-mqtt');
    const item = listed.find((row: {name:string}) => row.name === name);
    assert(item && item.hasPassword);
    assert(!Object.hasOwn(item, 'topic'), 'MQTT configuration must not expose a custom Topic');
    assert(!JSON.stringify(item).includes('mqtt-fixture-password'));
    destination = item.id;
    await request('POST', '/api/open-mqtt', config, 409);
    const event = await publish('device.data.reported');
    await until(() => messages.some((row) => row.body.deliveryId === event), 'telemetry was not published');
    const telemetry = messages.find((row) => row.body.deliveryId === event)!;
    assert.equal(telemetry.topic, `iot/${device}`);
    assert.equal(telemetry.body.data.points[0].value, 25.6);
    assert.equal(telemetry.body.data.points[0].id, readPoint);
    assert.equal(telemetry.body.data.points[0].access, 'ro');
    assert.equal(telemetry.body.data.points[1].id, writePoint);
    assert.equal(telemetry.body.data.points[1].access, 'rw');
    await until(() => webhookBodies.some((body) => body.deliveryId === event), 'Webhook copy was not delivered');
    assert.deepEqual(telemetry.body, webhookBodies.find((body) => body.deliveryId === event), 'MQTT and Webhook JSON differ');
    assert.match(telemetry.body.time, /Z$/);
    await until(async () => (await db`SELECT 1 FROM open_access_log WHERE mqtt_id=${destination} AND status='success'`).length > 0, 'MQTT success was not persisted');
    const alert = await publish('device.alert.triggered');
    const command = await publish('device.command.updated');
    await until(() => messages.some((row) => row.body.deliveryId === alert) && messages.some((row) => row.body.deliveryId === command), 'alert or command status was not published');
    for (const eventId of [alert, command]) {
        assert.equal(messages.find((row) => row.body.deliveryId === eventId)!.topic, telemetry.topic, 'event type must not add a topic level');
    }
    assert.equal(messages.find((row) => row.body.deliveryId === command)!.body.data.command.status, 'SUCCEEDED');
    await request('PUT', `/api/open-mqtt/${destination}`, { ...config, password: 'wrong-password' });
    const retry = await publish('device.data.reported');
    await until(async () => (await db`SELECT 1 FROM open_access_log WHERE mqtt_id=${destination} AND status='failed' AND message LIKE '%CONNACK=%'`).length > 0, 'authentication failure was not recorded');
    assert(!messages.some((row) => row.body.deliveryId === retry));
    if (gated) assert(rejectedConnects > 0, 'wrong credentials did not reach the authenticating gate');
    await request('PUT', `/api/open-mqtt/${destination}`, config);
    await until(() => messages.some((row) => row.body.deliveryId === retry), 'pending delivery did not recover after credentials were fixed');
    if (Bun.env.OPEN_MQTT_VERIFY_HOSTNAME === '1') {
        const failures = (await db`SELECT count(*)::int AS n FROM open_access_log WHERE mqtt_id=${destination} AND status='failed'`)[0].n;
        const wrongHost = new URL(config.brokerUrl);
        wrongHost.hostname = '127.0.0.1';
        await request('PUT', `/api/open-mqtt/${destination}`, { ...config, brokerUrl: wrongHost.href });
        const rejected = await publish('device.data.reported');
        await until(async () => (await db`SELECT count(*)::int AS n FROM open_access_log WHERE mqtt_id=${destination} AND status='failed'`)[0].n > failures, 'TLS hostname mismatch was not rejected');
        assert(!messages.some((row) => row.body.deliveryId === rejected));
        await request('PUT', `/api/open-mqtt/${destination}`, config);
        await until(() => messages.some((row) => row.body.deliveryId === rejected), 'TLS delivery did not recover with the correct hostname');
    }
    const logs = await request('GET', `/api/open-access-log?mqttId=${destination}`);
    assert(logs.list.every((row: {action:string; mqttId:string}) => row.action === 'mqtt' && row.mqttId === destination));
    assert(!JSON.stringify(logs).includes('mqtt-fixture-password'));
    await request('PUT', `/api/open-mqtt/${destination}`, { ...config, password: undefined });
    assert.equal((await db`SELECT password FROM open_mqtt WHERE id=${destination}`)[0].password, config.password);
    await request('PUT', `/api/open-access-key/${key}`, { scopes: ['device:realtime'] });
    await request('PUT', `/api/open-mqtt/${destination}`, config, 403);
    const denied = await publish('device.alert.triggered');
    await until(async () => Number((await redis.send('XPENDING', ['iot:channel:open-access:event','iot-engine:open-webhook']) as unknown[])[0]) === 0, 'revoked event was not drained');
    assert(!messages.some((row) => row.body.deliveryId === denied));
    await request('DELETE', `/api/open-access-key/${key}`);
    assert(!(await request('GET', '/api/open-mqtt')).some((row: {id:string}) => row.id === destination));
    console.log('PASS open MQTT: real Broker, telemetry/alert/command schema, auth rejection, retry, secret retention, scope revocation, CRUD and logs');
} finally {
    subscriber?.end();
    receiver.stop(true);
    gate.stop(true);
    if (key) await db`DELETE FROM open_access_log WHERE access_key_id=${key}`;
    // Soft-delete like the API: late access audits and delivery results still
    // reference these rows, and a missing row would fail their persistence.
    if (webhook) await db`UPDATE open_webhook SET deleted_at=NOW() WHERE id=${webhook}`;
    if (destination) await db`UPDATE open_mqtt SET deleted_at=NOW() WHERE id=${destination}`;
    if (key) { await db`DELETE FROM open_access_key_device WHERE access_key_id=${key}`; await db`UPDATE open_access_key SET deleted_at=NOW() WHERE id=${key}`; }
    await db`UPDATE device SET deleted_at=NOW() WHERE id=${device}`;
    await db`UPDATE link SET status='disabled',deleted_at=NOW() WHERE id=${link}`;
    await db`UPDATE protocol_config SET deleted_at=NOW() WHERE id=${model}`;
    await db`DELETE FROM sys_user WHERE id=${user}`;
    await redis.send('DEL', [`iot:v2:device:${device}:latest`]);
    redis.close();
    await db.close();
}
