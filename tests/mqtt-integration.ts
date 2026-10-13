import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin, username: 'admin', token_type: 'access', iat: now, exp: now + 3600 })}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
const point = crypto.randomUUID(),
    enabledPoint = crypto.randomUUID(),
    target = crypto.randomUUID(),
    derived = crypto.randomUUID();
const name = `mqtt-${crypto.randomUUID()}`;
let link = '',
    model = '',
    published = false,
    acknowledgements = 0,
    commands = 0;
const commandMessageId = crypto.randomUUID();
let resultSubscriptions = 0;
let pendingResult: {socket: Bun.Socket<{buffer: Buffer; subscribed: boolean}>; payload: Record<string, unknown>} | undefined;
const devices: string[] = [];
const clients = new Set<Bun.Socket<{ buffer: Buffer; subscribed: boolean }>>();
// 设备上报时间早于接收时间，派生值不得因此被误判为来自未来。
const time = Date.now() - 5000;

function packet(header: number, body: Buffer) {
    const length: number[] = [];
    let remaining = body.length;
    do {
        let byte = remaining % 128;
        remaining = Math.floor(remaining / 128);
        if (remaining) byte |= 128;
        length.push(byte);
    } while (remaining);
    return Buffer.concat([Buffer.from([header, ...length]), body]);
}
function text(input: string) {
    const value = Buffer.from(input);
    const length = Buffer.alloc(2);
    length.writeUInt16BE(value.length);
    return Buffer.concat([length, value]);
}
function telemetry(socket: Bun.Socket<{ buffer: Buffer; subscribed: boolean }>) {
    if (!published || !socket.data.subscribed) return;
    const payload = {
        devices: [
            { code: 'D001', metrics: { temperature: 256 }, time },
            { code: 'D002', metrics: { temperature: 281 }, time },
        ],
    };
    const frame = packet(
        0x32,
        Buffer.concat([
            text('factory/telemetry'),
            Buffer.from([0, 7]),
            Buffer.from(JSON.stringify(payload)),
        ])
    );
    socket.write(frame.subarray(0, 3));
    socket.write(frame.subarray(3));
    socket.write(packet(0x32,Buffer.concat([text('factory/state'),Buffer.from([0,8]),Buffer.from(JSON.stringify({device:'D001',state:{enabled:true},time}))])));
}
const broker = Bun.listen<{ buffer: Buffer; subscribed: boolean }>({
    hostname: '127.0.0.1',
    port: 0,
    socket: {
        open(socket) {
            socket.data = { buffer: Buffer.alloc(0), subscribed: false };
            clients.add(socket);
        },
        data(socket, data) {
            let buffer = Buffer.concat([socket.data.buffer, Buffer.from(data)]);
            for (;;) {
                if (buffer.length < 2) break;
                let length = 0,
                    position = 1,
                    multiplier = 1,
                    complete = false;
                for (let count = 0; count < 4 && position < buffer.length; ++count) {
                    const byte = buffer[position++];
                    length += (byte & 127) * multiplier;
                    multiplier *= 128;
                    if (!(byte & 128)) {
                        complete = true;
                        break;
                    }
                }
                if (!complete || buffer.length < position + length) break;
                const header = buffer[0],
                    body = buffer.subarray(position, position + length);
                buffer = buffer.subarray(position + length);
                if (header === 0x10) {
                    assert.equal(body.subarray(2, 6).toString(), 'MQTT');
                    assert.equal(body[6], 4);
                    socket.write(Buffer.from([0x20, 2, 0, 0]));
                } else if (header === 0x82) {
                    const granted: number[] = [];
                    for (let offset = 2; offset < body.length;) {
                        const size = body.readUInt16BE(offset);
                        const filter = body.subarray(offset + 2, offset + 2 + size).toString();
                        assert(['factory/telemetry', 'factory/state', 'factory/D001/result', 'factory/D002/result'].includes(filter));
                        if (filter.endsWith('/result')) ++resultSubscriptions;
                        granted.push(1);
                        offset += 3 + size;
                    }
                    socket.data.subscribed = true;
                    socket.write(packet(0x90, Buffer.from([body[0], body[1], ...granted])));
                    telemetry(socket);
                } else if (header === 0x40) {
                    assert([7,8].includes(body.readUInt16BE()));
                    ++acknowledgements;
                } else if (header === 0xc0) socket.write(Buffer.from([0xd0, 0]));
                else if (header >> 4 === 3) {
                    const size = body.readUInt16BE(),
                        topic = body.subarray(2, 2 + size).toString();
                    assert.equal(topic, 'factory/commands');
                    const payload = JSON.parse(body.subarray(4 + size).toString());
                    if (payload.requestId !== undefined) {
                        assert.equal(header & 1, 0, 'control must not be retained');
                        assert(Number.isSafeInteger(payload.requestId) && payload.requestId > 0);
                        assert.equal(payload.address, 'A001');
                        pendingResult = {socket, payload};
                    } else assert.deepEqual(payload, {
                        cmd: 'set',
                        device: 'D001',
                        control: { targetTemperature: 15, enabled: false },
                    });
                    ++commands;
                    socket.write(Buffer.from([0x40, 2, body[2 + size], body[3 + size]]));
                } else throw Error(`unexpected MQTT packet ${header}`);
            }
            socket.data.buffer = Buffer.from(buffer);
        },
        close(socket) {
            clients.delete(socket);
        },
        error(_socket, error) {
            throw error;
        },
    },
});

async function http(method: string, path: string, data?: unknown, auth = true) {
    const response = await fetch(apiBase + path, {
        method,
        headers: {
            ...(auth ? { Authorization: `Bearer ${token}` } : {}),
            'Content-Type': 'application/json',
        },
        body: data === undefined ? undefined : JSON.stringify(data),
        signal: AbortSignal.timeout(15000),
    });
    return { status: response.status, body: await response.json() };
}
async function ok(method: string, path: string, data?: unknown) {
    const response = await http(method, path, data);
    assert.equal(response.status, 200, JSON.stringify(response.body));
    assert.equal(response.body.code, 0, JSON.stringify(response.body));
    return response.body.data;
}
async function until(check: () => Promise<boolean>, reason: string) {
    const deadline = Date.now() + 25000;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(50);
    }
    throw Error(reason);
}

try {
    const config = {
        storagePolicy: 'report',
        topic: 'factory/telemetry',
        payloadFormat: 'json',
        recordsPath: '/devices',
        identitySource: 'payload',
        deviceCodeField: '/code',
        timeField: '/time',
        timeFormat: 'unix_ms',
        commandTopic: 'factory/commands',
        commandTemplate: '{"cmd":"set","device":"$deviceCode","control":{"targetTemperature":"$point:温度","enabled":"$point:开关"}}',
        qos: 1,
        derivedPoints: [{id:derived,name:'温度换算',kind:'expression',valueType:'number',expression:'x * 0.1 - 1',inputs:[{alias:'x',pointId:point}],unit:'℃',unitMode:'fixed',maxAgeSeconds:300,visible:true}],
        reports: [
            {id:crypto.randomUUID(),name:'测量',topic:'factory/telemetry',qos:1,recordsPath:'/devices',identitySource:'payload',deviceCodeField:'/code',timeField:'/time',timeFormat:'unix_ms',reportTemplate:'{"code":"$deviceCode","time":"$time","metrics":{"temperature":"$point:温度"}}',pointFields:[{pointId:point,field:'/metrics/temperature'}]},
            {id:crypto.randomUUID(),name:'状态',topic:'factory/state',qos:1,identitySource:'payload',deviceCodeField:'/device',timeField:'/time',timeFormat:'unix_ms',reportTemplate:'{"device":"$deviceCode","time":"$time","state":{"enabled":"$point:开关"}}',pointFields:[{pointId:enabledPoint,field:'/state/enabled'}]},
        ],
        points: [
            {
                id: point,
                name: '温度',
                field: '/metrics/temperature',
                dataType: 'DOUBLE',
                unit: '℃',
                writable: true,
            },
            {
                id: enabledPoint,
                name: '开关',
                field: '/metrics/enabled',
                dataType: 'BOOL',
                writable: true,
            },
        ],
    };
    assert.equal(
        (
            await http(
                'POST',
                '/v1/protocol/configs/preview-mqtt',
                { config, topic: 'factory/telemetry', payload: '{}' },
                false
            )
        ).status,
        401
    );
    const preview = await ok('POST', '/v1/protocol/configs/preview-mqtt', {
        config,
        topic: 'factory/telemetry',
        payload: JSON.stringify({
            devices: [{ code: 'D001', metrics: { temperature: 256 }, time }],
        }),
    });
    assert.equal(preview.records[0].deviceCode, 'D001');
    assert.equal(preview.records[0].points[0].value, '256');
    await ok('POST', '/v1/protocol/configs', { protocol: 'MQTT', name, enabled: true, config });
    model = (await db`SELECT id FROM protocol_config WHERE name=${name}`)[0].id;
    await ok('POST', '/v1/link', {
        name,
        protocol: 'MQTT',
        status: 'enabled',
        endpoint: {
            mode: 'TCP Client',
            ip: '',
            port: 0,
            targets: [
                {
                    id: target,
                    name: 'Broker',
                    ip: 'localhost',
                    port: broker.port,
                    status: 'enabled',
                    mqtt: { clientId: name, keepAliveSeconds: 30 },
                },
            ],
        },
    });
    link = (await db`SELECT id FROM link WHERE name=${name}`)[0].id;
    for (const code of ['D001', 'D002']) {
        await ok('POST', '/v1/device', {
            name: `${name}-${code}`,
            device_code: code,
            link_id: link,
            target_id: target,
            protocol_config_id: model,
            online_timeout: 60,
            remote_control: true,
            parameters: { address: code === 'D001' ? 'A001' : 'A002', channel: 2, enabled: true, metadata: { type: 'water' } },
            timezone: '+08:00',
            status: 'enabled',
        });
        devices.push((await db`SELECT id FROM device WHERE name=${`${name}-${code}`}`)[0].id);
    }
    published = true;
    for (const socket of clients) telemetry(socket);
    await until(
        async () =>
            (
                await db`SELECT DISTINCT device_id FROM device_data WHERE device_id IN (${devices[0]},${devices[1]}) AND protocol='MQTT'`
            ).length === 2,
        'MQTT batch did not reach both devices'
    );
    for (const [index, id] of devices.entries()) {
        const [row] =
            await db`SELECT data->'values'->${point}->>'value' AS value, data->'values'->${derived}->>'value' AS derived_value, EXTRACT(EPOCH FROM occurred_at)*1000 AS time FROM device_data WHERE device_id=${id} AND (data->'values') ? ${point} ORDER BY occurred_at DESC LIMIT 1`;
        assert(Math.abs(Number(row.value) - (index === 0 ? 256 : 281)) < 0.00001);
        assert(Math.abs(Number(row.derived_value) - (index === 0 ? 24.6 : 27.1)) < 0.00001);
        assert.equal(Number(row.time), time);
    }
    await until(async()=> (await db`SELECT id FROM device_data WHERE device_id=${devices[0]} AND data->'values'->${enabledPoint}->>'value'='true' LIMIT 1`).length===1,'second MQTT report did not use its own mapping');
    const statePreview=await ok('POST','/v1/protocol/configs/preview-mqtt',{config,topic:'factory/state',payload:JSON.stringify({device:'D001',state:{enabled:true},time}),deviceCode:'D001'});
    assert.equal(statePreview.records[0].points.length,1);
    assert.equal(statePreview.records[0].points[0].id,enabledPoint);
    await ok('PUT',`/v1/protocol/configs/${model}`,{config:{reports:[config.reports[0]]}});
    assert.equal((await ok('GET',`/v1/protocol/configs/${model}`)).config.reports.length,1,'deleted report survived save');
    await until(async () => acknowledgements > 0, 'Broker did not receive PUBACK');
    const command = await ok('POST', `/v1/device/${devices[0]}/commands`, {
        idempotency_key: crypto.randomUUID(),
        elements: [{ elementId: point, value: '15' }, { elementId: enabledPoint, value: '0' }],
    });
    assert.equal(command.command_ids.length, 1, 'one MQTT message must have one acknowledgement');
    await until(async () => commands === 1, 'MQTT command was not sent');
    await until(
        async () =>
            (await db`SELECT status FROM command_operation WHERE id=${command.command_ids[0]}`)[0]
                ?.status === 'SUCCEEDED',
        'MQTT command did not complete after PUBACK'
    );
    const responseConfig = {
        ...config, commandTopic: '', commandTemplate: '',
        commands: [{id: commandMessageId, name: '设置并确认', topic: 'factory/commands',
            template: '{"requestId":"$commandId","address":"$param:address","deviceUuid":"$deviceId","deviceCode":"$deviceCode","channel":"$param:channel","enabled":"$param:enabled","metadata":"$param:metadata","cmd":"set","value":"$point:温度"}',
            requiredPointIds: [point], response: {topic: 'factory/{deviceCode}/result', matchFields: ['/requestId','/address','/cmd'], codeField: '/body/code', successCode: 0, timeoutMs: 1000}}],
        points: [{...config.points[0], writable: true}, {...config.points[1], writable: false}],
    };
    await ok('PUT', `/v1/protocol/configs/${model}`, {config: responseConfig});
    await until(async () => resultSubscriptions >= 2, 'result subscriptions not projected');
    const submit = () => ok('POST', `/v1/device/${devices[0]}/commands`, {idempotency_key: crypto.randomUUID(), mqttMessageId: commandMessageId, elements: [{elementId: point, value: '15'}]});
    const status = async (id: string) => (await db`SELECT status,reason FROM command_operation WHERE id=${id}`)[0];
    const reply = (body: unknown, topic = 'factory/D001/result') => {
        assert(pendingResult);
        pendingResult.socket.write(packet(0x32, Buffer.concat([text(topic), Buffer.from([0, 7]), Buffer.from(JSON.stringify(body))])));
    };
    pendingResult = undefined;
    const confirmed = await submit();
    await until(async () => !!pendingResult, 'generic control not published');
    assert.notEqual((await status(confirmed.command_ids[0])).status, 'SUCCEEDED', 'PUBACK reported execution success');
    const sent = pendingResult!.payload;
    assert.equal(sent.deviceUuid, devices[0]);
    assert.equal(sent.deviceCode, 'D001');
    assert.equal(sent.address, 'A001');
    assert.equal(sent.channel, 2);
    assert.equal(sent.enabled, true);
    assert.deepEqual(sent.metadata, {type: 'water'});
    reply({...sent, requestId: 1, body: {code: 0}});
    reply({...sent, body: {code: 0}}, 'factory/D002/result');
    await Bun.sleep(100);
    assert.notEqual((await status(confirmed.command_ids[0])).status, 'SUCCEEDED', 'unmatched reply completed command');
    reply({...sent, body: {code: 0, response_hex: '0103020000B844'}});
    await until(async () => (await status(confirmed.command_ids[0])).status === 'SUCCEEDED', 'device success was not persisted');
    assert((await status(confirmed.command_ids[0])).reason.includes('0103020000B844'), 'result detail lost');
    pendingResult = undefined;
    const rejected = await submit();
    await until(async () => !!pendingResult, 'second control not published');
    assert.notEqual(pendingResult!.payload.requestId, sent.requestId, 'command ID reused');
    reply({...pendingResult!.payload, body: {code: 1, msg: 'timeout', response_hex: null}});
    await until(async () => (await status(rejected.command_ids[0])).status === 'FAILED', 'device failure was not persisted');
    pendingResult = undefined;
    const expired = await submit();
    await until(async () => !!pendingResult, 'timeout control not published');
    const beforeTimeout = commands;
    await until(async () => (await status(expired.command_ids[0])).status === 'UNKNOWN', 'missing result did not become unconfirmed');
    await Bun.sleep(1100);
    assert.equal(commands, beforeTimeout, 'control was automatically replayed');
    // Model a delayed collector result whose separate wake notification was lost.
    await redis.send('XADD', ['iot:v3:command-result', '*',
        'command_id', expired.command_ids[0], 'device_id', devices[0], 'device_code', 'D001',
        'success', '1', 'result_state', 'SUCCEEDED', 'message_id', crypto.randomUUID(),
        'reason', 'fixture_delayed_result']);
    await until(async () => (await status(expired.command_ids[0])).status === 'SUCCEEDED',
        'result with a missing wake notification was not consumed');
    console.log(
        `PASS MQTT integration: two devices, telemetry, legacy PUBACK, generic parameters, matched device success/failure, timeout without replay, missed result wake recovery`
    );
} finally {
    for (const device of devices) await http('DELETE', `/v1/device/${device}`);
    if (link) await http('DELETE', `/v1/link/${link}`);
    if (model) await http('DELETE', `/v1/protocol/configs/${model}`);
    for (const socket of clients) socket.end();
    broker.stop(true);
    redis.close();
    await db.close();
}
