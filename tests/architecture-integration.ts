import { databaseUrl, redisUrl, apiBase, publishFixtureEvent } from './architecture-fixture';
// Run against the disposable architecture fixture, never a deployed database.
import { createHmac } from 'node:crypto';
import assert from 'node:assert/strict';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const base = apiBase;
const admin = '00000000-0000-7000-8000-000000000002';
const id = () => crypto.randomUUID();
const link = id(),
    secondLink = id(),
    device = id(),
    secondDevice = id(),
    protocol = id(),
    point = id();
const instance = id();
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
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
async function api(path: string, body?: unknown) {
    const response = await fetch(base + path, {
        method: body ? 'POST' : 'GET',
        headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
            Accept: 'text/event-stream',
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(10000),
    });
    if (response.headers.get('content-type')?.includes('text/event-stream')) {
        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        let pending = '';
        try {
            for (;;) {
                const chunk = await reader.read();
                if (chunk.done) throw new Error('SSE ended before first snapshot');
                pending += decoder.decode(chunk.value, { stream: true });
                if (!pending.includes('\n\n') && !pending.includes('\r\n\r\n')) continue;
                const data = pending.match(/^data: ?(.+)$/m);
                if (data) return { status: response.status, body: JSON.parse(data[1]) };
            }
        } finally {
            await reader.cancel();
        }
    }
    return { status: response.status, body: await response.json() };
}
async function until(check: () => Promise<boolean>, message: string) {
    for (let count = 0; count < 100; count++) {
        if (await check()) return;
        await Bun.sleep(100);
    }
    throw new Error(message);
}

try {
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
    for (const item of [link, secondLink])
        await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution)
            VALUES(${item},${item},'Modbus','{"transport":"tcp","mode":"TCP Server","ip":"0.0.0.0","port":55199}'::jsonb,${admin},'collector')`;
    for (const [item, owner] of [
        [device, link],
        [secondDevice, secondLink],
    ])
        await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
            VALUES(${item},${item},${owner},${protocol},'{"device_code":"0000000001","modbus_mode":"TCP","remote_control":true}'::jsonb,${admin})`;
    let duplicateBlocked = false;
    try {
        const duplicate = id();
        await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
            VALUES(${duplicate},${duplicate},${link},${protocol},'{"device_code":"0000000001"}'::jsonb,${admin})`;
    } catch (error) {
        duplicateBlocked = (error as { errno: string }).errno === '23505';
    }
    assert(duplicateBlocked, 'same-link station/address duplicate must be rejected');
    console.log('PASS same address across links; duplicate on same link rejected');

    // Preserve the synthetic route across recovery projection, just as a fully
    // initialized collector connection does: the connection lease plus the link
    // owner that the command route lookup validates.
    await redis.send('HSET', [`iot:v2:device:${device}:latest`, '_device_id', device,
        '_element_ids', JSON.stringify({ [point]: true })]);
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
        'isolated-test-connection',
        'session_epoch',
        '1',
    ]);
    await redis.send('PEXPIRE', [`iot:v3:device:connection:${device}`, '120000']);
    const request = { idempotency_key: id(), elements: [{ elementId: point, value: '1' }] };
    const responses = await Promise.all(
        Array.from({ length: 8 }, () => api(`/v1/device/${device}/commands`, request))
    );
    assert(
        responses.every((response) => response.status === 200),
        JSON.stringify(responses)
    );
    const commandId = responses[0].body.data.command_ids[0];
    assert(responses.every((response) => response.body.data.command_ids[0] === commandId));
    const count =
        await db`SELECT count(*)::int AS count FROM command_operation WHERE id=${commandId}`;
    assert.equal(count[0].count, 1);
    const conflict = await api(`/v1/device/${device}/commands`, {
        ...request,
        elements: [{ elementId: point, value: '0' }],
    });
    assert.equal(conflict.status, 409, JSON.stringify(conflict));
    const missing = await api(`/v1/device/${device}/commands`, { elements: request.elements });
    assert.equal(missing.status, 400);
    console.log('PASS concurrent HTTP idempotency; payload conflict; missing key');

    const commandStatus = async () =>
        (await db`SELECT status::text AS status FROM command_operation WHERE id=${commandId}`)[0]?.status;
    await until(
        async () => (await commandStatus()) === 'AWAITING_RESULT',
        'durable dispatcher did not publish'
    );
    const queue = `iot:channel:command:worker:${instance}:0:high`;
    assert.equal(
        Number(await redis.send('XLEN', [queue])),
        1,
        'request replay must not enqueue twice'
    );
    // Arm a future deadline once, then perform only reads. Expiry must wake the
    // result worker without another database mutation or result notification.
    await db`UPDATE command_attempt SET deadline=clock_timestamp()+INTERVAL '2 seconds' WHERE operation_id=${commandId}`;
    await Bun.sleep(500);
    assert.equal(await commandStatus(), 'AWAITING_RESULT');
    await until(
        async () => (await commandStatus()) === 'UNKNOWN',
        'natural deadline expiry without a new notification must become UNKNOWN'
    );
    const result = [
        'command_id',
        commandId,
        'device_id',
        device,
        'device_code',
        '0000000001',
        'success',
        '1',
        'result_state',
        'SUCCEEDED',
        'actual_value_count',
        '0',
        'message_id',
        id(),
    ];
    await publishFixtureEvent(redis, 'iot:v3:command-result', result, 'command-result');
    await until(
        async () => (await commandStatus()) === 'SUCCEEDED',
        'late confirmed result must resolve UNKNOWN'
    );
    await publishFixtureEvent(redis, 'iot:v3:command-result', result, 'command-result');
    await Bun.sleep(1200);
    const events = await db`SELECT count(*)::int AS count FROM outbox_event
        WHERE event_type='device.command.updated' AND payload->'data'->>'commandId'=${commandId}
        AND payload->'data'->>'status'='SUCCEEDED'`;
    assert.equal(events[0].count, 1, 'duplicate result emitted duplicate completed event');
    assert.equal(await commandStatus(), 'SUCCEEDED');
    console.log(
        'PASS durable dispatch, timeout ambiguity, late confirmation, duplicate result, DB status authority'
    );

    const source = await Bun.file('service/features/command/command.entity.h').text();
    const script = source.match(/kPendingBatchDispatchScript = R"lua\(([\s\S]*?)\)lua"/)![1];
    const capacityQueue = `architecture-capacity:${id()}`;
    assert.equal(
        await redis.send('EVAL', [script, '1', capacityQueue, 'list', '1', '1', '1', 'first']),
        1
    );
    assert.equal(
        await redis.send('EVAL', [script, '1', capacityQueue, 'list', '1', '1', '1', 'second']),
        0
    );
    assert.deepEqual(await redis.send('LRANGE', [capacityQueue, '0', '-1']), ['first']);
    await redis.send('DEL', [capacityQueue]);
    console.log('PASS actual Redis Lua capacity backpressure preserves accepted work');
    const ownership = await Bun.file('service/features/collector/collector.service.h').text();
    const retainTarget = ownership.indexOf('retainTarget(const Redis&');
    assert(retainTarget >= 0, 'collector target lease operation is missing');
    const claim = ownership.slice(retainTarget).match(/R"lua\(([\s\S]*?)\)lua"/)![1];
    const lease = `architecture-lease:${id()}`;
    assert.equal(await redis.send('EVAL', [claim, '1', lease, 'instance-a']), 1);
    assert.equal(await redis.send('EVAL', [claim, '1', lease, 'instance-b']), 0);
    assert.equal(await redis.send('EVAL', [claim, '1', lease, 'instance-a']), 1);
    await redis.send('PEXPIRE', [lease, '1']);
    await Bun.sleep(10);
    assert.equal(await redis.send('EVAL', [claim, '1', lease, 'instance-b']), 1);
    assert.equal(await redis.send('EVAL', [claim, '1', lease, 'instance-a']), 0);
    await redis.send('DEL', [lease]);
    console.log('PASS actual Redis target lease exclusion, renewal and expired takeover');
} finally {
    await db.close();
    redis.close();
}
