import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const platform = '00000000-0000-7000-8000-000000000001';
const node = crypto.randomUUID(), link = crypto.randomUUID(), device = crypto.randomUUID();
const deniedLink = crypto.randomUUID();
const other = crypto.randomUUID(), role = crypto.randomUUID();
const identity = `99${String(Date.now()).slice(-12).padStart(12, '0')}0`;
const tag = `edge_link_${crypto.randomUUID()}`;
const hiddenName = `${tag}_stored`;
let actor = admin;
let model = '';
const streams: Awaited<ReturnType<typeof openSnapshotSubscription>>[] = [];

function token(userId: string) {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({ iss: 'iot-engine', aud: 'iot-engine-web', sub: userId, user_id: userId, username: userId, token_type: 'access', iat: now, exp: now + 3600 })}`;
    return `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}

async function request(method: string, path: string, body?: unknown, code = 0) {
    const response = await fetch(apiBase + path, {
        method, headers: { Authorization: `Bearer ${token(actor)}`, 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15000),
    });
    const result = await response.json();
    assert.equal(result.code, code, `${method} ${path}: ${JSON.stringify(result)}`);
    assert.equal(response.ok, code === 0);
    return result.data;
}

async function subscribe(userId = admin, debugLinkId = link) {
    const stream = await openSnapshotSubscription(`/v1/link/events?keyword=${tag}&debugLinkId=${debugLinkId}`, token(userId));
    streams.push(stream);
    return stream;
}

async function snapshot(stream: Awaited<ReturnType<typeof subscribe>>, eventName: string, code = 0) {
    for (;;) {
        const event = await stream.next();
        if (event.event !== eventName) continue;
        const result = JSON.parse(event.data);
        assert.equal(result.code, code, event.data);
        return result.data;
    }
}

function decode(buffer: Buffer) {
    const fields = new Map<number, bigint | Buffer>();
    let position = 0;
    const read = () => {
        let value = 0n, shift = 0n;
        for (;;) {
            const byte = buffer[position++];
            assert(byte !== undefined, 'truncated edge configuration');
            value |= BigInt(byte & 127) << shift;
            if (!(byte & 128)) return value;
            shift += 7n;
        }
    };
    while (position < buffer.length) {
        const tag = Number(read());
        if ((tag & 7) === 0) fields.set(tag >> 3, read());
        else {
            assert.equal(tag & 7, 2);
            const length = Number(read());
            fields.set(tag >> 3, buffer.subarray(position, position + length));
            position += length;
        }
    }
    return fields;
}

async function queuedEndpointDebug() {
    // Redis 返回十六进制，避免把二进制 Protobuf 当 UTF-8 字符串解码。
    const wires = await redis.send('EVAL', [
        "local r={};for _,v in ipairs(redis.call('LRANGE',KEYS[1],0,-1)) do local t={};for i=1,#v do t[i]=string.format('%02x',string.byte(v,i)) end;r[#r+1]=table.concat(t) end;return r",
        '1', `iot:edge:config:${node}`,
    ]) as string[];
    for (const wire of wires) {
        const envelope = decode(Buffer.from(wire, 'hex'));
        if (!envelope.has(31)) continue;
        const item = decode(envelope.get(31) as Buffer);
        if (!item.has(10)) continue;
        const endpoint = decode(item.get(10) as Buffer);
        assert.equal((endpoint.get(1) as Buffer).toString('hex'), link.replaceAll('-', ''));
        return endpoint.get(11) === 1n;
    }
    throw Error('edge configuration did not contain the link endpoint');
}

async function revisionCount() {
    return Number((await db`SELECT count(*)::int AS n FROM edge_config_revision WHERE node_id=${node}`)[0].n);
}

try {
    await db`INSERT INTO edge_node(id,platform_id,imei,name,hostname,enrollment_status,capability)
        VALUES(${node},${platform},${identity},${tag},'fixture-host','approved','{"deviceConfig":true}'::jsonb)`;
    await request('POST', '/v1/protocol/configs', { protocol: 'Modbus', name: tag,
        config: { storagePolicy: 'report', readInterval: 10, byteOrder: 'BIG_ENDIAN', registers: [] } });
    model = (await db`SELECT id FROM protocol_config WHERE name=${tag}`)[0].id;
    await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,edge_node_id,created_by)
        VALUES(${link},${hiddenName},'Modbus','{"transport":"tcp","interface":"lo","mode":"TCP Client","ip":"127.0.0.1","port":502}'::jsonb,'enabled','edge',${node},${admin})`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
        VALUES(${device},${tag},${link},${model},'{"device_code":"1","remote_control":false}'::jsonb,${admin})`;

    const list = await request('GET', `/v1/link?keyword=${tag}`);
    assert.equal(list.total, 1);
    assert.equal(list.list[0].name, `${tag} · Modbus`);
    assert.equal(list.list[0].id, link);
    assert.equal((await request('GET', `/v1/link?keyword=${hiddenName}`)).total, 0);
    await request('PUT', `/v1/link/${link}`, { name: 'forbidden', protocol: 'Modbus', endpoint: { mode: 'TCP Client', targets: [] } }, 15001);
    await request('DELETE', `/v1/link/${link}`, undefined, 15002);

    await request('PUT', `/v1/link/${link}/debug`, { enabled: true });
    assert.equal(await queuedEndpointDebug(), true);
    const revision = await revisionCount();
    const configEvents = Number((await db`SELECT count(*)::int AS n FROM outbox_event WHERE event_type='config.changed' AND aggregate_id=${link}`)[0].n);
    await request('PUT', `/v1/link/${link}/debug`, { enabled: true });
    assert.equal(await revisionCount(), revision, 'same debug value created another edge revision');
    assert.equal(Number((await db`SELECT count(*)::int AS n FROM outbox_event WHERE event_type='config.changed' AND aggregate_id=${link}`)[0].n), configEvents);

    const subscription = await subscribe();
    assert.equal((await snapshot(subscription, 'links')).list[0].debug_enabled, true);
    assert.deepEqual(await snapshot(subscription, 'packets'), []);
    await redis.send('DEL', [`iot:debug:lease:link:${link}`]);
    await subscription.expectQuiet(16000);
    assert.equal(Number(await redis.send('EXISTS', [`iot:debug:lease:link:${link}`])), 0,
        'SSE comment heartbeat recreated an expired debug lease');
    const renameCount = Number((await db`SELECT count(*)::int AS n FROM outbox_event WHERE event_type='query.changed' AND aggregate_type='link' AND aggregate_id='link'`)[0].n);
    const renamed = snapshot(subscription, 'links');
    await request('PUT', `/v1/edge/${node}/name`, { name: `${tag}_renamed` });
    assert.equal((await renamed).list[0].name, `${tag}_renamed · Modbus`);
    assert.equal(Number((await db`SELECT count(*)::int AS n FROM outbox_event WHERE event_type='query.changed' AND aggregate_type='link' AND aggregate_id='link'`)[0].n), renameCount + 1);
    await request('PUT', `/v1/edge/${node}/name`, { name: `${tag}_renamed` });
    assert.equal(Number((await db`SELECT count(*)::int AS n FROM outbox_event WHERE event_type='query.changed' AND aggregate_type='link' AND aggregate_id='link'`)[0].n), renameCount + 1);
    await subscription.expectQuiet(750);
    await subscription.close();
    assert.equal((await db`SELECT name FROM link WHERE id=${link}`)[0].name, hiddenName, 'display rename changed persisted link identity');

    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${other},${other},'unused-test-password')`;
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:link:query","iot:link:edit"]'::jsonb)`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${other},${role})`;
    // 未曾建立授权订阅的独立资源，避免已关闭旧 socket 的异步清理影响 TTL 证据。
    await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,edge_node_id,created_by,debug_enabled)
        VALUES(${deniedLink},${tag + '_denied'},'Modbus','{"transport":"tcp","interface":"lo","mode":"TCP Client","ip":"127.0.0.1","port":1502}'::jsonb,'disabled','edge',${node},${admin},true)`;
    actor = other;
    await request('PUT', `/v1/link/${link}/debug`, { enabled: false }, 15007);
    await redis.send('SET', [`iot:debug:lease:link:${deniedLink}`, '1', 'PX', '60000']);
    const deniedTtl = Number(await redis.send('PTTL', [`iot:debug:lease:link:${deniedLink}`]));
    const denied = await subscribe(other, deniedLink);
    assert.equal((await snapshot(denied, 'links')).total, 1);
    await snapshot(denied, 'packets', 15007);
    await denied.expectQuiet(16000);
    const remainingDeniedTtl = Number(await redis.send('PTTL', [`iot:debug:lease:link:${deniedLink}`]));
    assert(remainingDeniedTtl > 0 && remainingDeniedTtl < deniedTtl - 10000,
        `unauthorized debug SSE renewed another owner's debug lease: before=${deniedTtl}, after=${remainingDeniedTtl}`);
    await denied.close();
    assert.equal((await db`SELECT debug_enabled FROM link WHERE id=${link}`)[0].debug_enabled, true);
    await db`UPDATE sys_role SET permissions='["iot:link:query"]'::jsonb WHERE id=${role}`;
    await request('PUT', `/v1/link/${link}/debug`, { enabled: false }, 11007);
    actor = admin;
    await db`UPDATE link SET created_by=${other} WHERE id=${link}`;
    await request('PUT', `/v1/link/${link}/debug`, { enabled: false });
    assert.equal(await queuedEndpointDebug(), false);
    assert.equal(await revisionCount(), revision + 1);
    await db`UPDATE edge_node SET capability='{}'::jsonb WHERE id=${node}`;
    await request('PUT', `/v1/link/${link}/debug`, { enabled: true }, 15002);
    await db`UPDATE link SET debug_enabled=true WHERE id=${link}`;
    await request('PUT', `/v1/link/${link}/debug`, { enabled: false });
    assert.equal((await db`SELECT debug_enabled FROM link WHERE id=${link}`)[0].debug_enabled, false);
    assert.equal(await revisionCount(), revision + 1, 'unsupported node received an inapplicable debug configuration');
    await db`UPDATE edge_node SET capability='{"deviceConfig":true}'::jsonb,enrollment_status='pending' WHERE id=${node}`;
    await request('PUT', `/v1/link/${link}/debug`, { enabled: true }, 15002);
    await db`UPDATE edge_node SET enrollment_status='approved',name='',hostname='fixture-host' WHERE id=${node}`;
    assert.equal((await request('GET', '/v1/link?keyword=fixture-host')).list.find((entry: { id: string }) => entry.id === link).name, 'fixture-host · Modbus');
    await db`UPDATE edge_node SET hostname='' WHERE id=${node}`;
    assert.equal((await request('GET', `/v1/link?keyword=${identity}`)).list[0].name, `${identity} · Modbus`);
    console.log('PASS edge link debug HTTP/SSE, owner authorization, snapshot wire, derived name/search and rename event idempotency');
} finally {
    for (const stream of streams) await stream.close();
    await redis.send('DEL', [`iot:debug:lease:link:${link}`, `iot:debug:watch:link:${link}`,
        `iot:debug:lease:link:${deniedLink}`, `iot:debug:watch:link:${deniedLink}`,
        `iot:edge:config:${node}`, `iot:edge:config-revision:${node}`, `iot:edge:metadata:${node}`]);
    await db`DELETE FROM device WHERE id=${device}`;
    await db`DELETE FROM outbox_event WHERE aggregate_id IN (${link},${device},${model})`;
    await db`DELETE FROM edge_config_revision WHERE node_id=${node}`;
    await db`DELETE FROM link WHERE id IN (${link},${deniedLink})`;
    if (model) await db`DELETE FROM protocol_config WHERE id=${model}`;
    await db`DELETE FROM edge_node WHERE id=${node}`;
    await db`DELETE FROM sys_user_role WHERE user_id=${other}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${other}`;
    redis.close();
    await db.close();
}
