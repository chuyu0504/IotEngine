import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const streams: Awaited<ReturnType<typeof openSnapshotSubscription>>[] = [];
const admin = '00000000-0000-7000-8000-000000000002';
const platform = process.env.TEST_EDGE_PLATFORM_ID ?? '00000000-0000-7000-8000-000000000001';
const node = crypto.randomUUID(), pending = crypto.randomUUID();
const user = crypto.randomUUID(), role = crypto.randomUUID();
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
    const response = await fetch(apiBase + path, {method, headers: {'Content-Type':'application/json', ...(actor ? {Authorization:`Bearer ${token(actor)}`} : {})}, body:data === undefined ? undefined : JSON.stringify(data), signal:AbortSignal.timeout(15000)});
    const reply = await response.json() as {code:number;message:string;data:any};
    assert.equal(reply.code, code, `${method} ${path}: ${JSON.stringify(reply)}`);
    assert.equal(response.ok, code===0);
    return reply.data;
}
async function subscribe(path: string, eventName: string) {
    const stream = await openSnapshotSubscription(path, token(admin)); streams.push(stream);
    async function next() { for (;;) { const event=await stream.next(); if(event.event!==eventName) { assert.notEqual(event.event,'error',event.data); continue; } return JSON.parse(event.data); } }
    return {next, close:()=>stream.close()};
}
function imei() {
    const base = '99' + String(Math.floor(Math.random() * 1e12)).padStart(12, '0');
    let sum = 0;
    for (let i = 0; i < 14; i++) { let n = Number(base[i]) * (i % 2 ? 2 : 1); if (n > 9) n -= 9; sum += n; }
    return base + String((10 - sum % 10) % 10);
}
const identity = imei(), pendingIdentity = imei();
function integer(input: bigint | number) {
    let n = BigInt(input); const result: number[] = [];
    do { let byte = Number(n & 127n); n >>= 7n; if (n) byte |= 128; result.push(byte); } while (n);
    return Buffer.from(result);
}
function field(tag: number, value: bigint | number | string | Buffer) {
    if (typeof value === 'number' || typeof value === 'bigint') return Buffer.concat([integer(tag * 8), integer(value)]);
    const bytes = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag * 8 + 2), integer(bytes.length), bytes]);
}
function decode(data: Buffer) {
    const result = new Map<number, bigint | Buffer>(); let position = 0;
    const read = () => { let n = 0n, shift = 0n; for (;;) { const b = data[position++]; assert(b !== undefined); n |= BigInt(b & 127) << shift; if (!(b & 128)) return n; shift += 7n; } };
    while (position < data.length) {
        const key = Number(read()), wire = key & 7;
        if (!wire) result.set(key >> 3, read());
        else if (wire === 2) { const n = Number(read()); result.set(key >> 3, data.subarray(position, position + n)); position += n; }
        else if (wire === 1 || wire === 5) { const n = wire === 1 ? 8 : 4; position += n; }
        else throw Error('Invalid device frame');
    }
    return result;
}
async function until(check: () => Promise<boolean>, reason: string) {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) { if (await check()) return; await Bun.sleep(25); }
    throw Error(reason);
}
try {
    await request('GET','/v1/edge/groups',undefined,11004,'');
    await db`INSERT INTO edge_node(id,platform_id,imei,name,enrollment_status,capability) VALUES(${node},${platform},${identity},${tag},'approved','{"logs":true}'::jsonb),(${pending},${platform},${pendingIdentity},${tag + '_pending'},'pending','{}'::jsonb)`;
    await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);

    assert(Array.isArray(await request('GET', '/v1/edge/groups')));
    for (const suffix of ['a', 'b', 'c']) {
        const name = `${tag}_${suffix}`;
        await request('POST', '/v1/edge/groups', { name, parentId: groups.at(-1), status: 'enabled', sortOrder: groups.length });
        const group = (await db`SELECT id,created_by FROM edge_node_group WHERE name=${name}`)[0];
        groups.push(group.id);
        assert.equal(group.created_by, admin);
        assert((await request('GET', '/v1/edge/groups')).some((row: { id: string }) => row.id === group.id));
    }
    await request('PUT', `/v1/edge/groups/${groups[0]}`, { name: `${tag}_a`, parentId: groups[2], status: 'enabled' }, 17003);
    await request('PUT', `/v1/edge/groups/${groups[1]}`, { name: `${tag}_b`, parentId: groups[0], status: 'enabled' });
    await request('DELETE', `/v1/edge/groups/${groups[0]}`, undefined, 17004);
    await request('POST', '/v1/edge/groups', { name: `${tag}_a` }, 17002);
    console.log('PASS Edge group HTTP reads after writes, actor, duplicate, cycle and child constraints');

    const detailSubscription = await subscribe(`/v1/edge/events?nodeId=${node}`, 'detail');
    assert.equal((await detailSubscription.next()).data.id,node);
    const renamed = detailSubscription.next();
    await request('PUT', `/v1/edge/${node}/name`, { name: `${tag}_renamed` });
    assert.equal((await renamed).data.name, `${tag}_renamed`);
    await detailSubscription.close();
    await request('PUT', `/v1/edge/${node}/group`, { groupId: groups[2] });
    const filtered = await request('GET',`/v1/edge?page=1&pageSize=10&groupId=${groups[2]}&keyword=${tag}&status=approved`);
    assert.equal(filtered.total, 1);
    assert.equal(filtered.list[0].id, node);
    assert.equal(filtered.list[0].groupId, groups[2]);
    await request('DELETE', `/v1/edge/groups/${groups[2]}`, undefined, 17004);
    await request('PUT', `/v1/edge/${node}/group`, { groupId: '' });
    assert((await request('GET',`/v1/edge?groupId=ungrouped&keyword=${tag}`)).list.some((row: { id: string }) => row.id === node));
    await request('DELETE', `/v1/edge/${node}`, undefined, 17021);
    await request('DELETE', `/v1/edge/${pending}`, undefined);
    assert.equal((await db`SELECT id FROM edge_node WHERE id=${pending}`).length, 0);
    console.log('PASS Edge rename push, group filtering/assignment and enrollment guards');
    const inventoryNodes = Array.from({ length: 201 }, () => ({ id: crypto.randomUUID(), imei: imei() }));
    await db`INSERT INTO edge_node(id,platform_id,imei,name,enrollment_status,capability)
        SELECT x.id,${platform}::uuid,x.imei,${tag + '_inventory'},'approved','{}'::jsonb
        FROM jsonb_to_recordset(${JSON.stringify(inventoryNodes)}::text::jsonb) AS x(id uuid,imei text)`;
    const inventory = await openSnapshotSubscription('/v1/edge/events',token(admin)); streams.push(inventory);
    const inventoryEvent = await inventory.next(); assert.equal(inventoryEvent.event, 'nodes');
    const inventoryReply = JSON.parse(inventoryEvent.data); assert.equal(inventoryReply.code, 0);
    const inventoryIds = new Set(inventoryReply.data.map((item: {id:string}) => item.id));
    for (const item of inventoryNodes) assert(inventoryIds.has(item.id), 'inventory must include nodes beyond the first HTTP page');
    const commentsBefore = inventory.commentCount;
    await redis.send('XADD', ['iot:live:changes', '*', 'topic', 'edge']);
    await inventory.expectQuiet(300);
    assert.equal(inventory.commentCount, commentsBefore, 'unchanged notification must not send a heartbeat');
    await inventory.close();
    await db`DELETE FROM edge_node WHERE name=${tag + '_inventory'}`;
    console.log('PASS 201 additional nodes share one complete inventory; duplicate notification stays quiet');


    for (const [method,path,data] of [
        ['GET','/v1/edge?page=0',undefined], ['GET','/v1/edge?pageSize=101',undefined],
        ['GET','/v1/edge/invalid',undefined], ['PUT',`/v1/edge/${node}/name`,{name:42}],
        ['PUT',`/v1/edge/groups/${groups[0]}`,{name:`${tag}_a`,sortOrder:-1}],
        ['POST',`/v1/edge/${node}/network`,{interfaces:[]}], ['GET',`/v1/edge/events?nodeId=${node}&logs=true&limit=49`,undefined],
        ['PUT',`/v1/edge/${node}/logs/level`,{level:'trace'}],
    ] as const) await request(method,path,data,10001);
    await request('PUT',`/v1/edge/groups/${groups[0]}`,null,10002);
    await request('GET','/v1/edge?page=9223372036854775000&pageSize=100',undefined,17003);
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${user},${user},'unused-test-password')`;
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:edge:query"]'::jsonb)`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${user},${role})`;
    await request('GET','/v1/edge',undefined,0,user);
    await request('PUT', `/v1/edge/${node}/name`, { name: 'forbidden' }, 11007, user);
    await request('PUT', `/v1/edge/${node}/logs/level`, { level: 'warn' }, 11007, user);
    console.log('PASS Edge typed arguments, paging overflow and distinct query/edit/config permissions');
    const scoped = await openSnapshotSubscription(`/v1/edge/events?nodeId=${node}&logs=true&vpn=true`,token(user)); streams.push(scoped);
    const scopedEvents = [];
    for (let i = 0; i < 4; i++) scopedEvents.push(await scoped.next());
    assert.deepEqual(new Set(scopedEvents.filter(event => event.event !== 'vpn').map(event => event.event)), new Set(['nodes', 'detail', 'logs']));
    const deniedVpn = scopedEvents.find(event => event.event === 'vpn');
    assert(deniedVpn); assert.equal(JSON.parse(deniedVpn.data).code, 11007);
    await scoped.close();
    console.log('PASS shared edge channels preserve authorized views when VPN is denied');


    let sequence = 1n, epoch = 0n, captures = 0, levelChanges = 0;
    const networks: Map<number, bigint | Buffer>[] = [];
    const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
    const envelope = (tag: number, body: Buffer) => Buffer.concat([field(1, 6), field(2, bytes(crypto.randomUUID())), field(3, bytes(node)), field(4, bytes(platform)), field(5, epoch), field(6, Date.now()), field(8, sequence++), field(tag, body)]);
    device = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect');
    device.binaryType = 'arraybuffer';
    await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(Error('Device hello timeout')), 10000);
        device!.onopen = () => device!.send(envelope(20, Buffer.concat([field(1, identity), field(2, 'fixture'), field(3, 'event-test'), field(12, 1), field(23, 1), field(24, 3), field(32, 1)])));
        device!.onerror = () => reject(Error('Device WebSocket failed'));
        device!.onmessage = event => {
            const message = decode(Buffer.from(event.data as ArrayBuffer));
            assert.deepEqual(message.get(4), bytes(platform), 'Every outbound frame must retain the connected platform identity');
            if (message.has(21)) { epoch = message.get(5) as bigint; clearTimeout(timer); resolve(); }
            if (message.has(80)) device!.send(envelope(81, message.get(80) as Buffer));
            if (message.has(60)) {
                const request = decode(message.get(60) as Buffer);
                networks.push(request);
                device!.send(envelope(61, Buffer.concat([field(1, request.get(1) as Buffer), field(2, 1)])));
            }
            if (message.has(68)) {
                captures++;
                const request = decode(message.get(68) as Buffer);
                const line = Buffer.concat([field(1, 1789516800000), field(2, 'warn'), field(3, 'fixture'), field(4, `capture-${captures}`), field(5, 'node-push')]);
                device!.send(envelope(69, Buffer.concat([field(1, request.get(1) as Buffer), field(2, 1), field(4, line)])));
            }
            if (message.has(74)) {
                levelChanges++;
                const request = decode(message.get(74) as Buffer);
                device!.send(envelope(75, Buffer.concat([field(1, request.get(1) as Buffer), field(2, 1), field(3, request.get(2) as Buffer)])));
            }
        };
    });
    await until(async () => Boolean(await redis.send('GET', [`iot:edge:session:${node}`])), 'Device session not registered');
    await until(async () => (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->>'networkConfigVersion'='3'`).length > 0, 'Hello capabilities not projected');
    await db`INSERT INTO edge_node_interface(node_id,name) VALUES(${node},'eth1')`;
    await request('POST', `/v1/edge/${node}/network`, { interfaces: [{ operation: 'upsert', name: 'lan', mode: 'dhcp', device: 'eth1' }], rollbackTimeoutSec: 45 });
    await until(async () => networks.length > 0, 'Network command not sent to connected device');
    assert.equal(networks[0].get(3), 45n);
    const networkInterface = decode(networks[0].get(2) as Buffer);
    assert.equal((networkInterface.get(10) as Buffer).toString(), 'lan');
    assert.equal((networkInterface.get(11) as Buffer).toString(), 'eth1');
    assert.equal((await db`SELECT created_by FROM edge_task WHERE node_id=${node} AND task_type='network'`)[0].created_by, admin);
    await request('PUT', `/v1/edge/${node}/enrollment`, { status: 'approved', name: `${tag}_approved` });
    assert.equal((await db`SELECT approved_by FROM edge_node WHERE id=${node}`)[0].approved_by, admin);
    assert.equal(await redis.send('GET', [`iot:edge:auth:${identity}`]), `${node}|approved`);
    await request('POST', `/v1/edge/${node}/sync`, undefined);
    console.log('PASS Edge typed network command reaches its device; approval and snapshot RPC retain actor');
    const logSubscription = await subscribe(`/v1/edge/events?nodeId=${node}&logs=true&level=warn&source=fixture`, 'logs'); await logSubscription.next();
    const logChanged = logSubscription.next();
    await request('POST', `/v1/edge/${node}/logs/capture`, undefined);
    const lines = (await logChanged).data.lines;
    assert.equal(lines.length, 1);
    assert.equal(lines[0].message, 'capture-1');
    assert.equal(lines[0].time, '2026-09-16T00:00:00Z');
    assert.equal(captures, 1);
    await request('PUT', `/v1/edge/${node}/logs/level`, { level: 'warn' });
    assert.equal(levelChanges, 1);
    assert.equal((await db`SELECT status->'log'->>'level' AS level FROM edge_node WHERE id=${node}`)[0].level, 'warn');
    await logSubscription.close();
    const errorLogs = await subscribe(`/v1/edge/events?nodeId=${node}&logs=true&level=error`, 'logs');
    assert.deepEqual((await errorLogs.next()).data.lines, []);
    await errorLogs.close();
    const idleLogs = await openSnapshotSubscription(`/v1/edge/events?nodeId=${node}&logs=true`,token(admin)); streams.push(idleLogs);
    assert.deepEqual(new Set([(await idleLogs.next()).event, (await idleLogs.next()).event, (await idleLogs.next()).event]), new Set(['nodes', 'detail', 'logs']));
    await idleLogs.expectQuiet();
    assert(idleLogs.commentCount > 0, 'idle log subscription must use comment-only keepalive');
    await idleLogs.close();
    assert.equal(captures, 1, 'SSE heartbeat must not request device logs');
    console.log('PASS Edge log initial snapshot, explicit capture, protobuf response push and log-level notification');
    for (const path of ['/v1/edge', '/v1/edge/groups', `/v1/edge/${node}`]) {
        const response = await fetch(apiBase + path, { headers: { Authorization: `Bearer ${token(admin)}` } });
        assert.equal(response.status, 200, path);
    }
    console.log('PASS ordinary Edge HTTP snapshots available');
    const restricted = await openSnapshotSubscription('/v1/edge/events',token(user)); streams.push(restricted);
    assert.equal((await restricted.next()).event,'nodes');
    await db`UPDATE sys_role SET permissions='[]'::jsonb WHERE id=${role}`;
    let revoked; do { revoked=await restricted.next(); } while(revoked.event==='heartbeat');
    assert.equal(revoked.event,'nodes'); assert.equal(JSON.parse(revoked.data).code,11007);
    await restricted.close();
    console.log('PASS established SSE loses access immediately after permission revocation');
} finally {
    device?.close(); for(const stream of streams) await stream.close();
    await db`DELETE FROM sys_user_role WHERE user_id=${user}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${user}`;
    await db`DELETE FROM edge_node WHERE name=${tag + '_inventory'}`;
    await db`DELETE FROM edge_node WHERE id IN (${node},${pending})`;
    for (const id of groups.reverse()) await db`DELETE FROM edge_node_group WHERE id=${id}`;
    await redis.send('DEL', [`iot:edge:auth:${identity}`, `iot:edge:auth:${pendingIdentity}`, `iot:edge:logs:snapshot:${node}`]);
    await db.close(); redis.close();
}
