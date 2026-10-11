import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const user = crypto.randomUUID(), role = crypto.randomUUID();
const tag = `device_http_${crypto.randomUUID()}`;
const commandRequest = crypto.randomUUID();
const acquisition = crypto.randomUUID(), packet = crypto.randomUUID();
let link = '', model = '', group = '', device = '';
const streams: Awaited<ReturnType<typeof openSnapshotSubscription>>[] = [];
function token(userId: string) {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:userId,user_id:userId,username:userId,token_type:'access',iat:now,exp:now+3600})}`;
    return `${unsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
async function request(method: string, path: string, data?: unknown, code = 0, actor: string | null = admin) {
    const headers = new Headers({ Accept: 'application/json', 'Content-Type': 'application/json' });
    if (actor) headers.set('Authorization', `Bearer ${token(actor)}`);
    const response = await fetch(apiBase + path, {method, headers,
        body: data === undefined ? undefined : JSON.stringify(data), signal: AbortSignal.timeout(15000)});
    const result = await response.json();
    assert.equal(result.code, code, `${method} ${path}: ${JSON.stringify(result)}`);
    assert.equal(response.ok, code === 0);
    return result.data;
}
async function subscribe(path: string, actor = admin) {
    const stream = await openSnapshotSubscription(path, token(actor));
    streams.push(stream);
    return stream;
}
async function snapshot(stream: Awaited<ReturnType<typeof subscribe>>, eventName = 'snapshot') {
    let event = await stream.next();
    while (event.event !== eventName && event.event !== 'error') event = await stream.next();
    assert.equal(event.event, eventName);
    assert.equal(JSON.parse(event.data).code, 0);
    return JSON.parse(event.data).data;
}
async function collectEvents(stream: Awaited<ReturnType<typeof subscribe>>, names: string[]) {
    const remaining = new Set(names);
    const data = new Map<string, any>();
    while (remaining.size) {
        const event = await stream.next();
        if (event.event === 'error') {
            assert.equal(event.event, names[0]);
        }
        if (!remaining.has(event.event)) continue;
        assert.equal(JSON.parse(event.data).code, 0, event.event);
        data.set(event.event, JSON.parse(event.data).data);
        remaining.delete(event.event);
    }
    return data;
}
try {
    await request('GET', '/v1/device', undefined, 11004, null);
    await request('POST', '/v1/protocol/configs', {protocol:'Modbus',name:tag,config:{storagePolicy:'report',readInterval:10,byteOrder:'BIG_ENDIAN',registers:[]}});
    model = (await db`SELECT id FROM protocol_config WHERE name=${tag}`)[0].id;
    await request('POST', '/v1/link', {execution:'collector',name:tag,protocol:'Modbus',endpoint:{transport:'tcp',mode:'TCP Server',ip:'0.0.0.0',port:59961,targets:[]},status:'disabled'});
    link = (await db`SELECT id FROM link WHERE name=${tag}`)[0].id;
    await request('POST', '/v1/device/groups', {name:tag,status:'enabled',sort_order:1});
    group = (await db`SELECT id FROM device_group WHERE name=${tag}`)[0].id;
    const list = await subscribe('/v1/device/events');
    await collectEvents(list, ['devices', 'groups', 'realtime']);
    await request('POST', '/v1/device', {name:tag,device_code:`MB-${tag}`,link_id:link,protocol_config_id:model,group_id:group,status:'disabled',online_timeout:120,remote_control:true,modbus_mode:'TCP',slave_id:1,timezone:'+08:00',heartbeat:{mode:'OFF'},registration:{mode:'ASCII',content:tag},remark:'retained'});
    const created = await collectEvents(list, ['devices', 'groups', 'realtime']);
    const item = created.get('devices').list.find((row: {name:string}) => row.name === tag);
    assert(item);
    device = item.id;
    const countedGroup = created.get('groups').find((row: {id:string}) => row.id === group);
    assert.equal(countedGroup.deviceCount, 1, 'the shared connection must update group counts after device creation');
    await request('POST', `/v1/device/${device}/commands`, { elements: [] }, 10001);
    const detail = () => request('GET', `/v1/device/${device}`);
    assert.equal((await detail()).created_by, admin);
    assert.equal((await detail()).online_timeout, 120);
    const realtime = list;
    assert(created.get('realtime').list.some((row: {id:string}) => row.id === device));
    // Drain the create notification before isolating a telemetry-only change.
    await list.expectQuiet();
    assert(list.commentCount > 0, 'idle connection must use comment-only keepalive');
    const reported = Date.now();
    await redis.send('EVAL', [
        "redis.call('HSET',KEYS[1],'last_report_at_ms',ARGV[1]); return redis.call('XADD',KEYS[2],'*','topic','device.realtime')",
        '2', `iot:v2:runtime:device:${device}`, 'iot:live:changes', String(reported),
    ]);
    const telemetryEvent = await realtime.next();
    assert.equal(telemetryEvent.event, 'realtime', 'telemetry must not rebuild metadata or groups');
    const changedRealtime = JSON.parse(telemetryEvent.data).data.list.find((row: {id:string}) => row.id === device);
    assert.equal(Date.parse(changedRealtime.reportTime), Math.floor(reported / 1000) * 1000);
    // The metadata response also contains reportTime. A mistaken metadata query
    // would therefore emit a changed snapshot during this idle observation.
    await list.expectQuiet();
    const commentsBeforeDuplicate = list.commentCount;
    await redis.send('XADD', ['iot:live:changes', '*', 'topic', 'device.realtime']);
    await list.expectQuiet(1000);
    assert.equal(list.commentCount, commentsBeforeDuplicate, 'duplicate notification must not trigger a heartbeat');
    await list.expectQuiet();
    assert(Array.isArray(await request('GET', '/v1/device/options')));
    await request('PUT', `/v1/device/${device}`, {name:`${tag}_updated`,remote_control:false});
    assert.equal((await detail()).name, `${tag}_updated`);
    assert.equal((await detail()).remark, 'retained');
    await request('PUT', `/v1/device/${device}`, {device_code:`MB-${tag}`,remark:'unchanged legacy code saves'});
    await request('PUT', `/v1/device/${device}`, {device_code:'invalid/code'}, 18002);
    assert.equal((await detail()).device_code, `MB-${tag}`);
    await request('PUT', `/v1/device/${device}`, {online_timeout:0}, 10001);
    const history = await request('GET', `/v1/device/${device}/history?startTime=2026-01-01T00%3A00%3A00Z&endTime=2026-01-02T00%3A00%3A00Z`);
    assert(Array.isArray(history.list));
    await request('PUT', `/v1/device/${device}/debug`, {enabled:true});
    assert.equal((await detail()).debug_enabled, true);
    const debug = await subscribe(`/v1/device/events?debugDeviceId=${device}`);
    assert((await snapshot(debug, 'devices')).list.some((row: {id:string}) => row.id === device));
    assert(Array.isArray(await snapshot(debug, 'groups')));
    assert((await snapshot(debug, 'realtime')).list.some((row: {id:string}) => row.id === device));
    assert.deepEqual(await snapshot(debug, 'packets'), []);
    await redis.send('HSET', [`iot:debug:v4:acquisition:${acquisition}`, 'state', 'completed', 'started_at_ms', '1000']);
    await redis.send('HSET', [`iot:debug:v4:packet:${packet}`, 'acquisition_id', acquisition, 'payload_hex', '00FF', 'direction', 'rx', 'time_ms', '1001']);
    await redis.send('ZADD', [`iot:debug:v4:acquisition:${acquisition}:packets`, '1001', packet]);
    await redis.send('ZADD', [`iot:debug:v4:device:${device}`, '1000', acquisition]);
    await redis.send('XADD', ['iot:live:changes', '*', 'topic', 'packet-debug']);
    const packetEvent = await debug.next();
    assert.equal(packetEvent.event, 'packets', 'packet updates must not rebuild other page channels');
    assert.equal(JSON.parse(packetEvent.data).data[0].packets[0].payload_hex, '00FF');
    await debug.expectQuiet(1000);
    await request('PUT', `/v1/device/${device}`, { name: `${tag}_debug_open` });
    assert.equal((await snapshot(debug, 'devices')).list.find((row: {id:string}) => row.id === device).name, `${tag}_debug_open`);
    await debug.close();
    await request('GET', '/v1/device/events?debugDeviceId=invalid', undefined, 10001);
    console.log('device integration: command batch initial snapshot');
    await db`INSERT INTO command_request(id,actor,idempotency_key,device_id,payload) VALUES(${commandRequest},${admin},${crypto.randomUUID()},${device},'[]'::jsonb)`;
    const commandRows = await db`INSERT INTO command_operation(id,request_id,ordinal,device_id,device_code,protocol,status)
        SELECT gen_random_uuid(),${commandRequest}::uuid,n,${device}::uuid,${tag},'Modbus','ACCEPTED' FROM generate_series(1,256) n RETURNING id`;
    const commandIds = commandRows.map((row: {id:string}) => row.id);
    const commandScope = commandIds.join(',');
    const commands = await subscribe(`/v1/device/events?debugDeviceId=${device}&commandIds=${encodeURIComponent(commandScope)}`);
    const initialCommandPage = new Map<string, any>();
    while (initialCommandPage.size < 5) {
        const event = await commands.next();
        initialCommandPage.set(event.event, JSON.parse(event.data));
    }
    assert.equal(initialCommandPage.get('commands').code, 0);
    assert.equal(initialCommandPage.get('commands').data.statuses.length, 256);
    assert.equal(initialCommandPage.get('commands').data.complete, false);
    assert.equal(initialCommandPage.get('commands').data.statuses[0].status, 'ACCEPTED');
    await commands.expectQuiet();
    // No telemetry event: a command-only change must not query the metrics channel.
    await redis.send('HSET', [`iot:v2:runtime:device:${device}`, 'last_report_at_ms', String(reported + 30000)]);
    await db`UPDATE command_operation SET status='SUCCEEDED',completed_at=NOW(),actual_values='[{"elementId":"reading","name":"读数","kind":"DOUBLE","value":"12.5","unit":"V"}]'::jsonb WHERE request_id=${commandRequest}`;
    console.log('device integration: command batch completion');
    const completion = await commands.next();
    if (completion.event !== 'commands') {
        const changes = await redis.send('XREVRANGE', ['iot:live:changes', '+', '-', 'COUNT', '20']) as [string, string[]][];
        console.log('command isolation notification topics', changes.map(([id, fields]) => [id, fields[fields.indexOf('topic') + 1]]));
    }
    assert.equal(completion.event, 'commands');
    const completed = JSON.parse(completion.data).data;
    assert.equal(completed.complete, true);
    assert.equal(completed.statuses.length, 256);
    assert.equal(completed.statuses[0].actual_values[0].value, '12.5');
    await commands.expectQuiet(1000);
    await commands.close();
    await db`DELETE FROM command_operation WHERE request_id=${commandRequest}`;
    await db`DELETE FROM command_request WHERE id=${commandRequest}`;
    await request('PUT', `/v1/device/groups/${group}`, {name:`${tag}_updated`});
    assert.equal((await db`SELECT name FROM device_group WHERE id=${group}`)[0].name, `${tag}_updated`);
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${user},${user},'unused-test-password')`;
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:device:query","iot:device:edit","iot:device:delete","iot:device:share","iot:device-group:query","iot:device-group:share"]'::jsonb)`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${user},${role})`;
    await request('GET', `/v1/device/${device}`, undefined, 18001, user);
    assert((await request('GET', `/v1/device/${device}/share-targets`)).some((row: {subject_id:string}) => row.subject_id === user));
    await request('PUT', `/v1/device/${device}/shares`, {shares:[{subject_type:'user',subject_id:user,access_level:'view'}]});
    assert.equal((await request('GET', `/v1/device/${device}/shares`)).length, 1);
    const shared = await subscribe('/v1/device/events', user);
    assert((await snapshot(shared, 'devices')).list.some((row: {id:string}) => row.id === device));
    await request('PUT', `/v1/device/${device}`, {name:'forbidden'}, 18005, user);
    await request('DELETE', `/v1/device/${device}`, undefined, 18005, user);
    await request('PUT', `/v1/device/${device}/shares`, {shares:[]});
    assert(!(await snapshot(shared, 'devices')).list.some((row: {id:string}) => row.id === device));
    await request('PUT', `/v1/device/groups/${group}/shares`, {shares:[{subject_type:'user',subject_id:user,access_level:'view'}]});
    assert.equal((await request('GET', `/v1/device/${device}`, undefined, 0, user)).id, device);
    await request('PUT', `/v1/device/groups/${group}/shares`, {shares:[]});
    // Each channel enforces its own permission without terminating authorized channels.
    await db`UPDATE sys_role SET permissions='["iot:device:query"]'::jsonb WHERE id=${role}`;
    console.log('device integration: channel permission isolation');
    const partial = await subscribe(`/v1/device/events?debugDeviceId=${device}&commandIds=${commandIds[0]}`, user);
    const initialChannels = new Map<string, any>();
    while (initialChannels.size < 5) {
        const event = await partial.next();
        if (event.event !== 'heartbeat') initialChannels.set(event.event, JSON.parse(event.data));
    }
    assert.equal(initialChannels.get('devices').code, 0);
    assert.equal(initialChannels.get('realtime').code, 0);
    assert.notEqual(initialChannels.get('groups').code, 0);
    assert.equal(initialChannels.get('packets').code, 11007);
    assert.equal(initialChannels.get('commands').code, 11007);
    await db`UPDATE sys_role SET permissions='["iot:device-group:query"]'::jsonb WHERE id=${role}`;
    await redis.send('XADD', ['iot:live:changes', '*', 'topic', 'auth']);
    console.log('device integration: permission replacement');
    const revokedChannels = new Map<string, any>();
    while (revokedChannels.size < 3) {
        const event = await partial.next();
        if (event.event !== 'heartbeat') revokedChannels.set(event.event, JSON.parse(event.data));
    }
    assert.notEqual(revokedChannels.get('devices').code, 0);
    assert.notEqual(revokedChannels.get('realtime').code, 0);
    assert.equal(revokedChannels.get('groups').code, 0);
    for (const stream of streams) await stream.close();

    const bulkTag = `${tag}_bulk_`;
    try {
        const rows = await db`INSERT INTO device(id,name,link_id,protocol_config_id,created_by,status,protocol_params)
            SELECT gen_random_uuid(), ${bulkTag} || n, ${link}::uuid, ${model}::uuid,
                   ${admin}::uuid, 'disabled', jsonb_build_object('device_code', ${bulkTag} || n, 'slave_id', 1,
                       'registration', jsonb_build_object('mode','ASCII','content',${bulkTag} || n))
            FROM generate_series(1,400) AS n RETURNING id`;
        const ids = new Set(rows.map((row: {id:string}) => row.id));
        const stream = await subscribe('/v1/device/events');
        for (const eventName of ['devices', 'realtime']) {
            const data = await snapshot(stream, eventName);
            assert.equal(data.list.filter((row: {id:string}) => ids.has(row.id)).length, 400);
            assert(Buffer.byteLength(JSON.stringify(data)) > 64 * 1024);
        }
        await stream.close();
    } finally {
        await db`DELETE FROM device WHERE link_id=${link} AND name LIKE ${bulkTag+'%'}`;
    }
    await request('DELETE', `/v1/device/${device}`);
    await request('DELETE', `/v1/device/groups/${group}`);
    await request('GET', `/v1/device/${device}`, undefined, 18001);
    console.log('PASS device HTTP CRUD, legacy-code save, validation, history, debug SSE, sharing/revocation and 400-device SSE snapshots');
 } finally {
    await db`DELETE FROM command_operation WHERE request_id=${commandRequest}`;
    await db`DELETE FROM command_request WHERE id=${commandRequest}`;
    await redis.send('DEL', [`iot:debug:v4:device:${device}`, `iot:debug:v4:acquisition:${acquisition}`, `iot:debug:v4:acquisition:${acquisition}:packets`, `iot:debug:v4:packet:${packet}`]);
    for (const stream of streams) await stream.close();

    if (device) await redis.send('DEL', [`iot:v2:runtime:device:${device}`]);
    if (device) { await db`DELETE FROM device_access_grant WHERE device_id=${device}`; await db`DELETE FROM device WHERE id=${device}`; }
    if (group) { await db`DELETE FROM device_group_access_grant WHERE group_id=${group}`; await db`DELETE FROM device_group WHERE id=${group}`; }
    if (link) await db`DELETE FROM link WHERE id=${link}`;
    if (model) await db`DELETE FROM protocol_config WHERE id=${model}`;
    await db`DELETE FROM sys_user_role WHERE user_id=${user}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${user}`;
    await db.close();
    redis.close();
}
