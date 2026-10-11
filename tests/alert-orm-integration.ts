import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, publishFixtureEvent, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

const db = new Bun.SQL(databaseUrl);
const streams: Awaited<ReturnType<typeof openSnapshotSubscription>>[] = [];
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const deniedUser = crypto.randomUUID();
const device = crypto.randomUUID();
const link = crypto.randomUUID();
const model = crypto.randomUUID();
const point = crypto.randomUUID();
const prefix = `orm-alert-${crypto.randomUUID()}`;
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
    iss: 'iot-engine', aud: 'iot-engine-web', sub: admin, user_id: admin,
    username: 'admin', token_type: 'access', iat: now, exp: now + 3600,
})}`;
const token = `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;

async function until(check: () => Promise<boolean>, message: string) {
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(50);
    }
    throw new Error(message);
}

async function request(method: string, path: string, data?: unknown, code = 0) {
    const response = await fetch(apiBase+path,{method,headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:data === undefined ? undefined : JSON.stringify(data),signal:AbortSignal.timeout(15000)});
    const reply = await response.json();
    assert.equal(reply.code,code,`${method} ${path}: ${JSON.stringify(reply)}`);
    assert.equal(response.ok,code === 0);
    return reply.data;
}
async function subscribe(path: string) {
    const stream = await openSnapshotSubscription(path,token);
    streams.push(stream);
    return stream;
}
async function snapshot(stream: Awaited<ReturnType<typeof subscribe>>, eventName = 'records') {
    let event = await stream.next();
    while (event.event !== eventName && event.event !== 'error') event = await stream.next();
    assert.equal(event.event,eventName);
    assert.equal(JSON.parse(event.data).code, 0);
    return JSON.parse(event.data).data;
}

async function rule(suffix: string, condition: object) {
    const name = `${prefix}-${suffix}`;
    await request('POST','/v1/alert/rules', {
        name, device_id: device, severity: 'warning', conditions: [condition],
        silence_duration: 0, recovery_condition: 'reverse', recovery_wait_seconds: 0,
    });
    const rows = await db`SELECT id FROM alert_rule WHERE name=${name} AND device_id=${device}`;
    assert.equal(rows.length, 1);
    return rows[0].id as string;
}

async function publish(value: number, observedAt: number, messageId = crypto.randomUUID()) {
    const fields = [
        'acquisition_id', messageId, 'raw_packet_ids', '[]',
        'message_id', messageId, 'causation_id', messageId, 'link_id', link,
        'device_id', device, 'device_code', '1', 'protocol', 'Modbus',
        'connection_id', device, 'occurred_at_ms', String(observedAt),
        'observed_at_ms', String(observedAt), 'model_id', model,
        'storage_policy', 'change', 'source', 'collector', 'online_window_ms', '60000',
        'values_json', JSON.stringify({ values: { [point]: { name: 'temperature', value } } }),
        'raw_payload_hex', '[]',
    ];
    await publishFixtureEvent(redis, 'iot:v3:telemetry', fields, 'telemetry');
    await until(async () => (await db`SELECT 1 FROM alert_evaluation_receipt WHERE message_id=${messageId}`).length === 1,
        `alert evaluation did not acknowledge ${messageId}`);
    return { messageId, fields };
}

try {
    assert.equal((await fetch(apiBase+'/v1/alert/rules')).status,401);
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${deniedUser},${deniedUser},'unused-test-password')`;
    const deniedUnsigned = `${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:deniedUser,user_id:deniedUser,username:deniedUser,token_type:'access',iat:now,exp:now+3600})}`;
    const deniedToken = `${deniedUnsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(deniedUnsigned).digest('base64url')}`;
    for (const [method,path] of [
        ['GET','/v1/alert/rules'],['GET','/v1/alert/events'],
        ['POST','/v1/alert/rules'],['PUT',`/v1/alert/rules/${device}`],
        ['DELETE',`/v1/alert/rules/${device}`],['POST',`/v1/alert/records/${device}/ack`],
        ['POST','/v1/alert/records/batch-ack'],['POST','/v1/alert/rules/apply-template'],
        ['POST','/v1/alert/templates'],['PUT',`/v1/alert/templates/${device}`],
    ]) {
        const denied = await fetch(apiBase+path,{method,headers:{Authorization:`Bearer ${deniedToken}`,Accept:path.endsWith('/events')?'text/event-stream':'application/json'},signal:AbortSignal.timeout(15000)});
        assert.equal(denied.status,403,`${method} ${path}`);
        assert.equal((await denied.json()).code,11007);
    }

    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by)
        VALUES(${model},${prefix},'Modbus',${{ registers: [{ id: point, name: 'temperature', registerType: 'HOLDING_REGISTER', dataType: 'UINT16', address: 0, quantity: 1 }], storagePolicy: 'change' }}::jsonb,${admin})`;
    await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution,status)
        VALUES(${link},${prefix},'Modbus','{"transport":"tcp","mode":"TCP Server","ip":"0.0.0.0","port":55209,"targets":[]}'::jsonb,${admin},'collector','disabled')`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
        VALUES(${device},${prefix},${link},${model},'{"device_code":"1","slave_id":1,"modbus_mode":"TCP"}'::jsonb,${admin})`;
    const threshold = await rule('threshold', { type: 'threshold', elementKey: point, operator: '>', value: '50' });
    const rate = await rule('rate', { type: 'rate_of_change', elementKey: point, changeRate: '50', changeDirection: 'rise' });
    const bit = await rule('bit', { type: 'threshold', elementKey: point, operator: '==', value: '1', bitIndex: 0 });
    const timestamp = Date.now();
    await publish(10, timestamp);
    assert.equal((await db`SELECT id FROM open_alert_record WHERE device_id=${device}`).length, 0);
    const recordStream = await subscribe(`/v1/alert/events?deviceId=${device}&status=active`);
    assert.equal((await snapshot(recordStream)).total,0);
    const statsStream = recordStream;
    assert.equal((await snapshot(statsStream, 'stats')).total,0);
    const triggered = await publish(61, timestamp + 10);
    let pushed = await snapshot(recordStream);
    while (pushed.total < 3) pushed = await snapshot(recordStream);
    assert.equal(pushed.total,3);
    let pushedStats = await snapshot(statsStream, 'stats');
    while (pushedStats.total < 3) pushedStats = await snapshot(statsStream, 'stats');
    assert.equal(pushedStats.total,3);
    await statsStream.close();
    const pageQuery = `deviceId=${device}&status=active&page=2&pageSize=1`;
    const pageStream = await subscribe(`/v1/alert/events?${pageQuery}`);
    const secondPage = await snapshot(pageStream);
    const activeIds = new Set((await db`SELECT id FROM open_alert_record WHERE device_id=${device} AND status='active'`).map(row => row.id as string));
    assert.equal(secondPage.total, 3);
    assert.equal(secondPage.list.length, 1);
    assert(activeIds.has(secondPage.list[0].id));
    assert.equal((await snapshot(pageStream, 'stats')).total, 3, 'global statistics must not inherit record pagination');
    await pageStream.close();
    const records = await db`SELECT rule_id,status FROM open_alert_record WHERE device_id=${device}`;
    assert.deepEqual(new Set(records.map(row => row.rule_id)), new Set([threshold, rate, bit]));
    assert(records.every(row => row.status === 'active'));
    const replayId = String(await publishFixtureEvent(redis, 'iot:v3:telemetry:alerts', triggered.fields, 'telemetry-alerts'));
    await until(async () => (await redis.send('XRANGE', ['iot:v3:telemetry:alerts', replayId, replayId]) as unknown[]).length === 0,
        'duplicate alert input was not acknowledged');
    assert.equal((await db`SELECT id FROM open_alert_record WHERE device_id=${device}`).length, 3);
    await publish(20, timestamp + 20);
    assert((await db`SELECT status FROM open_alert_record WHERE device_id=${device}`).every(row => row.status === 'resolved'));
    const unchanged = await publish(20, timestamp + 30);
    await until(async () => (await db`SELECT record_id FROM device_latest_value WHERE device_id=${device} AND element_id=${point}`)[0]?.record_id === unchanged.messageId,
        'unchanged history input must still advance the latest read model');
    assert.equal(Number((await db`SELECT count(*) AS count FROM device_data WHERE device_id=${device}`)[0].count), 3,
        'change storage must persist changed samples once and skip unchanged samples');
    const latest = await db`SELECT value FROM device_latest_value WHERE device_id=${device} AND element_id=${point}`;
    assert.equal(latest[0].value.value, 20);
    const listing = await request('GET',`/v1/alert/rules?keyword=${prefix}`);
    assert.equal(listing.total, 3);
    assert.equal(listing.list.length, 3);
    const recoveredStatsStream = await subscribe(`/v1/alert/events?deviceId=${device}`);
    assert.equal(typeof (await snapshot(recoveredStatsStream, 'stats')).total, 'number');
    await recoveredStatsStream.close();
    const offline = await rule('offline', { type: 'offline', duration: 1 });
    await until(async () => (await db`SELECT id FROM open_alert_record WHERE rule_id=${offline} AND status='active'`).length === 1,
        'offline rule did not fire after its deadline');
    const templateName = `${prefix}-template`;
    await request('POST','/v1/alert/templates', { name: templateName, protocol_config_id: model,
        conditions: [{ type: 'threshold', elementKey: point, operator: '>', value: '100' }] });
    const template = (await db`SELECT id FROM alert_rule_template WHERE name=${templateName}`)[0].id;
    const applied = await request('POST','/v1/alert/rules/apply-template', { template_id: template, device_ids: [device] });
    assert.equal(applied.success, 1);
    assert.equal(applied.total, 1);
    assert.equal(applied.createdIds.length, 1);
    assert.equal((await request('POST','/v1/alert/rules/apply-template', { template_id: template, device_ids: [device] })).success, 0);
    assert.equal((await request('GET',`/v1/alert/templates/${template}`)).name, templateName);
    const offlineRecord = (await db`SELECT id FROM open_alert_record WHERE rule_id=${offline} AND status='active'`)[0] as { id: string } | undefined;
    assert(offlineRecord);
    const ackStream = await subscribe(`/v1/alert/events?deviceId=${device}&status=active`);
    assert((await snapshot(ackStream)).list.some((item: {id:string}) => item.id === offlineRecord.id));
    await request('POST',`/v1/alert/records/${offlineRecord.id}/ack`);
    assert(!(await snapshot(ackStream)).list.some((item: {id:string}) => item.id === offlineRecord.id));
    await ackStream.close();
    assert.equal((await db`SELECT acknowledged_by FROM open_alert_record WHERE id=${offlineRecord.id}`)[0].acknowledged_by, admin);
    await request('POST',`/v1/alert/records/${offlineRecord.id}/ack`,undefined,17003);
    await request('POST','/v1/alert/records/batch-ack', { ids: [offlineRecord.id] });
    const batchOffline = await rule('batch-offline', { type: 'offline', duration: 1 });
    await until(async () => (await db`SELECT id FROM open_alert_record WHERE rule_id=${batchOffline} AND status='active'`).length === 1,
        'batch acknowledgement fixture did not trigger');
    const batchRecord = (await db`SELECT id FROM open_alert_record WHERE rule_id=${batchOffline} AND status='active'`)[0];
    const batchStream = await subscribe(`/v1/alert/events?deviceId=${device}&status=active`);
    assert((await snapshot(batchStream)).list.some((item: {id:string}) => item.id === batchRecord.id));
    const batchStatsStream = batchStream;
    const beforeBatch = await snapshot(batchStatsStream, 'stats');
    await request('POST','/v1/alert/records/batch-ack', { ids: [batchRecord.id] });
    assert(!(await snapshot(batchStream)).list.some((item: {id:string}) => item.id === batchRecord.id));
    const afterBatch = await snapshot(batchStatsStream, 'stats');
    assert.equal(afterBatch.acknowledged, beforeBatch.acknowledged + 1);
    await batchStream.close();
    await request('POST','/v1/alert/records/batch-ack',{ids:[]},17002);
    for (const query of ['page=0','pageSize=101'])
        await request('GET',`/v1/alert/rules?${query}`,undefined,10001);
    await request('GET','/v1/alert/rules?deviceId=invalid',undefined,19002);
    const ruleName = `${prefix}-threshold-updated`;
    await request('PUT',`/v1/alert/rules/${threshold}`,{
        name: ruleName, device_id: device, severity: 'info',
        conditions: [{ type: 'threshold', elementKey: point, operator: '>', value: '999' }],
        status: 'disabled',
    });
    assert((await request('GET',`/v1/alert/rules?keyword=${prefix}`)).list.some((item: { id: string; name: string; status: string }) => item.id === threshold && item.name === ruleName && item.status === 'disabled'));
    await request('PUT',`/v1/alert/templates/${template}`,{
        name: templateName, category: prefix, protocol_config_id: model,
        conditions: [{ type: 'offline', duration: 60 }],
    });
    assert.equal((await request('GET',`/v1/alert/templates?category=${prefix}`)).total, 1);
    await request('DELETE',`/v1/alert/templates/${template}`);
    await request('GET',`/v1/alert/templates/${template}`,undefined,17003);
    await request('DELETE',`/v1/alert/rules/${threshold}`);
    await request('DELETE','/v1/alert/rules', { ids: [rate, bit, offline, batchOffline, ...applied.createdIds] });
    assert.equal((await request('GET',`/v1/alert/rules?keyword=${prefix}`)).total, 0);
    console.log('PASS alert HTTP/SSE: rules/templates CRUD, trigger and acknowledgement pushes, shared records/stats SSE, pagination, thresholds, recovery, receipts and change storage');
} finally {
    for (const stream of streams) await stream.close();
    await db`UPDATE alert_rule SET deleted_at=NOW() WHERE device_id=${device}`;
    await db`UPDATE alert_rule_template SET deleted_at=NOW() WHERE protocol_config_id=${model}`;
    await db`UPDATE device SET deleted_at=NOW() WHERE id=${device}`;
    await db`UPDATE link SET deleted_at=NOW() WHERE id=${link}`;
    await db`UPDATE protocol_config SET deleted_at=NOW() WHERE id=${model}`;
    await db`DELETE FROM sys_user WHERE id=${deniedUser}`;
    await db.close();
    redis.close();
}
