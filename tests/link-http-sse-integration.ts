import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
let actor: string | null = null;
const streams: Awaited<ReturnType<typeof openSnapshotSubscription>>[] = [];
const admin = '00000000-0000-7000-8000-000000000002';
const other = crypto.randomUUID();
const role = crypto.randomUUID();
const tag = `link_http_${crypto.randomUUID()}`;
const acquisition = crypto.randomUUID();
const packet = crypto.randomUUID();
let link = '';
function token(userId: string) {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now()/1000);
    const unsigned = `${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:userId,user_id:userId,username:userId,token_type:'access',iat:now,exp:now+3600})}`;
    return `${unsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
async function request(method: string, path: string, data?: unknown, code = 0, raw = false) {
    const headers = new Headers({'Content-Type':'application/json', Accept:'application/json'});
    if (actor) headers.set('Authorization', `Bearer ${token(actor)}`);
    const response = await fetch(apiBase + path, {method, headers, body:data === undefined ? undefined : raw ? String(data) : JSON.stringify(data), signal:AbortSignal.timeout(15000)});
    const reply = await response.json();
    assert.equal(reply.code,code,`${method} ${path}: ${JSON.stringify(reply)}`);
    assert.equal(response.ok,code === 0);
    return reply.data;
}
async function subscribe(path: string, userId = admin) {
    const stream = await openSnapshotSubscription(path,token(userId));
    streams.push(stream);
    return stream;
}
async function snapshot(stream: Awaited<ReturnType<typeof subscribe>>, eventName = 'links') {
    let event = await stream.next();
    while (event.event !== eventName && event.event !== 'error') event = await stream.next();
    assert.equal(event.event,eventName);
    assert.equal(JSON.parse(event.data).code, 0);
    return JSON.parse(event.data).data;
}
try {
    await request('GET','/v1/link',undefined,11004);
    actor = admin;
    const endpoint = {transport:'tcp',mode:'TCP Server',ip:'0.0.0.0',port:59888,targets:[]};
    const configuration = {execution:'collector',name:tag,protocol:'Modbus',endpoint,status:'disabled'};
    const subscription = await subscribe(`/v1/link/events?keyword=${tag}`);
    assert.equal((await snapshot(subscription)).total,0);
    const created = snapshot(subscription);
    await request('POST','/v1/link',configuration);
    const list = await created;
    assert.equal(list.total,1);
    link = list.list[0].id;
    const detail = list.list[0];
    assert.equal(detail.created_by,admin);
    assert.equal(detail.endpoint.port,59888);
    assert.equal(detail.endpoint.mode,'TCP Server');
    assert.equal((await db`SELECT count(*)::int AS n FROM outbox_event WHERE aggregate_type='link' AND aggregate_id=${link} AND action='created'`)[0].n,1);
    assert((await request('GET','/v1/link/enums')).protocols.includes('Modbus'));
    await request('PUT',`/v1/link/${link}`,{},10001);
    await request('PUT',`/v1/link/${link}`,{...configuration,name:''},15002);
    await request('PUT',`/v1/link/${link}`,{...configuration,protocol:'invalid'},10001);
    await request('PUT',`/v1/link/${link}`,{...configuration,protocol:'SL651'},15006);
    const renamed = snapshot(subscription);
    await request('PUT',`/v1/link/${link}`,{...configuration,name:`${tag}_updated`});
    assert.equal((await renamed).list[0].name,`${tag}_updated`);
    await subscription.close();
    await request('PUT',`/v1/link/${link}/debug`,{enabled:true});
    const debugSubscription = await subscribe(`/v1/link/events?keyword=${tag}&debugLinkId=${link}`);
    const debugLinks = await snapshot(debugSubscription);
    assert.equal(debugLinks.list[0].id, link);
    assert.equal(debugLinks.list[0].debug_enabled, true);
    assert.deepEqual(await snapshot(debugSubscription, 'packets'),[]);
    const debugPush = snapshot(debugSubscription, 'packets');
    await redis.send('HSET',[`iot:debug:v4:acquisition:${acquisition}`,'state','completed','started_at_ms','1000']);
    await redis.send('HSET',[`iot:debug:v4:packet:${packet}`,'acquisition_id',acquisition,'payload_hex','00FF','direction','rx','time_ms','1001']);
    await redis.send('ZADD',[`iot:debug:v4:acquisition:${acquisition}:packets`,'1001',packet]);
    await redis.send('ZADD',[`iot:debug:v4:link:${link}`,'1000',acquisition]);
    await redis.send('XADD',['iot:live:changes','*','schema_version','1','topic','packet-debug']);
    const debug = await debugPush;
    assert.equal(debug[0].id,acquisition);
    assert.equal(debug[0].packets[0].payload_hex,'00FF');
    await debugSubscription.expectQuiet(1000);
    await request('PUT', `/v1/link/${link}`, { ...configuration, name: `${tag}_debug_open` });
    assert.equal((await snapshot(debugSubscription)).list[0].name, `${tag}_debug_open`);
    await debugSubscription.close();
    await request('GET', '/v1/link/events?debugLinkId=invalid', undefined, 10001);
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${other},${other},'unused-test-password')`;
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:link:query","iot:link:edit","iot:link:delete"]'::jsonb)`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${other},${role})`;
    actor = other;
    await request('POST','/v1/link',{...configuration,name:`${tag}_denied`},11007);
    await request('PUT',`/v1/link/${link}`,configuration,15007);
    await request('DELETE',`/v1/link/${link}`,undefined,15007);
    await db`UPDATE sys_role SET permissions='["iot:link:query"]'::jsonb WHERE id=${role}`;
    const deniedDebug = await subscribe(`/v1/link/events?keyword=${tag}&debugLinkId=${link}`, other);
    assert.equal((await snapshot(deniedDebug)).total, 1);
    const deniedPacketEvent = await deniedDebug.next();
    assert.equal(deniedPacketEvent.event, 'packets');
    assert.equal(JSON.parse(deniedPacketEvent.data).code, 11007);
    actor = admin;
    await request('PUT', `/v1/link/${link}`, { ...configuration, name: `${tag}_query_only` });
    assert.equal((await snapshot(deniedDebug)).list[0].name, `${tag}_query_only`);
    await deniedDebug.close();
    await request('POST','/v1/link',{name:`${tag}_edge`,execution:'edge',edge_node_id:crypto.randomUUID(),protocol:'Modbus',endpoint:{transport:'serial',interface:'/dev/ttyS0',baud_rate:299},status:'disabled'},15002);
    await request('DELETE',`/v1/link/${link}`);
    assert.equal((await request('GET',`/v1/link?keyword=${tag}`)).total,0);
    assert.equal((await fetch(apiBase+'/v1/link')).status,401);
    console.log('PASS link HTTP/SSE: CRUD, typed update validation, committed outbox, debug push and original owner checks');
} finally {
    await redis.send('DEL',[`iot:debug:v4:link:${link}`,`iot:debug:v4:acquisition:${acquisition}`,`iot:debug:v4:acquisition:${acquisition}:packets`,`iot:debug:v4:packet:${packet}`]);
    if (link) { await db`DELETE FROM outbox_event WHERE aggregate_id=${link}`; await db`DELETE FROM link WHERE id=${link}`; }
    await db`DELETE FROM sys_user_role WHERE user_id=${other}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${other}`;
    for (const stream of streams) await stream.close();
    redis.close();
    await db.close();
}
