import { sharedChannel } from './redis_channel.fixture';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription, type SnapshotSubscription } from './sse-fixture';

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
const runtimeLink = crypto.randomUUID();
const runtimeOwner = crypto.randomUUID();
const runtimeTag = `link_runtime_${crypto.randomUUID()}`;
const runtimeTargets = [crypto.randomUUID(), crypto.randomUUID()];
const runtimeOwnerKey = `iot:v2:owner:link:${runtimeLink}`;
const runtimeKeys = [0, 1].map(worker => `iot:runtime:link:${runtimeLink}:worker:${runtimeOwner}:${worker}`);
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
// Entity changes of a visible link arrive as `{delta:{upsert,remove}}` in the snapshot row shape.
async function delta(stream: SnapshotSubscription, eventName = 'links') {
    let event = await stream.next();
    while (event.event !== eventName && event.event !== 'error') event = await stream.next();
    assert.equal(event.event,eventName);
    const payload = JSON.parse(event.data);
    assert.equal(payload.code, 0);
    assert(payload.delta, `${eventName} must answer an entity change with a delta: ${event.data}`);
    return payload.delta as {upsert:any[];remove:string[]};
}
function change(topic: string, ...ids: string[]) {
    return redis.send('PUBLISH',[sharedChannel(redisUrl, `live:topic:${topic}`),ids.join(',')]);
}

async function pubsubClients() {
    const clients = String(await redis.send('CLIENT', ['LIST', 'TYPE', 'PUBSUB'])).trim();
    return clients ? clients.split('\n').map(line => Object.fromEntries(line.trim().split(' ').map(field => {
        const separator = field.indexOf('=');
        return [field.slice(0, separator), field.slice(separator + 1)];
    }))) : [];
}

async function verifySubscriptionRaceAndRecovery() {
    const raceId = crypto.randomUUID();
    const marker = `${tag}_subscription_race`;
    const latest = `${marker}_committed`;
    const before = new Set((await pubsubClients()).map(client => client.id));
    let opening: Promise<SnapshotSubscription> | undefined;
    let first: SnapshotSubscription | undefined;
    let second: SnapshotSubscription | undefined;
    let clientId = '';
    try {
        await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,created_by)
            VALUES(${raceId},${marker},'Modbus','{"transport":"tcp","mode":"TCP Client","targets":[]}'::jsonb,'disabled','collector',${admin})`;
        await db.begin(async transaction => {
            // Block the initial business query without blocking Redis. The SSE
            // must already own a subscribed connection while its query waits.
            await transaction`LOCK TABLE link IN ACCESS EXCLUSIVE MODE`;
            opening = subscribe(`/v1/link/events?keyword=${marker}`);
            void opening.catch(() => {});
            const deadline = Date.now() + 10000;
            for (;;) {
                const clients = (await pubsubClients()).filter(client => !before.has(client.id) && Number(client.sub) >= 3);
                const [blocked] = await db`SELECT EXISTS (
                    SELECT 1 FROM pg_locks
                    WHERE relation='link'::regclass AND NOT granted
                ) AS waiting`;
                if (clients.length === 1 && blocked.waiting) {
                    clientId = clients[0].id;
                    break;
                }
                assert(Date.now() < deadline, 'SSE must subscribe before its blocked initial snapshot query');
                await Bun.sleep(10);
            }
            await transaction`UPDATE link SET name=${latest} WHERE id=${raceId}`;
            // Publish while the snapshot is suspended: it cannot disappear in
            // the subscribe/snapshot gap, even if PostgreSQL took an old view.
            assert(Number(await change('link', raceId)) > 0);
        });
        assert(opening);
        first = await opening;
        let answer = JSON.parse((await first.next()).data);
        while (!(answer.data?.list ?? answer.delta?.upsert ?? []).some((row: {id:string;name:string}) => row.id === raceId && row.name === latest)) {
            assert.equal(answer.code, 0);
            answer = JSON.parse((await first.next()).data);
        }
        await first.expectQuiet(1000);
        const firstClients = new Set((await pubsubClients()).map(client => client.id));
        second = await subscribe(`/v1/link/events?keyword=${marker}`);
        const secondInitial = await snapshot(second);
        assert.equal(secondInitial.list[0].name, latest);
        const secondClients = (await pubsubClients()).filter(client => !firstClients.has(client.id));
        assert.equal(secondClients.length, 1, 'each SSE must own a separate Redis subscription connection');
        assert.notEqual(secondClients[0].id, clientId);
        // Interrupt only the first subscription, not Redis command pools or the
        // second SSE. Identical persisted state still needs ONE recovery frame.
        assert.equal(Number(await redis.send('CLIENT', ['KILL', 'ID', clientId])), 1);
        const recovered = JSON.parse((await first.next()).data);
        assert.equal(recovered.code, 0);
        assert(recovered.data?.list, 'recovery must be a complete snapshot, not a delta');
        assert.deepEqual(recovered.data, secondInitial);
        await Promise.all([first.expectQuiet(1600), second.expectQuiet(1600)]);
        console.log('PASS SSE subscribes before snapshot, closes the snapshot race, and independently recovers once even for identical state');
    } finally {
        if (!first && opening) first = await opening.catch(() => undefined);
        await first?.close();
        await second?.close();
        await db`DELETE FROM outbox_event WHERE aggregate_id=${raceId}`;
        await db`DELETE FROM link WHERE id=${raceId}`;
    }
}
async function verifyRuntimeAggregation() {
    await redis.send('SET', [runtimeOwnerKey, runtimeOwner, 'EX', '300']);
    const endpoint = {mode:'TCP Client',targets:runtimeTargets.map((id,index) => ({id,name:`broker-${index}`,ip:'127.0.0.1',port:1883+index,status:'enabled'}))};
    await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,created_by) VALUES(${runtimeLink},${runtimeTag},'MQTT',${endpoint}::jsonb,'disabled','collector',${admin})`;
    type State = {state:string;reason:string;error:string;last_activity_at_ms:string};
    const connected = (time:string):State => ({state:'connected',reason:'',error:'',last_activity_at_ms:time});
    const standby:State = {state:'reconnecting',reason:'target_owned',error:'目标已由其他 Collector Worker 持有',last_activity_at_ms:'0'};
    const failed:State = {state:'reconnecting',reason:'connection_refused',error:'连接被拒绝',last_activity_at_ms:'1000'};
    const fatal:State = {...failed,state:'error',reason:'invalid_address',error:'目标地址无效'};
    async function worker(index:number, state:State, targets:State[], connections:number) {
        const fields:Record<string,string> = {state:state.state,state_reason:state.reason,error:state.error,connection_count:String(connections),remote_endpoints:connections ? `127.0.0.1:${1883+index}` : '',last_activity_at_ms:state.last_activity_at_ms,updated_at_ms:String(Date.now())};
        for (const [position,target] of targets.entries()) {
            for (const [name,value] of Object.entries(target)) fields[`target:${runtimeTargets[position]}:${name}`] = value;
        }
        await redis.send('HSET',[runtimeKeys[index],...Object.entries(fields).flat()]);
    }
    async function detail() { return (await request('GET',`/v1/link?keyword=${runtimeTag}`)).list[0]; }
    function healthy(item:any) {
        assert.equal(item.runtime.state,'connected');
        assert.equal(item.runtime.reason,'');
        assert.equal(item.runtime.error,'');
        assert.equal(item.runtime.clientCount,2);
        for (const target of item.endpoint.targets) {
            assert.equal(target.runtime.state,'connected');
            assert.equal(target.runtime.reason,'');
            assert.equal(target.runtime.error,'');
        }
        assert.equal(Date.parse(item.endpoint.targets[0].runtime.lastActivityAt),1000);
        assert.equal(Date.parse(item.endpoint.targets[1].runtime.lastActivityAt),5000);
    }
    await worker(0,connected('1234'),[connected('1234'),standby],1);
    await worker(1,standby,[standby,connected('5678')],1);
    healthy(await detail());
    const stream = await subscribe(`/v1/link/events?keyword=${runtimeTag}`);
    healthy((await snapshot(stream)).list[0]);
    // 换到另一 Worker 后仍取同一目标的完整有效状态，不能依赖 Redis 扫描顺序。
    await worker(0,standby,[standby,connected('5678')],1);
    await worker(1,connected('1234'),[connected('1234'),standby],1);
    healthy(await detail());
    async function changed() {
        const next = delta(stream);
        await change('link', runtimeLink);
        const {upsert, remove} = await next;
        assert.deepEqual(remove, []);
        assert.equal(upsert.length, 1);
        assert.equal(upsert[0].id, runtimeLink);
        return upsert[0];
    }
    await worker(0,standby,[standby,standby],0);
    await worker(1,failed,[failed,fatal],0);
    const disconnected = await changed();
    assert.equal(disconnected.runtime.state,'reconnecting');
    assert.equal(disconnected.runtime.reason,'connection_refused');
    assert.equal(disconnected.runtime.error,'连接被拒绝');
    assert.equal(disconnected.runtime.clientCount,0);
    assert.equal(disconnected.endpoint.targets[0].runtime.reason,'connection_refused');
    assert.equal(disconnected.endpoint.targets[1].runtime.state,'error');
    assert.equal(disconnected.endpoint.targets[1].runtime.error,'目标地址无效');
    await redis.send('DEL',[runtimeKeys[1]]);
    const waiting = await changed();
    for (const state of [waiting.runtime,...waiting.endpoint.targets.map((target:any) => target.runtime)]) {
        assert.equal(state.state,'connecting');
        assert.equal(state.reason,'');
        assert.equal(state.error,'');
    }
    await redis.send('DEL',[runtimeOwnerKey]);
    const stopped = await changed();
    assert.equal(stopped.runtime.state,'stopped');
    assert.equal(stopped.runtime.clientCount,0);
    assert(stopped.endpoint.targets.every((target:any) => target.runtime.state === 'stopped'));
    await redis.send('SET',[runtimeOwnerKey,runtimeOwner,'EX','300']);
    await worker(0,connected('1234'),[connected('1234'),standby],1);
    await worker(1,standby,[standby,connected('5678')],1);
    healthy(await changed());
    // Another link's change is not visible on this filtered page.
    await change('link', crypto.randomUUID());
    await stream.expectQuiet(1000);
    await stream.close();
}
try {
    await request('GET','/v1/link',undefined,11004);
    actor = admin;
    await verifyRuntimeAggregation();
    await verifySubscriptionRaceAndRecovery();
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
    const renamed = delta(subscription);
    await request('PUT',`/v1/link/${link}`,{...configuration,name:`${tag}_updated`});
    assert.deepEqual((await renamed).upsert.map(row => [row.id, row.name]), [[link, `${tag}_updated`]]);
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
    await change('packet-debug', link);
    const debug = await debugPush;
    assert.equal(debug[0].id,acquisition);
    assert.equal(debug[0].packets[0].payload_hex,'00FF');
    await debugSubscription.expectQuiet(1000);
    // Packets of another link must not reload this debug view.
    await change('packet-debug', crypto.randomUUID());
    await debugSubscription.expectQuiet(1000);
    await request('PUT', `/v1/link/${link}`, { ...configuration, name: `${tag}_debug_open` });
    assert.equal((await delta(debugSubscription)).upsert[0].name, `${tag}_debug_open`);
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
    assert.equal((await delta(deniedDebug)).upsert[0].name, `${tag}_query_only`);
    await deniedDebug.close();
    await request('POST','/v1/link',{name:`${tag}_edge`,execution:'edge',edge_node_id:crypto.randomUUID(),protocol:'Modbus',endpoint:{transport:'serial',interface:'/dev/ttyS0',baud_rate:299},status:'disabled'},15002);
    await request('DELETE',`/v1/link/${link}`);
    assert.equal((await request('GET',`/v1/link?keyword=${tag}`)).total,0);
    assert.equal((await fetch(apiBase+'/v1/link')).status,401);
    console.log('PASS link HTTP/SSE: multi-worker target aggregation, failure/recovery, CRUD, typed update validation, committed outbox, debug push and original owner checks');
} finally {
    await redis.send('DEL',[runtimeOwnerKey,...runtimeKeys]);
    await db`DELETE FROM outbox_event WHERE aggregate_id=${runtimeLink}`;
    await db`DELETE FROM link WHERE id=${runtimeLink}`;
    await redis.send('DEL',[`iot:debug:v4:link:${link}`,`iot:debug:v4:acquisition:${acquisition}`,`iot:debug:v4:acquisition:${acquisition}:packets`,`iot:debug:v4:packet:${packet}`]);
    if (link) { await db`DELETE FROM outbox_event WHERE aggregate_id=${link}`; await db`DELETE FROM link WHERE id=${link}`; }
    await db`DELETE FROM sys_user_role WHERE user_id=${other}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${other}`;
    for (const stream of streams) await stream.close();
    redis.close();
    await db.close();
}
