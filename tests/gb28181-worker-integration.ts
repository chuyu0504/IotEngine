import assert from 'node:assert/strict';
import { createHash, createHmac, randomUUID } from 'node:crypto';
import { createSocket } from 'node:dgram';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription } from './sse-fixture';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const streams: Awaited<ReturnType<typeof openSnapshotSubscription>>[] = [];
const sipPort = Number(process.env.GB_TEST_SIP_PORT);
assert(Number.isInteger(sipPort) && sipPort > 0 && sipPort <= 65535, 'GB_TEST_SIP_PORT is not set; run through the CTest gb28181 fixture');
const socket = createSocket('udp4');
const device = '34020000001320000091';
const realm = '3402000000';
const platform = '34020000002000000001';
const md5 = (text: string) => createHash('md5').update(text).digest('hex');
const inbox: string[] = [];
socket.on('message', data => inbox.push(data.toString()));
await new Promise<void>(resolve => socket.bind(0, '127.0.0.1', resolve));
const address = socket.address();
assert(typeof address === 'object');
let sequence = 0;
async function until<T>(read: () => Promise<T | undefined>, label: string, timeout = 15000): Promise<T> {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        const value = await read();
        if (value !== undefined) return value;
        await Bun.sleep(25);
    }
    throw new Error(label);
}
async function exchange(method: string, body = '', authorization = '') {
    const cseq = ++sequence;
    const request = `${method} sip:${realm} SIP/2.0\r\n` +
        `Via: SIP/2.0/UDP 127.0.0.1:${address.port};branch=z9hG4bK-fixture-${cseq}\r\n` +
        `From: <sip:${device}@${realm}>;tag=fixture\r\nTo: <sip:${device}@${realm}>\r\n` +
        `Contact: <sip:${device}@127.0.0.1:${address.port}>\r\n` +
        `Call-ID: gb-worker-fixture-${cseq}\r\nCSeq: ${cseq} ${method}\r\nExpires: 180\r\n` +
        authorization + (body ? 'Content-Type: Application/MANSCDP+xml\r\n' : '') +
        `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`;
    socket.send(request, sipPort, '127.0.0.1');
    return until(async () => {
        const index = inbox.findIndex(value => value.startsWith('SIP/2.0 ') && value.includes(`CSeq: ${cseq} ${method}\r\n`));
        return index < 0 ? undefined : inbox.splice(index, 1)[0];
    }, `No ${method} response for ${cseq}`, 35000);
}
async function register() {
    const challenge = await exchange('REGISTER');
    assert.match(challenge, /^SIP\/2\.0 401 /);
    const nonce = /nonce="([^"]+)"/.exec(challenge)?.[1];
    assert(nonce);
    const uri = `sip:${realm}`;
    const response = md5(`${md5(`${device}:${realm}:test`)}:${nonce}:00000001:fixture:auth:${md5(`REGISTER:${uri}`)}`);
    const authorization = `Authorization: Digest username="${device}", realm="${realm}", nonce="${nonce}", uri="${uri}", response="${response}", algorithm=MD5, qop=auth, nc=00000001, cnonce="fixture"\r\n`;
    assert.match(await exchange('REGISTER', '', authorization), /^SIP\/2\.0 200 /);
}
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:'00000000-0000-7000-8000-000000000002',user_id:'00000000-0000-7000-8000-000000000002',username:'admin',token_type:'access',iat:now,exp:now+3600})}`;
const jwt = `${unsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
async function request(method: string, path: string, data?: unknown, code = 0) {
    const response = await fetch(apiBase+path,{method,headers:{Authorization:`Bearer ${jwt}`,'Content-Type':'application/json',Accept:'application/json'},body:data === undefined ? undefined : JSON.stringify(data),signal:AbortSignal.timeout(60000)});
    const reply = await response.json();
    assert.equal(reply.code,code,`${method} ${path}: ${JSON.stringify(reply)}`);
    assert.equal(response.ok,code === 0);
    return reply.data;
}
async function subscribe(path: string) {
    const stream = await openSnapshotSubscription(path,jwt); streams.push(stream); return stream;
}
async function snapshot(stream: Awaited<ReturnType<typeof subscribe>>) {
    let event = await stream.next();
    while (event.event === 'heartbeat') event = await stream.next();
    assert.equal(event.event,'snapshot');
    return JSON.parse(event.data).data;
}
try {
    const controls = await until(async () => {
        const keys = await redis.send('KEYS', ['iot:gb28181:control:worker:*']) as string[];
        return keys.length === 2 ? keys : undefined;
    }, 'both Collector control consumers must start');
    await db`INSERT INTO gb28181_device(id,name,custom_name,last_seen_at) VALUES (${device},'Previous camera','Preserved operator name',NOW())`;
    await register();
    const rows = await db`SELECT online,custom_name FROM gb28181_device WHERE id=${device}`;
    assert.equal(rows[0]?.online, true, 'REGISTER 200 must follow committed online projection');
    assert.equal(rows[0]?.custom_name, 'Preserved operator name', 'fresh Collector registration must preserve durable metadata');
    const ownerKey = `iot:gb28181:owner:${device}`;
    const owner = await redis.get(ownerKey);
    assert(owner?.includes(':session:'));
    const ownerMatch = /^(.*):collector:(\d+):session:/.exec(owner);
    assert(ownerMatch);
    // Entries written straight into the projection stream need the same wake the
    // production publication script emits; consumers do not poll the stream.
    const wakeStream = `iot:service:${ownerMatch[1]}:work-available`;
    async function appendProjection(fields: string[]) {
        const entry = await redis.send('XADD', ['iot:gb28181:projection', '*', ...fields]) as string;
        await redis.send('XADD', [wakeStream, 'MAXLEN', '~', '1000', '*', 'task', 'gb28181-projector']);
        return entry;
    }
    console.log('PASS REGISTER 200 follows DB commit and preserves preexisting metadata');

    const health = await request('GET','/v1/gb28181/health');
    assert.equal(health.enabled,true);
    const deviceSubscription = await subscribe('/v1/gb28181/devices/events');
    assert((await snapshot(deviceSubscription)).items.some((item: {id: string}) => item.id === device));
    for (const [path,data] of [
        [`/v1/gb28181/devices/${device}/channels/${device}/ptz/left`,{speed:'80'}],
        [`/v1/gb28181/devices/${device}/channels/${device}/ptz/invalid`,{speed:80}],
        [`/v1/gb28181/devices/${device}/channels/${device}/ptz/position/set`,{pan:361,tilt:0,zoom:1}],
        ['/v1/gb28181/previews/%20/stop',undefined],
    ] as const) await request('POST',path,data,10001);
    await request('POST',`/v1/gb28181/devices/${'x'.repeat(129)}/catalog/query`,undefined,10001);
    assert.equal((await request('POST',`/v1/gb28181/devices/${device}/catalog/query`)).sent,true);
    assert.equal((await request('POST',`/v1/gb28181/devices/${device}/channels/${device}/ptz/left`,{speed:80})).speed,80);
    await until(async()=>inbox.find(frame=>frame.startsWith('MESSAGE ') && frame.includes('<PTZCmd>')), 'HTTP PTZ must reach original SIP transport');
    assert.equal(await redis.get(ownerKey),owner,'HTTP control must retain the accepting Collector');

    const keepalive = `<?xml version="1.0"?><Notify><CmdType>Keepalive</CmdType><SN>1</SN><DeviceID>${device}</DeviceID><Status>OK</Status></Notify>`;
    assert.match(await exchange('MESSAGE', keepalive), /^SIP\/2\.0 200 /);
    assert.equal(await redis.get(ownerKey), owner, 'Keepalive must keep the accepting Collector and connection generation');
    await request('PUT',`/v1/gb28181/devices/${device}/name`,{name:'Committed worker name'});
    assert.equal((await db`SELECT custom_name FROM gb28181_device WHERE id=${device}`)[0]?.custom_name,'Committed worker name','HTTP success must follow metadata commit');
    assert.equal(await redis.get(ownerKey),owner);
    let changedDevice = (await snapshot(deviceSubscription)).items.find((item: {id: string}) => item.id === device);
    while (changedDevice?.custom_name !== 'Committed worker name') changedDevice = (await snapshot(deviceSubscription)).items.find((item: {id: string}) => item.id === device);
    await deviceSubscription.close();
    assert.equal((await fetch(`${apiBase}/v1/gb28181/health`)).status,401);
    console.log('PASS HTTP rename routes to connection owner and returns after persistence; SSE change is visible');
    const preview = await request('POST',`/v1/gb28181/devices/${device}/channels/${device}/preview/start`);
    assert(preview.sent && preview.session_id && preview.stream_id);
    assert(preview.lease_timeout_seconds > 0);
    assert.equal(await redis.get(ownerKey),owner,'preview must retain original Collector ownership');
    await until(async () => inbox.find(frame => frame.startsWith('INVITE ') && frame.includes(preview.stream_id)) ?? inbox.find(frame => frame.startsWith('INVITE ')), 'preview INVITE must reach the original SIP socket');
    assert.equal((await request('POST',`/v1/gb28181/previews/${preview.session_id}/heartbeat`)).sent,true);
    const stopped = await request('POST',`/v1/gb28181/previews/${preview.session_id}/stop`);
    assert.equal(stopped.session_id,preview.session_id);
    assert.equal(stopped.stopped,true);
    assert.equal(stopped.rtp_server_closed,true);
    console.log('PASS HTTP preview start/lease/stop reaches original Collector and closes its RTP server');


    // Exercise the actual nonempty batch SQL and retry acknowledgement path.
    async function projectList(change: string, fields: string[]) {
        const projection = randomUUID();
        await appendProjection([
            'projection_id',projection,'owner_token',owner!, 'schema_version','1',
            'event_type','gb28181.device','change',change,'device_id',device,
            'name','Camera','online','1','last_seen_at',new Date().toISOString(),...fields]);
        assert.equal(await until(async () =>
            (await redis.get(`iot:gb28181:projection:done:${projection}`)) ?? undefined,
            `${change} projection must commit and acknowledge`), '1');
    }
    await projectList('catalog', ['channel_count','2',
        'channel.0.id','channel-1','channel.0.name',"Camera's entrance",'channel.0.online','1',
        'channel.0.manufacturer','测试厂商','channel.0.ptz_type','1',
        'channel.1.id','channel-2','channel.1.name','Second channel','channel.1.online','0','channel.1.ptz_type','0']);
    assert.equal((await db`SELECT count(*)::int AS count FROM gb28181_channel WHERE device_id=${device}`)[0].count,2);
    await db`UPDATE gb28181_channel SET custom_name='Operator name' WHERE device_id=${device} AND id='channel-1'`;
    await projectList('catalog', ['channel_count','1',
        'channel.0.id','channel-1','channel.0.name','Updated channel','channel.0.online','1','channel.0.ptz_type','1']);
    const channels = await db`SELECT name,custom_name FROM gb28181_channel WHERE device_id=${device}`;
    assert.equal(channels.length,1);
    assert.equal(channels[0].name,'Updated channel');
    assert.equal(channels[0].custom_name,'Operator name');
    const retiredProjection = randomUUID();
    await appendProjection([
        'projection_id',retiredProjection,'owner_token',owner!, 'schema_version','1',
        'event_type','gb28181.device','change','records','device_id',device,
        'name','Camera','online','1','last_seen_at',new Date().toISOString(),'channel_count','0']);
    assert.equal(await until(async () =>
        (await redis.get(`iot:gb28181:projection:done:${retiredProjection}`)) ?? undefined,
        'retired records projection must be acknowledged'), '0', 'retired records projection must not be applied');
    console.log('PASS nonempty catalog projections commit, remove stale channels and preserve operator names; retired change kinds are dropped');

    const wrongStream = controls.find(key => key !== `iot:gb28181:control:worker:${ownerMatch[1]}:${ownerMatch[2]}`);
    assert(wrongStream);
    const requestId = randomUUID();
    await redis.send('XADD',[wrongStream,'*','request_id',requestId,'owner_key',ownerKey,'owner_token',owner!,'operation','rename_device','payload',JSON.stringify({device_id:device,name:'Wrong Worker'}),'deadline_ms',String(Date.now()+15000),'reply_stream',`iot:gb28181:reply:${requestId}`]);
    const result = await until(async () => (await redis.get(`iot:gb28181:result:${requestId}`)) ?? undefined, 'wrong Collector must reject command');
    assert.match(result, /^error\n.*409/);
    assert.equal((await db`SELECT custom_name FROM gb28181_device WHERE id=${device}`)[0]?.custom_name, 'Committed worker name');
    console.log('PASS another Collector rejects the original connection token');

    let unlock: (() => void) | undefined;
    let lockReady: (() => void) | undefined;
    const locked = new Promise<void>(resolve => { lockReady = resolve; });
    const releaseLock = new Promise<void>(resolve => { unlock = resolve; });
    const blocker = db.begin(async transaction => {
        await transaction`SELECT pg_advisory_xact_lock(hashtextextended(${device},28181))`;
        lockReady!();
        await releaseLock;
    });
    await locked;
    const delayedRename = request('PUT',`/v1/gb28181/devices/${device}/name`,{name:'Delayed committed name'}).catch(error => String(error));
    let heartbeat: Promise<string> | undefined;
    try {
        const pendingId = await until(async () => {
            const entries = await redis.send('XRANGE',['iot:gb28181:projection','-','+']) as Array<[string,string[]]>;
            return entries.find(([,fields]) => fields.includes('Delayed committed name'))?.[0];
        }, 'blocked metadata projection must be published');
        // Cross the former 30-second producer timeout while DB commit is blocked.
        await Bun.sleep(31000);
        heartbeat = exchange('MESSAGE', keepalive);
        await Bun.sleep(500);
        const later = await redis.send('XRANGE',['iot:gb28181:projection',`(${pendingId}`,'+']) as Array<[string,string[]]>;
        assert(!later.some(([,fields]) => fields.includes(device) && fields.includes('status')),
            'Collector published a newer Status before the preceding increment was committed');
    } finally {
        unlock!();
        await blocker;
    }
    if (heartbeat) assert.match(await heartbeat, /^SIP\/2\.0 200 /);
    await delayedRename;
    assert.equal((await db`SELECT custom_name FROM gb28181_device WHERE id=${device}`)[0]?.custom_name, 'Delayed committed name');
    console.log('PASS DB stall cannot let a later Status overtake an uncommitted metadata change');

    const projectionId = randomUUID();
    let originalOrder = '';
    // Hold the aggregate lock while arranging the state a late PEL retry sees:
    // a newer projection has already committed before this older entry resumes.
    await db.begin(async transaction => {
        await transaction`SELECT pg_advisory_xact_lock(hashtextextended(${device},28181))`;
        const entry = await appendProjection([
            'projection_id',projectionId,'owner_token',owner!,
            'schema_version','1','event_type','gb28181.device','change','device_name',
            'device_id',device,'name',device,'custom_name','Stale retried name',
            'online','1','last_seen_at',new Date().toISOString(),'channel_count','0']) as string;
        originalOrder = entry;
        await transaction`UPDATE gb28181_device SET custom_name='Newer committed name',
            projection_cursor=split_part(${entry},'-',1)::numeric*18446744073709551616+
                split_part(${entry},'-',2)::numeric+1 WHERE id=${device}`;
    });
    await until(async () => (await redis.get(`iot:gb28181:projection:done:${projectionId}`)) ?? undefined,
        'older projection must be acknowledged without replaying its changes');
    assert.equal((await db`SELECT custom_name FROM gb28181_device WHERE id=${device}`)[0]?.custom_name, 'Newer committed name');
    console.log('PASS late projection retry cannot overwrite a newer committed value');

    // Simulate a completion receipt expiring after the stream entry was ACKed
    // and deleted. Redelivery gets a new transport ID but keeps its DB order.
    const receiptKey = `iot:gb28181:projection:done:${projectionId}`;
    await redis.del(receiptKey);
    const serviceSource = await Bun.file('service/features/gb28181/gb28181.service.h').text();
    const publishLua = serviceSource.match(/\bkPublishProjectionScript\s*=\s*R"lua\(([\s\S]*?)\)lua"/)?.[1];
    assert(publishLua, 'production projection publication Lua must be available');
    const replayFields = [
        'projection_id',projectionId,'owner_token',owner!,
        'schema_version','1','event_type','gb28181.device','change','device_name',
        'device_id',device,'name',device,'custom_name','Expired receipt stale replay',
        'online','1','last_seen_at',new Date().toISOString(),'channel_count','0'];
    const replay = await redis.send('EVAL', [publishLua,'3','iot:gb28181:projection',
        `iot:gb28181:projection:sent:${projectionId}`,wakeStream,originalOrder,originalOrder,
        'gb28181-projector','1000',...replayFields]) as string[];
    assert.equal(replay[0], originalOrder);
    assert.notEqual(replay[1], originalOrder);
    assert.equal(await until(async () => (await redis.get(receiptKey)) ?? undefined,
        'redelivery must reconstruct an expired completion receipt'), '1');
    assert.equal((await db`SELECT custom_name FROM gb28181_device WHERE id=${device}`)[0]?.custom_name,
        'Newer committed name', 'new transport ID must not turn an old projection into a newer update');
    console.log('PASS expired projection receipt is reconstructed without replaying an old update');

    const invalidProjection = randomUUID();
    const invalidFields = [...replayFields];
    invalidFields[1] = invalidProjection;
    await appendProjection([...invalidFields,
        'projection_order','18446744073709551616-0']);
    assert.equal(await until(async () =>
        (await redis.get(`iot:gb28181:projection:done:${invalidProjection}`)) ?? undefined,
        'overflowing projection order must be rejected instead of poisoning the consumer'), '0');

    const probeStream = `fixture:gb-projection:${randomUUID()}`;
    const probeSent = `${probeStream}:sent`;
    const probeWake = `${probeStream}:wake`;
    const probeProjection = randomUUID();
    try {
        const publish = (order = '', current = '') => redis.send('EVAL',
            [publishLua,'3',probeStream,probeSent,probeWake,order,current,'gb28181-projector','1000',
                'projection_id',probeProjection]) as Promise<string[]>;
        const first = await publish();
        // Lost first response: retry with no local IDs must recover the marker.
        assert.deepEqual(await publish(), first);
        assert.equal(Number(await redis.send('XLEN',[probeStream])), 1);
        await redis.send('XDEL',[probeStream,first[1]!]);
        const recovered = await publish(first[0],first[1]);
        assert.equal(recovered[0], first[0]);
        assert.notEqual(recovered[1], first[1]);
        await redis.del(probeSent);
        assert.deepEqual(await publish(recovered[0],recovered[1]), recovered,
            'expired sent marker must not lose local ordering or duplicate a pending entry');
        await redis.send('XDEL',[probeStream,recovered[1]!]);
        await redis.del(probeSent);
        const withoutMarker = await publish(recovered[0],recovered[1]);
        assert.equal(withoutMarker[0], first[0]);
        const entries = await redis.send('XRANGE',[probeStream,'-','+']) as Array<[string,string[]]>;
        assert.equal(entries.length, 1);
        const fields = entries[0]![1];
        assert.equal(fields[fields.indexOf('projection_order')+1], first[0]);
        console.log('PASS production publication recovers lost receipts and expired dedup markers');
    } finally {
        await redis.send('DEL',[probeStream,probeSent,probeWake]);
    }

    const controlKey = `iot:gb28181:control:worker:${ownerMatch[1]}:${ownerMatch[2]}`;
    const controlTtl = Number(await redis.send('TTL',[controlKey]));
    assert(controlTtl > 0 && controlTtl <= 600, 'new control stream must have bounded lifetime before a crash');

    await redis.del(ownerKey);
    await Bun.sleep(13000);
    assert.equal(await redis.get(ownerKey), null, 'expired ownership must never be recreated by renewal');
    const invalid = await exchange('MESSAGE', keepalive);
    assert(!invalid.startsWith('SIP/2.0 200 '), 'lost owner must stop accepting old session traffic');
    await until(async () => (await db`SELECT online FROM gb28181_device WHERE id=${device}`)[0]?.online === false ? true : undefined, 'lost owner must become offline durably');
    console.log('PASS lease loss fences old SIP session and clears durable online status');
} finally {
    for (const stream of streams) await stream.close();
    socket.close();
    await db.close();
    redis.close();
}
