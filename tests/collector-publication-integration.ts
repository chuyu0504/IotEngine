import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import net from 'node:net';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const reader = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const tag = `publication-${crypto.randomUUID()}`;
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
    iss: 'iot-engine',
    aud: 'iot-engine-web',
    sub: admin,
    user_id: admin,
    username: 'business-orm-test',
    token_type: 'access',
    iat: now,
    exp: now + 3600,
})}`;
const token = `${unsigned}.${createHmac(
    'sha256',
    'architecture-test-only-access-secret-000000000',
).update(unsigned).digest('base64url')}`;
const adminHeaders = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
};

type RequestHeaders = Record<string, string>;

async function jsonRequest(
    method: string,
    path: string,
    body?: unknown,
    expectedStatus = 200,
    headers: RequestHeaders = adminHeaders,
) {
    const response = await fetch(apiBase + path, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(15000),
    });
    const text = await response.text();
    assert.equal(response.status, expectedStatus, `${method} ${path}: ${text}`);
    const result = text.length === 0 ? undefined : JSON.parse(text);
    if (expectedStatus >= 200 && expectedStatus < 300)
        assert.equal(result?.code, 0, `${method} ${path}: ${text}`);
    return result;
}


async function until(check: () => Promise<boolean>, description: string) {
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(100);
    }
    throw new Error(description);
}
const probe = net.createServer();
await new Promise<void>((resolve) => probe.listen(0, '127.0.0.1', resolve));
const port = (probe.address() as net.AddressInfo).port;
await new Promise<void>((resolve) => probe.close(() => resolve()));
let socket: net.Socket | undefined;
let received = Buffer.alloc(0);
const stream = 'iot:v3:telemetry';
const savedStream = `${stream}:publication-test-backup`;
let fault = false;
let saved = false;
const pointId = crypto.randomUUID();
function frame(sequence: number, sendingSecond = 5, observationMinute = 4, value = 0x34) {
    const body = [0, sequence, 0x26, 0x09, 0x15, 0x03, 0x04, sendingSecond,
        0xf1, 0xf1, 0, 0, 0, 0, 1, 0x49, 0xf0, 0xf0, 0x26, 0x09, 0x15, 0x03, observationMinute,
        0x39, 0x12, 0x12, value];
    const bytes = Buffer.from([0x7e,0x7e,1,0,0,0,0,1,0,0,0x32,0,body.length,2,...body,3]);
    let crc = 0xffff;
    for (const byte of bytes) {
        crc ^= byte;
        for (let bit=0;bit<8;bit++) crc = crc & 1 ? (crc >> 1) ^ 0xa001 : crc >> 1;
    }
    return Buffer.concat([bytes, Buffer.from([crc >> 8, crc & 255])]);
}
try {
    await jsonRequest('POST','/v1/protocol/configs', { protocol:'SL651',name:tag,enabled:true,
        config:{responseMode:'M2',storagePolicy:'report',funcs:[{funcCode:'32',dir:'UP',name:'report',elements:[{
            id:pointId,name:'level',guideHex:'3912',encode:'BCD',length:2,digits:2,
        }]}]}});
    const protocol = (await db`SELECT id FROM protocol_config WHERE name=${tag}`)[0].id;
    await jsonRequest('POST','/v1/link',{name:tag,protocol:'SL651',execution:'collector',status:'enabled',
        endpoint:{transport:'tcp',mode:'TCP Server',ip:'0.0.0.0',port,targets:[]}});
    const link = (await db`SELECT id FROM link WHERE name=${tag}`)[0].id;
    await jsonRequest('POST','/v1/device',{name:tag,device_code:'0000000001',link_id:link,
        protocol_config_id:protocol,status:'enabled',online_timeout:120,remote_control:true,
        timezone:'+00:00',heartbeat:{mode:'OFF'},registration:{mode:'OFF'}});
    const device = (await db`SELECT id FROM device WHERE name=${tag}`)[0].id;
    await jsonRequest('PUT',`/v1/device/${device}/debug`,{enabled:true});
    await jsonRequest('PUT',`/v1/link/${link}/debug`,{enabled:true});
    // Debug switches reach the Collector through the outbox and a new runtime
    // snapshot. A station report is sent once, so capture starts only after
    // every Collector Worker applied a snapshot with both switches on.
    await until(async () => {
        const version = await redis.get('iot:config:runtime:active-version');
        if (!version) return false;
        const snapshot = `iot:config:runtime:${version}`;
        if (await redis.send('HGET', [`${snapshot}:device:${device}`, 'debug_enabled']) !== '1' ||
            await redis.send('HGET', [`${snapshot}:link:${link}`, 'debug_enabled']) !== '1') return false;
        const collectors = await redis.send('KEYS', ['iot:runtime:collector:*']) as string[];
        const applied = await Promise.all(collectors.map(key => redis.send('HGET', [key, 'version'])));
        return collectors.length > 0 && applied.every(value => value === version);
    }, 'collectors did not apply the debug switches');
    await until(async () => {
        const candidate = net.createConnection({host:'127.0.0.1',port});
        const connected = await new Promise<boolean>((resolve) => {
            candidate.once('connect',()=>resolve(true)); candidate.once('error',()=>resolve(false));
        });
        if (!connected) {candidate.destroy();return false;}
        socket=candidate;
        socket.on('data',(data)=>{received=Buffer.concat([received,data]);});
        socket.on('error',()=>{});
        return true;
    },'collector listener did not start');
    const ingress = reader.send('XREAD',['BLOCK','10000','STREAMS',stream,'$']);
    await Bun.sleep(100);
    socket!.write(frame(1));
    await until(async()=>received.length>=18,'first report was not acknowledged');
    await until(async()=>Number((await db`SELECT count(*) AS n FROM device_data WHERE device_id=${device}`)[0].n)>0,
        'first report was not persisted');
    const raw = await ingress as Record<string, [string, string[]][]>;
    assert(raw?.[stream]?.length, "raw telemetry publication was not observed");
    const fields = raw[stream][0][1];
    const first = fields[fields.indexOf("message_id") + 1];
    assert.match(first,/^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);
    const debugRows = async (scope: string, id: string) => {
        const rounds = await redis.send('ZRANGE',[`iot:debug:v4:${scope}:${id}`,'0','-1']) as string[];
        const ids = (await Promise.all(rounds.map(round=>redis.send('ZRANGE',[`iot:debug:v4:acquisition:${round}:packets`,'0','-1'])))).flat() as string[];
        const rows = await Promise.all(ids.map(async id=>[id,await redis.send('HGETALL',['iot:debug:v4:packet:'+id])] as const));
        return rows.map(([,values])=>values);
    };
    for (const [scope,id] of [['device',device],['link',link]]) {
        await until(async()=>(await debugRows(scope,id)).some(row=>row.parsed_json),'debug response parsing missing');
        const rows=await debugRows(scope,id);
        const receivedRows=rows.filter(row=>row.payload_hex===frame(1).toString('hex').toUpperCase());
        assert.equal(receivedRows.length,1,`${scope}: Redis duplicated the SL651 receive during persistence`);
        assert.equal(receivedRows[0].storage_status,undefined);
        assert.equal(receivedRows[0].history_id,undefined);
        assert(receivedRows[0].parsed_json);
        assert.equal(new Set(rows.map(row=>row.event_id)).size,rows.length,`${scope}: duplicate status events`);
        assert(rows.some(row=>row.direction==='TX' && row.reply_to_packet_id===receivedRows[0].event_id),'ACK must reference the received packet');
    }
    const initial = (await db`SELECT id,extract(epoch FROM report_time)*1000 AS sample_time FROM device_data WHERE device_id=${device}`)[0];
    assert.equal(Number(initial.sample_time), Date.parse('2026-09-15T03:04:00Z'));
    for (const second of [0x10, 0x15]) {
        // 重连清空会话缓存；清除 Redis 回执后仍必须由数据库唯一键保护历史。
        socket!.destroy();
        if (second === 0x15) assert.equal(Number(await redis.send('DEL', [`iot:telemetry:fanout:${initial.id}`])), 1,
            'receipt-loss test must remove an existing fanout receipt');
        received = Buffer.alloc(0);
        socket = net.createConnection({host:'127.0.0.1', port});
        socket.on('data', data => {received = Buffer.concat([received, data]);});
        socket.on('error', () => {});
        await new Promise<void>((resolve, reject) => {socket!.once('connect', resolve); socket!.once('error', reject);});
        socket.write(frame(1, second));
        await until(async () => received.length >= 25, 'replay after reconnect was not acknowledged');
        assert.equal(received.readUInt16BE(14), 1, 'ACK must echo the retransmitted serial');
        await until(async () => (await debugRows('device', device)).filter(row => row.direction === 'RX').length >= (second === 0x10 ? 2 : 3), 'replay receive record missing');
    }
    for (const [minute, value] of [[5, 0x34], [5, 0x35]]) {
        received = Buffer.alloc(0);
        socket!.write(frame(1, 0x15, minute, value));
        await until(async () => received.length >= 25, 'distinct observation was not acknowledged');
    }
    await until(async () => Number((await db`SELECT count(*) AS n FROM device_data WHERE device_id=${device}`)[0].n) >= 3, 'new observation or corrected value was discarded');
    await Bun.sleep(500);
    assert.equal(Number((await db`SELECT count(*) AS n FROM device_data WHERE device_id=${device}`)[0].n), 3, 'replay created duplicate history');
    const replayRows = (await debugRows('device', device)).filter(row => row.acquisition_id === initial.id);
    assert.equal(replayRows.filter(row => row.direction === 'RX').length, 3, 'replay receives must share one acquisition');
    assert.equal(replayRows.filter(row => row.direction === 'TX').length, 3, 'each replay must retain its ACK');
    console.log('PASS SL651 observation time, reconnect replay, receipt loss, per-packet ACK and corrected values');
    received=Buffer.alloc(0);
    saved=Number(await redis.send('EXISTS',[stream]))===1;
    if(saved) await redis.send('RENAME',[stream,savedStream]);
    await redis.send('SET',[stream,'publication-failure']); fault=true;
    socket!.write(frame(2));
    await Bun.sleep(700);
    assert.equal(received.length,0,'failed publication acknowledged the station');
    await until(async()=>(await debugRows('device',device)).some(row=>row.payload_hex===frame(2).toString('hex').toUpperCase() && row.parsed_json),'history publication failure hid the decoded response');
    await redis.send('DEL',[stream]); fault=false;
    if(saved) {await redis.send('RENAME',[savedStream,stream]);saved=false;}
    socket!.write(frame(2));
    await until(async()=>received.length>=18,'retransmitted report was not acknowledged after recovery');
    console.log('PASS live SL651 TCP report publication preserves acquisition identity, withholds ACK on Redis failure and acknowledges retry');
} finally {
    socket?.destroy();
    if(fault) await redis.send('DEL',[stream]);
    if(saved) await redis.send('RENAME',[savedStream,stream]);
    await db`UPDATE device SET deleted_at=NOW(),updated_at=NOW() WHERE name=${tag}`;
    await db`UPDATE link SET deleted_at=NOW(),updated_at=NOW() WHERE name=${tag}`;
    await db`UPDATE protocol_config SET deleted_at=NOW(),updated_at=NOW() WHERE name=${tag}`;
    reader.close(); redis.close(); await db.close();
}
