import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const user = crypto.randomUUID(), role = crypto.randomUUID();
const tag = `access_http_${crypto.randomUUID()}`;
let model = '', link = '', device = '', key = '', webhook = '';
function token(userId: string) {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:userId,user_id:userId,username:userId,token_type:'access',iat:now,exp:now+3600})}`;
    return `${unsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
async function request(method: string, path: string, data?: unknown, code = 0, actor: string | null = admin, raw = false) {
    const headers = new Headers({'Content-Type':'application/json',Accept:'application/json'});
    if (actor) headers.set('Authorization',`Bearer ${token(actor)}`);
    const response = await fetch(apiBase+path,{method,headers,body:data === undefined ? undefined : raw ? String(data) : JSON.stringify(data),signal:AbortSignal.timeout(15000)});
    const reply = await response.json();
    assert.equal(reply.code,code,`${method} ${path}: ${JSON.stringify(reply)}`);
    assert.equal(response.ok,code === 0);
    return reply.data;
}
async function publicList(accessKey: string, status = 200) {
    const response = await fetch(`${apiBase}/open-api/device/list`, {
        headers: {'X-Access-Key':accessKey, Accept:'application/json'},
        signal: AbortSignal.timeout(10000),
    });
    assert.equal(response.status, status);
    return response.json();
}
async function publicStream(accessKey: string) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(),10000);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
        const response = await fetch(`${apiBase}/open-api/device/list`, {
            headers:{'X-Access-Key':accessKey,Accept:'text/event-stream'},signal:controller.signal,
        });
        assert.equal(response.status,200);
        assert(response.headers.get('content-type')?.includes('text/event-stream'));
        assert(response.body);
        reader = response.body.getReader();
        const decoder = new TextDecoder();
        let pending = '';
        for (;;) {
            const part = await reader.read();
            assert(!part.done,'public SSE ended before initial data');
            pending += decoder.decode(part.value,{stream:true});
            pending = pending.replaceAll('\r\n','\n');
            let boundary = pending.indexOf('\n\n');
            while (boundary >= 0) {
                const frame = pending.slice(0,boundary);
                pending = pending.slice(boundary+2);
                if (frame.split('\n').includes('event: snapshot'))
                    return JSON.parse(frame.split('\n').filter(line=>line.startsWith('data:')).map(line=>line.slice(5).trimStart()).join('\n'));
                boundary = pending.indexOf('\n\n');
            }
        }
    } finally {
        clearTimeout(timeout);
        controller.abort();
        await reader?.cancel().catch(()=>{});
    }
}
try {
    await request('GET','/api/open-access-key',undefined,11004,null);
    await request('POST','/v1/protocol/configs', {protocol:'Modbus',name:tag,config:{storagePolicy:'report',readInterval:10,byteOrder:'BIG_ENDIAN',registers:[]}});
    model = (await db`SELECT id FROM protocol_config WHERE name=${tag}`)[0].id;
    await request('POST','/v1/link', {execution:'collector',name:tag,protocol:'Modbus',endpoint:{transport:'tcp',mode:'TCP Server',ip:'0.0.0.0',port:59963,targets:[]},status:'disabled'});
    link = (await db`SELECT id FROM link WHERE name=${tag}`)[0].id;
    await request('POST','/v1/device', {name:tag,device_code:tag.replace(/[^a-zA-Z0-9]/g,''),link_id:link,protocol_config_id:model,status:'disabled',online_timeout:120,remote_control:true,modbus_mode:'TCP',slave_id:1,timezone:'+08:00',heartbeat:{mode:'OFF'},registration:{mode:'ASCII',content:tag}});
    device = (await db`SELECT id FROM device WHERE name=${tag}`)[0].id;
    assert((await request('GET','/api/device/options')).some((row: {id:string})=>row.id===device));
    assert(Array.isArray(await request('GET','/api/open-access-key')));
    const created = await request('POST','/api/open-access-key', {name:tag,status:'enabled',scopes:['device:realtime'],deviceIds:[device],remark:'retained',expiresAt:'2099-01-01T00:00:00Z'});
    key = created.id;
    assert.match(created.accessKey, /^ak_[0-9a-f]{48}$/);
    assert((await request('GET','/api/open-access-key')).some((row: {id:string})=>row.id===key));
    const keys = () => request('GET','/api/open-access-key');
    await request('PUT',`/api/open-access-key/${key}`, {name:`${tag}_updated`});
    let stored = (await keys()).find((row:{id:string})=>row.id===key);
    assert.equal(stored.remark, 'retained');
    assert.equal(stored.expiresAt, '2099-01-01T00:00:00Z');
    assert.equal((await db`SELECT created_by FROM open_access_key WHERE id=${key}`)[0].created_by, admin);
    await request('PUT',`/api/open-access-key/${key}`, {remark:null,expiresAt:null});
    stored = (await keys()).find((row:{id:string})=>row.id===key);
    assert.equal(stored.remark, null);
    assert.equal(stored.expiresAt, null);
    for (const configuration of [{name:42},{scopes:[]},{deviceIds:[]},{status:'invalid'}])
        await request('PUT',`/api/open-access-key/${key}`,configuration,19002);
    assert.equal((await keys()).find((row: {id:string}) => row.id === key).lastUsedAt,null);
    const readLogs = () => request('GET',`/api/open-access-log?accessKeyId=${key}&pageSize=10`);
    assert.equal((await readLogs()).total,0);
    assert.equal((await publicList(created.accessKey)).data.list[0].id, device);
    // 后台消费异步落库；仅测试等待提交，产品不轮询 HTTP 或数据库。
    const deadline = Date.now() + 10000;
    while (true) {
        const [usage] = await db`SELECT last_used_at FROM open_access_key WHERE id=${key}`;
        const [logged] = await db`SELECT count(*)::int AS n FROM open_access_log WHERE access_key_id=${key}`;
        if (usage?.last_used_at && logged.n > 0) break;
        assert(Date.now() < deadline, 'access usage and log were not persisted');
        await Bun.sleep(25);
    }
    assert((await keys()).find((row: {id:string}) => row.id === key).lastUsedAt);
    assert((await readLogs()).total >= 1);
    const rotated = await request('POST',`/api/open-access-key/${key}/rotate`);
    assert.notEqual(rotated.accessKey, created.accessKey);
    await publicList(created.accessKey, 401);
    assert.equal((await publicList(rotated.accessKey)).data.list[0].id, device);
    assert.equal((await publicStream(rotated.accessKey)).data.list[0].id, device);
    const webhooks = () => request('GET',`/api/open-webhook?accessKeyId=${key}`);
    assert.deepEqual(await webhooks(),[]);
    webhook = (await request('POST','/api/open-webhook', {accessKeyId:key,name:tag,url:'https://example.test/events',status:'disabled',timeoutSeconds:9,skipTlsVerify:true,headers:{'X-Trace':tag},eventTypes:['device.data.reported'],secret:'test-only-webhook-secret'})).id;
    assert.equal((await webhooks())[0].id, webhook);
    await request('PUT',`/api/open-webhook/${webhook}`, {name:`${tag}_updated`});
    stored = (await webhooks())[0];
    assert.equal(stored.timeoutSeconds, 9);
    assert.equal(stored.skipTlsVerify, true);
    assert.equal(stored.hasSecret, true);
    assert.deepEqual(stored.headers, {'X-Trace':tag});
    await request('PUT',`/api/open-webhook/${webhook}`, {secret:null,skipTlsVerify:false});
    assert.equal((await webhooks())[0].hasSecret, false);
    assert.equal((await webhooks())[0].skipTlsVerify, false);
    for (const configuration of [{name:42},{timeoutSeconds:'5'},{timeoutSeconds:31},{skipTlsVerify:'false'},{eventTypes:[]},{eventTypes:['unsupported']},{url:'ftp://example.test/events'},{headers:[]},{accessKeyId:'invalid'}])
        await request('PUT',`/api/open-webhook/${webhook}`,configuration,19002);

    const reserved = ['host','content-length','connection','x-iot-event','x-iot-timestamp','x-iot-delivery','x-iot-signature','content-type','user-agent','transfer-encoding','trailer','te','upgrade','expect','proxy-connection'];
    const headerCases = ['{}','{"X-Test":"ok"}','{"X-Test":42}','{"X-Test":null}','{"X-Test":false}','{"X-Test":[]}','{"X-Test":{}}','{"":"ok"}','{"X Test":"ok"}','{"X:Test":"ok"}',String.raw`{"X-Test":"ok\r\nInjected"}`,String.raw`{"X-Test":"ok\u000aInjected"}`,String.raw`{"\u0048ost":"example.test"}`,String.raw`{"X-Test":42,"X-\u0054est":"ok"}`,'{"X-Test":"ok","X-Test":42}','{"X-Test":42,"X-Test":"ok"}','{"X-Test":"ok","x-test":"also ok"}',...reserved.map(name=>JSON.stringify({[name.toUpperCase()]:'blocked'}))];
    for (const headers of headerCases) {
        const pattern = "^[!#$%&'*+.^_`|~0-9A-Za-z-]+$";
        const newline = '[\\r\\n]';
        // 保留原始 JSON 的重复成员，避免后续合法值掩盖前面的非法 Header。
        const oracle = await db`SELECT NOT EXISTS (SELECT 1 FROM json_each(${headers}::text::json) AS header(key,value) WHERE key !~ ${pattern} OR json_typeof(value) <> 'string' OR (value #>> '{}') ~ ${newline} OR lower(key) IN (SELECT jsonb_array_elements_text(${JSON.stringify(reserved)}::text::jsonb))) AS valid`;
        const before = (await db`SELECT headers FROM open_webhook WHERE id=${webhook}`)[0].headers;
        await request('PUT',`/api/open-webhook/${webhook}`,`{"headers":${headers}}`,oracle[0].valid ? 0 : 19002,admin,true);
        assert.deepEqual((await db`SELECT headers FROM open_webhook WHERE id=${webhook}`)[0].headers, oracle[0].valid ? JSON.parse(headers) : before);
    }
    const logs = await request('GET',`/api/open-access-log?accessKeyId=${key}&page=1&pageSize=10`);
    assert(Array.isArray(logs.list));
    assert.equal(logs.pageSize,10);
    await request('GET','/api/open-access-log?page=9223372036854775000&pageSize=100',undefined,19002);
    await request('GET','/api/open-access-log?deviceId=invalid',undefined,10001);
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${user},${user},'unused-test-password')`;
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:open-access:query","iot:open-access:add"]'::jsonb)`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${user},${role})`;
    await request('DELETE',`/api/open-access-key/${key}`,undefined,11007,user);
    await request('POST','/api/open-access-key',{name:`${tag}_forbidden`,scopes:['device:realtime'],deviceIds:[device]},19011,user);
    assert.equal((await db`SELECT count(*)::int AS n FROM open_access_key WHERE name=${`${tag}_forbidden`}`)[0].n,0);
    await request('DELETE',`/api/open-webhook/${webhook}`);
    assert.deepEqual(await webhooks(),[]);
    await request('DELETE',`/api/open-access-key/${key}`);
    await publicList(rotated.accessKey,401);
    for (const path of ['/api/open-access-key','/api/open-webhook','/api/open-access-log','/api/device/options'])
        assert.equal((await fetch(apiBase+path)).status,401,path);
    console.log('PASS access HTTP/SSE: key/webhook CRUD, rotation, typed partial updates, null clearing, header SQL oracle, persisted usage/log queries, permissions, public HTTP/SSE compatibility');
} finally {
    if (webhook) await db`DELETE FROM open_webhook WHERE id=${webhook}`;
    if (key) { await db`DELETE FROM open_access_log WHERE access_key_id=${key}`; await db`DELETE FROM open_access_key_device WHERE access_key_id=${key}`; await db`DELETE FROM open_access_key WHERE id=${key}`; }
    if (device) await db`DELETE FROM device WHERE id=${device}`;
    if (link) await db`DELETE FROM link WHERE id=${link}`;
    if (model) await db`DELETE FROM protocol_config WHERE id=${model}`;
    await db`DELETE FROM sys_user_role WHERE user_id=${user}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${user}`;
    await db.close();
}
