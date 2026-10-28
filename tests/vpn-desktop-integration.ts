import assert from 'node:assert/strict';
import { pbkdf2Sync, randomBytes, randomUUID } from 'node:crypto';
import { consumeServerSentEvents } from '../web/lib/sse';
import { apiBase as api, databaseUrl, redisUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const presence = new Bun.RedisClient(redisUrl);
const platform = '00000000-0000-7000-8000-000000000001';
const admin = '00000000-0000-7000-8000-000000000002';
const network = '00000000-0000-7000-8000-000000000004';
const edgeA = randomUUID(), edgeB = randomUUID(), routeA = randomUUID();
const imeiA = '900000000000001', imeiB = '900000000000002';
const roleVpn = randomUUID(), roleEdge = randomUUID(), user = randomUUID(), other = randomUUID();
const suffix = randomBytes(4).toString('hex');
const username = `vpn-${suffix}`, othername = `other-${suffix}`;
const password = 'DisposableVpnTest123!';
const releases: Array<() => void> = [];
let seeded = false;
let token = '', otherToken = '';
type Reply = {data?: any; error?: {code:number; message:string}};
type DesktopDevice = {id: string; online: boolean};
function check(reply: Reply, code = 0) {
    assert.equal(reply.error?.code ?? 0,code,JSON.stringify(reply.error));
    return reply.data;
}
async function request(method: string, path: string, payload?: unknown, bearer = token): Promise<Reply> {
    const response = await fetch(api + path, {
        method, headers: { 'Content-Type': 'application/json', ...(bearer ? {Authorization: `Bearer ${bearer}`} : {}) },
        body: payload === undefined ? undefined : JSON.stringify(payload), signal: AbortSignal.timeout(15000),
    });
    const result = await response.json() as {code:number;message:string;data?:unknown};
    assert.equal(response.ok, result.code === 0, `${method} ${path}: ${JSON.stringify(result)}`);
    return result.code ? {error:{code:result.code,message:result.message}} : {data:result.data};
}
async function openStream(path: string, bearer = token, expectedCode = 0) {
    const controller = new AbortController();
    const response = await fetch(api + path, {
        headers: {Accept:'text/event-stream', ...(bearer ? {Authorization:`Bearer ${bearer}`} : {})}, signal:controller.signal,
    });
    if (expectedCode) {
        const error = await response.json() as {code:number};
        assert.equal(response.ok,false); assert.equal(error.code,expectedCode);
        controller.abort(); return null!;
    }
    assert.equal(response.status,200); assert.match(response.headers.get('content-type') ?? '', /text\/event-stream/);
    assert(response.body);
    const queue: Reply[]=[];
    let wake: (()=>void)|undefined;
    let ended=false, failure:unknown, received=0;
    let lastRoutes: unknown;
    void consumeServerSentEvents(response.body,event=>{
        if(event.event==='heartbeat') return;
        const value=JSON.parse(event.data);
        received++; lastRoutes=value.data?.allowedRoutes ?? value.code;
        queue.push(value.code ? {error:{code:value.code,message:value.message}} : {data:value.data}); wake?.();
    },controller.signal).catch(error=>{if(!controller.signal.aborted) failure=error;})
        .finally(()=>{ended=true;wake?.();});
    const release=()=>controller.abort(); releases.push(release);
    async function next(predicate:(value:Reply)=>boolean=()=>true):Promise<Reply> {
        for (;;) {
            const item=queue.shift();
            if (item) {if (predicate(item)) return item; continue;}
            if(failure) throw failure;
            assert(!ended, 'SSE ended before expected update: '+path);
            await new Promise<void>((resolve,reject)=>{
                const timer=setTimeout(()=>{wake=undefined;reject(new Error(`SSE subscription timed out: ${path}; expected ${predicate}; received=${received}, last routes=${JSON.stringify(lastRoutes)}`));},20000);
                wake=()=>{clearTimeout(timer);wake=undefined;resolve();};
            });
        }
    }
    return {close:release,next};
}
const publicKey = () => randomBytes(32).toString('base64');
const enroll = (key: string, ids: unknown, bearer = token) => request('POST', '/v1/vpn/desktop/peers', { name: 'Integration Windows', publicKey: key, edgeNodeIds: ids }, bearer);
const patch = (peer: string, ids: unknown, bearer = token) => request('PATCH', `/v1/vpn/desktop/peers/${peer}`, {edgeNodeIds:ids}, bearer);
function wireField(tag:number,value:number|bigint|Buffer|string) {
    const integer=(input:number|bigint)=>{let n=BigInt(input);const result:number[]=[];do{let b=Number(n&127n);n>>=7n;if(n)b|=128;result.push(b);}while(n);return Buffer.from(result);};
    if(typeof value==='number'||typeof value==='bigint') return Buffer.concat([integer(tag*8),integer(value)]);
    const data=typeof value==='string'?Buffer.from(value):value;
    return Buffer.concat([integer(tag*8+2),integer(data.length),data]);
}
function wireFields(data:Buffer) {
    const result=new Map<number,bigint|Buffer>();let position=0;
    const integer=()=>{let n=0n,shift=0n;for(;;){const b=data[position++];assert(b!==undefined);n|=BigInt(b&127)<<shift;if(!(b&128))return n;shift+=7n;}};
    while(position<data.length){const tag=Number(integer());if((tag&7)===0)result.set(tag>>3,integer());else{assert.equal(tag&7,2);const size=Number(integer());result.set(tag>>3,data.subarray(position,position+size));position+=size;}}
    return result;
}
async function connectEdge() {
    const id=(value:string)=>Buffer.from(value.replaceAll('-',''),'hex');
    const socket=new WebSocket(api.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    releases.push(()=>socket.close());let epoch=0n,sequence=1n;
    const envelope=(tag:number,payload:Buffer)=>Buffer.concat([wireField(1,6),wireField(2,id(randomUUID())),wireField(3,id(edgeA)),wireField(4,id(platform)),wireField(5,epoch),wireField(6,Date.now()),wireField(8,sequence++),wireField(tag,payload)]);
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(new Error('Edge Hello timed out')),10000);
        socket.onopen=()=>socket.send(envelope(20,Buffer.concat([wireField(1,imeiA),wireField(2,'fixture'),wireField(35,1),wireField(37,1)])));
        socket.onerror=()=>{clearTimeout(timer);reject(new Error('Edge connection failed'));};
        socket.onmessage=event=>{
            const incoming=wireFields(Buffer.from(event.data as ArrayBuffer));
            if(incoming.has(21)){epoch=incoming.get(5) as bigint;clearTimeout(timer);resolve();}
            if(incoming.has(84)){
                const request=wireFields(incoming.get(84) as Buffer);
                socket.send(envelope(85,Buffer.concat([wireField(1,request.get(1) as Buffer),wireField(2,request.get(2) as bigint),wireField(3,1)])));
            }
        };
    });
}
async function deadlineClaims() {
    const source = await Bun.file(new URL('../service/features/vpn/vpn.service.h', import.meta.url)).text();
    const script = (name: string) => {
        const match = source.match(new RegExp(`inline constexpr std::string_view ${name} = R"lua\\(([\\s\\S]*?)\\)lua";`));
        assert(match, `missing named Lua operation ${name}`);
        return match[1];
    };
    const update = script('kHubScheduleUpdateScript');
    const read = script('kHubScheduleReadScript');
    const claim = script('kHubScheduleClaimScript');
    const key = `iot:vpn:hub:schedule:fixture-${randomUUID()}`;
    const channel = `${key}:changed`;
    const write = (action: string, desktop = '') => presence.send('EVAL', [update, '2', key, channel, action, desktop]);
    const snapshot = async () => await presence.send('EVAL', [read, '1', key]) as number[];
    const take = () => presence.send('EVAL', [claim, '2', key, channel]);
    try {
        assert.equal(Number(await write('initialize')), 1);
        assert.equal(Number(await write('initialize')), 0, 'only one Worker initializes startup work');
        const [now, initial] = await snapshot();
        assert(initial <= now, 'startup is immediately due');
        const claimed = await Promise.all([take(), take()]);
        assert.deepEqual(claimed.map(Number).sort(), [0, 1], 'only one Worker claims the deadline');
        const [claimedNow, recovery] = await snapshot();
        assert(recovery > claimedNow + 290000, 'the claimed identity survives at its recovery deadline');
        await write('complete');
        assert.equal((await snapshot())[1], 0, 'success leaves no periodic business work');
        assert.equal(Number(await write('initialize')), 0, 'another Worker cannot turn success into a sweep');
        const desktop = Date.now() + 60000;
        await write('failed', String(desktop));
        assert.equal((await snapshot())[1], desktop, 'nearest desktop deadline precedes the failed-attempt retry');
        assert.equal(Number(await take()), 0, 'future deadlines are not claimable');
        await presence.send('ZADD', [key, String(Date.now() - 1), 'desktop', String(Date.now() - 1), 'retry']);
        assert.equal(Number(await take()), 1);
        assert.equal(Number(await take()), 0, 'one attempt claims all due identities, not one per Worker');
        assert.equal(Number(await presence.send('ZCARD', [key])), 3, 'in-flight desktop/retry identities are retained');
        await presence.send('ZADD', [key, String(Date.now() - 1), 'retry']);
        assert.equal(Number(await take()), 1, 'a lost Worker attempt becomes claimable again');
        await write('complete');
        assert.equal((await snapshot())[1], 0);
        const ttl = Number(await presence.send('PTTL', [key]));
        assert(ttl > 0 && ttl <= 600000, 'exited-instance schedule keys are reclaimed');
    } finally {
        await presence.del(key);
    }
    console.log('PASS actual VPN deadline claims, retained retry identities and no successful periodic sweep');
}

try {
    await deadlineClaims();
    assert.equal((await db`SELECT to_regclass('public.vpn_effective_edge_access') AS name`)[0].name, 'vpn_effective_edge_access');
    const salt = randomBytes(16); const hash = `pbkdf2_sha256$210000$${salt.toString('hex')}$${pbkdf2Sync(password, salt, 210000, 32, 'sha256').toString('hex')}`;
    await db`INSERT INTO sys_role(id,code,name,status,permissions) VALUES
        (${roleVpn},${'vpn-'+suffix},'VPN','enabled','["iot:vpn:query","iot:vpn:enroll"]'::jsonb),
        (${roleEdge},${'edge-'+suffix},'Edge','enabled','["iot:edge:query"]'::jsonb)`;
    seeded = true;
    await db`INSERT INTO sys_user(id,username,password_hash,nickname,status) VALUES
        (${user},${username},${hash},'VPN test','enabled'),(${other},${othername},${hash},'Other test','enabled')`;
    for (const account of [user,other]) for (const role of [roleVpn,roleEdge])
        await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES (${randomUUID()},${account},${role})`;
    await db`UPDATE vpn_network SET hub_public_key=${publicKey()},hub_endpoint='127.0.0.1',hub_listen_port=51820,status='enabled' WHERE id=${network}`;
    for (const [edge,name,address,imei] of [[edgeA,'Edge A','100.96.0.20',imeiA],[edgeB,'Edge B','100.96.0.21',imeiB]]) {
        await db`INSERT INTO edge_node(id,platform_id,imei,name,model,enrollment_status,approved_by,approved_at,capability) VALUES
            (${edge},${platform},${imei},${name},'test','approved',${admin},NOW(),'{"vpn":{"supportsVpn":true}}'::jsonb)`;
        // The gateway authorizes Hello from the Redis enrollment cache, which is hydrated only at startup.
        await presence.send('SET',[`iot:edge:auth:${imei}`,`${edge}|approved`]);
        await db`INSERT INTO vpn_peer(id,network_id,peer_type,edge_node_id,name,public_key,assigned_ipv4,status) VALUES
            (${edge},${network},'edge',${edge},${name},${publicKey()},${address}::inet,'active')`;
    }
    await db`INSERT INTO vpn_route(id,network_id,edge_peer_id,lan_interface,target_cidr,virtual_cidr,created_by,status) VALUES
        (${routeA},${network},${edgeA},'lan','192.168.1.0/24','172.31.10.0/24',${admin},'active')`;
    const login = check(await request('POST', '/v1/auth/login', { username, password }, ''));
    assert.ok(login.token); assert.ok(login.refresh_token);
    const refreshed = check(await request('POST', '/v1/auth/refresh', { refresh_token: login.refresh_token }, ''));
    token = refreshed.token; otherToken = check(await request('POST', '/v1/auth/login', { username: othername, password }, '')).token;
    console.log('PASS actual login and refresh_token contract');
    const initialDevices=check(await request('GET','/v1/vpn/desktop/devices'));
    assert.equal(initialDevices.filter((d:any)=>[edgeA,edgeB].includes(d.id)).length,2);
    check(await request('GET','/v1/vpn/networks?pageSize=101'),10001);
    check(await request('GET','/v1/vpn/desktop/peers/invalid/config'),10001);
    check(await request('POST','/v1/vpn/desktop/peers',[]),10002);
    check(await request('POST','/v1/vpn/peers',{}),11007);
    console.log('PASS ordinary HTTP snapshots, typed query/path validation and write permissions');
    assert.equal(check(await request('GET','/v1/vpn/networks')).list[0]?.id,network);
    console.log('PASS default network HTTP read');

    assert.equal(initialDevices.find((d: DesktopDevice) => d.id === edgeA).online, false);
    await db`UPDATE edge_node SET last_seen_at=NOW() - INTERVAL '1 day' WHERE id=${edgeA}`;
    await presence.set(`iot:edge:session:${edgeA}`,'disposable-session');
    assert.equal(check(await request('GET','/v1/vpn/desktop/devices')).find((d: DesktopDevice)=>d.id===edgeA).online,true);
    await db`UPDATE edge_node SET last_seen_at=NOW() WHERE id=${edgeA}`;
    await presence.del(`iot:edge:session:${edgeA}`);
    assert.equal(check(await request('GET','/v1/vpn/desktop/devices')).find((d: DesktopDevice)=>d.id===edgeA).online,false);
    console.log('PASS desktop online follows live Edge session, independent of stale or fresh telemetry timestamp');
    await connectEdge();
    assert.equal((await db`SELECT vpn_desktop_user_authorized(${user}::uuid) AS allowed`)[0].allowed, true);
    await db`INSERT INTO vpn_peer(id,network_id,peer_type,user_id,name,public_key,assigned_ipv4,status,client_managed)
        VALUES (${randomUUID()},${network},'windows',${user},'Previously revoked client',${publicKey()},'100.96.0.2'::inet,'revoked',TRUE)`;
    const key = publicKey(); const first = check(await enroll(key, [edgeA])); const peer = first.peerId;
    assert.equal(first.assignedIpv4,'100.96.0.2','historical revoked rows must no longer reserve an address');
    assert.deepEqual(first.edgeNodeIds, [edgeA]); assert.deepEqual(first.allowedRoutes.sort(), ['100.96.0.20/32','172.31.10.0/24']);
    assert.equal('privateKey' in first, false); assert.equal(first.publicKey, key);
    async function requestedState(enabled: boolean) {
        for(let attempt=0;attempt<650;++attempt) {
            const rows=await db`SELECT request->>'enabled' AS enabled,status FROM edge_task WHERE task_type='vpn' AND request->>'peerId'=${edgeA} ORDER BY created_at DESC,id DESC LIMIT 1`;
            if(rows[0]?.enabled===String(enabled)&&rows[0]?.status==='succeeded') return;
            await Bun.sleep(100);
        }
        assert.fail(`node demand did not become ${enabled}`);
    }
    await requestedState(false);
    assert.equal((await db`SELECT virtual_cidr::text AS cidr FROM vpn_route WHERE id=${routeA}`)[0].cidr,'172.31.10.0/24');
    assert.equal((await db`SELECT status FROM vpn_route WHERE id=${routeA}`)[0].status,'active','a successful tunnel stop must retain allocated route availability');
    assert.deepEqual(check(await request('GET','/v1/vpn/desktop/devices')).find((d:any)=>d.id===edgeA).virtualCidrs,['172.31.10.0/24']);
    const demandA=await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`); await demandA.next();
    await requestedState(true);
    const demandB=await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`); await demandB.next();
    demandA.close();
    for(let attempt=0;attempt<200&&Number(await presence.send('ZCARD',[`iot:vpn:desktop:sessions:${peer}`]))!==1;++attempt)
        await Bun.sleep(100);
    await requestedState(true);
    assert.equal(Number(await presence.send('ZCARD',[`iot:vpn:desktop:sessions:${peer}`])),1,'closing one connection preserves another');
    demandB.close(); await requestedState(false);
    const demandC=await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`); await demandC.next();
    await requestedState(true);
    check(await patch(peer,[])); await requestedState(false);
    check(await patch(peer,[edgeA])); await requestedState(true);
    demandC.close(); await requestedState(false);
    // A durable desktop session without an SSE connection models a client whose
    // Worker disappeared without running closeDesktopSession. Only its actual
    // Redis deadline may trigger demand shrinkage; no follow-up RPC does so.
    await presence.send('EVAL', [`local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000);
        redis.call('ZADD',KEYS[1],now+5000,'lost-worker-session');
        redis.call('PEXPIRE',KEYS[1],6000); return 1`, '1', `iot:vpn:desktop:sessions:${peer}`]);
    check(await patch(peer, [edgeA]));
    await requestedState(true);
    const expiryTasks = Number((await db`SELECT count(*) AS count FROM edge_task WHERE task_type='vpn' AND request->>'peerId'=${edgeA}`)[0].count);
    await requestedState(false);
    assert.equal(Number((await db`SELECT count(*) AS count FROM edge_task WHERE task_type='vpn' AND request->>'peerId'=${edgeA}`)[0].count), expiryTasks + 1,
        'deadline expiry sends one effective disable across Service Workers');
    await presence.del(`iot:vpn:desktop:sessions:${peer}`);
    assert.equal((await db`SELECT host(assigned_ipv4) AS address FROM vpn_peer WHERE id=${edgeA}`)[0].address,'100.96.0.20');
    assert.equal((await db`SELECT virtual_cidr::text AS cidr FROM vpn_route WHERE id=${routeA}`)[0].cidr,'172.31.10.0/24');
    console.log('PASS saved capability and preallocated mappings stay off, live selection enables VPN, last client closes it without reallocating');
    const sharedClient=check(await enroll(publicKey(),[edgeA]));
    const sharedA=await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`); await sharedA.next();
    const sharedB=await openStream(`/v1/vpn/desktop/peers/${sharedClient.peerId}/config/events`); await sharedB.next();
    await requestedState(true);
    const tasksBefore=Number((await db`SELECT count(*) AS count FROM edge_task WHERE task_type='vpn' AND request->>'peerId'=${edgeA}`)[0].count);
    await Bun.sleep(16000);
    assert.equal(Number((await db`SELECT count(*) AS count FROM edge_task WHERE task_type='vpn' AND request->>'peerId'=${edgeA}`)[0].count),tasksBefore,'lease renewal must not resend a VPN configuration');
    sharedA.close();
    for(let attempt=0;attempt<200&&Number(await presence.send('ZCARD',[`iot:vpn:desktop:sessions:${peer}`]))!==0;++attempt)
        await Bun.sleep(100);
    await requestedState(true);
    check(await request('DELETE',`/v1/vpn/desktop/peers/${sharedClient.peerId}`));
    await requestedState(false); sharedB.close();
    console.log('PASS two distinct clients share one enabled node; renewal sends no duplicate task and only the last user stops VPN');
    const retryKey = publicKey(); const retries = await Promise.all([enroll(retryKey,[edgeA]),enroll(retryKey,[edgeA])]);
    assert.equal(check(retries[0]).peerId,check(retries[1]).peerId);
    const allocated = await Promise.all([enroll(publicKey(),[edgeA]),enroll(publicKey(),[edgeB])]);
    assert.notEqual(check(allocated[0]).assignedIpv4,check(allocated[1]).assignedIpv4);
    const secondLogin = check(await request('POST', '/v1/auth/login',{username,password},''));
    const secondToken = secondLogin.token;
    assert.ok(secondToken);
    const keyA=publicKey(), keyB=publicKey();
    const [computerA,computerB]=await Promise.all([enroll(keyA,[edgeA],token),enroll(keyB,[edgeA],secondToken)]).then(results=>results.map(result=>check(result)));
    assert.notEqual(computerA.peerId,computerB.peerId);
    assert.notEqual(computerA.assignedIpv4,computerB.assignedIpv4);
    const retriedA=check(await enroll(keyA,[edgeA],token));
    assert.equal(retriedA.peerId,computerA.peerId);
    assert.equal(retriedA.assignedIpv4,computerA.assignedIpv4);
    const streamA=await openStream(`/v1/vpn/desktop/peers/${computerA.peerId}/config/events`,token);
    assert.equal((await streamA.next()).data.assignedIpv4,computerA.assignedIpv4); streamA.close();
    check(await request('DELETE', `/v1/vpn/desktop/peers/${computerA.peerId}`, undefined,token));
    const replacementA=check(await enroll(publicKey(),[edgeA],token));
    assert.equal(replacementA.assignedIpv4,computerA.assignedIpv4, 'revoked client address must be reused');
    assert.notEqual(replacementA.peerId,computerA.peerId);
    check(await request('DELETE', `/v1/vpn/desktop/peers/${computerA.peerId}`, undefined,token));
    assert.equal((await db`SELECT status FROM vpn_peer WHERE id=${replacementA.peerId}`)[0].status,'active',
        'late disconnect for an old peer must not revoke its replacement');
    check(await request('DELETE', `/v1/vpn/desktop/peers/${replacementA.peerId}`, undefined,token));
    check(await request('POST', '/v1/auth/logout',{},token));
    const streamB=await openStream(`/v1/vpn/desktop/peers/${computerB.peerId}/config/events`,secondToken);
    assert.equal((await streamB.next()).data.assignedIpv4,computerB.assignedIpv4); streamB.close();
    check(await request('DELETE', `/v1/vpn/desktop/peers/${computerB.peerId}`, undefined,secondToken));
    const recycled=await Promise.all([enroll(publicKey(),[edgeA],secondToken),enroll(publicKey(),[edgeB],secondToken)])
        .then(results=>results.map(result=>check(result)));
    assert.deepEqual(recycled.map(x=>x.assignedIpv4).sort(),[computerA.assignedIpv4,computerB.assignedIpv4].sort(),
        'concurrent connections must reuse the two released addresses without a collision');
    await assert.rejects(db`UPDATE vpn_peer SET status='active' WHERE id=${computerA.peerId}`,
        'database must reject reactivating an old peer whose address is now occupied');
    let cycling=recycled[0];
    for (let iteration=0; iteration<5; ++iteration) {
        check(await request('DELETE', `/v1/vpn/desktop/peers/${cycling.peerId}`, undefined,secondToken));
        const next=check(await enroll(publicKey(),[edgeA],secondToken));
        assert.equal(next.assignedIpv4,cycling.assignedIpv4,'repeated disconnect/reconnect must not grow the address');
        cycling=next;
    }
    console.log('PASS historical revoked address reuse, concurrent reuse, stale disconnect fencing and repeated reconnect');
    console.log('PASS same account simultaneous logins, distinct client IPs, stable retry IP and independent logout');
    console.log('PASS selected routes, split-role permissions, concurrent enrollment and retry');
    for (const ids of [null,{},[1],['bad'],[edgeA,edgeA],[edgeA,edgeA.toUpperCase()],Array(65).fill(edgeA)]) check(await enroll(key,ids),21001);
    for (const invalidKey of ['A'.repeat(43)+'=', 'H'.repeat(43)+'=', '='.repeat(44)]) check(await enroll(invalidKey,[edgeA]),21001);
    check(await enroll(publicKey(),[randomUUID()]),21001);
    check(await patch(peer,[edgeB],otherToken),21004);
    await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`,otherToken,21004);
    check(await request('DELETE', `/v1/vpn/desktop/peers/${peer}`, undefined,otherToken),21004);
    assert.deepEqual(check(await patch(peer,[])).allowedRoutes,[]);
    assert.equal((await db`SELECT count(*)::int AS count FROM vpn_peer_edge_selection WHERE peer_id=${peer}`)[0].count,0);
    const updates = await Promise.all([patch(peer,[edgeA]),patch(peer,[edgeB])]); updates.forEach(x=>check(x));
    const selected = await db`SELECT edge_node_id FROM vpn_peer_edge_selection WHERE peer_id=${peer}`;
    assert.equal(selected.length,1); assert.ok([edgeA,edgeB].includes(selected[0].edge_node_id));
    check(await patch(peer,[edgeA]));
    console.log('PASS validation, ownership, empty selection and concurrent replacement');
    const live=await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`); await live.next();
    await db`UPDATE vpn_route SET virtual_cidr='172.31.11.0/24' WHERE id=${routeA}`;
    const changed=await live.next(x=>x.data?.allowedRoutes.includes('172.31.11.0/24'));
    assert.deepEqual(changed.data.edgeNodeIds,[edgeA]); assert.ok(!changed.data.allowedRoutes.includes('172.31.10.0/24'));
    await db`UPDATE vpn_route SET enabled=false,status='disabled' WHERE id=${routeA}`;
    await live.next(x=>x.data?.allowedRoutes.length===1);
    await db`UPDATE edge_node SET enrollment_status='pending' WHERE id=${edgeA}`;
    await live.next(x=>x.data?.allowedRoutes.length===0);
    assert.equal(check(await enroll(key,[edgeA])).peerId,peer,'lost HTTP response can recover identity after Edge approval changes');
    check(await patch(peer,[edgeA]),21001);
    await db`UPDATE edge_node SET enrollment_status='approved' WHERE id=${edgeA}`;
    // This direct database fixture restores a fully applied mapping. Restoring
    // only enabled leaves a concurrent Edge acknowledgement's disabled status.
    await db`UPDATE vpn_route SET enabled=true,status='active' WHERE id=${routeA}`;
    await live.next(x=>x.data?.allowedRoutes.includes('172.31.11.0/24'));
    await db`UPDATE sys_role SET permissions='["iot:vpn:query"]'::jsonb WHERE id=${roleVpn}`;
    const revoked=await live.next(x=>!!x.error); assert.equal(revoked.error?.code,11007);
    assert.equal((await db`SELECT vpn_desktop_user_authorized(${user}::uuid) AS allowed`)[0].allowed,false);
    assert.equal((await db`SELECT count(*)::int AS count FROM vpn_effective_edge_access WHERE peer_id=${peer}`)[0].count,0);
    check(await request('GET','/v1/vpn/desktop/devices',undefined,token),11007);
    console.log('PASS one-stream route changes, Edge approval removal and enroll-permission revocation');
    await db`UPDATE sys_role SET permissions='["iot:vpn:query","iot:vpn:enroll"]'::jsonb WHERE id=${roleVpn}`;
    await db`UPDATE sys_role SET permissions='[]'::jsonb WHERE id=${roleEdge}`;
    check(await request('GET','/v1/vpn/desktop/devices',undefined,token),11007);
    await db`UPDATE sys_role SET permissions='["iot:edge:query"]'::jsonb WHERE id=${roleEdge}`;
    const pauseStream=await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`,token); await pauseStream.next();
    await db`UPDATE vpn_network SET status='disabled' WHERE id=${network}`;
    const paused=await pauseStream.next(x=>x.data?.networkEnabled===false);
    assert.deepEqual(paused.data.allowedRoutes,[]); assert.deepEqual(paused.data.edgeAddresses,[]);
    assert.deepEqual(check(await request('GET','/v1/vpn/desktop/devices')),[]);
    check(await enroll(publicKey(),[edgeA]),21004);
    await db`UPDATE vpn_network SET status='enabled' WHERE id=${network}`;
    await pauseStream.next(x=>x.data?.networkEnabled===true&&x.data.allowedRoutes.length>0); pauseStream.close();
    check(await request('DELETE', `/v1/vpn/desktop/peers/${peer}`, undefined));
    check(await request('DELETE', `/v1/vpn/desktop/peers/${peer}`, undefined));
    const revokedRegistration=await enroll(key,[edgeA]);
    check(revokedRegistration,21009);
    assert.equal(revokedRegistration.error?.code,21009,'only confirmed revocation clears a pending registration');
    await openStream(`/v1/vpn/desktop/peers/${peer}/config/events`,token,21004);
    console.log('PASS disabled network and idempotent revocation');
    console.log('All desktop API integration groups passed');
    for (const path of ['/v1/vpn/networks','/v1/vpn/desktop/devices','/v1/vpn/desktop/peers/'+peer+'/config'])
        assert.equal((await fetch(api+path)).status,401,path);
} finally {
    await presence.del(`iot:edge:session:${edgeA}`); await presence.send('DEL',[`iot:edge:auth:${imeiA}`,`iot:edge:auth:${imeiB}`]); presence.close();
    for (const release of releases) release();

    if (seeded) {
        await db`UPDATE vpn_network SET status='enabled' WHERE id=${network}`;
        await db`DELETE FROM vpn_peer WHERE user_id IN (${user},${other}) OR id IN (${edgeA},${edgeB})`;
        await db`DELETE FROM edge_node WHERE id IN (${edgeA},${edgeB})`;
        await db`DELETE FROM sys_user WHERE id IN (${user},${other})`;
        await db`DELETE FROM sys_role WHERE id IN (${roleVpn},${roleEdge})`;
    }
    await db.close();
}
