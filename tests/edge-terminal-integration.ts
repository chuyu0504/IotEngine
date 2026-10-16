import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { EdgeDebugConnection, DebugOperationError } from '../web/lib/edge_debug_connection';
const db = new Bun.SQL(databaseUrl), redis = new Bun.RedisClient(redisUrl);
const platform = '00000000-0000-7000-8000-000000000001', admin = '00000000-0000-7000-8000-000000000002';
const bytes = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
const now = Math.floor(Date.now()/1000);
const unsigned = `${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:admin,user_id:admin,username:'admin',token_type:'access',iat:now,exp:now+3600})}`;
const token = `${unsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
function integer(input: bigint | number) {
    let n = BigInt(input); const output: number[] = [];
    do { let byte = Number(n & 127n); n >>= 7n; if (n) byte |= 128; output.push(byte); } while(n);
    return Buffer.from(output);
}
function field(tag: number, value: bigint | number | string | Buffer) {
    if (typeof value === 'bigint' || typeof value === 'number') return Buffer.concat([integer(tag*8),integer(value)]);
    const buffer = typeof value === 'string' ? Buffer.from(value) : value;
    return Buffer.concat([integer(tag*8+2),integer(buffer.length),buffer]);
}
function decode(data: Buffer) {
    const result = new Map<number, bigint | Buffer>(); let position = 0;
    const read = () => { let n = 0n, shift = 0n; for (;;) { const b = data[position++]; assert(b !== undefined); n |= BigInt(b&127)<<shift; if (!(b&128)) return n; shift+=7n; } };
    while (position < data.length) {
        const key = Number(read()), wire = key&7;
        if (!wire) result.set(key>>3,read());
        else if (wire === 2) { const n = Number(read()); result.set(key>>3,data.subarray(position,position+n)); position+=n; }
        else if (wire === 1 || wire === 5) {const n=wire===1?8:4;result.set(key>>3,data.subarray(position,position+n));position+=n;}
        else throw Error(`invalid wire ${wire}`);
    }
    return result;
}
async function until(check: () => Promise<boolean>, reason: string) {
    const end = Date.now()+25000;
    while (Date.now()<end) {if(await check())return;await Bun.sleep(50);} throw Error(reason);
}
let connections = 0;
const channel = new EdgeDebugConnection(() => { connections++; return new WebSocket(apiBase.replace('http:', 'ws:') + '/v1/edge/debug'); });
channel.configureSessionRestore(async () => {
    await channel.request('edge.debug.authenticate', {token}, {anonymous:true});
});
async function rejected(operation: Promise<unknown>, code: number) {
    await assert.rejects(operation,error => error instanceof DebugOperationError && error.code === code);
}
function imei() {
    const base='99'+String(Math.floor(Math.random()*1e12)).padStart(12,'0');let sum=0;
    for(let i=0;i<14;i++){let n=Number(base[i])*(i%2?2:1);if(n>9)n-=9;sum+=n;}return base+String((10-sum%10)%10);
}
type TerminalEvent = { kind: string; content?: string; sequence?: number; reason?: string };
try {
    await until(async () => (await fetch(apiBase + '/internal/health/ready')).status === 200, 'workers not ready');
    for (const version of [6, 3]) {
        const node = crypto.randomUUID(), identity = imei();
        let socket: WebSocket | undefined, release: (() => void) | undefined;
        try {
            await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${identity},'approved')`;
            await redis.send('SET', [`iot:edge:auth:${identity}`, `${node}|approved`]);
            let sequence = 1n, epoch = 0n;
            const commands: {tag:number; fields:Map<number,bigint|Buffer>}[] = [];
            const envelope = (tag:number, payload:Buffer) => Buffer.concat([
                field(1,version), field(2,bytes(crypto.randomUUID())), field(3,bytes(node)),
                field(4,bytes(platform)), field(5,epoch), field(6,Date.now()), field(8,sequence++), field(tag,payload),
            ]);
            socket = new WebSocket(apiBase.replace('http:', 'ws:') + '/edge/v1/connect');
            socket.binaryType = 'arraybuffer';
            await new Promise<void>((resolve,reject) => {
                const timer=setTimeout(()=>reject(Error('node hello timeout')),10000);
                socket!.onopen=()=>socket!.send(envelope(20,Buffer.concat([field(1,identity),field(2,'fixture'),field(3,'terminal-test'),field(13,1)])));
                socket!.onerror=()=>reject(Error('node connection failed'));
                socket!.onmessage=event=>{
                    const message=decode(Buffer.from(event.data as ArrayBuffer));
                    if(message.has(21)){epoch=message.get(5) as bigint;clearTimeout(timer);resolve();}
                    if(message.has(80))socket!.send(envelope(81,message.get(80) as Buffer));
                    for(const tag of [70,71,72,73,77]) if(message.has(tag)) commands.push({tag,fields:decode(message.get(tag) as Buffer)});
                };
            });
            socket.send(envelope(26,field(4,1)));
            await until(async()=>(await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->>'terminal'='true'`).length>0,'terminal capability missing');
            const changesBefore = (await redis.send('XREVRANGE', ['iot:live:changes', '+', '-', 'COUNT', '1']) as [string, string[]][])[0]?.[0] ?? '0-0';
            const opened=await channel.request<{id:string}>('edge.terminal.open',{id:node,columns:100,rows:30});
            const session={id:node,sessionId:opened.id};
            const routes=[`iot:live:route:edge-terminal:${node}:${opened.id}`,`iot:live:route:edge-terminal:${node}:${opened.id}:input-ack`];
            for(const route of routes)assert.match(String(await redis.send('GET',[route])),/^iot:live:worker:/,'terminal frames are not routed to the browser worker');
            await until(async()=>commands.some(c=>c.tag===70),'terminal open not delivered');
            const open=commands.find(c=>c.tag===70)!.fields;
            assert.equal(open.get(3),100n);assert.equal(open.get(4),30n);
            assert((open.get(1) as Buffer).equals(bytes(opened.id)));
            if(version===3)assert.equal((open.get(2) as Buffer).toString(),opened.id,'legacy open ticket missing');
            const events:TerminalEvent[]=[];
            let subscriptionFailure:unknown;
            release=channel.subscribe<{events:TerminalEvent[]}>('edge.terminal.events.subscribe',session,{
                next: data=>events.push(...data.events),error:error=>{subscriptionFailure=error;},
            });
            if(version>=5)socket.send(envelope(76,field(1,bytes(opened.id))));
            await until(async()=>events.some(e=>e.kind==='ready'),'ready not pushed');
            const intruder=new EdgeDebugConnection(()=>new WebSocket(apiBase.replace('http:','ws:')+'/v1/edge/debug'));
            intruder.configureSessionRestore(async()=>{await intruder.request('edge.debug.authenticate',{token},{anonymous:true});});
            try { await rejected(intruder.request('edge.terminal.write',{...session,content:'AA=='}),17018); }
            finally { intruder.reset(); }
            await rejected(channel.request('edge.terminal.events.subscribe',session),17018);
            const payload=Buffer.alloc(9000);for(let i=0;i<payload.length;i++)payload[i]=i%256;
            let writeFinished=false;
            const write=channel.request('edge.terminal.write',{...session,content:payload.toString('base64')}).then(()=>{writeFinished=true;});
            for(let index=0;index<3;index++){
                await until(async()=>commands.filter(c=>c.tag===71).length>index,'terminal input chunk missing');
                const chunk=commands.filter(c=>c.tag===71)[index].fields;
                assert((chunk.get(2) as Buffer).equals(payload.subarray(index*4096,(index+1)*4096)));
                if(version>=5){
                    assert.equal(chunk.get(3),BigInt(index+1));
                    assert.equal(writeFinished,false,'write acknowledged before device accepted input');
                    assert.equal(commands.filter(c=>c.tag===71).length,index+1,'next chunk bypassed device backpressure');
                    socket.send(envelope(77,Buffer.concat([field(1,bytes(opened.id)),field(2,index+1)])));
                }
            }
            await write;
            const emit=(seq:number)=>socket!.send(envelope(71,Buffer.concat([field(1,bytes(opened.id)),field(2,Buffer.from([0,255,128,10])),field(3,seq)])));
            emit(version>=5?1:0);
            await until(async()=>events.filter(e=>e.kind==='data').length===1,'terminal output missing');
            assert.equal(events.find(e=>e.kind==='data')!.content,'AP+ACg==');
            if(version>=5){
                assert.equal(commands.filter(c=>c.tag===77).length,0,'server acknowledged output before browser consumption');
                await channel.request('edge.terminal.output.ack',{...session,sequence:1});
                await until(async()=>commands.some(c=>c.tag===77&&c.fields.get(2)===1n),'output acknowledgement missing');
            }
            // Legacy frames intentionally have identical bytes and sequence zero.
            emit(version>=5?2:0);
            await until(async()=>events.filter(e=>e.kind==='data').length===2,'identical legacy output was deduplicated');
            if(version>=5)await channel.request('edge.terminal.output.ack',{...session,sequence:2});
            await channel.request('edge.terminal.resize',{...session,columns:132,rows:43});
            await channel.request('edge.terminal.keepalive',session);
            await until(async()=>commands.some(c=>c.tag===72&&c.fields.get(2)===132n&&c.fields.get(3)===43n),'resize not delivered');
            assert.equal(subscriptionFailure,undefined);
            release();release=undefined;
            await channel.request('edge.terminal.close',session);
            await channel.request('edge.terminal.close',session);
            await until(async()=>commands.some(c=>c.tag===73&&(c.fields.get(1) as Buffer).equals(bytes(opened.id))),'explicit close not delivered');
            assert.equal(await redis.send('EXISTS',routes),0,'closed terminal kept its directed routes');
            const broadcast=await redis.send('XRANGE',['iot:live:changes',`(${changesBefore}`,'+']) as [string,string[]][];
            assert(!broadcast.some(([,fields])=>fields.some(field=>field.startsWith('edge-terminal:'))),'terminal frames reached the broadcast change stream');
            await rejected(channel.request('edge.terminal.write',{...session,content:'AA=='}),17018);
            const backlog=await channel.request<{id:string}>('edge.terminal.open',{id:node,columns:80,rows:24});
            if(version>=5)socket.send(envelope(76,field(1,bytes(backlog.id))));
            for(let i=1;i<=40;i++)socket.send(envelope(71,Buffer.concat([field(1,bytes(backlog.id)),field(2,Buffer.from('same')),field(3,version>=5?i:0)])));
            await until(async()=>Number(await redis.send('LLEN',[`iot:edge:terminal:out:${node}:${backlog.id}`]))===41,'terminal backlog was not saved');
            const backlogEvents:TerminalEvent[]=[];
            release=channel.subscribe<{events:TerminalEvent[]}>('edge.terminal.events.subscribe',{id:node,sessionId:backlog.id},{
                next:data=>backlogEvents.push(...data.events),error:error=>{subscriptionFailure=error;},
            });
            await until(async()=>backlogEvents.filter(e=>e.kind==='data').length===40,'known backlog stalled after first batch');
            assert.equal(backlogEvents[0].kind,'ready');
            if(version>=5)assert.deepEqual(backlogEvents.filter(e=>e.kind==='data').map(e=>e.sequence),Array.from({length:40},(_,i)=>i+1));
            assert.equal(subscriptionFailure,undefined);
            release();release=undefined;
            await channel.request('edge.terminal.close',{id:node,sessionId:backlog.id});
            const disconnected=await channel.request<{id:string}>('edge.terminal.open',{id:node,columns:80,rows:24});
            channel.reset();
            await until(async()=>commands.some(c=>c.tag===73&&(c.fields.get(1) as Buffer).equals(bytes(disconnected.id))),'disconnect did not close terminal');
            const logout=await channel.request<{id:string}>('edge.terminal.open',{id:node,columns:80,rows:24});
            assert.equal((await fetch(apiBase + '/v1/auth/logout', { method: 'POST', headers: { Authorization: `Bearer ${token}` } })).status, 200);
            channel.reset();
            await until(async()=>commands.some(c=>c.tag===73&&(c.fields.get(1) as Buffer).equals(bytes(logout.id))),'logout did not close terminal');
            channel.reset();
            console.log(`PASS terminal protocol ${version}: dedicated debug connection, ownership, input chunk/device ACK, binary output/browser ACK, resize, explicit/disconnect/logout cleanup`);
        } finally {
            release?.();channel.reset();socket?.close();
            await redis.send('DEL',[`iot:edge:auth:${identity}`]);
        }
    }
    assert.equal((await fetch(apiBase+'/edge/v1/terminal?ticket='+crypto.randomUUID())).status,404);
} finally {channel.reset();redis.close();await db.close();}
