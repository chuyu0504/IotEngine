import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
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
async function request(path: string, body: unknown, method = 'POST') {
    const response = await fetch(apiBase+path,{method,headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify(body)});
    const text = await response.text();assert.equal(response.status,200,`${path}: ${text}`);return JSON.parse(text);
}
async function databaseClockMilliseconds() {
    const [row]=await db`SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint::text AS value`;
    return BigInt(row.value);
}
function imei() {
    const base='99'+String(Math.floor(Math.random()*1e12)).padStart(12,'0');let sum=0;
    for(let i=0;i<14;i++){let n=Number(base[i])*(i%2?2:1);if(n>9)n-=9;sum+=n;}return base+String((10-sum%10)%10);
}
async function enqueueDatabaseCommand(deviceId: string, protocol: string, nodeId: string) {
    const commandId=crypto.randomUUID(),requestId=crypto.randomUUID();
    const deadlineMs=Date.now()+45000;
    const deadline=new Date(deadlineMs).toISOString().replace(/Z$/,'987+00:00');
    await db`INSERT INTO command_request(id,actor,idempotency_key,device_id,payload)
        VALUES(${requestId},${admin},${crypto.randomUUID()},${deviceId},'[]'::jsonb)`;
    await db`INSERT INTO command_operation(id,request_id,ordinal,device_id,device_code,protocol,status,elements,actual_values)
        VALUES(${commandId},${requestId},0,${deviceId},'000000123456',${protocol},'AWAITING_RESULT','[]'::jsonb,'[]'::jsonb)`;
    await db`INSERT INTO command_attempt(operation_id,queue_key,queue_kind,payload,submitted_by,node_id,max_length,deadline)
        VALUES(${commandId},${`iot:v2:edge:commands:${nodeId}`},'list','[]'::jsonb,${admin},${nodeId},1024,${deadline}::timestamptz)`;
    const request=Buffer.concat([field(1,bytes(commandId)),field(2,bytes(deviceId)),field(4,5000)]);
    const wire=field(50,request);
    const pushBinary=`local wire=ARGV[1]:gsub('..',function(pair) return string.char(tonumber(pair,16)) end); return redis.call('RPUSH',KEYS[1],wire)`;
    await redis.send('EVAL',[pushBinary,'1',`iot:v2:edge:commands:${nodeId}`,wire.toString('hex')]);
    return {commandId,deadlineMs,wire};
}
const node=crypto.randomUUID(),identity=imei();let socket:WebSocket|undefined;
try {
    await until(async () => (await fetch(apiBase + '/internal/health/ready')).status === 200,
        'fixture workers are not ready');
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${identity},'approved')`;
    await redis.send('SET',[`iot:edge:auth:${identity}`,`${node}|approved`]);
    const helloDatabaseClockBefore=await databaseClockMilliseconds();
    let helloDatabaseTimeMs:bigint|undefined;
    let sequence=1n,epoch=0n;const received = new Map<number,Buffer[]>();
    const envelope=(tag:number,payload:Buffer)=>Buffer.concat([field(1,6),field(2,bytes(crypto.randomUUID())),field(3,bytes(node)),field(4,bytes(platform)),field(5,epoch),field(6,Date.now()),field(8,sequence++),field(tag,payload)]);
    let nextHeartbeatNonce=BigInt(Date.now())+17n;
    const sendHeartbeat=()=>socket!.send(envelope(24,field(26,nextHeartbeatNonce++)));
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(envelope(20,Buffer.concat([field(1,identity),field(2,'fixture'),field(3,'0.3.46'),field(23,1),field(36,1)])));
        socket!.onerror=()=>reject(Error('WebSocket error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){
                const helloAck=decode(message.get(21) as Buffer);
                helloDatabaseTimeMs=helloAck.get(7) as bigint|undefined;
                epoch=message.get(5) as bigint;clearTimeout(timer);resolve();
            }
            if(message.has(80))socket!.send(envelope(81,message.get(80) as Buffer));
            for(const tag of [30,31,32,50,41,25])if(message.has(tag)){
                const queue=received.get(tag)??[];queue.push(message.get(tag) as Buffer);received.set(tag,queue);
            }
        };
    });
    assert(helloDatabaseTimeMs!==undefined,'capable HelloAck omitted its PostgreSQL time sample');
    const helloDatabaseClockAfter=await databaseClockMilliseconds();
    assert(helloDatabaseTimeMs>=helloDatabaseClockBefore&&helloDatabaseTimeMs<=helloDatabaseClockAfter,
        'HelloAck database_time_ms was not sampled from the configured PostgreSQL clock');
    const heartbeatNonce=nextHeartbeatNonce;
    const heartbeatDatabaseClockBefore=await databaseClockMilliseconds();
    sendHeartbeat();
    await until(async()=>(received.get(25)??[]).some(payload=>decode(payload).get(6)===heartbeatNonce),
        'HeartbeatAck did not echo its request nonce');
    const correlatedHeartbeatAck=decode((received.get(25)??[]).find(payload=>decode(payload).get(6)===heartbeatNonce)!);
    assert.equal(typeof correlatedHeartbeatAck.get(7),'bigint','correlated HeartbeatAck omitted its PostgreSQL sample');
    const heartbeatDatabaseTimeMs=correlatedHeartbeatAck.get(7) as bigint;
    const heartbeatDatabaseClockAfter=await databaseClockMilliseconds();
    assert(heartbeatDatabaseTimeMs>=heartbeatDatabaseClockBefore&&heartbeatDatabaseTimeMs<=heartbeatDatabaseClockAfter,
        'HeartbeatAck database_time_ms was not sampled from the configured PostgreSQL clock');
    socket.send(envelope(26,field(7,Buffer.from([1,2,3,4,5,6]))));
    await until(async()=> (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'protocols' ? 'FINS' AND capability->>'commandStartBefore'='true'`).length>0,'capabilities not persisted');
    let legacyFixture:{device:string;link:string}|undefined;
    for(const [protocol,number] of [['MC',4],['FINS',5],['DLT645',6]] as const){
        received.clear();const link=crypto.randomUUID(),device=crypto.randomUUID(),point=crypto.randomUUID(),name=crypto.randomUUID();
        const config={storagePolicy:'report',readInterval:1,connection:protocol==='MC'?{frame:'4E',network:2}:protocol==='FINS'?{sourceNode:10,destinationNode:20}:{version:'2007',wakeupBytes:2,writePassword:'00123456',operatorCode:'00000001'},points:[{id:point,name:'edge point',unit:'kWh',writable:true,dataType:protocol==='DLT645'?'BCD':'UINT16',...(protocol==='DLT645'?{identifier:'00000000',length:4,digits:2}:{area:'D',address:100,bit:0,byteOrder:protocol==='MC'?'LITTLE_ENDIAN':'BIG_ENDIAN'})}]};
        await request('/v1/protocol/configs',{name,protocol,config});
        const [model]=await db`SELECT id FROM protocol_config WHERE name=${name}`;
        const endpoint={transport:'tcp',interface:'lo',mode:'TCP Client',ip:'127.0.0.1',port:5001+(number-4)};
        await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,edge_node_id,created_by) VALUES(${link},${name},${protocol},${endpoint}::jsonb,'enabled','edge',${node},${admin})`;
        await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by) VALUES(${device},${name},${link},${model.id},${{device_code:'000000123456',remote_control:true}}::jsonb,${admin})`;
        await request(`/v1/device/${device}/debug`,{enabled:true},'PUT');
        let deviceConfig:Map<number,bigint|Buffer>|undefined,pointConfig:Map<number,bigint|Buffer>|undefined;
        let deviceRevision:bigint|undefined,pointRevision:bigint|undefined,commit:Map<number,bigint|Buffer>|undefined;
        await until(async()=>{
            for(const payload of received.get(31)??[]){const item=decode(payload);
                if(item.has(11)){const value=decode(item.get(11) as Buffer);if((value.get(1) as Buffer)?.equals(bytes(device))){deviceConfig=value;deviceRevision=item.get(1) as bigint;}}
                if(item.has(17)){const value=decode(item.get(17) as Buffer);if((value.get(1) as Buffer)?.equals(bytes(device))){pointConfig=value;pointRevision=item.get(1) as bigint;}}
            }
            if(!deviceConfig||!pointConfig||typeof deviceRevision!=='bigint'||deviceRevision!==pointRevision)return false;
            commit=(received.get(32)??[]).map(decode).find(value=>value.get(1)===deviceRevision);
            return !!commit;
        },`${protocol} configuration commit not delivered`);
        assert.equal(Number(deviceConfig!.get(5)),number);assert.equal((pointConfig!.get(2) as Buffer).toString(),point);
        const connection=decode(deviceConfig!.get(27) as Buffer);
        if(protocol==='MC'){assert.equal(connection.get(1),1n);assert.equal(connection.get(2),2n);}
        if(protocol==='FINS'){assert.equal(connection.get(8),10n);assert.equal(connection.get(11),20n);}
        if(protocol==='DLT645'){assert.equal(connection.get(13),2007n);assert.equal((connection.get(15) as Buffer).toString('hex'),'00123456');}
        socket.send(envelope(33,Buffer.concat([field(1,commit!.get(1) as bigint),field(2,commit!.get(2) as Buffer)])));
        const report=crypto.randomUUID();
        const scalar=protocol==='DLT645'?Buffer.concat([field(1,7),field(8,'12345678901234.56')]):Buffer.concat([field(1,3),field(4,42)]);
        const record=Buffer.concat([field(1,bytes(report)),field(2,bytes(device)),field(3,bytes(link)),field(4,number),field(5,'POLL'),field(7,'UP'),field(8,Date.now()),field(9,Buffer.concat([field(1,point),field(2,'edge point'),field(3,'kWh'),field(4,scalar)])),field(14,Buffer.from([1,2])),field(15,bytes(report)),field(17,1),field(18,bytes(crypto.randomUUID()))]);
        socket.send(envelope(40,field(1,record)));
        await until(async()=> (await db`SELECT 1 FROM device_data WHERE device_id=${device}`).length>0,`${protocol} edge telemetry not persisted`);
        if(protocol==='MC'){
            await until(async()=>(received.get(25)??[]).some(payload=>decode(payload).get(3)===1n),
                'telemetry status request did not produce its unsolicited HeartbeatAck');
            const unsolicited=decode((received.get(25)??[]).find(payload=>decode(payload).get(3)===1n)!);
            assert.equal(unsolicited.has(6),false,'unsolicited HeartbeatAck unexpectedly had a correlating nonce');
            assert.equal(unsolicited.has(7),false,'unsolicited HeartbeatAck was exposed as a database time sample');
        }
        const [persisted]=await db`SELECT data->'values'->${point}->>'value' AS value FROM device_data WHERE device_id=${device}`;
        assert.equal(persisted.value,protocol==='DLT645'?'12345678901234.56':'42');
        const accepted=await request(`/v1/device/${device}/commands`,{idempotency_key:crypto.randomUUID(),elements:[{elementId:point,value:protocol==='DLT645'?'13.25':'13'}]});
        await until(async()=> (received.get(50)?.length??0)>0,`${protocol} edge command not delivered`);
        const command=decode(received.get(50)![0]);assert((command.get(2) as Buffer).equals(bytes(device)));
        assert.equal((decode(command.get(3) as Buffer).get(1) as Buffer).toString(),point);
        assert.equal(command.get(4),5000n,'command timeout_ms changed');
        assert.equal(typeof command.get(8),'bigint','negotiated command omitted start_before_ms');
        const operationId=accepted.data.command_ids[0];
        const [persistedDeadline]=await db`SELECT floor(extract(epoch FROM deadline)*1000)::bigint::text AS deadline_ms
            FROM command_attempt WHERE operation_id=${operationId}`;
        assert.equal(command.get(8),BigInt(persistedDeadline.deadline_ms),'wire deadline must equal the persisted attempt deadline');
        if(protocol==='MC'){
            const commandResult=(state:number,ackMissing:boolean,message:string)=>Buffer.concat([
                field(1,bytes(operationId)),field(2,state),field(3,message),
                field(5,ackMissing?1:0),field(6,Date.now()),field(7,bytes(device)),
            ]);
            socket.send(envelope(52,commandResult(5,true,'write response was lost')));
            await until(async()=> (await db`SELECT status FROM command_operation WHERE id=${operationId}`)[0]?.status==='UNKNOWN',
                'lost write ACK was persisted as definite rejection instead of UNKNOWN');
            socket.send(envelope(52,commandResult(3,false,'readback verified')));
            await until(async()=> (await db`SELECT status FROM command_operation WHERE id=${operationId}`)[0]?.status==='SUCCEEDED',
                'late verified success failed to resolve UNKNOWN');
            socket.send(envelope(52,commandResult(5,true,'late ambiguous disconnect')));
            await Bun.sleep(100);
            assert.equal((await db`SELECT status FROM command_operation WHERE id=${operationId}`)[0]?.status,'SUCCEEDED',
                'late ambiguous result overwrote verified success');
            const persisted=await enqueueDatabaseCommand(device,protocol,node);
            received.clear();
            sendHeartbeat();
            await until(async()=> (received.get(50)?.length??0)>0,'database-backed command was not sent');
            const exact=decode(received.get(50)![0]);
            assert.equal(exact.get(8),BigInt(persisted.deadlineMs),'sub-millisecond database deadline was not rounded down to its stored millisecond');
            assert.equal(exact.get(4),5000n,'database-backed command timeout_ms changed');
            const [claim]=await db`SELECT sent_at IS NOT NULL AS sent,
                floor(extract(epoch FROM deadline)*1000)::bigint::text AS deadline_ms
                FROM command_attempt WHERE operation_id=${persisted.commandId}`;
            assert.equal(claim.sent,true,'gateway did not persist its one-time send claim');
            assert.equal(exact.get(8),BigInt(claim.deadline_ms),'gateway recomputed its deadline instead of using the stored value');
            const commandQueue=`iot:v2:edge:commands:${node}`;
            const observedCount=received.get(50)?.length??0;
            const pushBinary=`local wire=ARGV[1]:gsub('..',function(pair) return string.char(tonumber(pair,16)) end); return redis.call('RPUSH',KEYS[1],wire)`;
            await redis.send('EVAL',[pushBinary,'1',commandQueue,persisted.wire.toString('hex')]);
            sendHeartbeat();
            await until(async()=>Number(await redis.send('LLEN',[commandQueue]))===0,'duplicate command was not consumed');
            await Bun.sleep(100);
            assert.equal(received.get(50)?.length??0,observedCount,'claimed command was resent');
            legacyFixture={device,link};
        }
        if(protocol!=='MC'){
            await db`UPDATE device SET deleted_at=NOW() WHERE id=${device}`;
            await db`UPDATE link SET deleted_at=NOW(),status='disabled' WHERE id=${link}`;
        }
        console.log(`PASS ${protocol} edge capability, configuration, precise telemetry and command dispatch`);
    }
    assert(legacyFixture,'MC fixture was not retained for the legacy session test');
    const closed=new Promise<void>(resolve=>socket!.addEventListener('close',()=>resolve(),{once:true}));
    socket.close();await closed;
    await until(async()=>Number(await redis.send('EXISTS',[`iot:edge:session:${node}`]))===0,'closed edge session retained its lease');
    const legacy=await enqueueDatabaseCommand(legacyFixture.device,'MC',node);
    received.clear();let legacySequence=1n,legacyEpoch=0n;
    const legacyEnvelope=(tag:number,payload:Buffer)=>Buffer.concat([field(1,6),field(2,bytes(crypto.randomUUID())),field(3,bytes(node)),field(4,bytes(platform)),field(5,legacyEpoch),field(6,Date.now()),field(8,legacySequence++),field(tag,payload)]);
    let legacyHelloAck:Map<number,bigint|Buffer>|undefined;
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('legacy-compatible Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(legacyEnvelope(20,Buffer.concat([field(1,identity),field(2,'fixture'),field(3,'0.3.44'),field(23,1)])));
        socket!.onerror=()=>reject(Error('legacy-compatible WebSocket error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){legacyHelloAck=decode(message.get(21) as Buffer);legacyEpoch=message.get(5) as bigint;clearTimeout(timer);resolve();}
            if(message.has(80))socket!.send(legacyEnvelope(81,message.get(80) as Buffer));
            for(const tag of [25,50])if(message.has(tag)){const queue=received.get(tag)??[];queue.push(message.get(tag) as Buffer);received.set(tag,queue);}
        };
    });
    assert(legacyHelloAck,'legacy HelloAck was not captured');
    assert.equal(legacyHelloAck.has(7),false,'0.3.44-compatible HelloAck included the new time field');
    socket.send(legacyEnvelope(24,Buffer.alloc(0)));
    await until(async()=>(received.get(25)?.length??0)>0,'legacy HeartbeatAck was not sent');
    const legacyHeartbeatAck=decode(received.get(25)![0]);
    assert.equal(legacyHeartbeatAck.has(1),true,'legacy platform_time_ms was removed');
    assert.equal(legacyHeartbeatAck.has(6),false,'legacy HeartbeatAck unexpectedly included a nonce');
    assert.equal(legacyHeartbeatAck.has(7),false,'legacy HeartbeatAck included the new database time field');
    await until(async()=> (received.get(50)?.length??0)>0,'legacy command was not delivered');
    const legacyCommand=decode(received.get(50)![0]);
    assert.equal(legacyCommand.has(8),false,'legacy Hello unexpectedly enabled the optional deadline field');
    assert.equal(legacyCommand.get(4),5000n,'0.3.44-compatible timeout_ms changed');
    const [legacyClaim]=await db`SELECT sent_at IS NOT NULL AS sent,
        floor(extract(epoch FROM deadline)*1000)::bigint::text AS deadline_ms
        FROM command_attempt WHERE operation_id=${legacy.commandId}`;
    assert.equal(legacyClaim.sent,true,'legacy command send claim was not persisted');
    assert.equal(BigInt(legacyClaim.deadline_ms),BigInt(legacy.deadlineMs),'legacy attempt deadline changed during send claim');

    const legacyClosed=new Promise<void>(resolve=>socket!.addEventListener('close',()=>resolve(),{once:true}));
    socket.close();await legacyClosed;
    await until(async()=>Number(await redis.send('EXISTS',[`iot:edge:session:${node}`]))===0,'legacy session did not close');
    const recovery=await enqueueDatabaseCommand(legacyFixture.device,'MC',node);
    let recoverySequence=1n,recoveryEpoch=0n,probe:Buffer|undefined;
    const attempted:Buffer[]=[];
    const recoveryEnvelope=(tag:number,payload:Buffer)=>Buffer.concat([
        field(1,6),field(2,bytes(crypto.randomUUID())),field(3,bytes(node)),field(4,bytes(platform)),
        field(5,recoveryEpoch),field(6,Date.now()),field(8,recoverySequence++),field(tag,payload)]);
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    const unresponsiveClosed=new Promise<void>(resolve=>socket!.addEventListener('close',()=>resolve(),{once:true}));
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('unresponsive Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(recoveryEnvelope(20,Buffer.concat([field(1,identity),field(2,'fixture'),field(3,'0.3.79'),field(23,1),field(36,1)])));
        socket!.onerror=()=>reject(Error('unresponsive WebSocket error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){recoveryEpoch=message.get(5) as bigint;clearTimeout(timer);resolve();}
            if(message.has(80))probe=message.get(80) as Buffer;
            if(message.has(50))attempted.push(message.get(50) as Buffer);
        };
    });
    await until(async()=>probe!==undefined,'pending command did not trigger a connection probe');
    const nonce=decode(probe!).get(1) as bigint;
    socket.send(recoveryEnvelope(81,field(1,nonce+1n)));
    await Bun.sleep(250);
    assert.equal(attempted.length,0,'an unrelated Pong released the physical command');
    let [unsent]=await db`SELECT sent_at IS NULL AS unsent FROM command_attempt WHERE operation_id=${recovery.commandId}`;
    assert.equal(unsent.unsent,true,'the gateway claimed an unconfirmed connection');
    assert.equal(Number(await redis.send('LLEN',[`iot:v2:edge:commands:${node}`])),1,'probe removed the pending command');
    await Promise.race([unresponsiveClosed,Bun.sleep(12000).then(()=>{throw Error('unresponsive command probe did not close the connection');})]);
    assert.equal(attempted.length,0,'an unresponsive connection received the physical command');
    [unsent]=await db`SELECT sent_at IS NULL AS unsent FROM command_attempt WHERE operation_id=${recovery.commandId}`;
    assert.equal(unsent.unsent,true,'probe timeout marked the command as sent');
    await until(async()=>Number(await redis.send('EXISTS',[`iot:edge:session:${node}`]))===0,'unresponsive session retained its lease');
    await redis.send('LPUSH',[`iot:v2:edge:commands:${node}`,'invalid-envelope']);

    recoverySequence=1n;recoveryEpoch=0n;
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('recovery Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(recoveryEnvelope(20,Buffer.concat([field(1,identity),field(2,'fixture'),field(3,'0.3.79'),field(23,1),field(36,1)])));
        socket!.onerror=()=>reject(Error('recovery WebSocket error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){recoveryEpoch=message.get(5) as bigint;clearTimeout(timer);resolve();}
            if(message.has(80))socket!.send(recoveryEnvelope(81,message.get(80) as Buffer));
            if(message.has(50))attempted.push(message.get(50) as Buffer);
        };
    });
    await until(async()=>attempted.length===1,'a discarded invalid entry blocked the queued command after reconnect');
    assert((decode(attempted[0]).get(1) as Buffer).equals(bytes(recovery.commandId)),'reconnect changed the command identity');
    const sentClosed=new Promise<void>(resolve=>socket!.addEventListener('close',()=>resolve(),{once:true}));
    socket.close();await sentClosed;
    await until(async()=>Number(await redis.send('EXISTS',[`iot:edge:session:${node}`]))===0,'sent session retained its lease');

    recoverySequence=1n;recoveryEpoch=0n;
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('result replay Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(recoveryEnvelope(20,Buffer.concat([field(1,identity),field(2,'fixture'),field(3,'0.3.79'),field(23,1),field(36,1)])));
        socket!.onerror=()=>reject(Error('result replay WebSocket error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){recoveryEpoch=message.get(5) as bigint;clearTimeout(timer);resolve();}
            if(message.has(80))socket!.send(recoveryEnvelope(81,message.get(80) as Buffer));
            if(message.has(50))attempted.push(message.get(50) as Buffer);
        };
    });
    const replayedResult=Buffer.concat([field(1,bytes(recovery.commandId)),field(2,3),field(3,'readback verified'),
        field(6,Date.now()),field(7,bytes(legacyFixture.device))]);
    socket.send(recoveryEnvelope(52,replayedResult));
    await until(async()=>(await db`SELECT status FROM command_operation WHERE id=${recovery.commandId}`)[0]?.status==='SUCCEEDED',
        'replayed command result was not persisted after reconnect');
    socket.send(recoveryEnvelope(52,replayedResult));
    await Bun.sleep(250);
    assert.equal(attempted.length,1,'a claimed physical command was resent after reconnect');
    assert.equal((await db`SELECT status FROM command_operation WHERE id=${recovery.commandId}`)[0]?.status,'SUCCEEDED');
    console.log('PASS command waits for the matching Pong, survives an unresponsive connection and accepts a replayed result without resending the physical command');

    await db`UPDATE device SET deleted_at=NOW() WHERE id=${legacyFixture.device}`;
    await db`UPDATE link SET deleted_at=NOW(),status='disabled' WHERE id=${legacyFixture.link}`;
    console.log('PASS persisted command deadline milliseconds, one-time send claim and 0.3.44-compatible wire behavior');
    socket.send(recoveryEnvelope(26,Buffer.alloc(0)));
    await until(async()=> (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'protocols'='[]'::jsonb`).length>0,'legacy capability did not clear new protocols');
} finally {socket?.close();await redis.send('DEL',[`iot:edge:auth:${identity}`]);redis.close();await db.close();}
