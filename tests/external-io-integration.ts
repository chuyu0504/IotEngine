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
function doubleField(tag: number, value: number) {
    const bytes = Buffer.alloc(8); bytes.writeDoubleLE(value);
    return Buffer.concat([integer(tag * 8 + 1), bytes]);
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
function imei() {
    const base='99'+String(Math.floor(Math.random()*1e12)).padStart(12,'0'); let sum=0;
    for(let i=0;i<14;i++){let n=Number(base[i])*(i%2?2:1);if(n>9)n-=9;sum+=n;}return base+String((10-sum%10)%10);
}
const node=crypto.randomUUID(),identity=imei(),link=crypto.randomUUID(),device=crypto.randomUUID();
let primary=crypto.randomUUID(),io=crypto.randomUUID();
const point=crypto.randomUUID(),analog=crypto.randomUUID(),output=crypto.randomUUID(),derived=crypto.randomUUID(),ioDerived=crypto.randomUUID(),combined=crypto.randomUUID();
let socket:WebSocket|undefined,epoch=0n,sequence=1n;
const received=new Map<number,Buffer[]>();
const envelope=(tag:number,payload:Buffer)=>Buffer.concat([field(1,6),field(2,bytes(crypto.randomUUID())),field(3,bytes(node)),field(4,bytes(platform)),field(5,epoch),field(6,Date.now()),field(8,sequence++),field(tag,payload)]);
try {
    await until(async () => (await fetch(apiBase+'/internal/health/ready')).status===200,'fixture not ready');
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},${identity},'approved')`;
    await redis.send('SET',[`iot:edge:auth:${identity}`,`${node}|approved`]);
    const primaryConfig={storagePolicy:'change',readInterval:1,byteOrder:'BIG_ENDIAN',registers:[{id:point,name:'pressure',registerType:'HOLDING_REGISTER',dataType:'UINT16',address:100,quantity:1}]};
    const ioConfig={storagePolicy:'report',points:[{id:analog,name:'current',kind:'AI',channel:'AI0',unit:'uA'},{id:output,name:'switch',kind:'DO',channel:'do0'}],derivedPoints:[{id:ioDerived,name:'double current',kind:'expression',valueType:'number',expression:'x * 2',inputs:[{alias:'x',pointId:analog}],unit:'uA',unitMode:'fixed',maxAgeSeconds:30,visible:true}]};
    const primaryName=primary,ioName=io;
    await request('/v1/protocol/configs',{name:primaryName,protocol:'Modbus',config:primaryConfig});
    await request('/v1/protocol/configs',{name:ioName,protocol:'ExternalIO',config:ioConfig});
    primary=(await db`SELECT id FROM protocol_config WHERE name=${primaryName}`)[0].id;
    io=(await db`SELECT id FROM protocol_config WHERE name=${ioName}`)[0].id;
    const endpoint={transport:'tcp',interface:'lo',mode:'TCP Client',ip:'127.0.0.1',port:502};
    await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,edge_node_id,created_by) VALUES(${link},${link},'Modbus',${endpoint}::jsonb,'enabled','edge',${node},${admin})`;
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by) VALUES(${device},${device},${link},${primary},${{device_code:'1',remote_control:true,modbus_mode:'TCP',slave_id:1}}::jsonb,${admin})`;
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(envelope(20,Buffer.concat([field(1,identity),field(2,'Ebyte T113-I ECM30'),field(3,'0.3.78'),field(23,1),field(36,1),field(37,1),field(38,1),field(39,1),field(41,1)])));
        socket!.onerror=()=>reject(Error('WebSocket error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){epoch=message.get(5) as bigint; assert.equal(decode(message.get(21) as Buffer).get(9),1n);clearTimeout(timer);resolve();}
            if(message.has(80))socket!.send(envelope(81,message.get(80) as Buffer));
            for(const tag of [30,31,32,41,50])if(message.has(tag)){const queue=received.get(tag)??[];queue.push(message.get(tag) as Buffer);received.set(tag,queue);}
        };
    });
    const channel=(name:string,kind:number,controllable=false)=>field(11,Buffer.concat([field(1,name),field(2,kind),field(3,1),field(4,Number(controllable))]));
    socket.send(envelope(26,Buffer.concat([field(7,Buffer.from([1,2,3,4,5,6])),field(10,1),field(12,1),channel('AI0',3),channel('AI1',3),channel('do0',2,true)])));
    await until(async()=> (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'externalIo'->>'supported'='true'`).length>0,'IO capability missing');
    const definition={id:derived,name:'sum',kind:'expression',valueType:'number',expression:'p + io',inputs:[{alias:'p',pointId:point},{alias:'io',pointId:analog}],unit:'',unitMode:'fixed',maxAgeSeconds:30,visible:true};
    const chained={...definition,id:combined,name:'chained sum',inputs:[{alias:'p',pointId:point},{alias:'io',pointId:ioDerived}]};
    await request(`/v1/device/${device}`,{externalIo:{protocolConfigId:io,derivedPoints:[definition,chained]}},'PUT');
    const response=await fetch(`${apiBase}/v1/device/${device}`,{headers:{Authorization:`Bearer ${token}`}}); const detail=await response.json();
    assert.equal(response.status,200,JSON.stringify(detail));assert.equal(detail.data.externalIo.protocolConfigId,io);assert.equal(detail.data.externalIo.bindings,undefined,'device must only select a scheme');
    assert.deepEqual((await db`SELECT point_id,channel FROM device_external_io_binding WHERE device_id=${device} ORDER BY channel`).map(row=>[row.point_id,row.channel]),[[analog,'AI0'],[output,'do0']]);
    const otherNode=crypto.randomUUID();
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${otherNode},${platform},${imei()},'approved')`;
    try {
        await assert.rejects(async()=>{await db`UPDATE device_external_io SET node_id=${otherNode} WHERE device_id=${device}`;},(error: any)=>['23514','23503'].includes(error.errno),'a binding must not be moved to another edge node');
    } finally {
        await db`DELETE FROM edge_node WHERE id=${otherNode}`;
    }
    const invalid=await fetch(`${apiBase}/v1/protocol/configs/${primary}`,{method:'PUT',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:JSON.stringify({config:{registers:[]}})});
    assert.notEqual(invalid.status,200,'model edit removed an input used by a device-level derived point');
    const changedPoints=[{...ioConfig.points[0],name:'current updated'},ioConfig.points[1]];
    await request(`/v1/protocol/configs/${io}`,{config:{points:changedPoints}},'PUT');
    await until(async()=> (received.get(31)??[]).some(payload=>{const item=decode(payload);if(!item.has(20))return false;const value=decode(item.get(20) as Buffer);return (value.get(3) as Buffer)?.toString()===analog && (value.get(6) as Buffer)?.toString()==='current updated';}),'IO model edit did not sync to its edge node');
    await request(`/v1/edge/${node}/sync`,{});
    await until(async()=> (received.get(31)??[]).filter(payload=>decode(payload).has(20)).length>=2,'external IO config not sent');
    const items=(received.get(31)??[]).map(decode).filter(value=>value.has(20)).map(value=>decode(value.get(20) as Buffer));
    assert(items.some(value=>(value.get(1) as Buffer).equals(bytes(device))&&(value.get(2) as Buffer).equals(bytes(io))&&(value.get(3) as Buffer).toString()===analog));
    const [desired]=await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`;
    const desiredRevision=BigInt(desired.revision);
    await until(async()=> (received.get(32)??[]).some(payload=>decode(payload).get(1)===desiredRevision),'configuration commit not delivered');
    const commit=decode((received.get(32)??[]).find(payload=>decode(payload).get(1)===desiredRevision)!);
    const previousDigest=commit.get(2) as Buffer;
    socket.send(envelope(33,Buffer.concat([field(1,commit.get(1) as bigint),field(2,commit.get(2) as Buffer)])));
    await until(async()=> (await db`SELECT 1 FROM edge_node WHERE id=${node} AND status->'config'->>'state'='applied' AND status->'config'->>'activeVersion'=status->'config'->>'desiredVersion'`).length>0,'configuration was not applied');
    const sendRecord=(record:Buffer)=>socket!.send(envelope(40,field(1,record)));
    const t=Date.now();
    const common=(id:string,protocol:number,model:string,time:number,digest=previousDigest)=>[field(1,bytes(id)),field(2,bytes(device)),field(3,bytes(link)),field(4,protocol),field(8,time),field(12,bytes(model)),field(21,digest)];
    sendRecord(Buffer.concat([...common(crypto.randomUUID(),2,primary,t),field(14,Buffer.from([0,7,0,0,0,5,1,3,2,0,10])),field(20,Buffer.from([0,7,0,0,0,6,1,3,0,100,0,1]))]));
    const aiValue=Buffer.concat([field(1,analog),field(4,Buffer.concat([field(1,4),doubleField(5,0.002)]))]);
    sendRecord(Buffer.concat([...common(crypto.randomUUID(),7,io,t+1),field(9,aiValue)]));
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE device_id=${device} AND data->'values'->${derived}->>'value'='12'`).length>0,'cross-source derived value not persisted');
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE device_id=${device} AND data->'values'->${combined}->>'value'='14'`).length>0,'chained cross-source value not persisted');
    // Derived history commits with the acquisition journal before raw telemetry
    // reaches its history consumer; wait for both durable results independently.
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE device_id=${device} AND protocol='ExternalIO' AND source='edge' AND data->'values'->${analog}->>'value'='2'`).length>0,'raw IO value not persisted');
    const rows=await db`SELECT protocol,source,data->'values' AS values FROM device_data WHERE device_id=${device}`;
    assert(rows.some(row=>row.protocol==='ExternalIO'&&row.values[analog]?.value===2));
    assert(rows.some(row=>row.source==='derived'&&row.values[derived]?.value===12));
    const accepted=await request(`/v1/device/${device}/commands`,{idempotency_key:crypto.randomUUID(),elements:[{elementId:output,value:'1'}]});
    await until(async()=> (received.get(50)?.length??0)>0,'IO command not delivered');
    const command=decode(received.get(50)!.at(-1)!);assert.equal(command.get(9),1n);assert.equal((decode(command.get(3) as Buffer).get(1) as Buffer).toString(),output);
    const operation=accepted.data.command_ids[0];
    socket.send(envelope(52,Buffer.concat([field(1,bytes(operation)),field(2,3),field(6,Date.now()),field(7,bytes(device))])));
    await until(async()=> (await db`SELECT status FROM command_operation WHERE id=${operation}`)[0]?.status==='SUCCEEDED','IO command result missing');
    const [saved]=await db`SELECT protocol,model_id FROM command_operation WHERE id=${operation}`;assert.equal(saved.protocol,'ExternalIO');assert.equal(saved.model_id,io);
    sendRecord(Buffer.concat([...common(crypto.randomUUID(),7,io,t+2),field(9,aiValue)]));
    await until(async()=> Number((await db`SELECT count(*) AS count FROM device_data WHERE device_id=${device} AND protocol='ExternalIO' AND source='edge' AND data->'values'->${analog}->>'value'='2'`)[0].count)>=2,'IO report policy inherited the acquisition protocol change policy');
    // An older sample must be retained in history without moving the combined
    // calculation backwards or replacing the newer live value.
    const late=crypto.randomUUID();
    const oldAi=Buffer.concat([field(1,analog),field(4,Buffer.concat([field(1,4),doubleField(5,0.001)]))]);
    sendRecord(Buffer.concat([...common(late,7,io,t-1000),field(9,oldAi)]));
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE device_id=${device} AND data->'values'->${analog}->>'value'='1'`).length>0,'late IO sample missing from history');
    await until(async()=> {
        const value=await redis.send('HGET',[`iot:v2:device:${device}:latest`,derived]);
        const sourceValue=await redis.send('HGET',[`iot:v2:device:${device}:latest`,ioDerived]);
        const chainedValue=await redis.send('HGET',[`iot:v2:device:${device}:latest`,combined]);
        return value !== null && JSON.parse(String(value)).value === '12' && sourceValue !== null && JSON.parse(String(sourceValue)).value === '4' && chainedValue !== null && JSON.parse(String(chainedValue)).value === '14';
    },'combined value did not reach the live device view');
    const live=JSON.parse(String(await redis.send('HGET',[`iot:v2:device:${device}:latest`,derived])));
    assert.equal(live.value,'12','late IO sample regressed the live derived value');

    // Exercise durable incremental state and a late replay spanning multiple pages.
    for(let index=0;index<300;index++)sendRecord(Buffer.concat([...common(crypto.randomUUID(),2,primary,t+3+index*2),field(14,Buffer.from([0,7,0,0,0,5,1,3,2,0,10])),field(20,Buffer.from([0,7,0,0,0,6,1,3,0,100,0,1]))]));
    await until(async()=> Number((await db`SELECT count(*) AS count FROM edge_acquisition_sample WHERE node_id=${node} AND digest=${previousDigest.toString('hex')} AND device_id=${device}`)[0].count)>=304,'incremental acquisition journal did not commit all inputs');
    const historicalLate=crypto.randomUUID();
    sendRecord(Buffer.concat([...common(historicalLate,2,primary,t+104),field(14,Buffer.from([0,7,0,0,0,5,1,3,2,0,6])),field(20,Buffer.from([0,7,0,0,0,6,1,3,0,100,0,1]))]));
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE device_id=${device} AND source='derived' AND configuration_digest=${previousDigest.toString('hex')} AND report_time=to_timestamp(${t+104}/1000.0) AND data->'values'->${derived}->>'value'='8'`).length>0,'late input did not repair historical values across replay page boundaries');
    const [checkpoint]=await db`SELECT observed_at_ms FROM edge_acquisition_calculation WHERE node_id=${node} AND digest=${previousDigest.toString('hex')} AND device_id=${device}`;
    assert.equal(Number(checkpoint.observed_at_ms),t+601,'late replay regressed the durable calculation cursor');
    await until(async()=> (await db`SELECT 1 FROM device_latest_value WHERE device_id=${device} AND element_id=${derived} AND value->>'value'='12'`).length>0,'current derived values were not persisted for restart recovery');

    // Disconnect the node with its previous snapshot and change both sources.
    // New samples use the new digest; delayed old records must be parsed with
    // their original snapshot and must not alter current values or alerts.
    const disconnected=socket;socket=undefined;disconnected.onclose=null;
    await new Promise<void>((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('edge close timeout')),5000);disconnected.addEventListener('close',()=>{clearTimeout(timer);resolve();},{once:true});disconnected.close();});
    await until(async()=>Number(await redis.send('EXISTS',[`iot:edge:session:${node}`]))===0,'edge session survived disconnect');
    const newRegisters=primaryConfig.registers.map(value=>({...value,address:101}));
    await request(`/v1/protocol/configs/${primary}`,{config:{registers:newRegisters}},'PUT');
    await request(`/v1/protocol/configs/${io}`,{config:{points:changedPoints.map(point=>point.id===analog?{...point,channel:'AI1'}:point)}},'PUT');
    assert.equal((await db`SELECT channel FROM device_external_io_binding WHERE device_id=${device} AND point_id=${analog}`)[0].channel,'AI1','scheme channel update must atomically rebuild device reservations');
    await request(`/v1/edge/${node}/sync`,{});
    const [newDesired]=await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`;
    const newRevision=BigInt(newDesired.revision);
    received.clear();epoch=0n;sequence=1n;
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('reconnect Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(envelope(20,Buffer.concat([field(1,identity),field(2,'Ebyte T113-I ECM30'),field(3,'0.3.78'),field(8,desiredRevision),field(23,1),field(36,1),field(37,1),field(38,1),field(39,1),field(41,1)])));
        socket!.onerror=()=>reject(Error('WebSocket reconnect error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){epoch=message.get(5) as bigint;assert.equal(decode(message.get(21) as Buffer).get(9),1n);clearTimeout(timer);resolve();}
            if(message.has(80))socket!.send(envelope(81,message.get(80) as Buffer));
            for(const tag of [30,31,32,41,50])if(message.has(tag)){const queue=received.get(tag)??[];queue.push(message.get(tag) as Buffer);received.set(tag,queue);}
        };
    });
    socket.send(envelope(26,Buffer.concat([field(7,Buffer.from([1,2,3,4,5,6])),field(10,1),field(12,1),channel('AI0',3),channel('AI1',3),channel('do0',2,true)])));
    await until(async()=> (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'externalIo'->>'supported'='true' AND capability->>'derivedPoints'='true'`).length>0,'IO capability was not restored after reconnect');
    await until(async()=> (received.get(32)??[]).some(payload=>decode(payload).get(1)===newRevision),'replacement configuration not delivered');
    const nextCommit=decode((received.get(32)??[]).find(payload=>decode(payload).get(1)===newRevision)!);
    const nextDigest=nextCommit.get(2) as Buffer;assert(!nextDigest.equals(previousDigest));
    socket.send(envelope(33,Buffer.concat([field(1,newRevision),field(2,nextDigest)])));
    await until(async()=> (await db`SELECT 1 FROM edge_node WHERE id=${node} AND status->'config'->>'state'='applied' AND status->'config'->>'activeVersion'=status->'config'->>'desiredVersion'`).length>0,'replacement configuration not applied');
    const nextTime=Date.now();
    sendRecord(Buffer.concat([...common(crypto.randomUUID(),2,primary,nextTime,nextDigest),field(14,Buffer.from([0,8,0,0,0,5,1,3,2,0,11])),field(20,Buffer.from([0,8,0,0,0,6,1,3,0,101,0,1]))]));
    const nextAi=Buffer.concat([field(1,analog),field(4,Buffer.concat([field(1,4),doubleField(5,0.005)]))]);
    sendRecord(Buffer.concat([...common(crypto.randomUUID(),7,io,nextTime+1,nextDigest),field(9,nextAi)]));
    await until(async()=>{const value=await redis.send('HGET',[`iot:v2:device:${device}:latest`,derived]);return value!==null&&JSON.parse(String(value)).value==='16';},'new configuration did not calculate its own values');
    const oldRaw=crypto.randomUUID(),oldIo=crypto.randomUUID(),lateTime=Date.now();
    const replayedRaw=Buffer.concat([...common(oldRaw,2,primary,lateTime,previousDigest),field(14,Buffer.from([0,9,0,0,0,5,1,3,2,0,99])),field(20,Buffer.from([0,9,0,0,0,6,1,3,0,100,0,1]))]);
    sendRecord(replayedRaw);sendRecord(replayedRaw);
    const oldIoValue=Buffer.concat([field(1,analog),field(4,Buffer.concat([field(1,4),doubleField(5,0.099)]))]);
    sendRecord(Buffer.concat([...common(oldIo,7,io,lateTime,previousDigest),field(9,oldIoValue)]));
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE id=${oldRaw} AND data->'values'->${point}->>'value'='99'`).length>0 && (await db`SELECT 1 FROM device_data WHERE id=${oldIo} AND data->'values'->${analog}->>'value'='99'`).length>0,'old snapshot samples did not replay into history');
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE device_id=${device} AND source='derived' AND configuration_digest=${previousDigest.toString('hex')} AND data->'values'->${derived}->>'value'='198'`).length>0,'old snapshot cross-source derived history was not recomputed');
    assert.equal(Number((await db`SELECT count(*) AS count FROM device_data WHERE id=${oldRaw}`)[0].count),1,'replaying a sample twice must not duplicate history');
    const oldEqual=crypto.randomUUID();
    sendRecord(Buffer.concat([...common(oldEqual,2,primary,Date.now(),previousDigest),field(14,Buffer.from([0,10,0,0,0,5,1,3,2,0,11])),field(20,Buffer.from([0,10,0,0,0,6,1,3,0,100,0,1]))]));
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE id=${oldEqual}`).length>0,'old snapshot sample equal to the current value was incorrectly discarded');
    const latestPrimary=JSON.parse(String(await redis.send('HGET',[`iot:v2:device:${device}:latest`,point])));
    const latestIo=JSON.parse(String(await redis.send('HGET',[`iot:v2:device:${device}:latest`,analog])));
    const latestDerived=JSON.parse(String(await redis.send('HGET',[`iot:v2:device:${device}:latest`,derived])));
    assert.equal(latestPrimary.value,'11');assert.equal(latestIo.value,'5');assert.equal(latestDerived.value,'16');

    // Queue a sample after projection while an exclusive config transaction
    // holds every final consumer at its version fence. Commit a changed scheme
    // before releasing them; all four consumers must re-check the old epoch.
    const raced=crypto.randomUUID(),raceTime=Date.now();
    const [activeEpoch]=await db`SELECT epoch FROM edge_acquisition_epoch WHERE node_id=${node}`;
    const fields={acquisition_id:raced,raw_packet_ids:'[]',event_type:'device.data.parsed',schema_version:'2',aggregate_id:device,model_id:primary,event_kind:'sample',history_only:'0',configuration_digest:nextDigest.toString('hex'),configuration_epoch:String(activeEpoch.epoch),message_id:raced,causation_id:raced,link_id:link,device_id:device,device_code:'1',protocol:'Modbus',connection_id:node,occurred_at_ms:String(raceTime),observed_at_ms:String(raceTime),storage_policy:'report',online_window_ms:'300000',source:'edge',values_json:JSON.stringify({values:{[point]:{value:777,quality:'good',sample_time_ms:raceTime}}}),raw_payload_hex:'[]'};
    const flat=Object.entries(fields).flat();
    await db.begin(async tx=>{
        await tx`SELECT pg_advisory_xact_lock(5282804697543808077::bigint)`;
        for(const kind of ['history','latest','alerts','delivery']) {
            const queue=`iot:v3:telemetry:${kind}`;
            const id=String(await redis.send('XADD',[queue,'*',...flat]));
            const database=Number(new URL(redisUrl).pathname.slice(1)||0);
            await redis.send('PUBLISH',[`iot:db:${database}:consumer:input:${queue}`,id]);
        }
        await until(async()=>Number((await tx`SELECT count(*) AS count FROM pg_locks WHERE locktype='advisory' AND NOT granted`)[0].count)>0,'final consumers did not wait at the configuration fence');
        await tx`UPDATE protocol_config SET name=name||' raced' WHERE id=${io}`;
    });
    await until(async()=> (await db`SELECT 1 FROM device_data WHERE id=${raced}`).length>0,'sample queued across configuration switch was not retained as history');
    await until(async()=>Number(await redis.send('XLEN',['iot:v3:telemetry:latest']))===0&&Number(await redis.send('XLEN',['iot:v3:telemetry:alerts']))===0&&Number(await redis.send('XLEN',['iot:v3:telemetry:delivery']))===0,'stale consumer copies were not drained');
    assert.equal(JSON.parse(String(await redis.send('HGET',[`iot:v2:device:${device}:latest`,point]))).value,'11','queued old version overwrote realtime values');
    assert.notEqual((await db`SELECT data->'values'->${point}->>'value' AS value FROM alert_input_state WHERE device_id=${device}`)[0]?.value,'777','queued old version entered current alert evaluation');

    // Firmware without mA IO can still support derived points for other protocols.
    const oldSocket=socket; socket=undefined; oldSocket.onclose=null;
    await new Promise<void>((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('edge close timeout')),5000);oldSocket.addEventListener('close',()=>{clearTimeout(timer);resolve();},{once:true});oldSocket.close();});
    await until(async()=>Number(await redis.send('EXISTS',[`iot:edge:session:${node}`]))===0,'old edge session survived');
    received.clear(); epoch=0n; sequence=1n;
    socket=new WebSocket(apiBase.replace('http:','ws:')+'/edge/v1/connect');socket.binaryType='arraybuffer';
    await new Promise<void>((resolve,reject)=>{
        const timer=setTimeout(()=>reject(Error('legacy Hello timeout')),10000);
        socket!.onopen=()=>socket!.send(envelope(20,Buffer.concat([field(1,identity),field(2,'legacy-test'),field(3,'0.3.77'),field(23,1),field(38,1),field(39,1)])));
        socket!.onerror=()=>reject(Error('legacy WebSocket error'));
        socket!.onmessage=event=>{
            const message=decode(Buffer.from(event.data as ArrayBuffer));
            if(message.has(21)){epoch=message.get(5) as bigint;assert.notEqual(decode(message.get(21) as Buffer).get(9),1n);clearTimeout(timer);resolve();}
            if(message.has(80))socket!.send(envelope(81,message.get(80) as Buffer));
            for(const tag of [30,31,32])if(message.has(tag)){const queue=received.get(tag)??[];queue.push(message.get(tag) as Buffer);received.set(tag,queue);}
        };
    });
    socket.send(envelope(26,Buffer.concat([field(7,Buffer.from([1,2,3,4,5,6])),field(10,1)])));
    await until(async()=> (await db`SELECT 1 FROM edge_node WHERE id=${node} AND capability->'externalIo'->>'supported'='false' AND capability->>'derivedPoints'='true'`).length>0,'legacy capability not reset');
    received.clear();await request(`/v1/edge/${node}/sync`,{});
    const [revisionRow]=await db`SELECT status->'config'->>'desiredVersion' AS revision FROM edge_node WHERE id=${node}`;
    const revision=BigInt(revisionRow.revision);
    await until(async()=> (received.get(32)??[]).some(payload=>decode(payload).get(1)===revision),'legacy configuration not delivered');
    const legacyItems=(received.get(31)??[]).map(decode).filter(value=>value.get(1)===revision);
    assert(legacyItems.some(value=>value.has(11)),'legacy acquisition device was omitted');
    assert(legacyItems.every(value=>!value.has(20)),'legacy firmware received IO configuration');
    console.log('PASS mixed Modbus + external IO configuration, telemetry, cross-source calculation and command');
} catch (error) {
    console.error('IO integration diagnostics', JSON.stringify(await db`SELECT protocol,source,data->'values' AS values FROM device_data WHERE device_id=${device} ORDER BY report_time DESC LIMIT 8`));
    throw error;
} finally {
    if(socket){socket.onclose=null;socket.close();}
    await db`UPDATE device SET deleted_at=NOW() WHERE id=${device}`;
    await db`UPDATE link SET status='disabled',deleted_at=NOW() WHERE id=${link}`;
    await db`UPDATE protocol_config SET deleted_at=NOW() WHERE id IN (${primary},${io})`;
    await redis.send('DEL',[`iot:edge:auth:${identity}`]);redis.close();await db.close();
}
