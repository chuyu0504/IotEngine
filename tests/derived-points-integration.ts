import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
const db=new Bun.SQL(databaseUrl),redis=new Bun.RedisClient(redisUrl);
const uuid=()=>crypto.randomUUID(), admin='00000000-0000-7000-8000-000000000002';
const link=uuid(),target=uuid(),model=uuid(),device=uuid(),pressure=uuid(),mode=uuid(),derived=uuid(),average=uuid();
const encode=(v:unknown)=>Buffer.from(JSON.stringify(v)).toString('base64url'), now=Math.floor(Date.now()/1000);
const unsigned=`${encode({alg:'HS256',typ:'JWT'})}.${encode({iss:'iot-engine',aud:'iot-engine-web',sub:admin,user_id:admin,username:'derived-test',token_type:'access',iat:now,exp:now+3600})}`;
const token=`${unsigned}.${createHmac('sha256','architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
async function http(method:string,path:string,body?:unknown) {
    const response=await fetch(apiBase+path,{method,headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(15000)});
    const reply=await response.json(); assert.equal(reply.code,0,JSON.stringify(reply));return reply.data;
}
async function until(check:()=>Promise<boolean>,reason:string) {
    const deadline=Date.now()+20000;while(Date.now()<deadline){if(await check())return;await Bun.sleep(50);}throw Error(reason);
}
let unitMode=0,silent=false;
const server=Bun.listen<{buffer:Buffer}>({hostname:'127.0.0.1',port:0,socket:{
    open(socket){socket.data={buffer:Buffer.alloc(0)};},
    data(socket,input){socket.data.buffer=Buffer.concat([socket.data.buffer,Buffer.from(input)]);
        while(socket.data.buffer.length>=12){const request=socket.data.buffer.subarray(0,12);socket.data.buffer=socket.data.buffer.subarray(12);if(silent)continue;
            const address=request.readUInt16BE(8),count=request.readUInt16BE(10),reply=Buffer.alloc(9+2*count);
            request.copy(reply,0,0,4);reply.writeUInt16BE(3+2*count,4);reply[6]=request[6];reply[7]=3;reply[8]=2*count;
            for(let i=0;i<count;i++)reply.writeUInt16BE(address+i===0?2500:unitMode,9+2*i);socket.write(reply);
        }
    },error(){},close(){}
}});
const latest=async()=>{
    const rows=await db`SELECT data FROM device_data WHERE device_id=${device} AND (data->'values') ? ${derived} ORDER BY report_time DESC,id DESC LIMIT 1`;
    return rows[0]?.data?.values??{};
};
try {
    const registers=[{id:pressure,name:'pressure',unit:'kPa',address:0},{id:mode,name:'unit mode',unit:'',address:1}].map(p=>({...p,registerType:'HOLDING_REGISTER',dataType:'UINT16',quantity:1}));
    const definition={id:derived,name:'normalized pressure',kind:'expression',valueType:'number',expression:'if(mode == 1, p / 1000, p)',inputs:[{alias:'p',pointId:pressure},{alias:'mode',pointId:mode}],unit:'kPa',unitMode:'conditional',unitRules:[{condition:'mode == 1',unit:'MPa'}],maxAgeSeconds:3,visible:true};
    const config={storagePolicy:'report',readInterval:1,byteOrder:'BIG_ENDIAN',registers,derivedPoints:[definition,{...definition,id:average,name:'average',kind:'average',sourceAlias:'p',windowSeconds:2,unitMode:'fixed',unitRules:[],unit:'kPa',inputs:[{alias:'p',pointId:pressure}],maxAgeSeconds:10}],pointVisibility:{[pressure]:false}};
    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by) VALUES(${model},${model},'Modbus',${config}::jsonb,${admin})`;
    const endpoint={transport:'tcp',mode:'TCP Client',ip:'',port:0,targets:[{id:target,name:'derived fixture',ip:'127.0.0.1',port:server.port,status:'enabled'}]};
    await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,created_by) VALUES(${link},${link},'Modbus',${endpoint}::jsonb,'enabled','collector',${admin})`;
    const params={device_code:'1',target_id:target,modbus_mode:'TCP',slave_id:1};
    await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by) VALUES(${device},${device},${link},${model},${params}::jsonb,${admin})`;
    await http('PUT',`/v1/device/${device}/debug`,{enabled:true});
    await until(async()=>{const points=await latest();return points[derived]?.value===2500 && points[derived]?.unit==='kPa' && points[average]?.value===2500;},'collector did not calculate initial derived values');
    unitMode=1;
    await until(async()=>{const points=await latest();return points[derived]?.value===2.5 && points[derived]?.unit==='MPa';},'value/unit snapshot did not change together');
    const histories=await db`SELECT data FROM device_data WHERE device_id=${device}`;
    assert(histories.some(row=>row.data.values[derived]?.unit==='kPa') && histories.some(row=>row.data.values[derived]?.unit==='MPa'),'history lost per-sample units');
    const stateKeys=await redis.send('KEYS',[`iot:derived:state:${device}:*`]) as string[];
    assert.equal(stateKeys.length,1,'state not persisted in Redis');
    silent=true;
    await until(async()=>{const points=await latest();return points[derived]?.quality==='stale' && points[derived]?.value===null;},'no-sample expiration did not invalidate derived value');
    const expired=(await latest())[derived];assert.equal(expired.unit,'','invalid conditional unit was guessed');
    console.log('PASS real Modbus -> Redis calculation -> history: conditional units, averages, timer expiration and invalid quality');
} catch (error) {
    const keys=await redis.send('KEYS',[`iot:derived:state:${device}:*`]) as string[];
    console.error('Derived diagnostics', JSON.stringify({latest:await latest(), history:await db`SELECT source, data->'values' AS points FROM device_data WHERE device_id=${device} ORDER BY report_time DESC LIMIT 5`, states:await Promise.all(keys.map(async key=>JSON.parse(await redis.get(key)??'{}').latest))},null,2));
    throw error;
} finally {
    server.stop(true);
    await db`UPDATE link SET status='disabled' WHERE id=${link}`;
    await db`UPDATE device SET deleted_at=NOW(),updated_at=NOW() WHERE id=${device}`;
    await db`UPDATE link SET deleted_at=NOW(),updated_at=NOW() WHERE id=${link}`;
    await db`UPDATE protocol_config SET deleted_at=NOW(),updated_at=NOW() WHERE id=${model}`;
    redis.close();await db.close();
}
