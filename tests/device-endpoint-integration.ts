import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
let actor: string | null = '00000000-0000-7000-8000-000000000002';
const admin = '00000000-0000-7000-8000-000000000002';
const tag = `protocol_http_${crypto.randomUUID()}`;
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

const node=crypto.randomUUID(), model=crypto.randomUUID();
await db`INSERT INTO edge_node(id,platform_id,imei,name,enrollment_status,capability) VALUES(${node},'00000000-0000-7000-8000-000000000001','999999999999996','设备连接验收','approved','{"deviceConfig":true}'::jsonb)`;
await db`INSERT INTO edge_node_serial(node_id,path,available) VALUES(${node},'/dev/ttyS1',true)`;
await db`INSERT INTO edge_node_interface(node_id,name,ipv4) VALUES(${node},'br-lan','192.168.1.1')`;
await db`INSERT INTO protocol_config(id,name,protocol,config,created_by) VALUES(${model},${tag},'Modbus','{"byteOrder":"BIG_ENDIAN","readInterval":10,"storagePolicy":"report","registers":[]}'::jsonb,${admin})`;
const endpoint={transport:'serial',interface:'/dev/ttyS1',baud_rate:9600,data_bits:8,stop_bits:1,parity:'none',rs485:true};
const connection={name:'测试串口连接',edge_node_id:node,protocol:'Modbus',endpoint,status:'enabled'};
const body={name:tag,device_code:'1',protocol_config_id:model,slave_id:1,modbus_mode:'RTU',edge_connection:connection};
try {
    await request('POST','/v1/device',body);
    let device=(await db`SELECT id,link_id FROM device WHERE name=${tag}`)[0];
    const deviceId=device.id, firstLink=device.link_id;
    const detail=await request('GET',`/v1/device/${deviceId}`);
    assert.equal(detail.edge_node_id,node); assert.equal(detail.edge_interface,'/dev/ttyS1');
    assert.equal(detail.serial_baud_rate,9600);
    const page=await request('GET','/v1/link?page=1&pageSize=100');
    assert(page.list.some((item:any)=>item.id===firstLink));
    await request('POST','/v1/link',{...connection,execution:'edge'},15002);
    await request('PUT',`/v1/link/${firstLink}`,{...connection,execution:'edge'},15002);
    await request('DELETE',`/v1/link/${firstLink}`,undefined,15002);
    // Packet debugging stays available on device-managed edge links of an approved node.
    await request('PUT',`/v1/link/${firstLink}/debug`,{enabled:true});
    assert.equal((await db`SELECT debug_enabled FROM link WHERE id=${firstLink}`)[0].debug_enabled,true);
    await request('PUT',`/v1/link/${firstLink}/debug`,{enabled:false});
    assert.equal((await db`SELECT debug_enabled FROM link WHERE id=${firstLink}`)[0].debug_enabled,false);
    // Repeated saving must reuse the endpoint rather than generating a new row.
    await request('PUT',`/v1/device/${deviceId}`,body);
    assert.equal((await db`SELECT link_id FROM device WHERE id=${deviceId}`)[0].link_id,firstLink);
    await request('PUT',`/v1/device/${deviceId}`,{...body,edge_connection:{...connection,endpoint:{...endpoint,baud_rate:19200}}});
    assert.equal((await request('GET',`/v1/device/${deviceId}`)).serial_baud_rate,19200);
    await request('POST','/v1/device',{...body,name:tag+'conflict',device_code:'2',slave_id:2},15002);
    const count=(await db`SELECT count(*)::int n FROM link WHERE edge_node_id=${node}`)[0].n;
    // Failure after endpoint resolution must roll back its insertion.
    await request('POST','/v1/device',{...body,edge_connection:{...connection,endpoint:{transport:'tcp',interface:'br-lan',mode:'TCP Client',ip:'192.168.1.51',port:502}}},18004);
    assert.equal((await db`SELECT count(*)::int n FROM link WHERE edge_node_id=${node}`)[0].n,count);
    await request('POST','/v1/device',{...body,name:tag+'shared',device_code:'2',slave_id:2,edge_connection:{...connection,endpoint:{...endpoint,baud_rate:19200}}});
    const sibling=(await db`SELECT id,link_id FROM device WHERE name=${tag+'shared'}`)[0];
    assert.equal(sibling.link_id,firstLink);
    const tcp={transport:'tcp',interface:'br-lan',mode:'TCP Client',ip:'192.168.1.50',port:502};
    await request('PUT',`/v1/device/${deviceId}`,{...body,modbus_mode:'TCP',edge_connection:{...connection,endpoint:tcp}});
    const changed=await request('GET',`/v1/device/${deviceId}`);
    assert.equal(changed.edge_ip,'192.168.1.50');assert.equal(changed.edge_port,502);
    const nextPage=await request('GET','/v1/link?page=1&pageSize=100');
    assert(nextPage.list.some((item:any)=>item.id===firstLink));
    await request('DELETE',`/v1/device/${sibling.id}`);
    assert(!(await request('GET','/v1/link?page=1&pageSize=100')).list.some((item:any)=>item.id===firstLink));
    assert(nextPage.list.some((item:any)=>item.id===changed.link_id));
    await request('DELETE',`/v1/device/${deviceId}`);
    assert(!(await request('GET','/v1/link?page=1&pageSize=100')).list.some((item:any)=>item.id===changed.link_id));
    assert.equal((await db`SELECT status FROM link WHERE id=${changed.link_id}`)[0].status,'disabled');
    console.log('Device endpoint integration passed: device-owned configuration, read-only link list, atomic rollback, reuse and update.');
} finally { await db.close(); }
