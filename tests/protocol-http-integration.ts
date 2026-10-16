import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
let actor: string | null = null;
const admin = '00000000-0000-7000-8000-000000000002';
const tag = `protocol_http_${crypto.randomUUID()}`;
const other = crypto.randomUUID();
const role = crypto.randomUUID();
const ids: string[] = [];
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
const register = {id:crypto.randomUUID(), name:'temperature', registerType:'HOLDING_REGISTER', dataType:'UINT16', address:0, quantity:1, writable:true};
const config = {storagePolicy:'report',readInterval:10,byteOrder:'BIG_ENDIAN',registers:[register]};
try {
    await request('GET','/v1/protocol/configs',undefined,11004);
    await request('POST','/v1/protocol/configs/test-expression',{},11004);
    await request('POST','/v1/protocol/configs/validate-expression',{expression:'p',variables:['p']},11004);
    await request('POST','/v1/protocol/configs','{',11004,true);
    actor = admin;
    const probe = {expression:'if(p > 100, p / 1000, p)',inputs:[{alias:'p',value:2500}],unit:'kPa',unitRules:[{condition:'p > 100',unit:'MPa'}]};
    const beforeProbe = (await db`SELECT count(*)::int AS count FROM protocol_config`)[0].count;
    assert.deepEqual(await request('POST','/v1/protocol/configs/test-expression',probe),{value:2.5,unit:'MPa',matchedRule:1});
    assert.deepEqual(await request('POST','/v1/protocol/configs/test-expression',{...probe,inputs:[{alias:'p',value:25}]}),{value:25,unit:'kPa',matchedRule:0});
    assert.equal((await request('POST','/v1/protocol/configs/test-expression',{...probe,expression:'if(p == 0, 0, 1 / p)',inputs:[{alias:'p',value:0}],unitRules:[]})).value,0);
    for (const invalid of [
        {...probe,expression:'p / 0'}, {...probe,expression:'sqrt(-1)'},
        {...probe,expression:'if(true, p, unknown)'}, {...probe,expression:'p +'},
        {...probe,inputs:[{alias:'p',value:1},{alias:'p',value:2}]},
        {...probe,unitRules:[{condition:'true',unit:'MPa'},{condition:'missing > 0',unit:'kPa'}]},
        {...probe,unitRules:[{condition:'p / 0',unit:'MPa'}]},
    ]) await request('POST','/v1/protocol/configs/test-expression',invalid,16004);
    await request('POST','/v1/protocol/configs/test-expression',{...probe,inputs:[{alias:'p',value:null}]},10001);
    assert.equal(await request('POST','/v1/protocol/configs/validate-expression',{expression:'if(p > 100, p / 1000, q)',variables:['p','q']}),undefined);
    assert.equal(await request('POST','/v1/protocol/configs/validate-expression',{expression:'p / 0',variables:['p']}),undefined);
    for (const invalid of [{expression:'p +',variables:['p']},{expression:'p + unknown',variables:['p']}])
        await request('POST','/v1/protocol/configs/validate-expression',invalid,16004);
    assert.equal((await db`SELECT count(*)::int AS count FROM protocol_config`)[0].count,beforeProbe,'manual validation saved a configuration');

    await request('POST','/v1/protocol/configs',{protocol:'Modbus',name:tag,config,remark:'original'});
    const item = (await request('GET','/v1/protocol/configs?protocol=Modbus')).list.find((row: {name:string}) => row.name === tag);
    assert(item);
    const id = item.id;
    ids.push(id);
    const detail = () => request('GET',`/v1/protocol/configs/${id}`);
    assert.equal((await detail()).config.registers[0].writable,true);
    const derived = {
        id:crypto.randomUUID(),name:'derived pressure',kind:'expression',valueType:'number',
        expression:'if(p > 100, p / 1000, p)',inputs:[{alias:'p',pointId:register.id}],
        unit:'kPa',unitMode:'conditional',unitRules:[{condition:'p > 100',unit:'MPa'}],maxAgeSeconds:10,visible:true,
    };
    await request('PUT',`/v1/protocol/configs/${id}`,{config:{derivedPoints:[derived],pointVisibility:{[register.id]:false}}});
    assert.deepEqual((await detail()).config.derivedPoints,[derived]);
    for (const bad of [
        {...derived,expression:'if(true, p, unknown)'},
        {...derived,inputs:[{alias:'p',pointId:derived.id}]},
        {...derived,unitRules:[{condition:'unknown == 1',unit:'MPa'}]},
        {...derived,inputs:[{alias:'p',pointId:crypto.randomUUID()}]},
    ]) await request('PUT',`/v1/protocol/configs/${id}`,{config:{derivedPoints:[bad]}},16004);
    await request('PUT',`/v1/protocol/configs/${id}`,{config:{registers:[]}},16004);
    const windows=['average','minimum','maximum'].map(kind=>({...derived,id:crypto.randomUUID(),kind,sourceAlias:'p',windowSeconds:60,unitMode:'fixed',unitRules:[]}));
    await request('PUT',`/v1/protocol/configs/${id}`,{config:{derivedPoints:[derived,...windows]}});
    assert.equal((await detail()).config.derivedPoints.length,4);
    const edgeNode=crypto.randomUUID(),edgeLink=crypto.randomUUID(),edgeDevice=crypto.randomUUID();
    try {
        await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${edgeNode},'00000000-0000-7000-8000-000000000001','999999999999998','approved')`;
        await db`INSERT INTO link(id,name,protocol,execution,edge_node_id,endpoint,created_by) VALUES(${edgeLink},${edgeLink},'Modbus','edge',${edgeNode},'{"transport":"serial","interface":"/dev/ttyS1","baud_rate":9600,"data_bits":8,"stop_bits":1,"parity":"none","rs485":true}'::jsonb,${admin})`;
        await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by) VALUES(${edgeDevice},${edgeDevice},${edgeLink},${id},'{"device_code":"1","slave_id":1}'::jsonb,${admin})`;
        await request('PUT',`/v1/protocol/configs/${id}`,{config:{derivedPoints:[derived]}},16009);
        assert.equal((await detail()).config.derivedPoints.length,4,'unsupported firmware rejection mutated the configuration');
    } finally {
        await db`DELETE FROM device WHERE id=${edgeDevice}`;
        await db`DELETE FROM link WHERE id=${edgeLink}`;
        await db`DELETE FROM edge_node WHERE id=${edgeNode}`;
    }

    await request('PUT',`/v1/protocol/configs/${id}`,{config:{derivedPoints:[],pointVisibility:{}}});

    assert.equal((await db`SELECT created_by FROM protocol_config WHERE id=${id}`)[0].created_by,admin);
    assert((await request('GET','/v1/protocol/configs/options?protocol=Modbus')).list.some((row: {id:string})=>row.id===id));
    await request('PUT',`/v1/protocol/configs/${id}`,`{"config":{"readInterval":1e3,"large":9007199254740993}}`,0,true);
    assert.equal((await detail()).config.readInterval,1000);
    assert.equal((await db`SELECT config->>'large' AS number FROM protocol_config WHERE id=${id}`)[0].number,'9007199254740993');
    for (const scale of ['1000000000.000000000001','-1000000000.000000000001']) {
        await request('PUT',`/v1/protocol/configs/${id}`,`{"config":{"registers":[${JSON.stringify({...register,scale:'TOKEN'}).replace('"TOKEN"',scale)}]}}`,16004,true);
    }
    // 经确认的新 Model 契约：可选标量字段的 null 与缺失一致，remark 仍表示清空。
    await request('PUT',`/v1/protocol/configs/${id}`,{protocol:null,name:null,enabled:null});
    assert.equal((await detail()).protocol,'Modbus');
    assert.equal((await detail()).enabled,true);
    for (const patch of [{name:42},{remark:42}]) await request('PUT',`/v1/protocol/configs/${id}`,{...patch},16002);
    for (const patch of [{enabled:'false'},{config:null},{config:[]},{config:{readInterval:'1e3'}},{config:{registers:[{...register,writable:'true'}]}}]) await request('PUT',`/v1/protocol/configs/${id}`,{...patch},16004);
    await request('PUT',`/v1/protocol/configs/${id}`,{protocol:'S7'},16006);
    await request('PUT','/v1/protocol/configs/invalid',{},10001);
    await request('PUT',`/v1/protocol/configs/${id}`,{enabled:false,config:{readInterval:20,extra:{text:'escaped " quote, ] and \\ slash'}}});
    const updated = await detail();
    assert.equal(updated.enabled,false);
    assert.equal(updated.remark,'original');
    assert.equal(updated.config.registers[0].quantity,1);
    assert.equal(updated.config.readInterval,20);
    assert(!(await request('GET','/v1/protocol/configs/options?protocol=Modbus')).list.some((row: {id:string})=>row.id===id));
    await request('PUT',`/v1/protocol/configs/${id}`,{remark:null});
    assert.equal((await db`SELECT remark FROM protocol_config WHERE id=${id}`)[0].remark,null);
    await request('PUT',`/v1/protocol/configs/${id}`,{remark:'restored'});
    await request('PUT',`/v1/protocol/configs/${id}`,{remark:''});
    assert.equal((await db`SELECT remark FROM protocol_config WHERE id=${id}`)[0].remark,null);
    await request('POST','/v1/protocol/configs',{protocol:'Modbus',name:tag,config},16005);
    await request('POST','/v1/protocol/configs',{protocol:'Modbus',name:`${tag}_bad`,config:{}},16004);
    // Exercise every configuration array affected by replacing the private parser.
    const pointName = 'quoted " point, ] with \\ slash';
    const variants = [
        {protocol:'S7',config:{storagePolicy:'report',plcModel:'S7-1200',connection:{},areas:[{id:crypto.randomUUID(),name:pointName,area:'DB',dbNumber:1,start:0,size:2,dataType:'UINT16'}]}},
        {protocol:'MC',config:{storagePolicy:'report',connection:{frame:'3E'},points:[{id:crypto.randomUUID(),name:pointName,area:'D',address:0,dataType:'UINT16'}]}},
        {protocol:'FINS',config:{storagePolicy:'report',connection:{},points:[{id:crypto.randomUUID(),name:pointName,area:'D',address:0,dataType:'UINT16',bit:0}]}},
        {protocol:'DLT645',config:{storagePolicy:'report',connection:{version:'2007'},points:[{id:crypto.randomUUID(),name:pointName,identifier:'00000000',dataType:'BCD',length:4,digits:2}]}},
    ];
    for (const variant of variants) {
        const name = `${tag}_${variant.protocol}`;
        await request('POST','/v1/protocol/configs',{...variant,name});
        const [row] = await db`SELECT id FROM protocol_config WHERE name=${name}`;
        ids.push(row.id);
        assert.deepEqual((await request('GET',`/v1/protocol/configs/${row.id}`)).config,variant.config);
        await request('PUT',`/v1/protocol/configs/${row.id}`,{config:variant.protocol === 'S7' ? {areas:[false]} : {...variant.config,points:[false]}},16004);
        await request('DELETE',`/v1/protocol/configs/${row.id}`);
    }
    await db`INSERT INTO sys_user(id,username,password_hash) VALUES(${other},${other},'unused-test-password')`;
    await db`INSERT INTO sys_role(id,name,code,permissions) VALUES(${role},${role},${role},'["iot:protocol:query","iot:protocol:edit","iot:protocol:delete"]'::jsonb)`;
    await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${crypto.randomUUID()},${other},${role})`;
    actor = other;
    assert.equal((await request('POST','/v1/protocol/configs/test-expression',probe)).value,2.5);
    await request('POST','/v1/protocol/configs',{protocol:'Modbus',name:`${tag}_denied`,config},11007);
    await request('POST','/v1/protocol/configs',{name:42},11007);
    await request('POST','/v1/protocol/configs','{',11007,true);
    await request('PUT',`/v1/protocol/configs/${id}`,{name:`${tag}_denied`},16007);
    await request('DELETE',`/v1/protocol/configs/${id}`,undefined,16007);
    await db`UPDATE sys_role SET permissions='["iot:protocol:edit"]'::jsonb WHERE id=${role}`;
    await request('POST','/v1/protocol/configs/validate-expression',{expression:'p',variables:['p']},11007);
    actor = admin;
    await request('DELETE',`/v1/protocol/configs/${id}`);
    await request('GET',`/v1/protocol/configs/${id}`,undefined,16001);
    assert.equal((await db`SELECT count(*)::int AS n FROM outbox_event WHERE aggregate_type='protocol' AND aggregate_id=${id} AND action='deleted'`)[0].n,1);
    assert.equal((await fetch(apiBase+'/v1/protocol/configs')).status,401);
    console.log('PASS protocol HTTP: CRUD, exact JSON numbers, merge/null semantics, permissions and ownership');
} finally {
    for (const id of ids) { await db`DELETE FROM outbox_event WHERE aggregate_id=${id}`; await db`DELETE FROM protocol_config WHERE id=${id}`; }
    await db`DELETE FROM sys_user_role WHERE user_id=${other}`;
    await db`DELETE FROM sys_role WHERE id=${role}`;
    await db`DELETE FROM sys_user WHERE id=${other}`;
    await db.close();
}
