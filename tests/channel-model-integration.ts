// Transactional checks in the disposable architecture fixture only.
import assert from 'node:assert/strict';
import { databaseUrl } from './architecture-fixture';
const db=new Bun.SQL(databaseUrl);
const admin='00000000-0000-7000-8000-000000000002',platform='00000000-0000-7000-8000-000000000001';
const id=()=>crypto.randomUUID();const rollback=new Error('fixture rollback');
try { await db.begin(async tx=>{
    const node=id(),channel=id(),model=id(),first=id(),second=id();
    await tx`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},'999999999999999','approved')`;
    await tx`INSERT INTO protocol_config(id,name,protocol,config,created_by) VALUES(${model},${model},'Modbus','{"registers":[],"storagePolicy":"report"}'::jsonb,${admin})`;
    await tx`INSERT INTO link(id,name,protocol,execution,edge_node_id,endpoint,created_by)
        VALUES(${channel},${channel},'Modbus','edge',${node},'{"transport":"serial","interface":"/dev/ttyS1","baud_rate":9600,"data_bits":8,"stop_bits":1,"parity":"none","rs485":true}'::jsonb,${admin})`;
    for(const [device,slave] of [[first,1],[second,2]] as const) {
        const params={device_code:String(slave),slave_id:slave,modbus_mode:'TCP'};
        await tx`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by)
            VALUES(${device},${device},${channel},${model},${params}::jsonb,${admin})`;
    }
    const devices=await tx`SELECT protocol_params FROM device WHERE link_id=${channel}`;
    assert.equal(devices.length,2);
    assert(devices.every(row=>row.protocol_params.modbus_mode==='RTU'));
    async function rejects(query:()=>Promise<unknown>,pattern:RegExp) {
        await tx.unsafe('SAVEPOINT rejected');
        await assert.rejects(query(),pattern);
        await tx.unsafe('ROLLBACK TO SAVEPOINT rejected');
    }
    await rejects(()=>tx`UPDATE device SET protocol_params=jsonb_set(protocol_params,'{slave_id}','1') WHERE id=${second}`,/unique|duplicate/i);
    await rejects(()=>tx`UPDATE link SET endpoint='{"transport":"tcp","interface":"eth0","mode":"TCP Client","ip":"127.0.0.1","port":502}'::jsonb WHERE id=${channel}`,/changing channel identity/);
    await tx`UPDATE link SET endpoint=jsonb_set(endpoint,'{baud_rate}','19200') WHERE id=${channel}`;
    await tx`UPDATE device SET deleted_at=NOW() WHERE id=${first}`;
    assert.equal((await tx`SELECT 1 FROM link WHERE id=${channel} AND deleted_at IS NULL`).length,1);
    await rejects(()=>tx`UPDATE link SET deleted_at=NOW() WHERE id=${channel}`,/changing channel identity/);
    await tx`UPDATE protocol_config SET config='{"registers":[],"storagePolicy":"change"}'::jsonb WHERE id=${model}`;
    assert.equal((await tx`SELECT config FROM device_model WHERE device_id=${second}`)[0].config.storagePolicy,'change');
    throw rollback;
}); }catch(error){if(error!==rollback)throw error;
    console.log('PASS shared serial channels, slave uniqueness, transport guards, deletion and current device type updates');
}finally{await db.close();}
