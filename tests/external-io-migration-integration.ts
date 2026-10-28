import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';
const db = new Bun.SQL(databaseUrl);
const phase = Bun.argv[2], migrationId = '0061_external_io_scheme_channels';
const path = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(path.startsWith(resolve('build') + sep));
const admin='00000000-0000-7000-8000-000000000002', platform='00000000-0000-7000-8000-000000000001';
type State={node:string;link:string;primary:string;scheme:string;point:string;devices:string[];history:string[];checksum:string;entry:string;beforeFailure?:string[]};
async function history(){return (await db`SELECT row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id<>${migrationId} ORDER BY migration_id`).map(row=>row.entry as string);}
async function entry(){const rows=await db`SELECT checksum,row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id=${migrationId}`;assert.equal(rows.length,1);return rows[0]!;}
async function mappings(state:State,split:boolean){
    const rows=await db`SELECT s.device_id,s.protocol_config_id,p.config,b.channel,b.point_id,s.derived_config FROM device_external_io s JOIN protocol_config p ON p.id=s.protocol_config_id JOIN device_external_io_binding b ON b.device_id=s.device_id WHERE s.device_id IN ${db(state.devices)} ORDER BY s.device_id`;
    assert.equal(rows.length,2);
    for(const row of rows){const expected=split&&row.device_id===state.devices[1]?'AI1':'AI0';assert.equal(row.channel,expected);assert.equal(row.point_id,state.point);assert.equal(row.config.points[0].id,state.point);if(split)assert.equal(row.config.points[0].channel,expected);assert.equal(row.derived_config.derivedPoints[0].inputs[0].pointId,state.point);}
    if(split)assert.notEqual(rows[0].protocol_config_id,rows[1].protocol_config_id,'different legacy mappings must become different schemes');
}
async function legacy(state:State,failure:boolean){
    await db.begin(async tx=>{
        await tx.unsafe('DROP TRIGGER IF EXISTS refresh_external_io_scheme ON protocol_config');
        await tx.unsafe('DROP TRIGGER IF EXISTS refresh_device_external_io ON device_external_io');
        await tx.unsafe('DROP FUNCTION IF EXISTS refresh_external_io_scheme()');
        await tx.unsafe('DROP FUNCTION IF EXISTS refresh_device_external_io()');
        await tx.unsafe('DROP TRIGGER IF EXISTS guard_device_external_io_model ON device_external_io');
        await tx.unsafe('DROP FUNCTION IF EXISTS guard_device_external_io_model()');
        await tx.unsafe('ALTER TABLE device_external_io_binding DROP CONSTRAINT IF EXISTS device_external_io_channel_key');
        await tx`UPDATE protocol_config SET config=jsonb_set(config,'{points}',jsonb_build_array((config->'points'->0)-'channel')) WHERE id=${state.scheme}`;
        await tx`UPDATE device_external_io SET protocol_config_id=${state.scheme} WHERE device_id IN ${tx(state.devices)}`;
        await tx.unsafe('ALTER TABLE device_external_io_binding DISABLE TRIGGER guard_device_external_io_binding');
        await tx`UPDATE device_external_io_binding SET channel='AI1' WHERE device_id=${state.devices[1]}`;
        await tx.unsafe('ALTER TABLE device_external_io_binding ENABLE TRIGGER guard_device_external_io_binding');
        // Restore the predecessor's model-change guard, which the migration replaces.
        await tx.unsafe(`CREATE FUNCTION guard_device_external_io_model() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN
          IF OLD.protocol_config_id IS DISTINCT FROM NEW.protocol_config_id AND EXISTS(SELECT 1 FROM device_external_io_binding WHERE device_id=NEW.device_id) THEN RAISE EXCEPTION 'remove IO bindings before changing the IO model' USING ERRCODE='23514'; END IF;
          RETURN NEW; END$$`);
        await tx.unsafe('CREATE TRIGGER guard_device_external_io_model BEFORE UPDATE OF protocol_config_id ON device_external_io FOR EACH ROW EXECUTE FUNCTION guard_device_external_io_model()');
        await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migrationId}`;
        if(failure)await tx.unsafe('ALTER TABLE device_external_io_binding ADD CONSTRAINT device_external_io_channel_key UNIQUE(device_id,channel)');
    });
}
try{
 if(phase==='initial'){
    const state:State={node:crypto.randomUUID(),link:crypto.randomUUID(),primary:crypto.randomUUID(),scheme:crypto.randomUUID(),point:crypto.randomUUID(),devices:[crypto.randomUUID(),crypto.randomUUID()],history:[],checksum:'',entry:''};
    const io={storagePolicy:'report',points:[{id:state.point,name:'current',kind:'AI',channel:'AI0',unit:'uA'}]};
    const capability={externalIo:{supported:true,channels:[{channel:'AI0',kind:'AI',available:true,controllable:false},{channel:'AI1',kind:'AI',available:true,controllable:false}]}};
    await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status,capability) VALUES(${state.node},${platform},'999999999906101','approved',${capability}::jsonb)`;
    await db`INSERT INTO protocol_config(id,name,protocol,config,created_by) VALUES(${state.primary},${state.primary},'Modbus','{"storagePolicy":"report","registers":[]}'::jsonb,${admin}),(${state.scheme},${state.scheme},'ExternalIO',${io}::jsonb,${admin})`;
    await db`INSERT INTO link(id,name,protocol,endpoint,execution,edge_node_id,created_by) VALUES(${state.link},${state.link},'Modbus','{"transport":"tcp","mode":"TCP Client","interface":"lo","ip":"127.0.0.1","port":502}'::jsonb,'edge',${state.node},${admin})`;
    for(const [index,device] of state.devices.entries()){
       await db`INSERT INTO device(id,name,link_id,protocol_config_id,protocol_params,created_by) VALUES(${device},${device},${state.link},${state.primary},${{device_code:String(index+1),modbus_mode:'TCP',slave_id:index+1}}::jsonb,${admin})`;
       const derived={derivedPoints:[{id:crypto.randomUUID(),name:'scaled',kind:'expression',valueType:'number',expression:'x*2',inputs:[{alias:'x',pointId:state.point}]}]};
       await db`INSERT INTO device_external_io(device_id,link_id,node_id,protocol_config_id,derived_config) VALUES(${device},${state.link},${state.node},${state.scheme},${derived}::jsonb)`;
    }
    await mappings(state,false);const current=await entry();Object.assign(state,{history:await history(),checksum:current.checksum,entry:current.entry});await Bun.write(path,JSON.stringify(state));
    console.log('PASS new schema resolves device mappings from a reusable IO scheme');
 }else{
    const state:State=await Bun.file(path).json();assert.deepEqual(await history(),state.history,'historical migration journal changed');
    if(phase==='restart'){assert.equal((await entry()).entry,state.entry);await mappings(state,false);}
    else if(phase==='repeated'){assert.equal((await entry()).entry,state.entry);await mappings(state,true);}
    else if(phase==='prepare-upgrade'||phase==='prepare-failure'){
       await legacy(state,phase==='prepare-failure');
       if(phase==='prepare-failure'){state.beforeFailure=(await db`SELECT id FROM protocol_config ORDER BY id`).map(row=>row.id);await Bun.write(path,JSON.stringify(state));}
    }else if(phase==='upgraded'){
       await mappings(state,true);const current=await entry();assert.equal(current.checksum,state.checksum);state.entry=current.entry;await Bun.write(path,JSON.stringify(state));console.log('PASS legacy mapping variants split into schemes while preserving IDs and derived inputs');
    }else if(phase==='prepare-drift')await db`UPDATE sys_schema_migrations SET checksum='fixture-drift' WHERE migration_id=${migrationId}`;
    else if(phase==='check-drift'){assert.equal((await entry()).checksum.trim(),'fixture-drift');await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migrationId}`;}
    else if(phase==='check-failure'){
       assert.equal((await db`SELECT 1 FROM sys_schema_migrations WHERE migration_id=${migrationId}`).length,0);
       assert.deepEqual((await db`SELECT id FROM protocol_config ORDER BY id`).map(row=>row.id),state.beforeFailure);
       assert((await db`SELECT protocol_config_id FROM device_external_io WHERE device_id IN ${db(state.devices)}`).every(row=>row.protocol_config_id===state.scheme),'failed upgrade did not roll back source changes');
       await db.unsafe('ALTER TABLE device_external_io_binding DROP CONSTRAINT device_external_io_channel_key');console.log('PASS failed scheme upgrade rolls back cloned schemes, associations and journal');
    }else throw Error(`unknown phase ${phase}`);
 }
}finally{await db.close();}
