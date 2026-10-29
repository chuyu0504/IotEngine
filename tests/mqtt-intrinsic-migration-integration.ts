import assert from 'node:assert/strict';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const source = await Bun.file(new URL('../service/config/mqtt.h', import.meta.url)).text();
const original = source.match(/kMqttDeviceAddressMigration = R"sql\(([\s\S]*?)\)sql"/)?.[1];
const migration = source.match(/kMqttIntrinsicPropertiesMigration = R"sql\(([\s\S]*?)\)sql"/)?.[1];
assert(original && migration);
const temporaryFunctions = (sql:string) => sql.replace(/\bFUNCTION (bind_device_channel|refresh_mqtt_device_identity)\(\)/g, 'FUNCTION pg_temp.$1()');
const rollback = new Error('rollback disposable migration objects');
try {
    await db.begin(async sql => {
        await sql.unsafe('SET LOCAL search_path TO pg_temp,public');
        await sql.unsafe(`
            CREATE TEMP TABLE link(id UUID PRIMARY KEY,protocol TEXT,execution TEXT,endpoint JSONB,deleted_at TIMESTAMPTZ) ON COMMIT DROP;
            CREATE TEMP TABLE protocol_config(id UUID PRIMARY KEY,protocol TEXT,config JSONB) ON COMMIT DROP;
            CREATE TEMP TABLE device(id UUID PRIMARY KEY,link_id UUID,protocol_config_id UUID,protocol_params JSONB,protocol_address TEXT,deleted_at TIMESTAMPTZ) ON COMMIT DROP;
            CREATE UNIQUE INDEX mqtt_test_address ON device(link_id,protocol_address) WHERE deleted_at IS NULL;
        `);
        await sql.unsafe(temporaryFunctions(original));
        await sql.unsafe('CREATE TRIGGER bind_device_channel BEFORE INSERT OR UPDATE ON device FOR EACH ROW EXECUTE FUNCTION pg_temp.bind_device_channel()');
        const link = crypto.randomUUID(), model = crypto.randomUUID(), device = crypto.randomUUID(), point = crypto.randomUUID();
        const configuration = {
            storagePolicy: 'report', topic: 'devices/{deviceCode}/up', timeField: '/clock',
            reportTemplate: '{"serial":"$deviceCode","clock":"$time","value":"$point:温度"}',
            parameters: [{name:'用户地址',dataType:'STRING'}],
            points: [{id:point,name:'温度',field:'/value',dataType:'DOUBLE',writable:true}],
            commands: [{id:crypto.randomUUID(),name:'设置',topic:'devices/{deviceCode}/set',
                template:'{"request":"$commandId","uuid":"$deviceId","serial":"$deviceCode","address":"$param:用户地址","value":"$point:温度"}',
                requiredPointIds:[point],response:{topic:'devices/{deviceCode}/result',matchFields:['/request'],codeField:'/code',successCode:0}}],
        };
        await sql`INSERT INTO link VALUES (${link},'MQTT','collector','{"mode":"TCP Client"}',NULL)`;
        await sql`INSERT INTO protocol_config VALUES (${model},'MQTT',${configuration}::jsonb)`;
        await sql`INSERT INTO device VALUES (${device},${link},${model},${{device_code:'D001',target_id:'broker',parameters:{用户地址:'A001'}}}::jsonb,NULL,NULL)`;
        const legacyModel=crypto.randomUUID(), legacyDevice=crypto.randomUUID();
        const legacy={storagePolicy:'report',topic:'shared',identitySource:'payload',deviceCodeField:'/serial',
            parameters:[{name:'设备标识符',dataType:'STRING'}],commandTopic:'commands',commandTemplate:'{"data":"$values"}',
            points:[{id:crypto.randomUUID(),name:'温度',field:'/metrics/0/value',dataType:'DOUBLE',writable:true}]};
        await sql`INSERT INTO protocol_config VALUES (${legacyModel},'MQTT',${legacy}::jsonb)`;
        await sql`INSERT INTO device VALUES (${legacyDevice},${link},${legacyModel},${{device_code:'D002',target_id:'broker',parameters:{设备标识符:'original-value'}}}::jsonb,NULL,NULL)`;
        await sql.unsafe(temporaryFunctions(migration));
        const updated = (await sql`SELECT config FROM protocol_config WHERE id=${model}`)[0].config;
        const stored = (await sql`SELECT protocol_params,protocol_address FROM device WHERE id=${device}`)[0];
        const identifier = updated.identityProperty;
        assert.equal(stored.protocol_params.parameters[identifier], 'D001');
        assert.equal(stored.protocol_params.parameters.用户地址, 'A001');
        assert.equal(JSON.parse(stored.protocol_address)[1], 'D001');
        assert.equal(updated.timeField, '/clock');
        assert.equal(JSON.parse(updated.reportTemplate).clock, 0);
        assert.equal(updated.topic, `devices/{param:${identifier}}/up`);
        const command = updated.commands[0], template = JSON.parse(command.template);
        const request = updated.points.find((item:{id:string})=>command.requiredPointIds.includes(item.id) && item.id!==point);
        assert(request);
        assert.equal(template.request, `$point:${request.name}`);
        assert.equal(template.serial, `$param:${identifier}`);
        assert.equal(stored.protocol_params.parameters[template.uuid.slice(7)], device);
        assert.deepEqual(command.response.matchFields, ['/request']);
        const converted=(await sql`SELECT config FROM protocol_config WHERE id=${legacyModel}`)[0].config;
        const legacyValues=(await sql`SELECT protocol_params FROM device WHERE id=${legacyDevice}`)[0].protocol_params.parameters;
        assert.notEqual(converted.identityProperty,'设备标识符');
        assert.equal(legacyValues.设备标识符,'original-value');
        assert.equal(legacyValues[converted.identityProperty],'D002');
        assert.equal(JSON.parse(converted.commandTemplate).data.metrics[0].value,'$point:温度');
        assert.equal(JSON.parse(converted.commandTemplate).data.serial,`$param:${converted.identityProperty}`);
        await sql.unsafe(temporaryFunctions(migration));
        assert.deepEqual((await sql`SELECT config FROM protocol_config WHERE id=${model}`)[0].config, updated);
        for (const parameters of [{}, {[identifier]:null}, {[identifier]:12}]) {
            await sql.unsafe('SAVEPOINT invalid_identifier');
            await assert.rejects(sql`UPDATE device SET protocol_params=jsonb_set(protocol_params,'{parameters}',${parameters}::jsonb) WHERE id=${device}`);
            await sql.unsafe('ROLLBACK TO SAVEPOINT invalid_identifier');
        }
        await sql.unsafe('SAVEPOINT duplicate_identifier');
        await assert.rejects(sql`INSERT INTO device VALUES (${crypto.randomUUID()},${link},${model},${{device_code:'other-code',target_id:'broker',parameters:stored.protocol_params.parameters}}::jsonb,NULL,NULL)`);
        await sql.unsafe('ROLLBACK TO SAVEPOINT duplicate_identifier');
        await sql.unsafe('SAVEPOINT changed_identifier');
        await assert.rejects(sql`UPDATE protocol_config SET config=jsonb_set(config,'{identityProperty}','"missing"'::jsonb) WHERE id=${model}`);
        await sql.unsafe('ROLLBACK TO SAVEPOINT changed_identifier');
        assert.deepEqual((await sql`SELECT config FROM protocol_config WHERE id=${model}`)[0].config, updated);
        const fullModel=crypto.randomUUID();
        const fullConfig={...legacy,parameters:Array.from({length:256},(_,index)=>({name:`property${index}`,dataType:'STRING'}))};
        await sql`INSERT INTO protocol_config VALUES (${fullModel},'MQTT',${fullConfig}::jsonb)`;
        await sql.unsafe('SAVEPOINT property_limit');
        await assert.rejects(sql.unsafe(temporaryFunctions(migration)),/exceeds the property limit/);
        await sql.unsafe('ROLLBACK TO SAVEPOINT property_limit');
        assert.deepEqual((await sql`SELECT config FROM protocol_config WHERE id=${fullModel}`)[0].config,fullConfig);
        throw rollback;
    });
} catch (error) {
    if (error!==rollback) throw error;
    console.log('PASS MQTT intrinsic property migration: values, templates, user correlation, repeat execution, identifier constraints and rollback');
} finally {
    await db.close();
}
