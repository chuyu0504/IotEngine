import assert from 'node:assert/strict';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const source = await Bun.file(new URL('../service/config/device_parameter_definitions.h',import.meta.url)).text();
const migration = source.match(/R"sql\(([\s\S]*?)\)sql"/)?.[1];
assert(migration);
const model = crypto.randomUUID();
const values = {address:'A001',channel:2,enabled:false,extra:{x:null},mixed:1};
try {
    await db.begin(async sql => {
        await sql.unsafe('CREATE TEMP TABLE protocol_config(id UUID,protocol TEXT,config JSONB,updated_at TIMESTAMPTZ) ON COMMIT DROP');
        await sql.unsafe('CREATE TEMP TABLE device(protocol_config_id UUID,protocol_params JSONB) ON COMMIT DROP');
        const config = {parameters:[{name:'existing',dataType:'BOOL'}],commands:[{template:'{"address":"$param:address","absent":"$param:absent"}'}]};
        await sql`INSERT INTO pg_temp.protocol_config VALUES (${model},'MQTT',${config}::jsonb,NOW())`;
        await sql`INSERT INTO pg_temp.device VALUES (${model},${{parameters:values}}::jsonb),(${model},${{parameters:{mixed:'text'}}}::jsonb)`;
        await sql.unsafe(migration);
        const updated=(await sql`SELECT config FROM pg_temp.protocol_config`)[0].config;
        assert.deepEqual(Object.fromEntries(updated.parameters.map((field:{name:string;dataType:string})=>[field.name,field.dataType])),{existing:'BOOL',address:'STRING',channel:'DOUBLE',enabled:'BOOL',extra:'JSON',mixed:'JSON',absent:'STRING'});
        assert.deepEqual(updated.commands,config.commands);
        assert.deepEqual((await sql`SELECT protocol_params FROM pg_temp.device LIMIT 1`)[0].protocol_params.parameters,values);
        await sql.unsafe('ALTER TABLE pg_temp.protocol_config DROP CONSTRAINT ck_protocol_parameter_definitions');
        await sql.unsafe(migration);
        assert.deepEqual((await sql`SELECT config FROM pg_temp.protocol_config`)[0].config,updated);
        await sql.unsafe('SAVEPOINT invalid');
        await assert.rejects(sql`UPDATE pg_temp.protocol_config SET config='{"parameters":{}}'::jsonb`);
        await sql.unsafe('ROLLBACK TO SAVEPOINT invalid');
    });
    console.log('PASS parameter definitions migration: inferred types, template references, preserved values and repeat execution');
} finally { await db.close(); }
