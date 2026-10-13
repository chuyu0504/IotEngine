import assert from 'node:assert/strict';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const source = await Bun.file(new URL('../service/config/device_parameters.h', import.meta.url)).text();
const migration = source.match(/R"sql\(([\s\S]*?)\)sql"/)?.[1];
assert(migration);
const model = crypto.randomUUID(), first = crypto.randomUUID(), second = crypto.randomUUID();
const parameters = [
    {deviceCode:'D001',name:'address',value:'A001'},
    {deviceCode:'D001',name:'channel',value:2},
    {deviceCode:'D002',name:'address',value:'A002'},
    {deviceCode:'D002',name:'metadata',value:{enabled:true, nullable:null}},
];
async function setup(sql: Bun.TransactionSQL, values: unknown = parameters) {
    await sql.unsafe('CREATE TEMP TABLE protocol_config(id UUID,config JSONB,updated_at TIMESTAMPTZ) ON COMMIT DROP');
    await sql.unsafe('CREATE TEMP TABLE device(id UUID,protocol_config_id UUID,protocol_params JSONB,updated_at TIMESTAMPTZ) ON COMMIT DROP');
    await sql`INSERT INTO pg_temp.protocol_config VALUES (${model},${{deviceParameters:values,commands:[{template:'{"addr":"$param:address"}'}]}}::jsonb,NOW())`;
    await sql`INSERT INTO pg_temp.device VALUES (${first},${model},' {"device_code":"D001","parameters":{"existing":false}}'::jsonb,NOW()),(${second},${model},'{"device_code":"D002"}'::jsonb,NOW())`;
}
try {
    await db.begin(async sql => {
        await setup(sql);
        await sql.unsafe(migration);
        const rows = await sql`SELECT id,protocol_params FROM pg_temp.device`;
        assert.deepEqual(rows.find(row => row.id === first).protocol_params.parameters,{address:'A001',channel:2,existing:false});
        assert.deepEqual(rows.find(row => row.id === second).protocol_params.parameters,{address:'A002',metadata:{enabled:true,nullable:null}});
        const [{config}] = await sql`SELECT config FROM pg_temp.protocol_config`;
        assert(!('deviceParameters' in config));
        assert.equal(config.commands[0].template,'{"addr":"$param:address"}');
        await sql.unsafe('ALTER TABLE pg_temp.protocol_config DROP CONSTRAINT ck_protocol_config_device_parameters; ALTER TABLE pg_temp.device DROP CONSTRAINT ck_device_parameters');
        await sql.unsafe(migration);
        assert.deepEqual(await sql`SELECT id,protocol_params FROM pg_temp.device`,rows);
    });
    for (const invalid of [[...parameters,{deviceCode:'missing',name:'address',value:'X'}],[...parameters,parameters[0]]]) {
        await assert.rejects(db.begin(async sql => { await setup(sql,invalid); await sql.unsafe(migration); }), /unbound|duplicate/);
    }
    await db.begin(async sql => {
        await setup(sql);
        await sql`UPDATE pg_temp.device SET protocol_params=protocol_params || '{"parameters":{"address":"conflict"}}'::jsonb WHERE id=${first}`;
        await sql.unsafe('SAVEPOINT before_migration');
        await assert.rejects(sql.unsafe(migration), /conflicting/);
        await sql.unsafe('ROLLBACK TO SAVEPOINT before_migration');
        assert.deepEqual((await sql`SELECT config FROM pg_temp.protocol_config`)[0].config.deviceParameters,parameters);
    });
    console.log('PASS device parameter migration: ownership, JSON types, stable templates, repeat, conflict and rollback');
} finally { await db.close(); }
