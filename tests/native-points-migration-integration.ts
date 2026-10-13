import assert from 'node:assert/strict';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const source = await Bun.file(new URL('../service/config/native_points.h', import.meta.url)).text();
const migration = source.match(/R"sql\(([\s\S]*?)\)sql"/)?.[1];
assert(migration);
const oldId = crypto.randomUUID();
const dependentId = crypto.randomUUID();
const modelId = crypto.randomUUID();
const base = {
    storagePolicy: 'report',
    registers: [{id:oldId,name:'压力',unit:'kPa',dataType:'UINT16',scale:0.1,decimals:1,writable:false}],
    derivedPoints: [{id:dependentId,name:'依赖值',kind:'expression',valueType:'number',expression:'p * 2',inputs:[{alias:'p',pointId:oldId}],unitMode:'fixed',unit:'kPa',maxAgeSeconds:300,visible:true}],
};
async function fixture(sql: Bun.TransactionSQL, config: unknown = base, protocol = 'Modbus') {
    await sql.unsafe('CREATE TEMP TABLE protocol_config(id UUID, protocol TEXT, config JSONB, updated_at TIMESTAMPTZ) ON COMMIT DROP');
    await sql.unsafe('CREATE TEMP TABLE device(protocol_config_id UUID, link_id UUID, deleted_at TIMESTAMPTZ) ON COMMIT DROP');
    await sql.unsafe('CREATE TEMP TABLE link(id UUID, edge_node_id UUID, deleted_at TIMESTAMPTZ, execution TEXT) ON COMMIT DROP');
    await sql.unsafe('CREATE TEMP TABLE edge_node(id UUID, capability JSONB) ON COMMIT DROP');
    await sql`INSERT INTO pg_temp.protocol_config VALUES (${modelId},${protocol},${config}::jsonb,NOW())`;
}
try {
    await db.begin(async sql => {
        await fixture(sql);
        await sql.unsafe(migration);
        const [{config}] = await sql`SELECT config FROM pg_temp.protocol_config`;
        const raw = config.registers[0];
        const converted = config.derivedPoints[1];
        assert.notEqual(raw.id, oldId);
        assert(!('scale' in raw) && !('offset' in raw));
        assert.equal(raw.decimals, -1);
        assert.equal(config.pointVisibility[raw.id], false);
        assert.equal(converted.id, oldId);
        assert.equal(converted.unit, 'kPa');
        assert.equal(converted.inputs[0].pointId, raw.id);
        assert.equal(converted.expression, 'round((x * (0.1) + (0)) * 10) / 10');
        assert.equal(config.derivedPoints[0].inputs[0].pointId, oldId, 'existing dependencies must retain their meaning');
        await sql.unsafe('ALTER TABLE pg_temp.protocol_config DROP CONSTRAINT ck_protocol_config_native_points');
        await sql.unsafe(migration);
        assert.deepEqual((await sql`SELECT config FROM pg_temp.protocol_config`)[0].config, config, 'repeated migration changed UUIDs');
    });
    await db.begin(async sql => {
        const invalid = {...base,registers:[{...base.registers[0],writable:true}]};
        await fixture(sql, invalid);
        await sql.unsafe('SAVEPOINT migration_failure');
        await assert.rejects(sql.unsafe(migration), /writable scaling/);
        await sql.unsafe('ROLLBACK TO SAVEPOINT migration_failure');
        assert.deepEqual((await sql`SELECT config FROM pg_temp.protocol_config`)[0].config, invalid, 'failed migration must roll back the original configuration');
    });
    await db.begin(async sql => {
        await fixture(sql, {points:[{id:oldId,name:'温度',field:'/value',dataType:'DOUBLE',scale:2,offset:-1}],reportTemplate:'{"value":"$point:温度"}'}, 'MQTT');
        await sql.unsafe(migration);
        const [{config}] = await sql`SELECT config FROM pg_temp.protocol_config`;
        assert.equal(config.derivedPoints[0].id, oldId);
        assert.equal(config.derivedPoints[0].expression, 'x * (2) + (-1)');
        assert.equal(config.points[0].name, '温度', 'MQTT report placeholder must still match');
        assert.equal(config.reportTemplate, '{"value":"$point:温度"}');
    });
    await assert.rejects(db.begin(async sql => {
        await fixture(sql, {...base,registers:[{...base.registers[0],writable:true}]});
        await sql.unsafe(migration);
    }), /writable scaling/);
    await assert.rejects(db.begin(async sql => {
        await fixture(sql);
        const link = crypto.randomUUID(), node = crypto.randomUUID();
        await sql`INSERT INTO pg_temp.device VALUES (${modelId},${link},NULL)`;
        await sql`INSERT INTO pg_temp.link VALUES (${link},${node},NULL,'edge')`;
        await sql`INSERT INTO pg_temp.edge_node VALUES (${node},'{}'::jsonb)`;
        await sql.unsafe(migration);
    }), /edge firmware/);
    await db.begin(async sql => {
        await fixture(sql, {...base,registers:[{...base.registers[0],scale:1,offset:0}]});
        await sql.unsafe(migration);
        const [{config}] = await sql`SELECT config FROM pg_temp.protocol_config`;
        assert.equal(config.registers[0].id, oldId);
        assert.equal(config.derivedPoints.length, 1);
    });
    await assert.rejects(db.begin(async sql => {
        await fixture(sql, {registers:[{id:oldId,dataType:'UINT16'}]});
        await sql.unsafe(migration);
        await sql`UPDATE pg_temp.protocol_config SET config=jsonb_set(config,'{registers,0,scale}','1')`;
    }), /ck_protocol_config_native_points/);
    console.log('PASS native point migration: stable result IDs, raw UUIDs, formulas, MQTT placeholders, repeat execution, guards and database constraint');
} finally {
    await db.close();
}
