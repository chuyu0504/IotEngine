import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';
const phase = Bun.argv[2];
const path = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(path.startsWith(resolve('build') + sep));
const db = new Bun.SQL(databaseUrl);
const migration = '0048_packet_debug_switches';
async function journal() {
    return [...await db`SELECT migration_id,checksum FROM sys_schema_migrations WHERE migration_id<>${migration} ORDER BY migration_id`];
}
async function schema() {
    const rows = await db`SELECT table_name,data_type,is_nullable,column_default FROM information_schema.columns
        WHERE table_schema='public' AND table_name IN ('device','link') AND column_name='debug_enabled' ORDER BY table_name`;
    assert.deepEqual(rows.map(row => [row.table_name,row.data_type,row.is_nullable,row.column_default]),
        [['device','boolean','NO','false'],['link','boolean','NO','false']]);
}
try {
    if (phase === 'initial') {
        await schema();
        const checksum = (await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`)[0].checksum;
        const id = crypto.randomUUID();
        await db`INSERT INTO link(id,name,protocol,endpoint,status,execution,created_by,debug_enabled)
            VALUES(${id},${id},'SL651','{"transport":"tcp","mode":"TCP Server","ip":"0.0.0.0","port":59780}'::jsonb,'disabled','collector','00000000-0000-7000-8000-000000000002',true)`;
        await Bun.write(path,JSON.stringify({checksum,id,journal:await journal()}));
    } else {
        const state = await Bun.file(path).json();
        assert.deepEqual(await journal(),state.journal);
        if(phase==='restart') {
            await schema();
            assert.equal((await db`SELECT debug_enabled FROM link WHERE id=${state.id}`)[0].debug_enabled,true);
        } else if(phase==='prepare-upgrade'||phase==='prepare-failure') {
            await db.begin(async tx => {
                await tx.unsafe('ALTER TABLE device DROP COLUMN debug_enabled');
                await tx.unsafe('ALTER TABLE link DROP COLUMN debug_enabled');
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migration}`;
                if(phase==='prepare-failure') await tx.unsafe('ALTER TABLE link ADD COLUMN debug_enabled text');
            });
        } else if(phase==='upgraded'||phase==='repeated') {
            await schema();
            assert.equal((await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`)[0].checksum,state.checksum);
            assert.equal((await db`SELECT debug_enabled FROM link WHERE id=${state.id}`)[0].debug_enabled,false);
        } else if(phase==='prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='debug-drift' WHERE migration_id=${migration}`;
        } else if(phase==='check-drift') {
            assert.equal((await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`)[0].checksum.trim(),'debug-drift');
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migration}`;
        } else if(phase==='check-failure') {
            assert.equal((await db`SELECT 1 FROM sys_schema_migrations WHERE migration_id=${migration}`).length,0);
            assert.equal((await db`SELECT 1 FROM information_schema.columns WHERE table_name='device' AND column_name='debug_enabled'`).length,0);
            await db.unsafe('ALTER TABLE link DROP COLUMN debug_enabled');
        } else throw Error(`unknown phase ${phase}`);
    }
    console.log(`PASS packet debug migration ${phase}`);
} finally {await db.close();}
