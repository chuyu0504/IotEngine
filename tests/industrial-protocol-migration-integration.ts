import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';

const phase = Bun.argv[2];
const path = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(path.startsWith(resolve('build') + sep));
const db = new Bun.SQL(databaseUrl);
const migration = '0049_industrial_protocols';
const tables = ['link', 'protocol_config', 'device_data'];
async function journal() {
    return [...await db`SELECT migration_id,checksum FROM sys_schema_migrations WHERE migration_id<>${migration} ORDER BY migration_id`];
}
async function constraints() {
    return db`SELECT c.relname,pg_get_constraintdef(k.oid) AS definition FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        WHERE c.relname IN ('link','protocol_config','device_data') AND k.conname=c.relname||'_protocol_check' ORDER BY c.relname`;
}
async function verify() {
    const rows = await constraints(); assert.equal(rows.length, 3);
    for (const row of rows) for (const protocol of ['MC', 'FINS', 'DLT645']) assert(row.definition.includes(`'${protocol}'`));
    const [trigger] = await db`SELECT pg_get_functiondef('bind_device_channel()'::regprocedure) AS definition`;
    assert(trigger.definition.includes("WHEN 'DLT645'"));
}
try {
    if (phase === 'initial') {
        await verify();
        const [entry] = await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`;
        await Bun.write(path, JSON.stringify({checksum: entry.checksum, journal: await journal()}));
    } else {
        const state = await Bun.file(path).json(); assert.deepEqual(await journal(), state.journal);
        if (phase === 'restart' || phase === 'upgraded' || phase === 'repeated') {
            await verify();
            assert.equal((await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`)[0].checksum, state.checksum);
        } else if (phase === 'prepare-upgrade' || phase === 'prepare-failure') {
            const source = await Bun.file('service/config/current_device_model.h').text();
            const original = source.slice(source.indexOf('CREATE OR REPLACE FUNCTION bind_device_channel()'), source.indexOf('END $fn$;') + 9);
            await db.begin(async tx => {
                for (const table of tables) {
                    // Names are fixed test constants; no external text enters these DDL statements.
                    await tx.unsafe(`ALTER TABLE ${table} DROP CONSTRAINT ${table}_protocol_check`);
                    await tx.unsafe(`ALTER TABLE ${table} ADD CONSTRAINT ${table}_protocol_check CHECK(protocol IN ('SL651','Modbus','S7'))`);
                }
                await tx.unsafe(original);
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migration}`;
                if (phase === 'prepare-failure') await tx.unsafe('ALTER TABLE protocol_config RENAME CONSTRAINT protocol_config_protocol_check TO industrial_fixture_blocker');
            });
        } else if (phase === 'prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='industrial-drift' WHERE migration_id=${migration}`;
        } else if (phase === 'check-drift') {
            assert.equal((await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`)[0].checksum.trim(), 'industrial-drift');
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migration}`;
        } else if (phase === 'check-failure') {
            assert.equal((await db`SELECT 1 FROM sys_schema_migrations WHERE migration_id=${migration}`).length, 0);
            const rows = await constraints();
            const link = rows.find(row => row.relname === 'link'); assert(link && !link.definition.includes("'MC'"), 'failed migration did not roll back earlier DDL');
            await db.unsafe('ALTER TABLE protocol_config RENAME CONSTRAINT industrial_fixture_blocker TO protocol_config_protocol_check');
        } else throw Error(`unknown phase ${phase}`);
    }
    console.log(`PASS industrial protocol migration ${phase}`);
} finally { await db.close(); }
