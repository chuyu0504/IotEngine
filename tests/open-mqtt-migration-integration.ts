import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const phase = Bun.argv[2];
const path = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(path.startsWith(resolve('build') + sep));
const migration = '0053_open_mqtt';
async function journal() {
    return [...await db`SELECT migration_id, checksum FROM sys_schema_migrations WHERE migration_id<>${migration} ORDER BY migration_id`];
}
async function verify() {
    assert.equal((await db`SELECT to_regclass('public.open_mqtt') AS name`)[0].name, 'open_mqtt');
    assert.equal((await db`SELECT data_type FROM information_schema.columns WHERE table_name='open_access_log' AND column_name='mqtt_id'`)[0].data_type, 'uuid');
    assert.equal((await db`SELECT count(*)::int AS n FROM information_schema.columns WHERE table_name='open_mqtt' AND column_name='topic'`)[0].n, 0);
    assert.equal((await db`SELECT count(*)::int AS n FROM pg_constraint WHERE conname IN ('open_mqtt_access_key_fkey','open_mqtt_status_check','open_mqtt_timeout_check','open_access_log_mqtt_fkey')`)[0].n, 4);
}
try {
    if (phase === 'initial') {
        await verify();
        const [entry] = await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`;
        await Bun.write(path, JSON.stringify({ checksum: entry.checksum, journal: await journal() }));
    } else {
        const state = await Bun.file(path).json();
        assert.deepEqual(await journal(), state.journal, 'historical migration checksums changed');
        if (['restart','upgraded','repeated'].includes(phase)) {
            await verify();
            assert.equal((await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`)[0].checksum, state.checksum);
        } else if (phase === 'prepare-upgrade' || phase === 'prepare-failure') {
            await db.begin(async (tx) => {
                await tx`ALTER TABLE open_access_log DROP COLUMN mqtt_id`;
                await tx`DROP TABLE open_mqtt`;
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migration}`;
                if (phase === 'prepare-failure') await tx`ALTER TABLE open_access_log ADD COLUMN mqtt_id integer`;
            });
        } else if (phase === 'prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='open-mqtt-drift' WHERE migration_id=${migration}`;
        } else if (phase === 'check-drift') {
            assert.equal((await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`)[0].checksum.trim(), 'open-mqtt-drift');
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migration}`;
        } else if (phase === 'check-failure') {
            assert.equal((await db`SELECT to_regclass('public.open_mqtt') AS name`)[0].name, null, 'failed migration did not roll back table creation');
            assert.equal((await db`SELECT 1 FROM sys_schema_migrations WHERE migration_id=${migration}`).length, 0);
            assert.equal((await db`SELECT data_type FROM information_schema.columns WHERE table_name='open_access_log' AND column_name='mqtt_id'`)[0].data_type, 'integer');
            await db`ALTER TABLE open_access_log DROP COLUMN mqtt_id`;
        } else throw new Error(`Unknown migration phase ${phase}`);
    }
    console.log(`PASS open MQTT migration: ${phase}`);
} finally {
    await db.close();
}
