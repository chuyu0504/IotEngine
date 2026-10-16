import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';

const phase = Bun.argv[2];
const path = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(path.startsWith(resolve('build') + sep));
const db = new Bun.SQL(databaseUrl);
const migration = '0052_mqtt_client';
const tables = ['link', 'protocol_config', 'device_data'];
async function journal() {
    return [...await db`SELECT migration_id,checksum FROM sys_schema_migrations WHERE migration_id<>${migration} ORDER BY migration_id`];
}
async function constraints() {
    return db`SELECT c.relname,pg_get_constraintdef(k.oid) AS definition FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        WHERE c.relname IN ('link','protocol_config','device_data') AND k.conname=c.relname||'_protocol_check' ORDER BY c.relname`;
}
async function verify() {
    const rows = await constraints();
    assert.equal(rows.length, 3);
    for (const row of rows) assert(row.definition.includes("'MQTT'"));
    const [trigger] =
        await db`SELECT pg_get_functiondef('bind_device_channel()'::regprocedure) AS definition`;
    assert(trigger.definition.includes("WHEN 'MQTT'"));
    assert.equal(
        (await db`SELECT 1 FROM pg_constraint WHERE conname='link_mqtt_client_check'`).length,
        1
    );
}
try {
    if (phase === 'initial') {
        await verify();
        const [entry] =
            await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`;
        await Bun.write(
            path,
            JSON.stringify({ checksum: entry.checksum, journal: await journal() })
        );
    } else {
        const state = await Bun.file(path).json();
        assert.deepEqual(await journal(), state.journal);
        if (phase === 'restart' || phase === 'upgraded' || phase === 'repeated') {
            await verify();
            assert.equal(
                (
                    await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`
                )[0].checksum,
                state.checksum
            );
        } else if (phase === 'prepare-upgrade' || phase === 'prepare-failure') {
            const [trigger] =
                await db`SELECT pg_get_functiondef('bind_device_channel()'::regprocedure) AS definition`;
            const oldTrigger = trigger.definition.replace(
                /WHEN 'MQTT' THEN[\s\S]*?WHEN 'DLT645'/,
                "WHEN 'DLT645'"
            );
            assert(!oldTrigger.includes("WHEN 'MQTT'"));
            await db.begin(async (tx) => {
                await tx.unsafe('ALTER TABLE link DROP CONSTRAINT link_mqtt_client_check');
                for (const table of tables) {
                    // Table names and DDL are fixed fixture constants. NOT VALID retains prior test history.
                    await tx.unsafe(`ALTER TABLE ${table} DROP CONSTRAINT ${table}_protocol_check`);
                    await tx.unsafe(
                        `ALTER TABLE ${table} ADD CONSTRAINT ${table}_protocol_check CHECK(protocol IN ('SL651','Modbus','S7','MC','FINS','DLT645')) NOT VALID`
                    );
                }
                await tx.unsafe(oldTrigger);
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migration}`;
                if (phase === 'prepare-failure')
                    await tx.unsafe(
                        'ALTER TABLE protocol_config RENAME CONSTRAINT protocol_config_protocol_check TO mqtt_fixture_blocker'
                    );
            });
        } else if (phase === 'prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='mqtt-drift' WHERE migration_id=${migration}`;
        } else if (phase === 'check-drift') {
            assert.equal(
                (
                    await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id=${migration}`
                )[0].checksum.trim(),
                'mqtt-drift'
            );
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migration}`;
        } else if (phase === 'check-failure') {
            assert.equal(
                (await db`SELECT 1 FROM sys_schema_migrations WHERE migration_id=${migration}`)
                    .length,
                0
            );
            const rows = await constraints();
            const link = rows.find((row) => row.relname === 'link');
            assert(
                link && !link.definition.includes("'MQTT'"),
                'failed migration did not roll back earlier DDL'
            );
            assert.equal(
                (await db`SELECT 1 FROM pg_constraint WHERE conname='link_mqtt_client_check'`)
                    .length,
                0
            );
            const [trigger] =
                await db`SELECT pg_get_functiondef('bind_device_channel()'::regprocedure) AS definition`;
            assert(!trigger.definition.includes("WHEN 'MQTT'"));
            await db.unsafe(
                'ALTER TABLE protocol_config RENAME CONSTRAINT mqtt_fixture_blocker TO protocol_config_protocol_check'
            );
        } else throw Error(`unknown phase ${phase}`);
    }
    console.log(`PASS MQTT migration ${phase}`);
} finally {
    await db.close();
}
