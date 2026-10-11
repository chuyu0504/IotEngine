import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';

const phase = Bun.argv[2];
const statePath = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(statePath.startsWith(resolve('build') + sep), 'migration state must remain in the disposable build fixture');
const db = new Bun.SQL(databaseUrl);
const migrationId = '0047_outbox_replay_counter';
type State = { history: string[]; checksum: string; entry: string; sentinel: string };

async function history() {
    const rows = await db`SELECT row_to_json(m)::text AS entry FROM sys_schema_migrations m
        WHERE migration_id<>${migrationId} ORDER BY migration_id`;
    return rows.map(row => row.entry as string);
}
async function entry() {
    const rows = await db`SELECT checksum, row_to_json(m)::text AS entry FROM sys_schema_migrations m
        WHERE migration_id=${migrationId}`;
    assert.equal(rows.length, 1);
    return rows[0]!;
}
async function checkSchema() {
    const columns = await db`SELECT column_name,data_type,is_nullable FROM information_schema.columns
        WHERE table_schema='public' AND table_name='outbox_replay_counter' ORDER BY ordinal_position`;
    assert.deepEqual(columns.map(row => [row.column_name,row.data_type,row.is_nullable]), [
        ['id','text','NO'], ['replays','bigint','NO'], ['updated_at','timestamp with time zone','NO'],
    ]);
    const id = `migration-default-${randomUUID()}`;
    await db.begin(async tx => {
        const rows = await tx`INSERT INTO outbox_replay_counter(id) VALUES(${id})
            RETURNING replays::text AS replays, updated_at IS NOT NULL AS timestamp_present`;
        assert.equal(rows[0]?.replays, '0');
        assert.equal(rows[0]?.timestamp_present, true);
        await tx`DELETE FROM outbox_replay_counter WHERE id=${id}`;
    });
}

try {
    if (phase === 'initial') {
        await checkSchema();
        const sentinel = randomUUID();
        await db`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version,dead_lettered_at)
            VALUES(${sentinel},'query.changed','auth',${sentinel},'migration-sentinel',1,now())`;
        const current = await entry();
        await Bun.write(statePath, JSON.stringify({ history: await history(), checksum: current.checksum,
            entry: current.entry, sentinel } satisfies State));
        console.log('PASS fresh migration creates the expected counter columns and defaults');
    } else {
        const state: State = await Bun.file(statePath).json();
        assert.deepEqual(await history(), state.history, 'historical migration journal changed');
        assert.equal((await db`SELECT action FROM outbox_event WHERE id=${state.sentinel}`)[0]?.action,
            'migration-sentinel', 'existing business data changed');
        if (phase === 'restart') {
            assert.equal((await entry()).entry, state.entry, 'restart reapplied the migration');
            console.log('PASS restart retains durable data and skips applied migration');
        } else if (phase === 'prepare-upgrade' || phase === 'prepare-failure') {
            await db.begin(async tx => {
                await tx.unsafe('DROP TABLE outbox_replay_counter');
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migrationId}`;
                if (phase === 'prepare-failure')
                    await tx.unsafe("CREATE VIEW outbox_replay_counter AS SELECT 'collision'::text AS id");
            });
        } else if (phase === 'upgraded') {
            await checkSchema();
            const current = await entry();
            assert.equal(current.checksum, state.checksum);
            state.entry = current.entry;
            await Bun.write(statePath, JSON.stringify(state));
            console.log('PASS upgrade applies only the new migration and preserves existing business data');
        } else if (phase === 'prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='fixture-checksum-drift' WHERE migration_id=${migrationId}`;
        } else if (phase === 'check-drift') {
            assert.equal((await entry()).checksum.trim(), 'fixture-checksum-drift', 'startup overwrote a drifted checksum');
            await checkSchema();
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migrationId}`;
            console.log('PASS checksum drift is rejected without rewriting the migration journal');
        } else if (phase === 'check-failure') {
            assert.equal((await db`SELECT count(*)::int AS count FROM sys_schema_migrations WHERE migration_id=${migrationId}`)[0]?.count, 0);
            assert.equal((await db`SELECT relkind FROM pg_class WHERE oid='public.outbox_replay_counter'::regclass`)[0]?.relkind, 'v');
            await db.unsafe('DROP VIEW outbox_replay_counter');
            console.log('PASS failed DDL leaves no applied migration record or partial counter table');
        } else if (phase === 'repeated') {
            assert.equal((await entry()).entry, state.entry);
            await db`DELETE FROM outbox_event WHERE id=${state.sentinel}`;
            console.log('PASS recovered migration remains idempotent on another full startup');
        } else {
            throw new Error(`Unknown migration phase: ${phase}`);
        }
    }
} finally {
    await db.close();
}
