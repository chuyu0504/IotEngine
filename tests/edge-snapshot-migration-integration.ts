import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const phase = Bun.argv[2];
const migrationId = '0060_edge_acquisition_snapshot';
const statePath = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(statePath.startsWith(resolve('build') + sep));
type State = { history: string[]; checksum: string; entry: string; node: string };

async function history() {
    return (await db`SELECT row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id<>${migrationId} ORDER BY migration_id`).map(row => row.entry as string);
}
async function entry() {
    const rows = await db`SELECT checksum,row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id=${migrationId}`;
    assert.equal(rows.length, 1);
    return rows[0]!;
}
async function verifyTable(node: string) {
    const [constraint] = await db`SELECT 1 FROM pg_constraint WHERE conname='edge_acquisition_sample_snapshot_fkey'`;
    if (!constraint) await db.unsafe('ALTER TABLE edge_acquisition_sample ADD CONSTRAINT edge_acquisition_sample_snapshot_fkey FOREIGN KEY(node_id,digest) REFERENCES edge_acquisition_snapshot(node_id,digest) ON DELETE CASCADE');
    const [checkpointConstraint] = await db`SELECT 1 FROM pg_constraint WHERE conname='edge_acquisition_calculation_snapshot_fkey'`;
    if (!checkpointConstraint) await db.unsafe('ALTER TABLE edge_acquisition_calculation ADD CONSTRAINT edge_acquisition_calculation_snapshot_fkey FOREIGN KEY(node_id,digest) REFERENCES edge_acquisition_snapshot(node_id,digest) ON DELETE CASCADE');
    const digest = 'a'.repeat(64);
    await db`INSERT INTO edge_acquisition_snapshot(node_id,digest,revision,payload) VALUES(${node},${digest},1,'{}'::jsonb)`;
    await assert.rejects(async () => db`INSERT INTO edge_acquisition_snapshot(node_id,digest,revision,payload) VALUES(${node},${digest},2,'{}'::jsonb)`);
    await assert.rejects(async () => db`INSERT INTO edge_acquisition_snapshot(node_id,digest,revision,payload) VALUES(${node},${'b'.repeat(64)},1,'{}'::jsonb)`);
    assert.equal(Number((await db`SELECT count(*) AS count FROM edge_acquisition_snapshot WHERE node_id=${node}`)[0]!.count), 1);
    await db`DELETE FROM edge_acquisition_snapshot WHERE node_id=${node}`;
}

try {
    if (phase === 'initial') {
        const node = crypto.randomUUID();
        const platform = Bun.env.TEST_EDGE_PLATFORM_ID ?? '00000000-0000-7000-8000-000000000001';
        await db`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},'999999999906001','approved')`;
        await verifyTable(node);
        const current = await entry();
        await Bun.write(statePath, JSON.stringify({ history: await history(), checksum: current.checksum, entry: current.entry, node } satisfies State));
        console.log('PASS new database creates snapshot table and constraints');
    } else {
        const state: State = await Bun.file(statePath).json();
        assert.deepEqual(await history(), state.history, 'historical migration journal changed');
        assert.equal((await db`SELECT id FROM edge_node WHERE id=${state.node}`).length, 1);
        if (phase === 'restart' || phase === 'repeated') {
            assert.equal((await entry()).entry, state.entry, 'migration reapplied');
            await verifyTable(state.node);
            console.log('PASS repeated startup preserves snapshot schema and existing data');
        } else if (phase === 'prepare-upgrade' || phase === 'prepare-failure') {
            await db.begin(async tx => {
                await tx.unsafe('ALTER TABLE edge_acquisition_sample DROP CONSTRAINT IF EXISTS edge_acquisition_sample_snapshot_fkey');
                await tx.unsafe('ALTER TABLE edge_acquisition_calculation DROP CONSTRAINT IF EXISTS edge_acquisition_calculation_snapshot_fkey');
                await tx.unsafe('DROP TABLE edge_acquisition_snapshot');
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migrationId}`;
                if (phase === 'prepare-failure') await tx.unsafe('CREATE TABLE edge_acquisition_snapshot(marker integer)');
            });
        } else if (phase === 'upgraded') {
            await verifyTable(state.node);
            const current = await entry();
            assert.equal(current.checksum, state.checksum);
            state.entry = current.entry;
            await Bun.write(statePath, JSON.stringify(state));
            console.log('PASS previous database upgrades without changing its migration history');
        } else if (phase === 'prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='fixture-drift' WHERE migration_id=${migrationId}`;
        } else if (phase === 'check-drift') {
            assert.equal((await entry()).checksum.trim(), 'fixture-drift');
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migrationId}`;
            console.log('PASS migration checksum drift rejected');
        } else if (phase === 'check-failure') {
            assert.equal((await db`SELECT migration_id FROM sys_schema_migrations WHERE migration_id=${migrationId}`).length, 0);
            assert.equal((await db`SELECT column_name FROM information_schema.columns WHERE table_name='edge_acquisition_snapshot' AND column_name='marker'`).length, 1);
            await db.unsafe('DROP TABLE edge_acquisition_snapshot');
            console.log('PASS failed migration rolls back its journal and table changes');
        } else throw new Error(`Unknown phase: ${phase}`);
    }
} finally {
    await db.close();
}
