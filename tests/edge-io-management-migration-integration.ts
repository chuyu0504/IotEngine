import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';
const db = new Bun.SQL(databaseUrl),
    id = '0063_edge_io_management',
    phase = Bun.argv[2];
const path = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(path.startsWith(resolve('build') + sep));
const journal = async () =>
    Array.from(
        await db`SELECT row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id<>${id} ORDER BY migration_id`,
        (row) => row.entry
    );
async function entry() {
    const rows =
        await db`SELECT checksum,row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id=${id}`;
    assert.equal(rows.length, 1);
    return rows[0]!;
}
async function verify(present: boolean) {
    const constraints =
        await db`SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint WHERE conrelid='edge_task'::regclass AND conname='edge_task_task_type_check'`;
    assert.equal(constraints[0].definition.includes("'io'"), present);
    assert.equal(
        (
            await db`SELECT 1 FROM pg_trigger WHERE tgrelid='device_external_io_binding'::regclass AND tgname='guard_io_binding_control'`
        ).length,
        present ? 1 : 0
    );
}
async function predecessor(failure: boolean) {
    await db.begin(async (tx) => {
        await tx.unsafe(`DROP TRIGGER guard_node_io_control ON edge_task; DROP TRIGGER guard_io_binding_control ON device_external_io_binding;
 DROP TRIGGER edge_task_io_insert ON edge_task; DROP TRIGGER edge_task_io_update ON edge_task; DROP TRIGGER edge_task_io_delete ON edge_task;
 DROP TRIGGER edge_io_binding_insert ON device_external_io_binding; DROP TRIGGER edge_io_binding_delete ON device_external_io_binding; DROP TRIGGER edge_io_binding_update ON device_external_io_binding;
 DROP FUNCTION guard_edge_io_control_ownership(); DROP FUNCTION publish_edge_task_change();
 DROP INDEX edge_io_pending_channel; DROP INDEX edge_io_pending_deadline;
 ALTER TABLE edge_task DROP CONSTRAINT edge_task_task_type_check;
 ALTER TABLE edge_task ADD CONSTRAINT edge_task_task_type_check CHECK(task_type IN ('network','firmware','modem','vpn','platform_upsert','platform_delete'));
 CREATE TRIGGER fixture_old_edge_task_insert AFTER INSERT ON edge_task REFERENCING NEW TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge','node_id');
 CREATE TRIGGER fixture_old_edge_task_update AFTER UPDATE ON edge_task REFERENCING NEW TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge','node_id');
 CREATE TRIGGER fixture_old_edge_task_delete AFTER DELETE ON edge_task REFERENCING OLD TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('edge','node_id');`);
        await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${id}`;
        if (failure) await tx.unsafe('CREATE INDEX edge_io_pending_channel ON edge_task(node_id)');
    });
    await verify(false);
}
try {
    if (phase === 'initial') {
        await verify(true);
        const current = await entry();
        await Bun.write(
            path,
            JSON.stringify({
                entry: current.entry,
                checksum: current.checksum,
                journal: await journal(),
            })
        );
    } else {
        const state = await Bun.file(path).json();
        assert.deepEqual(await journal(), state.journal, 'historical journal changed');
        if (phase === 'restart' || phase === 'repeated') {
            await verify(true);
            assert.equal((await entry()).entry, state.entry);
        } else if (phase === 'prepare-upgrade' || phase === 'prepare-failure')
            await predecessor(phase === 'prepare-failure');
        else if (phase === 'upgraded') {
            await verify(true);
            const current = await entry();
            assert.equal(current.checksum, state.checksum);
            state.entry = current.entry;
            await Bun.write(path, JSON.stringify(state));
        } else if (phase === 'prepare-drift')
            await db`UPDATE sys_schema_migrations SET checksum='fixture-drift' WHERE migration_id=${id}`;
        else if (phase === 'check-drift') {
            assert.equal((await entry()).checksum.trim(), 'fixture-drift');
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${id}`;
        } else if (phase === 'check-failure') {
            await verify(false);
            assert.equal(
                (await db`SELECT 1 FROM sys_schema_migrations WHERE migration_id=${id}`).length,
                0
            );
            assert.equal(
                (await db`SELECT 1 FROM pg_proc WHERE proname='guard_edge_io_control_ownership'`)
                    .length,
                0
            );
            await db.unsafe('DROP INDEX edge_io_pending_channel');
        } else throw Error('unknown phase ' + phase);
    }
    console.log('PASS node IO migration ' + phase);
} finally {
    await db.close();
}
