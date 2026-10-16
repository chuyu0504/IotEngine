import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';

// Migration 0057 rebuilds the live query triggers so each statement publishes
// one schema-2 change carrying its topic and the distinct changed entity ids.
const db = new Bun.SQL(databaseUrl);
const phase = Bun.argv[2];
const migrationId = '0057_entity_live_changes';
const platform = Bun.env.TEST_EDGE_PLATFORM_ID ?? '00000000-0000-7000-8000-000000000001';
const statePath = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(statePath.startsWith(resolve('build') + sep));
type State = { history: string[]; checksum: string; entry: string; sentinel: string };
type Change = { id: string; topic: string; ids: string; version: number };

// The functions as migrations 0043 and 0051 left them, before 0057.
const legacyFunctions = `
CREATE OR REPLACE FUNCTION publish_query_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM query_changed_rows) THEN RETURN NULL; END IF;
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed',TG_ARGV[0],TG_TABLE_NAME,TG_OP,1);
  RETURN NULL;
END $fn$;
CREATE OR REPLACE FUNCTION publish_edge_dtu_change() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
  IF TG_OP = 'UPDATE' AND OLD IS NOT DISTINCT FROM NEW THEN RETURN NULL; END IF;
  INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
  VALUES(gen_random_uuid(),'query.changed','edge-dtu:' || COALESCE(NEW.node_id,OLD.node_id)::text,
    COALESCE(NEW.channel_id,OLD.channel_id)::text,TG_OP,1);
  RETURN NULL;
END $fn$;`;

// Pre-0057 trigger names and topics: sys_role kept its 0036 name and
// device_group still published the device topic.
const legacyTriggers = ['INSERT', 'UPDATE', 'DELETE'].map((operation) => {
    const transition = operation === 'DELETE' ? 'OLD' : 'NEW';
    return `DROP TRIGGER live_sys_role_${operation.toLowerCase()} ON sys_role;
DROP TRIGGER live_device_group_${operation.toLowerCase()} ON device_group;
CREATE TRIGGER live_roles_${operation.toLowerCase()} AFTER ${operation} ON sys_role REFERENCING ${transition} TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('auth');
CREATE TRIGGER live_device_group_${operation.toLowerCase()} AFTER ${operation} ON device_group REFERENCING ${transition} TABLE AS query_changed_rows FOR EACH STATEMENT EXECUTE FUNCTION publish_query_change('device');`;
}).join('\n');

async function history() {
    return (await db`SELECT row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id<>${migrationId} ORDER BY migration_id`).map(row => row.entry as string);
}
async function entry() {
    const rows = await db`SELECT checksum,row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id=${migrationId}`;
    assert.equal(rows.length, 1);
    return rows[0]!;
}
async function triggerNames(table: string) {
    return (await db`SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relname=${table} AND t.tgfoid='publish_query_change()'::regprocedure AND NOT t.tgisinternal ORDER BY t.tgname`).map(row => row.tgname as string);
}

async function checkEntityChanges() {
    const rollback = new Error('fixture rollback');
    const first = crypto.randomUUID(), second = crypto.randomUUID(), node = crypto.randomUUID();
    const tag = `live-${first.slice(0, 8)}`;
    try {
        await db.begin(async tx => {
            const seen = new Set<string>();
            // Changes written by the statements of this transaction since the last call.
            const fresh = async () => {
                const rows = (await tx`SELECT id,aggregate_type AS topic,aggregate_id AS ids,schema_version AS version FROM outbox_event
                    WHERE event_type='query.changed' AND xmin::text=pg_current_xact_id()::text`) as Change[];
                const result = rows.filter(row => !seen.has(row.id));
                for (const row of result) seen.add(row.id);
                return result;
            };
            await fresh();
            const admin = (await tx`SELECT id FROM sys_user ORDER BY created_at LIMIT 1`)[0]!.id as string;

            await tx`INSERT INTO device_group(id,name,created_by) VALUES(${first},${tag + '-a'},${admin}),(${second},${tag + '-b'},${admin})`;
            let changes = await fresh();
            assert.equal(changes.length, 1, 'one statement publishes one change');
            assert.equal(changes[0]!.topic, 'device-group');
            assert.equal(changes[0]!.version, 2);
            assert.deepEqual(changes[0]!.ids.split(',').sort(), [first, second].sort());

            await tx`UPDATE device_group SET remark='x' WHERE id='00000000-0000-0000-0000-000000000000'`;
            assert.deepEqual(await fresh(), [], 'zero-row statements publish nothing');

            await tx`UPDATE device_group SET remark='y' WHERE id=${first}`;
            changes = await fresh();
            assert.deepEqual(changes.map(row => [row.topic, row.ids]), [['device-group', first]], 'update publishes only the changed id');

            await tx`DELETE FROM device_group WHERE id=${second}`;
            changes = await fresh();
            assert.deepEqual(changes.map(row => [row.topic, row.ids]), [['device-group', second]], 'delete publishes the removed id');

            await tx`INSERT INTO device_group(id,name,created_by) SELECT gen_random_uuid(),${tag}||'-'||n,${admin} FROM generate_series(1,1001) n`;
            changes = await fresh();
            assert.deepEqual(changes.map(row => [row.topic, row.ids, row.version]), [['device-group', '', 2]], 'more than 1000 ids publish the whole topic');

            await tx`UPDATE sys_role SET updated_at=updated_at WHERE id=(SELECT id FROM sys_role LIMIT 1)`;
            changes = await fresh();
            assert.deepEqual(changes.map(row => [row.topic, row.ids, row.version]), [['auth', '', 2]], 'auth changes are scope-wide');

            await tx`INSERT INTO edge_node(id,platform_id,imei,enrollment_status) VALUES(${node},${platform},'999999999905701','approved')`;
            await fresh();
            await tx`INSERT INTO edge_node_interface(node_id,name,ipv4) VALUES(${node},'br-lan','192.168.1.1')`;
            changes = await fresh();
            assert.deepEqual(changes.map(row => [row.topic, row.ids]), [['edge', node]], 'node child tables publish their node id');

            await tx`INSERT INTO edge_dtu(node_id,channel_id,config,wire_hex) VALUES(${node},${crypto.randomUUID()},'{}'::jsonb,'00')`;
            changes = await fresh();
            assert.deepEqual(changes.map(row => [row.topic, row.ids, row.version]), [['edge-dtu', node, 2]], 'DTU changes publish the node id');
            throw rollback;
        });
    } catch (error) { if (error !== rollback) throw error; }
    assert.deepEqual(await triggerNames('sys_role'), ['live_sys_role_delete', 'live_sys_role_insert', 'live_sys_role_update'], 'legacy trigger names were replaced');
    assert.deepEqual(await triggerNames('device_group'), ['live_device_group_delete', 'live_device_group_insert', 'live_device_group_update']);
}

try {
    if (phase === 'initial') {
        await checkEntityChanges();
        const sentinel = crypto.randomUUID();
        await db`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version,dead_lettered_at) VALUES(${sentinel},'query.changed','auth',${sentinel},'migration-sentinel',1,now())`;
        const current = await entry();
        await Bun.write(statePath, JSON.stringify({ history: await history(), checksum: current.checksum, entry: current.entry, sentinel } satisfies State));
        console.log('PASS fresh schema publishes entity-level live changes');
    } else {
        const state: State = await Bun.file(statePath).json();
        assert.deepEqual(await history(), state.history, 'historical migration journal changed');
        assert.equal((await db`SELECT action FROM outbox_event WHERE id=${state.sentinel}`)[0]?.action, 'migration-sentinel');
        if (phase === 'restart' || phase === 'repeated') {
            assert.equal((await entry()).entry, state.entry, 'migration reapplied');
            await checkEntityChanges();
            if (phase === 'repeated') await db`DELETE FROM outbox_event WHERE id=${state.sentinel}`;
            console.log('PASS repeated startup preserves migration record and business data');
        } else if (phase === 'prepare-upgrade' || phase === 'prepare-failure') {
            await db.begin(async tx => {
                await tx.unsafe(legacyFunctions);
                await tx.unsafe(legacyTriggers);
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migrationId}`;
                if (phase === 'prepare-failure') {
                    // A foreign trigger holding a name 0057 creates makes its DDL fail.
                    await tx.unsafe(`DROP TRIGGER live_link_insert ON link;
                        CREATE FUNCTION fixture_live_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
                        CREATE TRIGGER live_link_insert AFTER INSERT ON link FOR EACH STATEMENT EXECUTE FUNCTION fixture_live_noop();`);
                }
            });
        } else if (phase === 'upgraded') {
            await checkEntityChanges();
            const current = await entry();
            assert.equal(current.checksum, state.checksum);
            state.entry = current.entry;
            await Bun.write(statePath, JSON.stringify(state));
            console.log('PASS legacy triggers upgrade to entity-level changes');
        } else if (phase === 'prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='fixture-drift' WHERE migration_id=${migrationId}`;
        } else if (phase === 'check-drift') {
            assert.equal((await entry()).checksum.trim(), 'fixture-drift');
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migrationId}`;
            console.log('PASS migration checksum drift rejected');
        } else if (phase === 'check-failure') {
            assert.equal((await db`SELECT migration_id FROM sys_schema_migrations WHERE migration_id=${migrationId}`).length, 0);
            const source = (await db`SELECT prosrc FROM pg_proc WHERE oid='publish_query_change()'::regprocedure`)[0]!.prosrc as string;
            assert(source.includes('TG_TABLE_NAME'), 'failed DDL must roll back the replaced trigger function');
            assert.deepEqual(await triggerNames('sys_role'), ['live_roles_delete', 'live_roles_insert', 'live_roles_update'], 'failed DDL must keep the legacy triggers');
            await db.unsafe('DROP TRIGGER live_link_insert ON link; DROP FUNCTION fixture_live_noop();');
            console.log('PASS failed DDL rolls back trigger rebuild and migration journal');
        } else throw new Error(`Unknown phase: ${phase}`);
    }
} finally { await db.close(); }
