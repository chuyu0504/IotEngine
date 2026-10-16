import assert from 'node:assert/strict';
import { resolve, sep } from 'node:path';
import { databaseUrl } from './architecture-fixture';

const db = new Bun.SQL(databaseUrl);
const phase = Bun.argv[2];
const migrationId = '0050_dead_letter_query_changes';
const statePath = resolve(Bun.env.ARCHITECTURE_MIGRATION_STATE ?? '');
assert(statePath.startsWith(resolve('build') + sep));
type State = { history: string[]; checksum: string; entry: string; sentinel: string };
async function history() {
    return (await db`SELECT row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id<>${migrationId} ORDER BY migration_id`).map(row => row.entry as string);
}
async function entry() {
    const rows = await db`SELECT checksum,row_to_json(m)::text AS entry FROM sys_schema_migrations m WHERE migration_id=${migrationId}`;
    assert.equal(rows.length,1);
    return rows[0]!;
}
async function checkTrigger() {
    const trigger = await db`SELECT tgname FROM pg_trigger WHERE tgname='live_dead_letters' AND tgrelid='outbox_event'::regclass`;
    assert.equal(trigger.length,1);
    const id = crypto.randomUUID();
    const rollback = new Error('fixture rollback');
    try {
        await db.begin(async tx => {
            const count = async () => Number((await tx`SELECT count(*) AS count FROM outbox_event WHERE event_type='query.changed' AND aggregate_type='system' AND aggregate_id='outbox_event'`)[0].count);
            const initial = await count();
            await tx`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version) VALUES(${id},'query.changed','auth',${id},'migration-test',1)`;
            assert.equal(await count(),initial,'ordinary outbox inserts must not recursively invalidate');
            await tx`UPDATE outbox_event SET dead_lettered_at=now() WHERE id=${id}`;
            assert.equal(await count(),initial+1);
            await tx`UPDATE outbox_event SET last_error='new error' WHERE id=${id}`;
            assert.equal(await count(),initial+2);
            await tx`UPDATE outbox_event SET last_error=last_error WHERE id=${id}`;
            assert.equal(await count(),initial+2,'unchanged rows must not invalidate');
            await tx`UPDATE outbox_event SET dead_lettered_at=NULL WHERE id=${id}`;
            assert.equal(await count(),initial+3);
            await tx`UPDATE outbox_event SET dead_lettered_at=now() WHERE id=${id}`;
            await tx`DELETE FROM outbox_event WHERE id=${id}`;
            assert.equal(await count(),initial+5);
            throw rollback;
        });
    } catch (error) { if (error !== rollback) throw error; }
    assert.equal((await db`SELECT id FROM outbox_event WHERE id=${id}`).length,0);
}
try {
    if (phase==='initial') {
        await checkTrigger();
        const sentinel = crypto.randomUUID();
        await db`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version,dead_lettered_at) VALUES(${sentinel},'query.changed','auth',${sentinel},'migration-sentinel',1,now())`;
        const current = await entry();
        await Bun.write(statePath,JSON.stringify({history:await history(),checksum:current.checksum,entry:current.entry,sentinel} satisfies State));
        console.log('PASS fresh dead-letter migration and transactional change notifications without immediate recursion');
    } else {
        const state: State = await Bun.file(statePath).json();
        assert.deepEqual(await history(),state.history,'historical migration journal changed');
        assert.equal((await db`SELECT action FROM outbox_event WHERE id=${state.sentinel}`)[0]?.action,'migration-sentinel');
        if (phase==='restart' || phase==='repeated') {
            assert.equal((await entry()).entry,state.entry,'migration reapplied');
            await checkTrigger();
            if (phase==='repeated') await db`DELETE FROM outbox_event WHERE id=${state.sentinel}`;
            console.log('PASS repeated startup preserves migration record and business data');
        } else if (phase==='prepare-upgrade' || phase==='prepare-failure') {
            await db.begin(async tx => {
                await tx.unsafe('DROP TRIGGER live_dead_letters ON outbox_event; DROP FUNCTION publish_dead_letter_change();');
                await tx`DELETE FROM sys_schema_migrations WHERE migration_id=${migrationId}`;
                if (phase==='prepare-failure') {
                    await tx.unsafe(`CREATE FUNCTION fixture_dead_letter_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
                        CREATE TRIGGER live_dead_letters AFTER INSERT ON outbox_event FOR EACH ROW EXECUTE FUNCTION fixture_dead_letter_noop();`);
                }
            });
        } else if (phase==='upgraded') {
            await checkTrigger();
            const current = await entry();
            assert.equal(current.checksum,state.checksum);
            state.entry=current.entry;
            await Bun.write(statePath,JSON.stringify(state));
            console.log('PASS old database upgrade and migration recovery');
        } else if (phase==='prepare-drift') {
            await db`UPDATE sys_schema_migrations SET checksum='fixture-drift' WHERE migration_id=${migrationId}`;
        } else if (phase==='check-drift') {
            assert.equal((await entry()).checksum.trim(),'fixture-drift');
            await checkTrigger();
            await db`UPDATE sys_schema_migrations SET checksum=${state.checksum} WHERE migration_id=${migrationId}`;
            console.log('PASS migration checksum drift rejected');
        } else if (phase==='check-failure') {
            assert.equal((await db`SELECT migration_id FROM sys_schema_migrations WHERE migration_id=${migrationId}`).length,0);
            assert.equal((await db`SELECT to_regprocedure('publish_dead_letter_change()')::text AS name`)[0].name,null,'failed trigger DDL must roll back the preceding function creation');
            await db.unsafe('DROP TRIGGER live_dead_letters ON outbox_event; DROP FUNCTION fixture_dead_letter_noop();');
            console.log('PASS failed DDL rolls back new function and migration journal');
        } else throw new Error(`Unknown phase: ${phase}`);
    }
} finally { await db.close(); }
