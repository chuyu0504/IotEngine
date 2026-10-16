import { databaseUrl, redisUrl, apiBase } from './architecture-fixture';
// Only the disposable architecture fixture, never a deployed database.
import assert from 'node:assert/strict';

const db = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const ids: string[] = [];
const liveStream = 'iot:live:changes';
const savedLiveStream = `iot:test:outbox:live:${crypto.randomUUID()}`;
let liveStreamPaused = false;
async function restoreLiveStream() {
    if (!liveStreamPaused) return;
    await redis.send('EVAL', [
        "redis.call('DEL', KEYS[1]); if redis.call('EXISTS', KEYS[2]) == 1 then redis.call('RENAME', KEYS[2], KEYS[1]) end; return 1",
        '2', liveStream, savedLiveStream,
    ]);
    liveStreamPaused = false;
}
async function until(check: () => Promise<boolean>, message: string, timeout = 10000) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(50);
    }
    throw new Error(message);
}
async function published(id: string) {
    const rows = await db`SELECT published_at IS NOT NULL AS done FROM outbox_event WHERE id=${id}`;
    return rows[0]?.done === true;
}
async function insert(delaySeconds = 0) {
    const id = crypto.randomUUID();
    ids.push(id);
    await db`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version,available_at)
        VALUES(${id},'query.changed','auth',${id},'test',1,clock_timestamp()+make_interval(secs=>${delaySeconds}))`;
    return id;
}
async function checks() {
    const response = await fetch(apiBase + '/internal/metrics', { signal: AbortSignal.timeout(5000) });
    assert.equal(response.status, 200);
    const text = await response.text();
    const matches = [...text.matchAll(/^iot_engine_outbox_dispatch_checks_total(?:\{[^}]*\})?\s+(\d+)/gm)];
    assert(matches.length > 0, 'outbox dispatch counter is missing');
    return matches.reduce((sum, match) => sum + Number(match[1]), 0);
}

try {
    await until(async () => {
        const rows = await db`SELECT count(*)::int AS n FROM pg_stat_activity
            WHERE datname=current_database() AND application_name='iot-engine-outbox-listener' AND state='idle'`;
        return rows[0].n >= 1;
    }, 'LISTEN connection not established');
    await until(async () => (await db`SELECT count(*)::int AS n FROM outbox_event
        WHERE published_at IS NULL AND dead_lettered_at IS NULL`)[0].n === 0, 'fixture outbox did not drain');
    // Statistics timers still run, but an empty queue must not cause dispatch SQL.
    // Worker metrics are exported in five-second snapshots.
    await Bun.sleep(5500);
    const idle = await checks();
    await Bun.sleep(5500);
    assert.equal(await checks(), idle, 'empty outbox is still periodically dispatched');
    await db`UPDATE sys_department SET name=name WHERE id=${crypto.randomUUID()}`;
    await Bun.sleep(5500);
    assert.equal(await checks(), idle, 'zero-row UPDATE emitted a false query change');
    console.log('PASS empty queue does not poll for dispatch work');

    const rolledBack = crypto.randomUUID();
    const rollback = new Error('intentional rollback');
    try {
        await db.begin(async (tx) => {
            await tx`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
                VALUES(${rolledBack},'query.changed','auth',${rolledBack},'test',1)`;
            await Bun.sleep(500);
            assert.equal(await checks(), idle, 'uncommitted INSERT woke dispatch');
            throw rollback;
        });
    } catch (error) { if (error !== rollback) throw error; }
    assert.equal((await db`SELECT id FROM outbox_event WHERE id=${rolledBack}`).length, 0);
    const immediate = await insert();
    await until(() => published(immediate), 'committed event was not dispatched');
    console.log('PASS commit wakes dispatch; rollback does not publish');

    // Schema 2 query changes carry entity ids into the live change; schema 1
    // rows (table names in aggregate_id) publish the whole topic.
    const liveChange = async (topic: string) => {
        const entries = await redis.send('XREVRANGE', [liveStream, '+', '-', 'COUNT', '200']) as unknown as [string, string[]][];
        const fields = entries.map(([, values]) => values).find((values) => values[values.indexOf('topic') + 1] === topic);
        return fields && { version: fields[fields.indexOf('schema_version') + 1], ids: fields[fields.indexOf('ids') + 1] };
    };
    const entityTopic = `test-${crypto.randomUUID()}`, legacyTopic = `test-${crypto.randomUUID()}`;
    const entityId = crypto.randomUUID(), legacyId = crypto.randomUUID();
    ids.push(entityId, legacyId);
    await db`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version)
        VALUES(${entityId},'query.changed',${entityTopic},'a,b','test',2),(${legacyId},'query.changed',${legacyTopic},'device','test',1)`;
    await until(async () => (await published(entityId)) && (await published(legacyId)), 'query changes were not dispatched');
    assert.deepEqual(await liveChange(entityTopic), { version: '2', ids: 'a,b' });
    assert.deepEqual(await liveChange(legacyTopic), { version: '2', ids: '' });
    console.log('PASS outbox query changes publish entity ids');

    const future = await insert(2);
    await Bun.sleep(500);
    assert.equal(await published(future), false, 'future event published before its deadline');
    await until(() => published(future), 'future event stranded without a new notification');
    console.log('PASS durable future deadline wakes dispatch without another insert');

    const locked = await insert(1);
    await db.begin(async (tx) => {
        await tx`SELECT id FROM outbox_event WHERE id=${locked} FOR UPDATE`;
        await Bun.sleep(1500);
        assert.equal(await published(locked), false);
    });
    await until(() => published(locked), 'row skipped while locked was stranded after unlock');
    console.log('PASS concurrent row lock release does not strand pending work');

    const old = await db`SELECT pid FROM pg_stat_activity WHERE datname=current_database()
        AND application_name='iot-engine-outbox-listener'`;
    assert(old.length > 0);
    await db`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database()
        AND application_name='iot-engine-outbox-listener'`;
    const recovering = await insert();
    await until(() => published(recovering), 'reconnected listener did not catch up durable work', 15000);
    await until(async () => {
        const now = await db`SELECT pid FROM pg_stat_activity WHERE datname=current_database()
            AND application_name='iot-engine-outbox-listener' AND state='idle'`;
        return now.length >= old.length && now.every((row: { pid: number }) =>
            !old.some((previous: { pid: number }) => previous.pid === row.pid));
    }, 'LISTEN sessions did not reconnect', 15000);
    console.log('PASS listener disconnect/reconnect catches up committed events');

    // This fixture owns Redis. Temporarily make the destination reject XADD to
    // exercise the actual failure update, backoff and terminal attempt boundary.
    await redis.send('EVAL', [
        "if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('RENAME', KEYS[1], KEYS[2]) end; redis.call('SET', KEYS[1], 'outbox-test'); return 1",
        '2', liveStream, savedLiveStream,
    ]);
    liveStreamPaused = true;
    const retryId = await insert();
    await until(async () => {
        const rows = await db`SELECT attempts, last_error, published_at,
            extract(epoch FROM (available_at - occurred_at))::float8 AS delay
            FROM outbox_event WHERE id=${retryId}`;
        if (rows[0].attempts === 0) return false;
        assert.equal(rows[0].attempts, 1);
        assert.equal(rows[0].published_at, null);
        assert.match(rows[0].last_error, /WRONGTYPE/);
        assert(rows[0].delay >= 1 && rows[0].delay < 3, 'first retry must wait one second');
        return true;
    }, 'failed publish did not persist its retry');
    const exhaustedId = crypto.randomUUID();
    ids.push(exhaustedId);
    await db`INSERT INTO outbox_event(id,event_type,aggregate_type,aggregate_id,action,schema_version,attempts)
        VALUES(${exhaustedId},'query.changed','auth',${exhaustedId},'test',1,19)`;
    await until(async () => {
        const rows = await db`SELECT attempts, published_at, dead_lettered_at,
            extract(epoch FROM (available_at - dead_lettered_at))::float8 AS delay
            FROM outbox_event WHERE id=${exhaustedId}`;
        if (rows[0].dead_lettered_at === null) return false;
        assert.equal(rows[0].attempts, 20);
        assert.equal(rows[0].published_at, null);
        assert.equal(rows[0].delay, 256);
        return true;
    }, 'twentieth failed attempt was not dead-lettered');
    await restoreLiveStream();
    await until(() => published(retryId), 'recovered destination did not publish the retried event');
    const retried = await db`SELECT attempts, last_error FROM outbox_event WHERE id=${retryId}`;
    assert(retried[0].attempts >= 2);
    assert.equal(retried[0].last_error, null);
    assert.equal(await published(exhaustedId), false);
    console.log('PASS publish retry, exponential backoff, recovery and dead-letter boundary');
} finally {
    await restoreLiveStream();
    if (ids.length) await db`DELETE FROM outbox_event WHERE id IN ${db(ids)}`;
    await db.close();
    redis.close();
}
