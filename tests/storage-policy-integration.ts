// Device data storage policy migration against a real TimescaleDB database:
// fresh schema, repeat, checksum drift rejection, v1 upgrade, on/off/on and
// rollback of a failed policy change. Runs `iot-engine --migrate-only` from the
// managed storage-policy fixture (IOT_TEST_ENGINE, IOT_TEST_FIXTURE_DIRECTORY).
import assert from 'node:assert/strict';
import { copyFile, link } from 'node:fs/promises';
import { basename, join } from 'node:path';
import { databaseUrl, fixtureDirectory } from './architecture-fixture';

const engine = Bun.env.IOT_TEST_ENGINE;
assert(engine, 'IOT_TEST_ENGINE is required; run through CTest');
// iot-engine reads its configuration only from .env beside the executable.
const executable = join(fixtureDirectory, basename(engine));
await link(engine, executable).catch(() => copyFile(engine, executable));
const url = new URL(databaseUrl);
const db = new Bun.SQL(databaseUrl);
// Rows of one text column `value`, joined for whole-state comparison.
const scalar = async (query: Promise<Array<{ value: unknown }>>) => (await query).map(row => String(row.value)).join(',');
const policyState = () => scalar(db`SELECT checksum || ':' || applied_at::text AS value FROM sys_schema_migrations
    WHERE starts_with(migration_id,'runtime_device_data_storage_policy_')`);
const compressionJobQuery = `SELECT job_id AS value FROM timescaledb_information.jobs
    WHERE hypertable_name='device_data' AND proc_name='policy_compression'`;
const schemaChecksumQuery = `SELECT string_agg(migration_id || ':' || checksum, ',' ORDER BY migration_id) AS value
    FROM sys_schema_migrations WHERE migration_id ~ '^[0-9]{4}_'`;

async function migrate(enabled: boolean, chunkHours: number, expectSuccess = true) {
    const settings = {
        DB_HOST: url.hostname,
        DB_PORT: url.port || '5432',
        DB_USERNAME: decodeURIComponent(url.username),
        DB_PASSWORD: decodeURIComponent(url.password),
        DB_DATABASE: url.pathname.slice(1),
        GB28181_ENABLED: 'false',
        VPN_HUB_ENABLED: 'false',
        EDGE_PUBLIC_BASE_URL: 'http://127.0.0.1:1',
        EDGE_PLATFORM_ID: '00000000-0000-7000-8000-000000000001',
        PACKET_LOG_DIRECTORY: join(fixtureDirectory, 'logs'),
        DEVICE_DATA_COMPRESSION_POLICY_ENABLED: String(enabled),
        DEVICE_DATA_CHUNK_INTERVAL_HOURS: String(chunkHours),
        DEVICE_DATA_COMPRESSION_AFTER_HOURS: '168',
        DEVICE_DATA_MUTABLE_WINDOW_HOURS: '48',
    };
    await Bun.write(join(fixtureDirectory, '.env'),
        Object.entries(settings).map(([name, value]) => `${name}=${value}\n`).join(''));
    const child = Bun.spawn([executable, '--migrate-only'], { cwd: fixtureDirectory, stdout: 'pipe', stderr: 'pipe' });
    const timer = setTimeout(() => child.kill(), 60000);
    const [status, stdout, stderr] = await Promise.all([
        child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
    clearTimeout(timer);
    assert.equal(status === 0, expectSuccess, `unexpected migration exit ${status}\n${stdout}${stderr}`);
    return stdout + stderr;
}

async function assertPolicy(enabled: boolean, chunkHours: number) {
    const id = `runtime_device_data_storage_policy_v2_${enabled ? 'on' : 'off'}_chunk_${chunkHours}_after_168_mutable_48`;
    const records = await db`SELECT migration_id FROM sys_schema_migrations
        WHERE starts_with(migration_id,'runtime_device_data_storage_policy_')`;
    assert.deepEqual(records.map(row => row.migration_id), [id], 'unexpected policy migration records');
    const jobs = await db`SELECT (config->>'compress_after')::interval = interval '168 hours' AS age
        FROM timescaledb_information.jobs WHERE hypertable_name='device_data' AND proc_name='policy_compression'`;
    assert.equal(jobs.length, enabled ? 1 : 0, 'unexpected compression policy jobs');
    if (enabled) assert.equal(jobs[0].age, true, 'unexpected compression age');
    const [dimension] = await db`SELECT time_interval = make_interval(hours => ${chunkHours}) AS matches
        FROM timescaledb_information.dimensions WHERE hypertable_name='device_data' AND column_name='report_time'`;
    assert.equal(dimension?.matches, true, 'unexpected chunk interval');
}

try {
    await migrate(true, 168);
    await assertPolicy(true, 168);
    let before = await policyState();
    await migrate(true, 168);
    assert.equal(await policyState(), before, 'repeated migration was not skipped');
    console.log('PASS fresh schema and repeated migration');

    const checksums = await scalar(db.unsafe(schemaChecksumQuery));
    const [cursor] = await db`SELECT checksum FROM sys_schema_migrations WHERE migration_id='0044_gb28181_projection_cursor'`;
    assert(cursor, 'migration 0044_gb28181_projection_cursor is missing');
    await db`DELETE FROM sys_schema_migrations WHERE migration_id='orm_schema_checksum_transition_20260914'`;
    await db`UPDATE sys_schema_migrations SET checksum=repeat('f',64) WHERE migration_id='0044_gb28181_projection_cursor'`;
    assert.match(await migrate(true, 168, false), /schema checksum differs from the audited migration/);
    assert.equal((await db`SELECT 1 FROM sys_schema_migrations WHERE migration_id='orm_schema_checksum_transition_20260914'`).length, 0,
        'failed checksum transition was recorded');
    await db`UPDATE sys_schema_migrations SET checksum=${cursor.checksum} WHERE migration_id='0044_gb28181_projection_cursor'`;
    assert.equal(await scalar(db.unsafe(schemaChecksumQuery)), checksums, 'failed checksum transition partially changed historical checksums');
    await migrate(true, 168);
    console.log('PASS unknown checksum drift is rejected atomically and retry succeeds');

    await db`UPDATE sys_schema_migrations SET migration_id=replace(migration_id,'_v2_','_v1_'), checksum=repeat('0',64)
        WHERE starts_with(migration_id,'runtime_device_data_storage_policy_')`;
    await migrate(true, 168);
    await assertPolicy(true, 168);
    console.log('PASS existing v1 record upgrades without checksum collision');

    await migrate(false, 24);
    await assertPolicy(false, 24);
    await migrate(true, 168);
    await assertPolicy(true, 168);
    console.log('PASS on/off/on restores compression and chunk settings');

    before = await policyState();
    const jobBefore = await scalar(db.unsafe(compressionJobQuery));
    await db.unsafe(`CREATE FUNCTION reject_storage_policy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF starts_with(NEW.migration_id,'runtime_device_data_storage_policy_v2_off_') THEN
    RAISE EXCEPTION 'storage policy test injected failure';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER reject_storage_policy BEFORE INSERT ON sys_schema_migrations
FOR EACH ROW EXECUTE FUNCTION reject_storage_policy();`);
    assert.match(await migrate(false, 24, false), /storage policy test injected failure/);
    await assertPolicy(true, 168);
    assert.equal(await policyState(), before, 'failure changed migration bookkeeping');
    assert.equal(await scalar(db.unsafe(compressionJobQuery)), jobBefore, 'failure replaced compression job');
    await db.unsafe('DROP TRIGGER reject_storage_policy ON sys_schema_migrations; DROP FUNCTION reject_storage_policy();');
    await migrate(false, 24);
    await assertPolicy(false, 24);
    console.log('PASS failure rolls back policy, chunk settings and bookkeeping; retry succeeds');
} finally {
    await db.close();
}
