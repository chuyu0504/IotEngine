// One disposable local process must already be running with media and VPN enabled.
import assert from 'node:assert/strict';
import type {Subprocess} from 'bun';
import {copyFile, link, mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import {createServer} from 'node:net';
import {basename, join} from 'node:path';
import {apiBase as base, databaseUrl, fixtureDirectory, redisUrl} from './architecture-fixture';

const health = await fetch(`${base}/internal/health/ready`, { signal: AbortSignal.timeout(5000) });
assert.equal(health.status, 200, 'Single process is not ready');
const state = await health.json();
for (const component of ['collector', 'telemetry', 'edge-projector', 'outbox', 'gb28181-projector', 'vpn']) {
    assert.equal(state.components?.[component]?.state, 'ready', `${component} must run in the same process`);
}
for (const path of ['/v1/device', '/v1/gb28181/health', '/v1/vpn/networks']) {
    const response = await fetch(base + path, { signal: AbortSignal.timeout(5000) });
    assert.equal(response.status, 401, `Expected authenticated route on the shared listener: ${path}`);
    await response.body?.cancel();
}
console.log('PASS single-process readiness and shared API/media/VPN listener');

// A real advisory-lock barrier must hold every Worker before any new reader
// can observe the configuration projection. No Worker index is privileged.
const engine = Bun.env.IOT_TEST_ENGINE;
assert(engine, 'IOT_TEST_ENGINE is required');
const directory = await mkdtemp(join(fixtureDirectory, 'startup-claim-'));
const coordinator = new Bun.SQL(databaseUrl, {max: 1});
const redis = new Bun.RedisClient(redisUrl);
const lock = 5282804697543808076n;
let child: Subprocess<'ignore', 'pipe', 'pipe'> | undefined;
let output: Promise<string> | undefined;
let locked = false;
try {
    const socket = createServer();
    await new Promise<void>((resolve, reject) => {
        socket.once('error', reject);
        socket.listen(0, '127.0.0.1', resolve);
    });
    const address = socket.address();
    assert(address && typeof address === 'object');
    const port = address.port;
    await new Promise<void>((resolve, reject) => socket.close(error => error ? reject(error) : resolve()));
    const executable = join(directory, basename(engine));
    await link(engine, executable).catch(() => copyFile(engine, executable));
    const original = await readFile(join(fixtureDirectory, '.env'), 'utf8');
    const overrides: Record<string, string> = {
        HOST: '127.0.0.1', PORT: String(port),
        SERVICE_WORKERS: '2', COLLECTOR_WORKERS: '2',
        GB28181_ENABLED: 'false', VPN_HUB_ENABLED: 'false',
        EDGE_FIRMWARE_DIR: join(directory, 'firmware'),
        PACKET_LOG_DIRECTORY: join(directory, 'logs'),
    };
    const settings = original.split('\n').filter(line => !Object.hasOwn(overrides, line.split('=', 1)[0] ?? ''));
    for (const [name, value] of Object.entries(overrides)) settings.push(`${name}=${value}`);
    await writeFile(join(directory, '.env'), settings.join('\n'));
    const prefix = 'iot:v3:startup:configuration:';
    const before = new Set(await redis.send('KEYS', [prefix + '*']) as string[]);
    await coordinator`SELECT pg_advisory_lock(${String(lock)}::bigint)`;
    locked = true;
    child = Bun.spawn([executable], {cwd: directory, stdout: 'pipe', stderr: 'pipe'});
    output = Promise.all([
        new Response(child.stdout).text(), new Response(child.stderr).text(),
    ]).then(parts => parts.join('\n'));
    const deadline = Date.now() + 20000;
    for (;;) {
        if (child.exitCode !== null) throw new Error(`startup process exited: ${await output}`);
        const [waiting] = await coordinator`
            SELECT count(*)::int AS count FROM pg_locks
            WHERE locktype='advisory' AND NOT granted
              AND classid=${String(lock >> 32n)}::oid
              AND objid=${String(lock & 0xffffffffn)}::oid`;
        if (waiting?.count === 2) break;
        assert(Date.now() < deadline, 'both Workers did not wait on the startup claim');
        await Bun.sleep(25);
    }
    const premature = await fetch(`http://127.0.0.1:${port}/internal/health/ready`,
        {signal: AbortSignal.timeout(200)}).catch(() => undefined);
    assert.notEqual(premature?.status, 200, 'readers became ready before configuration projection');
    await premature?.body?.cancel();
    await coordinator`SELECT pg_advisory_unlock(${String(lock)}::bigint)`;
    locked = false;
    for (;;) {
        if (child.exitCode !== null) throw new Error(`startup process exited: ${await output}`);
        const ready = await fetch(`http://127.0.0.1:${port}/internal/health/ready`,
            {signal: AbortSignal.timeout(500)}).catch(() => undefined);
        const status = ready?.status;
        await ready?.body?.cancel();
        if (status === 200) break;
        assert(Date.now() < deadline + 20000, 'Workers did not become ready after claim completion');
        await Bun.sleep(25);
    }
    const after = await redis.send('KEYS', [prefix + '*']) as string[];
    const added = after.filter(key => !before.has(key));
    assert.equal(added.length, 1, 'Workers must share one completed startup projection');
    const instance = added[0]!.slice(prefix.length);
    for (const key of [
        added[0]!, `iot:v3:startup:latest:${instance}`,
        `iot:v3:edge:projector:startup:${instance}`,
    ]) {
        assert.equal(await redis.get(key), 'ready', 'readiness preceded a required startup projection');
        assert(Number(await redis.send('TTL', [key])) > 0, 'startup metadata must expire');
    }
    console.log('PASS two Workers wait for claimed projections before readiness');
} finally {
    if (locked) await coordinator`SELECT pg_advisory_unlock(${String(lock)}::bigint)`;
    if (child && child.exitCode === null) {
        child.kill('SIGTERM');
        const timeout = setTimeout(() => child?.kill('SIGKILL'), 10000);
        await child.exited;
        clearTimeout(timeout);
    }
    await output;
    redis.close();
    await coordinator.close();
    await rm(directory, {recursive: true, force: true});
}
