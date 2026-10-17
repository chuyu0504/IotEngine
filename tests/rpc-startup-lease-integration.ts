import { sharedChannel } from './redis_channel.fixture';
import assert from 'node:assert/strict';
import { copyFile, link } from 'node:fs/promises';
import { basename, join } from 'node:path';
import { clearTimeout, setTimeout } from 'node:timers';
import { apiBase, databaseUrl, fixtureDirectory, redisUrl } from './architecture-fixture';

// A MANAGED fixture owns this process. Never kill the shared api fixture.
const engine = Bun.env.IOT_TEST_ENGINE;
assert(engine, 'IOT_TEST_ENGINE is required; run through CTest');
const executable = join(fixtureDirectory, basename(engine));
await link(engine, executable).catch(() => copyFile(engine, executable));
const database = new URL(databaseUrl);
const redisAddress = new URL(redisUrl);
const api = new URL(apiBase);
const settings = {
    JWT_SECRET: 'architecture-test-only-access-secret-000000000',
    JWT_REFRESH_SECRET: 'architecture-test-only-refresh-secret-00000000',
    DB_HOST: database.hostname,
    DB_PORT: database.port || '5432',
    DB_USERNAME: decodeURIComponent(database.username),
    DB_PASSWORD: decodeURIComponent(database.password),
    DB_DATABASE: database.pathname.slice(1),
    REDIS_HOST: redisAddress.hostname,
    REDIS_PORT: redisAddress.port || '6379',
    REDIS_PASSWORD: decodeURIComponent(redisAddress.password),
    REDIS_DATABASE: redisAddress.pathname.slice(1),
    HOST: api.hostname,
    PORT: api.port,
    SERVICE_WORKERS: '2',
    COLLECTOR_WORKERS: '1',
    GB28181_ENABLED: 'false',
    VPN_HUB_ENABLED: 'false',
    EDGE_PUBLIC_BASE_URL: apiBase,
    EDGE_PLATFORM_ID: '00000000-0000-7000-8000-000000000001',
    PACKET_LOG_DIRECTORY: join(fixtureDirectory, 'logs'),
};
await Bun.write(join(fixtureDirectory, '.env'),
    Object.entries(settings).map(([name, value]) => `${name}=${value}\n`).join(''));

// Observe the server's real group and expiry in one Redis command. No product
// source is read and no command can interleave between these observations.
const kSnapshotRpcLeasesScript = `
local keys = redis.call('KEYS', 'iot:rpc:requests:*:worker:*')
table.sort(keys)
local result = {}
for _, key in ipairs(keys) do
  local control = 0
  for _, group in ipairs(redis.call('XINFO', 'GROUPS', key)) do
    for index = 1, #group, 2 do
      if group[index] == 'name' and group[index + 1] == 'control' then control = 1 end
    end
  end
  result[#result + 1] = {key, redis.call('PTTL', key), control}
end
return result`;
const kEnqueueRpcProbeScript = `
local entry = redis.call('XADD', KEYS[1], '*', unpack(ARGV))
redis.call('PUBLISH', KEYS[2], entry)
return entry`;
// Accelerate only an already bounded residual lease. A missing lease is an
// error, not something the test may repair by assigning its own expiry.
const kShortenExistingLeasesScript = `
local ttls = {}
for index, key in ipairs(KEYS) do
  ttls[index] = redis.call('PTTL', key)
  if ttls[index] <= 0 then return redis.error_reply('residual RPC lease is missing') end
end
for index, key in ipairs(KEYS) do
  redis.call('PEXPIRE', key, math.min(ttls[index], tonumber(ARGV[1])))
end
return #KEYS`;

type LeaseSnapshot = [stream: string, pttl: number, control: number];
const redis = new Bun.RedisClient(redisUrl);
const snapshots = async () =>
    await redis.send('EVAL', [kSnapshotRpcLeasesScript, '0']) as LeaseSnapshot[];
function assertLeased(streams: LeaseSnapshot[]) {
    for (const [stream, pttl, control] of streams) {
        assert.equal(Number(control), 1, `${stream} has no own control group`);
        assert(Number(pttl) > 0 && Number(pttl) <= 86400000,
            `${stream} is visible without its bounded initial lease: PTTL=${pttl}`);
    }
}
async function until(check: () => Promise<boolean>, message: string, timeout = 10000) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(25);
    }
    throw new Error(message);
}
async function waitFor<T>(promise: Promise<T>, message: string): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    try {
        return await Promise.race([
            promise,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error(message)), 10000);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}
async function probeMissingStream(stream: string) {
    const request = crypto.randomUUID();
    const replyKey = `iot:rpc:reply:${request}`;
    const subscriber = new Bun.RedisClient(redisUrl);
    let notify!: () => void;
    const notified = new Promise<void>((resolve) => { notify = resolve; });
    try {
        assert.equal(await subscriber.subscribe(replyKey, () => notify()), 1);
        await redis.send('DEL', [stream]);
        await redis.send('EVAL', [kEnqueueRpcProbeScript,
            '2', stream, sharedChannel(redisUrl, `consumer:input:${stream}`),
            'version', '1', 'id', request, 'component', 'startup-lease-probe',
            'operation', 'unavailable', 'payload', '{}', 'deadline', String(Date.now() + 30000)]);
        await waitFor(notified, 'RPC consumer did not repair its missing request group');
        assert.equal(await redis.send('GET', [replyKey]),
            'ERR\n503\n10004\nBackground component is unavailable',
            'the recovered own consumer did not persist its real RPC result');
        const recovered = await snapshots();
        assertLeased(recovered);
        assert(recovered.some(([key]) => key === stream), 'repair changed the own request stream identity');
    } finally {
        subscriber.close();
        await redis.send('DEL', [replyKey, `iot:rpc:claim:${request}`, `iot:rpc:cancelled:${request}`]);
    }
}

const child = Bun.spawn([executable], { cwd: fixtureDirectory, stdout: 'pipe', stderr: 'pipe' });
const stdout = new Response(child.stdout).text();
const stderr = new Response(child.stderr).text();
let completed = false;
let firstStreamAt = 0;
try {
    let initial: LeaseSnapshot[] = [];
    await until(async () => {
        assert.equal(child.exitCode, null, 'new fixture exited before readiness');
        const observed = await snapshots();
        if (observed.length && !firstStreamAt) firstStreamAt = Date.now();
        assertLeased(observed);
        const response = await fetch(`${apiBase}/internal/health/ready`, {
            signal: AbortSignal.timeout(1000),
        }).catch(() => undefined);
        if (!response) return false;
        const ready = response.status === 200;
        await response.text();
        if (!ready) return false;
        // This is the first successful readiness observation, before any RPC.
        initial = await snapshots();
        assertLeased(initial);
        assert.equal(initial.length, 2, 'new fixture did not create both own RPC streams');
        return true;
    }, 'new fixture did not become ready with leased RPC streams', 120000);
    assert(Date.now() - firstStreamAt < 60000,
        'initial lease assertion ran after the first periodic renewal');
    console.log('PASS every new RPC stream has its initial TTL before business readiness');

    await probeMissingStream(initial[0][0]);
    assert.deepEqual((await snapshots()).map(([stream]) => stream), initial.map(([stream]) => stream),
        'missing-group repair replaced or added another Worker request stream');
    console.log('PASS missing RPC stream recovery restores its group and TTL before replying');

    assert(Date.now() - firstStreamAt < 60000, 'crash must occur before the first periodic renewal');
    assert.equal(child.exitCode, null, 'fixture exited before the abnormal shutdown');
    child.kill('SIGKILL');
    assert.notEqual(await child.exited, 0, 'fixture did not exit abnormally');
    const residual = await snapshots();
    assert.deepEqual(residual.map(([stream]) => stream), initial.map(([stream]) => stream),
        'abnormal exit did not leave the actual Worker request streams to expire');
    assertLeased(residual);
    assert.equal(Number(await redis.send('EVAL', [kShortenExistingLeasesScript,
        String(residual.length), ...residual.map(([stream]) => stream), '250'])), residual.length);
    await until(async () => (await snapshots()).length === 0,
        'abnormally stopped RPC request streams did not expire without a running owner', 3000);
    console.log('PASS abnormal first-minute exit leaves bounded leases that expire without cleanup');
    completed = true;
} finally {
    if (child.exitCode === null) child.kill('SIGKILL');
    await child.exited;
    const output = (await stdout) + (await stderr);
    if (!completed) console.error(output);
    redis.close();
}
