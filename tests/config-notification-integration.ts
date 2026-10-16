import assert from 'node:assert/strict';
import { redisUrl } from './architecture-fixture';

const redis = new Bun.RedisClient(redisUrl);
const active = 'iot:config:runtime:active-version';
const stream = 'iot:live:changes';
const copied: string[] = [];
const prefix = (version: string) => `iot:config:runtime:${version}`;
async function until(check: () => Promise<boolean>, message: string, limit = 10000) {
    const end = Date.now() + limit;
    while (Date.now() < end) {
        if (await check()) return;
        await Bun.sleep(100);
    }
    throw new Error(message);
}
async function clone(version: string) {
    const next = crypto.randomUUID();
    const keys = await redis.send('KEYS', [`${prefix(version)}:*`]) as string[];
    assert(keys.length > 0, 'runtime snapshot is missing');
    for (const key of keys) {
        const target = prefix(next) + key.slice(prefix(version).length);
        await redis.send('COPY', [key, target]);
        copied.push(target);
    }
    await redis.send('HSET', [`${prefix(next)}:metadata`, 'version', next]);
    return next;
}
async function activate(version: string, dropHint = false) {
    await redis.send('EVAL', [
        `redis.call('SET',KEYS[1],ARGV[1]);
         local id=redis.call('XADD',KEYS[2],'*','schema_version','2','topic','runtime-config','ids','');
         if ARGV[2]=='1' then redis.call('XDEL',KEYS[2],id) end; return id`,
        '2', active, stream, version, dropHint ? '1' : '0']);
}

const original = await redis.get(active);
assert(original, 'start both disposable API instances before this test');
const collectors = await redis.send('KEYS', ['iot:runtime:collector:*']) as string[];
assert(collectors.length >= 2, 'multiple collector workers are required');
const instances = new Set(collectors.map((key) => key.split(':').slice(0, -1).join(':')));
assert.equal(instances.size, 2, 'use a clean Redis fixture with exactly two live instances');
const applied = async (version: string) => (await Promise.all(collectors.map((key) =>
    redis.send('HGET', [key, 'version'])))).every((value) => value === version);
try {
    const notified = await clone(original);
    await activate(notified);
    await until(() => applied(notified), 'runtime change did not reach every instance/collector');
    console.log(`PASS config notification reached ${collectors.length} collectors across two instances`);

    const missed = await clone(original);
    // Delete the hint atomically before readers can consume it. Connections stay
    // up; this models stream retention losing an unread invalidation.
    await activate(missed, true);
    await Bun.sleep(1000);
    assert(await applied(notified), 'dropped-hint fixture unexpectedly delivered the hint');
    console.log('Checking the 60-second missed-hint recovery window...');
    await until(() => applied(missed), 'trimmed hint left stale config indefinitely', 70000);
    console.log('PASS connected collectors repair a lost hint within the recovery window');
} finally {
    await activate(original);
    await until(() => applied(original), 'fixture config restoration failed');
    if (copied.length) await redis.send('DEL', copied);
    redis.close();
}
