import { sharedChannel } from './redis_channel.fixture';
import assert from 'node:assert/strict';
import { apiBase as base, redisUrl } from './architecture-fixture';

// Run this script only while the disposable iot-engine application fixture is
// running. It inserts one unknown-kind entry, so the real Projector can test
// pending recovery, fencing, ACK, and cleanup without requiring a device row.
const redis = new Bun.RedisClient(redisUrl);
const suffix = crypto.randomUUID().replaceAll('-', '');
const instance = `edge-fixture-${suffix}`;
const worker = '0';
const group = 'iot-engine:edge-projector';
const registry = 'iot:v3:edge:projector:streams';
const recoveryDeadlines = 'iot:v3:edge:projector:deadlines';
const stream = `iot:v3:edge:projector:${instance}:${worker}`;
const lease = `iot:v3:edge:projector:lease:${instance}:${worker}`;
let scanBlocked = false;

async function until(check: () => Promise<boolean>, message: string, timeout = 45000) {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (await check()) return;
        await Bun.sleep(100);
    }
    throw new Error(message);
}

try {
    const candidates = await redis.send('KEYS', ['iot:v3:edge:projector:lease:*']) as string[];
    const liveLeases: Array<{ key: string; token: string }> = [];
    for (const key of candidates) {
        if (key.includes(instance)) continue;
        const token = await redis.send('GET', [key]);
        if (typeof token === 'string' && token.length > 0) {
            liveLeases.push({ key, token });
        }
    }
    assert(liveLeases.length > 0, 'no live application projector lease was found');

    const readyBefore = await fetch(`${base}/internal/health/ready`);
    const readyBeforeText = await readyBefore.text();
    assert.equal(readyBefore.status, 200, readyBeforeText);

    // Dead-stream recovery is deadline-driven through ZRANGEBYSCORE on the
    // recovery deadline ZSET, not periodic SCAN sweeps. Deny ZRANGEBYSCORE
    // across a lease renewal/discovery window, then restore it and prove every
    // original worker lease and readiness survived.
    await redis.send('ACL', ['LOG', 'RESET']);
    await redis.send('ACL', ['SETUSER', 'default', '-zrangebyscore']);
    scanBlocked = true;
    await redis.send('ZADD', [recoveryDeadlines, '0', stream]);
    for (const { key } of liveLeases) {
        const streamName = key.replace(':lease:', ':');
        await redis.send('PUBLISH', [sharedChannel(redisUrl, `consumer:input:${streamName}`), '0-0']);
    }
    await Bun.sleep(1000);
    const aclLog = await redis.send('ACL', ['LOG']);
    assert(
        JSON.stringify(aclLog).toLowerCase().includes('zrangebyscore'),
        'Edge Projector did not issue a denied ZRANGEBYSCORE during the fault window'
    );
    await redis.send('ACL', ['SETUSER', 'default', '+zrangebyscore']);
    scanBlocked = false;
    await redis.send('ZREM', [recoveryDeadlines, stream]);

    for (const { key, token } of liveLeases) {
        assert.equal(Number(await redis.send('EXISTS', [key])), 1, `projector lease disappeared: ${key}`);
        assert.equal(await redis.send('GET', [key]), token, `projector lease token changed: ${key}`);
    }
    await until(
        async () => {
            const response = await fetch(`${base}/internal/health/ready`);
            const status = response.status;
            await response.text();
            return status === 200;
        },
        'application readiness did not recover after the denied SCAN',
        10000
    );
    console.log('PASS Edge Projector survived a denied SCAN and recovered readiness');

    await redis.send('DEL', [stream, lease]);
    await redis.send('SET', [lease, `${instance}:${worker}`, 'PX', '30000']);
    await redis.send('XADD', [stream, '*', 'kind', 'application-recovery-probe']);
    await redis.send('XGROUP', ['CREATE', stream, group, '0']);
    const pending = await redis.send('XREADGROUP', [
        'GROUP', group, 'crashed-owner', 'COUNT', '1', 'STREAMS', stream, '>',
    ]) as Record<string, unknown>;
    const rows = pending[stream] as unknown[][];
    assert(Array.isArray(rows) && rows.length === 1, 'fixture entry was not left pending');
    await redis.send('SADD', [registry, stream]);

    // Only expose the dead owner after its pending entry is fully prepared.
    await redis.send('DEL', [lease]);
    await redis.send('ZADD', [recoveryDeadlines, '0', stream]);
    for (const { key } of liveLeases) {
        const streamName = key.replace(':lease:', ':');
        await redis.send('PUBLISH', [sharedChannel(redisUrl, `consumer:input:${streamName}`), '0-0']);
    }

    const startedAt = Date.now();
    await until(
        async () => Number(await redis.send('EXISTS', [stream])) === 0,
        'running Edge Projector did not recover and drain the fixture stream'
    );
    assert(
        Date.now() - startedAt < 60000,
        'fixture recovery exceeded the projector recovery deadline'
    );
    console.log('PASS running Edge Projector recovered and drained a crashed owner');

    const liveLease = liveLeases[0]?.key;
    assert(liveLease, 'no original live application projector lease was retained');
    await redis.send('DEL', [liveLease]);
    await until(async () => {
        const response = await fetch(`${base}/internal/health/ready`);
        if (response.status !== 503) return false;
        const status = await response.text();
        return status.includes('edge-projector') && status.includes('worker lease lost');
    }, 'projector lease loss was not reflected in aggregate readiness', 20000);
    console.log('PASS projector lease loss makes application readiness fail');
} finally {
    try {
        if (scanBlocked) {
            await redis.send('ACL', ['SETUSER', 'default', '+zrangebyscore']);
            scanBlocked = false;
        }
    } finally {
        await redis.send('DEL', [stream, lease]);
        await redis.send('SREM', [registry, stream]);
        await redis.send('ZREM', [recoveryDeadlines, stream]);
        redis.close();
    }
}
