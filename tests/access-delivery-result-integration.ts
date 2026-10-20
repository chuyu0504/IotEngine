import { sharedChannel } from './redis_channel.fixture';
import assert from 'node:assert/strict';

const redisUrl = Bun.env.ARCHITECTURE_REDIS_URL;
assert(redisUrl, 'ARCHITECTURE_REDIS_URL must point to a disposable Redis fixture');
const redis = new Bun.RedisClient(redisUrl);
const subscriber = new Bun.RedisClient(redisUrl);
const prefix = `iot:test:access-result:${crypto.randomUUID()}`;
const stream = `${prefix}:results`;
const progress = `${prefix}:progress`;
const wake = `${prefix}:wake`;
const inputChannel = sharedChannel(redisUrl, `consumer:input:${stream}`);
const wakeChannel = sharedChannel(redisUrl, `consumer:input:${wake}`);
const barrierChannel = `${prefix}:barrier`;
const notices: { payload: string; channel: string }[] = [];
let barrier: (() => void) | undefined;
const source = await Bun.file('service/features/access/access.entity.h').text();
const lua = source.match(/\bkDeliveryResultScript\s*=\s*R"lua\(([\s\S]*?)\)lua"/)?.[1];
assert(lua, 'missing production Lua constant: kDeliveryResultScript');
const publish = (target: string, completed: boolean) => redis.send('EVAL', [
    lua, '5', stream, progress, wake, inputChannel, wakeChannel,
    target, '100000', '60', '1000', 'webhook', completed ? '1' : '0',
    'target', target, 'status', completed ? 'success' : 'failed',
]);
async function drainNotices() {
    const ready = Promise.withResolvers<void>();
    barrier = ready.resolve;
    await redis.send('PUBLISH', [barrierChannel, '']);
    await ready.promise;
    barrier = undefined;
    return notices.splice(0);
}

try {
    for (const channel of [inputChannel, wakeChannel]) {
        await subscriber.subscribe(channel, (payload, channel) => notices.push({ payload, channel }));
    }
    await subscriber.subscribe(barrierChannel, () => barrier?.());
    const first = String(await publish('webhook-1', true));
    const [wakeEntry] = await redis.send('XRANGE', [wake, '-', '+']) as [string, string[]][];
    assert.deepEqual(await drainNotices(), [
        { payload: first, channel: inputChannel },
        { payload: wakeEntry[0], channel: wakeChannel },
    ]);
    assert.deepEqual(wakeEntry[1], ['task', 'webhook']);
    assert.equal(await redis.send('HGET', [progress, 'webhook-1']), '1');
    assert(Number(await redis.send('TTL', [progress])) > 0);
    assert.equal(await publish('webhook-1', true), null);
    assert.deepEqual(await drainNotices(), [], 'completed targets must not append or publish twice');
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    assert.equal(Number(await redis.send('XLEN', [wake])), 1);

    // Failed MQTT deliveries remain retryable; every actual retry result and
    // corresponding durable task entry must wake its consumer.
    const failed = String(await publish('mqtt-1', false));
    const retry = String(await publish('mqtt-1', true));
    const wakeEntries = await redis.send('XRANGE', [wake, '-', '+']) as [string, string[]][];
    assert.deepEqual(await drainNotices(), [
        { payload: failed, channel: inputChannel },
        { payload: wakeEntries[1][0], channel: wakeChannel },
        { payload: retry, channel: inputChannel },
        { payload: wakeEntries[2][0], channel: wakeChannel },
    ]);
    assert.equal(await redis.send('HGET', [progress, 'mqtt-1']), '1');
    assert.equal(await publish('mqtt-1', true), null);
    assert.deepEqual(await drainNotices(), []);
    console.log('PASS durable access delivery result input notifications and completion deduplication');
} finally {
    await redis.send('DEL', [stream, progress, wake]);
    await subscriber.unsubscribe().catch(() => {});
    subscriber.close();
    redis.close();
}
