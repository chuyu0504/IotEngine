import assert from 'node:assert/strict';

const redisUrl = Bun.env.ARCHITECTURE_REDIS_URL;
assert(redisUrl, 'ARCHITECTURE_REDIS_URL must point to a disposable Redis fixture');
const redis = new Bun.RedisClient(redisUrl);
const prefix = `iot:test:gb-projection:${crypto.randomUUID()}`;
const stream = `${prefix}:stream`;
const marker = `${prefix}:sent`;
const wake = `${prefix}:wake`;
const receipt = `${prefix}:receipt`;
const receipts = `${prefix}:receipts`;
const group = 'gb-projection-test';
const deadlines = `iot:consumer:pending:${group}:${stream}`;
const deadlineChannel = `iot:consumer:deadline:${group}:${stream}`;
const subscriber = new Bun.RedisClient(redisUrl);
const notices: { payload: string; channel: string }[] = [];
const barrierChannel = `${prefix}:barrier`;
let barrier: (() => void) | undefined;
for (const channel of [receipts, deadlineChannel]) {
    await subscriber.subscribe(channel, (payload, channel) => notices.push({ payload, channel }));
}
await subscriber.subscribe(barrierChannel, () => barrier?.());
async function drainNotices() {
    const ready = Promise.withResolvers<void>();
    barrier = ready.resolve;
    await redis.send('PUBLISH', [barrierChannel, '']);
    await ready.promise;
    barrier = undefined;
    return notices.splice(0);
}
const source = await Bun.file('service/features/gb28181/gb28181.service.h').text();
function script(name: string) {
    const lua = source.match(new RegExp(`\\b${name}\\s*=\\s*R"lua\\(([\\s\\S]*?)\\)lua"`))?.[1];
    assert(lua, `missing production Lua constant: ${name}`);
    return lua;
}
const publishLua = script('kPublishProjectionScript');
const doneLua = script('kMarkProjectionDoneScript');
const publish = (order: string, current: string) => redis.send('EVAL', [
    publishLua, '3', stream, marker, wake, order, current, 'gb28181-projector', '1000', 'projection_id', prefix,
]) as Promise<string[]>;
const wakes = async () => Number(await redis.send('XLEN', [wake]));
const markDone = async (entry: string, applied: string) => Number(await redis.send('EVAL', [
    doneLua, '5', receipt, stream, receipts, deadlines, deadlineChannel, applied, '60', group, entry, prefix,
]));

try {
    const first = await publish('', '');
    assert.equal(first.length, 2);
    assert.equal(first[0], first[1]);
    assert.equal(await wakes(), 1, 'a new projection entry must wake the Service Workers');
    const [wakeEntry] = await redis.send('XRANGE', [wake, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(wakeEntry[1], ['task', 'gb28181-projector']);
    assert.deepEqual(await publish('', ''), first);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    await redis.send('DEL', [marker]);
    assert.deepEqual(await publish(first[0], first[1]), first);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    assert.equal(await wakes(), 1, 'deduplicated publication must not wake again');
    await redis.send('XDEL', [stream, first[1]]);
    const repaired = await publish(first[0], first[1]);
    assert.equal(repaired[0], first[0]);
    assert.notEqual(repaired[1], first[1]);
    assert.equal(await wakes(), 2, 'a repair entry must wake the Service Workers');
    assert.deepEqual(await publish(first[0], first[1]), repaired);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    const entries = await redis.send('XRANGE', [stream, repaired[1], repaired[1]]);
    assert(JSON.stringify(entries).includes('projection_order'));
    await assert.rejects(publish('invalid', repaired[1]));
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    console.log('PASS GB28181 projection deduplication, marker loss, ordered repair and wake');

    // Completion commits durable state, clears PEL/deadline and publishes the
    // projection ID directly, without a receipt stream or route registration.
    await redis.send('XGROUP', ['CREATE', stream, group, '0']);
    await redis.send('XREADGROUP', ['GROUP', group, 'consumer', 'COUNT', '10', 'STREAMS', stream, '>']);
    await redis.send('ZADD', [deadlines, String(Date.now() + 60000), repaired[1]]);
    assert.equal(await markDone(repaired[1], '1'), 1);
    assert.equal(await redis.send('GET', [receipt]), '1');
    assert.equal(Number(await redis.send('XLEN', [stream])), 0);
    assert.equal(Number(await redis.send('EXISTS', [receipts])), 0, 'receipt notification created a stream');
    assert.equal(await redis.send('ZSCORE', [deadlines, repaired[1]]), null);
    assert.deepEqual(await drainNotices(), [
        { payload: repaired[1], channel: deadlineChannel },
        { payload: prefix, channel: receipts },
    ]);
    assert.equal(await markDone(repaired[1], '1'), 0, 'duplicate completion acknowledged twice');
    assert.deepEqual(await drainNotices(), [{ payload: prefix, channel: receipts }]);

    // A disconnected subscriber does not lose the authoritative result.
    await subscriber.unsubscribe(receipts);
    const next = await publish('', '');
    await redis.send('DEL', [marker]);
    await redis.send('XREADGROUP', ['GROUP', group, 'consumer', 'COUNT', '10', 'STREAMS', stream, '>']);
    await redis.send('ZADD', [deadlines, String(Date.now() + 60000), next[1]]);
    assert.equal(await markDone(next[1], '0'), 1);
    assert.equal(await redis.send('GET', [receipt]), '0');
    assert.deepEqual(await drainNotices(), [{ payload: next[1], channel: deadlineChannel }]);
    assert.equal(Number(await redis.send('ZCARD', [deadlines])), 0);
    assert.equal(Number(await redis.send('XLEN', [stream])), 0);
    assert.equal(Number(await redis.send('EXISTS', [receipts])), 0);
    console.log('PASS GB28181 durable receipt, atomic acknowledgement and direct Pub/Sub delivery');
} finally {
    await redis.send('DEL', [stream, marker, wake, receipt, receipts, deadlines]);
    subscriber.close();
    redis.close();
}
