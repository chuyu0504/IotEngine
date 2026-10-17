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
const inputChannel = `iot:consumer:input:${stream}`;
const wakeChannel = `iot:consumer:input:${wake}`;
const receiptChannel = `iot:consumer:input:${receipts}`;
const deadlines = `iot:consumer:pending:${group}:${stream}`;
const deadlineChannel = `iot:consumer:deadline:${group}:${stream}`;
const subscriber = new Bun.RedisClient(redisUrl);
const notices: { payload: string; channel: string }[] = [];
const barrierChannel = `${prefix}:barrier`;
let barrier: (() => void) | undefined;
for (const channel of [inputChannel, wakeChannel, receiptChannel, deadlineChannel]) {
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
    publishLua, '5', stream, marker, wake, inputChannel, wakeChannel,
    order, current, 'gb28181-projector', '1000', 'projection_id', prefix,
]) as Promise<string[]>;
const wakes = async () => Number(await redis.send('XLEN', [wake]));
const markDone = async (entry: string, applied: string) => Number(await redis.send('EVAL', [
    doneLua, '6', receipt, stream, receipts, receiptChannel, deadlines, deadlineChannel,
    applied, '60', group, entry, prefix,
]));

try {
    const first = await publish('', '');
    assert.equal(first.length, 2);
    assert.equal(first[0], first[1]);
    assert.equal(await wakes(), 1, 'a new projection entry must wake the Service Workers');
    const [wakeEntry] = await redis.send('XRANGE', [wake, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(wakeEntry[1], ['task', 'gb28181-projector']);
    assert.deepEqual(await drainNotices(), [
        { payload: first[1], channel: inputChannel },
        { payload: wakeEntry[0], channel: wakeChannel },
    ], 'each durable append must publish its actual entry ID');
    assert.deepEqual(await publish('', ''), first);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    await redis.send('DEL', [marker]);
    assert.deepEqual(await publish(first[0], first[1]), first);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    assert.equal(await wakes(), 1, 'deduplicated publication must not wake again');
    assert.deepEqual(await drainNotices(), [], 'deduplicated publication emitted an input notification');
    await redis.send('XDEL', [stream, first[1]]);
    const repaired = await publish(first[0], first[1]);
    assert.equal(repaired[0], first[0]);
    assert.notEqual(repaired[1], first[1]);
    assert.equal(await wakes(), 2, 'a repair entry must wake the Service Workers');
    const wakeEntries = await redis.send('XRANGE', [wake, '-', '+']) as [string, string[]][];
    assert.deepEqual(await drainNotices(), [
        { payload: repaired[1], channel: inputChannel },
        { payload: wakeEntries[1][0], channel: wakeChannel },
    ]);
    assert.deepEqual(await publish(first[0], first[1]), repaired);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    const entries = await redis.send('XRANGE', [stream, repaired[1], repaired[1]]);
    assert(JSON.stringify(entries).includes('projection_order'));
    await assert.rejects(publish('invalid', repaired[1]));
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    assert.deepEqual(await drainNotices(), [], 'duplicate or invalid projection emitted a notification');
    console.log('PASS GB28181 projection deduplication, marker loss, ordered repair and wake');

    // Completion acknowledges the entry and sends a directed receipt only while
    // the publishing Collector's receipt stream exists.
    await redis.send('XGROUP', ['CREATE', stream, group, '0']);
    await redis.send('XREADGROUP', ['GROUP', group, 'consumer', 'COUNT', '10', 'STREAMS', stream, '>']);
    await redis.send('ZADD', [deadlines, String(Date.now() + 60000), repaired[1]]);
    assert.equal(await markDone(repaired[1], '1'), 1);
    assert.equal(await redis.send('GET', [receipt]), '1');
    assert.equal(Number(await redis.send('XLEN', [stream])), 0);
    assert.equal(Number(await redis.send('EXISTS', [receipts])), 0, 'receipt created a stream for a stopped Collector');
    assert.equal(await redis.send('ZSCORE', [deadlines, repaired[1]]), null);
    assert.deepEqual(await drainNotices(), [{ payload: repaired[1], channel: deadlineChannel }],
        'completion removes its pending deadline without waking a stopped Collector');
    const next = await publish('', '');
    await drainNotices();
    await redis.send('DEL', [marker]);
    await redis.send('XADD', [receipts, '*', 'init', '1']);
    await redis.send('XREADGROUP', ['GROUP', group, 'consumer', 'COUNT', '10', 'STREAMS', stream, '>']);
    await redis.send('ZADD', [deadlines, String(Date.now() + 60000), next[1]]);
    assert.equal(await markDone(next[1], '0'), 1);
    const completionNotices = await drainNotices();
    assert.equal(completionNotices[0]?.channel, deadlineChannel);
    assert.equal(completionNotices[0]?.payload, next[1]);
    const receiptEntries = await redis.send('XRANGE', [receipts, '-', '+']) as [string, string[]][];
    assert.deepEqual(completionNotices.slice(1), [{ payload: receiptEntries[1][0], channel: receiptChannel }]);
    assert.equal(await markDone(next[1], '0'), 0, 'duplicate completion acknowledged twice');
    const delivered = await redis.send('XRANGE', [receipts, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(delivered[1][1], ['projection_id', prefix, 'applied', '0']);
    const duplicateNotices = await drainNotices();
    assert.equal(duplicateNotices.length, 1, 'duplicate ACK must not publish a removed deadline again');
    assert.equal(duplicateNotices[0].channel, receiptChannel);
    assert.equal(duplicateNotices[0].payload, delivered[2][0]);
    const ttl = Number(await redis.send('TTL', [receipts]));
    assert(ttl > 0 && ttl <= 60);
    console.log('PASS GB28181 projection receipt, acknowledgement and directed delivery');
} finally {
    await redis.send('DEL', [stream, marker, wake, receipt, receipts, deadlines]);
    subscriber.close();
    redis.close();
}
