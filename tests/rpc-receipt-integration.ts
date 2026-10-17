import assert from 'node:assert/strict';
import { redisUrl } from './architecture-fixture';

const redis = new Bun.RedisClient(redisUrl);
const subscriber = new Bun.RedisClient(redisUrl);
const lateSubscriber = new Bun.RedisClient(redisUrl);
const prefix = `iot:test:rpc-receipt:${crypto.randomUUID()}`;
const input = `${prefix}:input`;
const reply = `${prefix}:reply`;
const missedReply = `${prefix}:missed-reply`;
const deadlines = `iot:consumer:pending:group:${input}`;
const deadlineChannel = `iot:consumer:deadline:group:${input}`;
const source = await Bun.file('service/features/messaging/rpc/rpc.service.h').text();
const lua = source.match(/\bkReplyScript\s*=\s*R"lua\(([\s\S]*?)\)lua"/)?.[1];
assert(lua, 'missing production kReplyScript');

async function waitFor<T>(promise: Promise<T>, message: string): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    try {
        return await Promise.race([
            promise,
            new Promise<never>((_, reject) => {
                timer = setTimeout(() => reject(new Error(message)), 5000);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

async function pendingRequest() {
    const id = String(await redis.send('XADD', [input, '*', 'id', 'request']));
    await redis.send('XREADGROUP', ['GROUP', 'group', 'worker', 'STREAMS', input, '>']);
    const pending = await redis.send('XPENDING', [input, 'group']) as unknown[];
    assert.equal(Number(pending[0]), 1, 'request must be pending before reply');
    await redis.send('ZADD', [deadlines, String(Date.now() + 60000), id]);
    return id;
}

async function durableReply(key: string, payload: string, requestId: string) {
    assert.equal(await redis.send('GET', [key]), payload);
    const ttl = Number(await redis.send('TTL', [key]));
    assert(ttl > 0 && ttl <= 120);
    assert.equal(Number(await redis.send('XLEN', [input])), 0);
    const pending = await redis.send('XPENDING', [input, 'group']) as unknown[];
    assert.equal(Number(pending[0]), 0, 'persisted reply must acknowledge its request');
    assert.equal(await redis.send('ZSCORE', [deadlines, requestId]), null,
        'reply acknowledgement must clear its pending visibility deadline');
}

try {
    await redis.send('XGROUP', ['CREATE', input, 'group', '0', 'MKSTREAM']);
    const id = await pendingRequest();
    const payload = '{"code":0,"data":{"value":"保持原始 JSON"}}';
    let notify!: (message: string) => void;
    const notified = new Promise<string>(resolve => { notify = resolve; });
    assert.equal(await subscriber.subscribe(reply, (message, channel) => {
        assert.equal(channel, reply);
        notify(message);
    }), 1, 'subscription must be confirmed before replying');
    assert.equal(Number(await redis.send('EVAL', [lua, '4', reply, input, deadlines, deadlineChannel,
        payload, '120', 'group', id])), 1);
    assert.equal(await waitFor(notified, 'RPC reply notification timed out'), '',
        'Pub/Sub carries only a wakeup; payload stays in the persistent reply key');
    await durableReply(reply, payload, id);

    // No receiver is listening when this result is published. A later waiter
    // must still recover the result without replaying the request or its effect.
    const missedId = await pendingRequest();
    const listeners = await redis.send('PUBSUB', ['NUMSUB', missedReply]) as unknown[];
    assert.equal(Number(listeners[1]), 0);
    assert.equal(Number(await redis.send('EVAL', [lua, '4', missedReply, input, deadlines, deadlineChannel,
        payload, '120', 'group', missedId])), 1);
    const messages: string[] = [];
    const marker = `barrier:${crypto.randomUUID()}`;
    let mark!: () => void;
    const barrier = new Promise<void>(resolve => { mark = resolve; });
    assert.equal(await lateSubscriber.subscribe(missedReply, message => {
        if (message === marker) mark();
        else messages.push(message);
    }), 1, 'late subscriber must be confirmed before reading the persisted fallback');
    await durableReply(missedReply, payload, missedId);
    await redis.send('PUBLISH', [missedReply, marker]);
    await waitFor(barrier, 'late subscriber barrier timed out');
    assert.deepEqual(messages, [], 'lost wakeups are not replayed to late subscribers');
    console.log('PASS RPC durable reply, expiry, Pub/Sub wakeup, acknowledgement and lost-notification fallback');
} finally {
    await redis.send('DEL', [input, reply, missedReply, deadlines]);
    subscriber.close();
    lateSubscriber.close();
    redis.close();
}
