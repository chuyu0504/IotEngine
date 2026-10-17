import assert from 'node:assert/strict';

const redisUrl = Bun.env.ARCHITECTURE_REDIS_URL;
assert(redisUrl, 'ARCHITECTURE_REDIS_URL must point to a disposable Redis fixture');
const redis = new Bun.RedisClient(redisUrl);
const prefix = `iot:test:gb-control:${crypto.randomUUID()}`;
const keys = [`${prefix}:owner`, `${prefix}:claim`, `${prefix}:cancel`];
const source = await Bun.file('service/features/gb28181/gb28181.service.h').text();
function script(name: string) {
    const lua = source.match(new RegExp(`\\b${name}\\s*=\\s*R"lua\\(([\\s\\S]*?)\\)lua"`))?.[1];
    assert(lua, `missing production Lua constant: ${name}`);
    return lua;
}
const lua = script('kClaimControlExecutionScript');
const claim = async (token: string, deadline: string) =>
    Number(await redis.send('EVAL', [lua, '3', ...keys, token, deadline]));
const publishLua = script('kPublishControlCommandScript');
const stream = `${prefix}:commands`;
const inputChannel = `iot:consumer:input:${stream}`;
const inputSubscriber = new Bun.RedisClient(redisUrl);
const inputNotices: string[] = [];
const inputBarrierChannel = `${prefix}:input-barrier`;
let inputBarrier: (() => void) | undefined;
await inputSubscriber.subscribe(inputChannel, (payload) => inputNotices.push(payload));
await inputSubscriber.subscribe(inputBarrierChannel, () => inputBarrier?.());
const publish = async (token: string) => Number(await redis.send('EVAL', [
    publishLua, '3', stream, keys[0], inputChannel, token,
    'request_id', 'command-1', 'operation', 'test-operation', 'payload', '{"value":1}',
]));
const replyLua = script('kPublishReplyAndAcknowledgeScript');
const reply = `${prefix}:reply`;
const result = `${prefix}:result`;
const deadlineKey = `iot:consumer:pending:group:${stream}`;
const deadlineChannel = `iot:consumer:deadline:group:${stream}`;
const deadlineSubscriber = new Bun.RedisClient(redisUrl);
const deadlineNotices: string[] = [];
let subscriber = new Bun.RedisClient(redisUrl);
const unrelated = new Bun.RedisClient(redisUrl);
const unrelatedNotices: string[] = [];
let receive: ((notice: { payload: string; channel: string }) => void) | undefined;
const subscribe = async () => {
    await subscriber.subscribe(reply, (payload, channel) => receive?.({ payload, channel }));
};
const publishReply = async (entryId: string, payload = '{"done":true}', claimOwner = 'owner') => Number(await redis.send('EVAL', [
    replyLua, '6', reply, stream, result, keys[1], deadlineKey, deadlineChannel,
    'group', entryId, `ok\n${payload}`, '600', '600', claimOwner,
]));
async function notified(action: () => Promise<unknown>) {
    const notice = Promise.withResolvers<{ payload: string; channel: string }>();
    receive = notice.resolve;
    const timer = setTimeout(() => notice.reject(new Error('GB28181 reply Pub/Sub notification timed out')), 3000);
    try {
        await action();
        return await notice.promise;
    } finally {
        clearTimeout(timer);
        receive = undefined;
    }
}

try {
    const time = await redis.send('TIME', []) as string[];
    const now = Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000);
    const future = String(now + 60000);
    assert.equal(await claim('owner', future), -1);
    await redis.send('SET', [keys[0], 'owner']);
    await redis.send('SET', [keys[2], '1']);
    assert.equal(await claim('owner', future), -2);
    assert.equal(Number(await redis.send('EXISTS', [keys[1]])), 0);
    await redis.send('DEL', [keys[2]]);
    assert.equal(await claim('owner', String(now - 1)), -3);
    assert.equal(Number(await redis.send('EXISTS', [keys[1]])), 0);
    assert.equal(await claim('owner', future), 1);
    assert.equal(await redis.send('GET', [keys[1]]), 'owner');
    const ttl = Number(await redis.send('TTL', [keys[1]]));
    assert(ttl > 60 && ttl <= 600);
    assert.equal(await claim('owner', future), 0);
    await redis.send('SET', [keys[0], 'replacement']);
    assert.equal(await claim('owner', future), -1);
    assert.equal(await claim('replacement', future), 0);
    assert.equal(await redis.send('GET', [keys[1]]), 'owner');
    assert.equal(await publish('owner'), 0);
    assert.equal(Number(await redis.send('EXISTS', [stream])), 0);
    const firstInput = Promise.withResolvers<void>();
    inputBarrier = firstInput.resolve;
    await redis.send('PUBLISH', [inputBarrierChannel, '']);
    await firstInput.promise;
    assert.deepEqual(inputNotices, [], 'rejected owner must not emit an input notification');
    assert.equal(await publish('replacement'), 1);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    const entries = await redis.send('XRANGE', [stream, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(entries[0][1], [
        'request_id', 'command-1', 'operation', 'test-operation', 'payload', '{"value":1}',
    ]);
    const publishedInput = Promise.withResolvers<void>();
    inputBarrier = publishedInput.resolve;
    await redis.send('PUBLISH', [inputBarrierChannel, '']);
    await publishedInput.promise;
    assert.deepEqual(inputNotices, [entries[0][0]], 'new control entries must publish their durable ID');
    const streamTtl = Number(await redis.send('TTL', [stream]));
    assert(streamTtl > 0 && streamTtl <= 600);
    await redis.send('DEL', [keys[0]]);
    assert.equal(await publish('replacement'), 0);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    const rejectedInput = Promise.withResolvers<void>();
    inputBarrier = rejectedInput.resolve;
    await redis.send('PUBLISH', [inputBarrierChannel, '']);
    await rejectedInput.promise;
    assert.deepEqual(inputNotices, [entries[0][0]], 'expired owner emitted a duplicate wake');
    console.log('PASS GB28181 control claim owner fencing, cancellation, deadline and duplicate exclusion');
    console.log('PASS GB28181 control publication owner fencing, payload and stream expiry');

    // A subscriber is optional for durable completion; no route/worker stream
    // is needed, and notification loss must not erase the persisted result.
    await redis.send('XGROUP', ['CREATE', stream, 'group', '0']);
    const claimNext = async () => {
        const [[entryId]] = await redis.send('XRANGE', [stream, '-', '+']) as unknown as [string, string[]][];
        await redis.send('XREADGROUP', ['GROUP', 'group', 'collector', 'COUNT', '1', 'STREAMS', stream, '>']);
        await redis.send('ZADD', [deadlineKey, String(Date.now() + 60000), entryId]);
        return entryId;
    };
    const pendingCount = async () => Number((await redis.send('XPENDING', [stream, 'group']) as unknown[])[0]);
    const entryId = await claimNext();
    assert.equal(await pendingCount(), 1);
    await deadlineSubscriber.subscribe(deadlineChannel, payload => deadlineNotices.push(payload));
    assert.equal(await publishReply(entryId), 1);
    assert.equal(await pendingCount(), 0);
    await deadlineSubscriber.ping();
    assert.deepEqual(deadlineNotices, [''], 'ACK notifies the consumer after removing its pending deadline');
    assert.equal(Number(await redis.send('ZCARD', [deadlineKey])), 0);
    assert.equal(Number(await redis.send('XLEN', [stream])), 0, 'reply acknowledges and deletes the control entry');
    assert.equal(await redis.send('GET', [result]), 'ok\n{"done":true}');
    assert.equal(await redis.send('GET', [keys[1]]), null, 'completion releases only its execution claim');
    assert.equal(Number(await redis.send('EXISTS', [reply])), 0, 'the reply topic has no duplicate result stream');
    const resultTtl = Number(await redis.send('TTL', [result]));
    assert(resultTtl > 0 && resultTtl <= 600);

    await subscribe();
    await unrelated.subscribe(`${reply}:other`, payload => unrelatedNotices.push(payload));
    await redis.send('SET', [keys[1], 'replacement']);
    await redis.send('XADD', [stream, '*', 'request_id', 'command-1']);
    const duplicateId = await claimNext();
    const notice = await notified(() => publishReply(duplicateId, '{"duplicate":true}'));
    assert.deepEqual(notice, { payload: '', channel: reply }, 'empty notifications publish directly on the reply topic');
    assert.equal(await pendingCount(), 0, 'notification observes an already-acknowledged command');
    assert.equal(await redis.send('GET', [result]), 'ok\n{"done":true}', 'duplicate completion keeps the original durable result');
    assert.equal(await redis.send('GET', [keys[1]]), 'replacement', 'a stale completion cannot release another owner claim');
    assert.equal(Number(await redis.send('XLEN', [stream])), 0);
    assert.deepEqual(unrelatedNotices, []);

    // Lose the subscription while completion commits, then recreate and confirm
    // before reading the authoritative persisted result.
    await subscriber.unsubscribe(reply);
    subscriber.close();
    await redis.send('DEL', [result]);
    await redis.send('XADD', [stream, '*', 'request_id', 'command-1']);
    assert.equal(await publishReply(await claimNext(), '{"recovered":true}', 'replacement'), 1);
    assert.equal(Number(await redis.send('EXISTS', [reply])), 0);
    subscriber = new Bun.RedisClient(redisUrl);
    await subscribe();
    assert.equal(await redis.send('GET', [result]), 'ok\n{"recovered":true}');
    assert.equal(await pendingCount(), 0);
    assert.equal(await redis.send('GET', [keys[1]]), null);
    // Repeating an ACK still publishes a hint without changing durable state.
    assert.deepEqual(await notified(() => publishReply(entryId, '{"late":true}')), { payload: '', channel: reply });
    assert.equal(await redis.send('GET', [result]), 'ok\n{"recovered":true}');
    await deadlineSubscriber.ping();
    assert.deepEqual(deadlineNotices, ['', '', ''], 'an already-removed deadline does not publish another deadline notification');
    assert.equal(Number(await redis.send('ZCARD', [deadlineKey])), 0);
    console.log('PASS GB28181 directed Pub/Sub receipts, durable recovery, atomic ACK and idempotent result/claim fencing');
} finally {
    subscriber.close();
    inputSubscriber.close();
    unrelated.close();
    deadlineSubscriber.close();
    await redis.send('DEL', [...keys, stream, reply, result, deadlineKey]);
    redis.close();
}
