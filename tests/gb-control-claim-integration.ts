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
const publish = async (token: string) => Number(await redis.send('EVAL', [
    publishLua, '2', stream, keys[0], token,
    'request_id', 'command-1', 'operation', 'test-operation', 'payload', '{"value":1}',
]));
const replyLua = script('kPublishReplyAndAcknowledgeScript');
const reply = `${prefix}:reply`;
const result = `${prefix}:result`;
const route = `iot:live:route:${reply}`;
const waiter = `${prefix}:waiter`;
const publishReply = async (entryId: string) => Number(await redis.send('EVAL', [
    replyLua, '5', reply, stream, result, keys[1], route,
    'command-1', 'test-operation', 'ok', '{"done":true}', 'group', entryId, 'ok\n{"done":true}', '600', '600', '600', '', '10000', '600000',
]));

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
    assert.equal(await publish('replacement'), 1);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    const entries = await redis.send('XRANGE', [stream, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(entries[0][1], [
        'request_id', 'command-1', 'operation', 'test-operation', 'payload', '{"value":1}',
    ]);
    const streamTtl = Number(await redis.send('TTL', [stream]));
    assert(streamTtl > 0 && streamTtl <= 600);
    await redis.send('DEL', [keys[0]]);
    assert.equal(await publish('replacement'), 0);
    assert.equal(Number(await redis.send('XLEN', [stream])), 1);
    console.log('PASS GB28181 control claim owner fencing, cancellation, deadline and duplicate exclusion');
    console.log('PASS GB28181 control publication owner fencing, payload and stream expiry');

    // The reply wakes only the Service Worker that routed the reply stream.
    await redis.send('XGROUP', ['CREATE', stream, 'group', '0']);
    const claimNext = async () => {
        const [[entryId]] = await redis.send('XRANGE', [stream, '-', '+']) as unknown as [string, string[]][];
        await redis.send('XREADGROUP', ['GROUP', 'group', 'collector', 'COUNT', '1', 'STREAMS', stream, '>']);
        return entryId;
    };
    const entryId = await claimNext();
    assert.equal(await publishReply(entryId), 1);
    assert.equal(Number(await redis.send('XLEN', [stream])), 0, 'reply acknowledges and deletes the control entry');
    assert.equal(Number(await redis.send('EXISTS', [waiter])), 0, 'an unrouted reply notifies no worker');
    const replies = await redis.send('XRANGE', [reply, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(replies.map(([, fields]) => fields), [['request_id', 'command-1', 'operation', 'test-operation', 'status', 'ok', 'payload', '{"done":true}']]);
    await redis.send('SET', [route, waiter, 'EX', '20']);
    await redis.send('XADD', [stream, '*', 'request_id', 'command-2']);
    assert.equal(await publishReply(await claimNext()), 1);
    const notifications = await redis.send('XRANGE', [waiter, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(notifications.map(([, fields]) => fields), [['topic', reply]]);
    const waiterTtl = Number(await redis.send('PTTL', [waiter]));
    assert(waiterTtl > 0 && waiterTtl <= 600000, 'directed stream expires with its worker');
    console.log('PASS GB28181 reply publication notifies only the routed Service Worker stream');
} finally {
    await redis.send('DEL', [...keys, stream, reply, result, route, waiter]);
    redis.close();
}
