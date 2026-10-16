import assert from 'node:assert/strict';
import { redisUrl } from './architecture-fixture';

const redis = new Bun.RedisClient(redisUrl);
const prefix = `iot:test:rpc-receipt:${crypto.randomUUID()}`;
const input = `${prefix}:input`;
const reply = `${prefix}:reply`;
const workerStream = `${prefix}:worker`;
const source = await Bun.file('service/features/messaging/rpc/rpc.service.h').text();
const start = source.indexOf('kReplyScript =');
assert(start >= 0);
const lua = source.slice(start).match(/R"lua\(([\s\S]*?)\)lua"/)?.[1];
assert(lua);
try {
    await redis.send('XGROUP', ['CREATE', input, 'group', '0', 'MKSTREAM']);
    const id = String(await redis.send('XADD', [input, '*', 'id', 'request']));
    await redis.send('XREADGROUP', ['GROUP', 'group', 'worker', 'STREAMS', input, '>']);
    const payload = '{"code":0,"data":{"value":"保持原始 JSON"}}';
    assert.equal(Number(await redis.send('EVAL', [lua, '3', reply, workerStream, input,
        payload, '120', 'group', id, '10000', '600000'])), 1);
    assert.equal(await redis.send('GET', [reply]), payload);
    const ttl = Number(await redis.send('TTL', [reply]));
    assert(ttl > 0 && ttl <= 120);
    assert.equal(Number(await redis.send('XLEN', [input])), 0);
    const pending = await redis.send('XPENDING', [input, 'group']) as unknown[];
    assert.equal(Number(pending[0]), 0);
    // The reply is announced only on the consuming worker's directed stream,
    // never on the broadcast change stream.
    const notifications = await redis.send('XRANGE', [workerStream, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(notifications.map(([, fields]) => fields), [['topic', reply]]);
    const streamTtl = Number(await redis.send('PTTL', [workerStream]));
    assert(streamTtl > 0 && streamTtl <= 600000, 'directed stream expires with its worker');
    console.log('PASS RPC reply persistence, expiry, directed notification and input acknowledgement');
} finally {
    await redis.send('DEL', [input, reply, workerStream]);
    redis.close();
}
