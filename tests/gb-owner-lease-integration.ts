import assert from 'node:assert/strict';

const redisUrl = Bun.env.ARCHITECTURE_REDIS_URL;
assert(redisUrl, 'ARCHITECTURE_REDIS_URL must point to a disposable Redis fixture');
const redis = new Bun.RedisClient(redisUrl);
const prefix = `iot:test:gb-owner:${crypto.randomUUID()}`;
const ownerPrefix = `${prefix}:owner:`;
const key = `${ownerPrefix}camera-1`;
const otherKey = `${ownerPrefix}camera-2`;
const deadlines = `${prefix}:deadlines`;
const wake = `${prefix}:wake`;
const wakeChannel = `iot:consumer:input:${wake}`;
const subscriber = new Bun.RedisClient(redisUrl);
const notices: string[] = [];
const barrierChannel = `${prefix}:barrier`;
let barrier: (() => void) | undefined;
await subscriber.subscribe(wakeChannel, (payload) => notices.push(payload));
await subscriber.subscribe(barrierChannel, () => barrier?.());
async function drainNotices() {
    const ready = Promise.withResolvers<void>();
    barrier = ready.resolve;
    await redis.send('PUBLISH', [barrierChannel, '']);
    await ready.promise;
    barrier = undefined;
    return notices.splice(0);
}
const member = 'device:camera-1';
const otherMember = 'device:camera-2';
const source = await Bun.file('service/features/gb28181/gb28181.service.h').text();
function script(name: string) {
    const lua = source.match(new RegExp(`\\b${name}\\s*=\\s*R"lua\\(([\\s\\S]*?)\\)lua"`))?.[1];
    assert(lua, `missing production Lua constant: ${name}`);
    return lua;
}
const claimLua = script('kClaimOwnerLeaseScript');
const renewLua = script('kRenewOwnerLeaseScript');
const releaseLua = script('kReleaseOwnerLeaseScript');
const claimDueLua = script('kClaimDueOwnersScript');
const completeLua = script('kCompleteOwnerExpiryScript');

const claim = async (token: string, previous: string, lease = '15000', owner = key, deadlineMember = member) =>
    Number(await redis.send('EVAL', [claimLua, '4', owner, deadlines, wake, wakeChannel, token, previous, lease, deadlineMember, 'gb28181-projector', '1000']));
const renew = async (token: string, lease = '15000', deadlineMember = member) =>
    Number(await redis.send('EVAL', [renewLua, '2', key, deadlines, token, lease, deadlineMember]));
const release = async (token: string, deadlineMember = '') =>
    Number(await redis.send('EVAL', [releaseLua, '2', key, deadlines, token, deadlineMember]));
const claimDue = async () =>
    await redis.send('EVAL', [claimDueLua, '1', deadlines, ownerPrefix, '64', '30000']) as string[];
const score = async (deadlineMember: string) => Number(await redis.send('ZSCORE', [deadlines, deadlineMember]));
const wakes = async () => Number(await redis.send('XLEN', [wake]));

try {
    // Owner fencing: replacement, renewal and release only by the current token.
    assert.equal(await renew('first'), 0);
    assert.equal(Number(await redis.send('EXISTS', [key])), 0);
    assert.equal(await claim('first', ''), 1);
    assert.equal(await claim('intruder', ''), 0);
    assert.equal(await claim('next', 'wrong'), 0);
    assert.equal(await claim('next', 'first'), 1);
    assert.equal(await renew('first'), 0);
    assert.equal(await release('first'), 0);
    assert.equal(await redis.send('GET', [key]), 'next');
    assert.equal(await renew('next'), 1);
    const ttl = Number(await redis.send('PTTL', [key]));
    assert(ttl > 0 && ttl <= 15000);
    console.log('PASS GB28181 owner replacement, renewal and release fencing');

    // The first tracked deadline wakes the Service Workers; later deadlines,
    // replacements and renewals only move a deadline later and stay silent.
    assert.equal(await wakes(), 1);
    const [entry] = await redis.send('XRANGE', [wake, '-', '+']) as unknown as [string, string[]][];
    assert.deepEqual(entry[1], ['task', 'gb28181-projector']);
    assert.deepEqual(await drainNotices(), [entry[0]], 'only the appended wake entry emits an input notification');
    const renewed = await score(member);
    assert(renewed > Date.now() + 10000);
    assert.equal(await claim('other', '', '60000', otherKey, otherMember), 1);
    assert.equal(await wakes(), 1, 'a later deadline must not wake Service Workers');
    assert.equal(await claim('session', '', '15000', `${ownerPrefix}session`, ''), 1);
    assert.equal(Number(await redis.send('ZCARD', [deadlines])), 2, 'session owners carry no deadline');
    assert.deepEqual(await drainNotices(), [], 'later deadlines and session claims must not emit input notifications');
    assert.equal(await claim('short', '', '100', `${ownerPrefix}camera-3`, 'device:camera-3'), 1);
    assert.equal(await wakes(), 2, 'an earlier deadline must wake Service Workers');
    const wakeEntries = await redis.send('XRANGE', [wake, '-', '+']) as [string, string[]][];
    assert.deepEqual(await drainNotices(), [wakeEntries[1][0]]);
    console.log('PASS GB28181 owner deadline tracking and wake on earlier deadline');

    // Release without a durable offline projection keeps the deadline; release
    // after it removes the deadline.
    assert.equal(await release('next'), 1);
    assert.equal(Number(await redis.send('EXISTS', [key])), 0);
    assert(await score(member) > 0, 'release without offline projection dropped the deadline');
    assert.equal(await claim('again', ''), 1);
    assert.equal(await release('again', member), 1);
    assert.equal(await redis.send('ZSCORE', [deadlines, member]), null);
    console.log('PASS GB28181 owner release keeps or removes the deadline');

    // Due members: a live owner is rescheduled, an expired owner is claimed with
    // a processing deadline and completed only while the claim score is unchanged.
    await redis.send('ZADD', [deadlines, '0', member, '0', otherMember, '0', 'malformed']);
    await Bun.sleep(150);
    const due = await claimDue();
    const now = Number(due[0]);
    assert(now > 0);
    const claimed = new Map<string, string>();
    for (let index = 2; index < due.length; index += 2) {
        claimed.set(due[index], due[index + 1]);
    }
    assert.deepEqual([...claimed.keys()].sort(), ['device:camera-1', 'device:camera-3']);
    assert.equal(Number(claimed.get(member)), now + 30000);
    assert.equal(await redis.send('ZSCORE', [deadlines, 'malformed']), null);
    const rescheduled = await score(otherMember);
    assert(rescheduled > now + 50000 && rescheduled <= now + 60000, 'live owner was not rescheduled to its expiry');
    assert.equal(due[1], String(now + 30000), 'next deadline is the earliest remaining score');
    assert.deepEqual((await claimDue()).slice(2), [], 'claimed members were claimed twice');
    assert.equal(Number(await redis.send('EVAL', [completeLua, '1', deadlines, member, '1'])), 0, 'stale claim completed a re-tracked owner');
    assert.equal(Number(await redis.send('EVAL', [completeLua, '1', deadlines, member, claimed.get(member)!])), 1);
    assert.equal(await redis.send('ZSCORE', [deadlines, member]), null);
    console.log('PASS GB28181 due owner claim, reschedule and fenced completion');
} finally {
    await redis.send('DEL', [key, otherKey, `${ownerPrefix}session`, `${ownerPrefix}camera-3`, deadlines, wake]);
    redis.close();
    subscriber.close();
}
