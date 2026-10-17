// Uses only the disposable architecture Redis fixture.
import assert from 'node:assert/strict';
import { redisUrl } from './architecture-fixture';
const redis = new Bun.RedisClient(redisUrl);
const subscriber = new Bun.RedisClient(redisUrl);
const source = await Bun.file('service/features/edge/session/session.service.h').text();
const script = (name: string) => {
    const position = source.indexOf(`${name} =`);
    assert(position >= 0);
    return source.slice(position).match(/R"lua\(([\s\S]*?)\)lua"/)![1];
};
const prefix = `test:edge-session:${crypto.randomUUID()}`;
const sessionPrefix = `${prefix}:session:`;
const node = crypto.randomUUID();
const keys = [`${sessionPrefix}${node}`, `${prefix}:deadlines`, `${prefix}:edge-channel`];
const run = (name: string, args = ['old-session', node], selected = keys) =>
    redis.send('EVAL', [script(name), String(selected.length), ...selected, ...args]);
const messages: string[] = [];
const barriers = new Map<string, () => void>();
const entries = async () => {
    const marker = `barrier:${crypto.randomUUID()}`;
    const received = new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Pub/Sub barrier timed out')), 5000);
        barriers.set(marker, () => { clearTimeout(timer); resolve(); });
    });
    await redis.send('PUBLISH', [keys[2], marker]);
    await received;
    return [...messages];
};
try {
    assert.equal(await subscriber.subscribe(keys[2], (payload, channel) => {
        assert.equal(channel, keys[2]);
        const barrier = barriers.get(payload);
        if (barrier) {
            barriers.delete(payload);
            barrier();
        } else {
            messages.push(payload);
        }
    }), 1, 'subscription must be confirmed before mutating sessions');
    assert.equal(await run('kClaimScript'), 1);
    assert.deepEqual(await entries(), [node], 'claim publishes the node id');
    assert.equal(await run('kRefreshScript'), 1);
    assert.equal((await entries()).length, 1, 'renewal must not trigger query churn');
    assert.equal(await run('kClaimScript', ['new-session', node]), 1);
    assert.equal(await run('kRefreshScript'), 0, 'old process cannot renew new session');
    assert.equal(await run('kReleaseScript'), 0, 'old close cannot remove new session');
    assert.equal(await redis.send('GET', [keys[0]]), 'new-session');
    assert.equal(await run('kReleaseScript', ['new-session', node]), 1);
    assert.equal(await redis.send('EXISTS', [keys[0]]), 0);
    assert.equal(await redis.send('ZCARD', [keys[1]]), 0);
    assert.deepEqual(await entries(), [node, node, node]);

    // Two crashed owners expire in one script run: one change lists both nodes,
    // a second run (another worker or instance) finds nothing to claim.
    const other = crypto.randomUUID();
    const otherKey = `${sessionPrefix}${other}`;
    await run('kClaimScript');
    await run('kClaimScript', ['other-session', other], [otherKey, keys[1], keys[2]]);
    for (const key of [keys[0], otherKey]) {
        await redis.send('PEXPIRE', [key, '1']);
        await redis.send('ZADD', [keys[1], '0', key]);
    }
    await Bun.sleep(10);
    assert.equal(await run('kExpireScript', [sessionPrefix], [keys[1], keys[2]]), 2);
    assert.equal(await run('kExpireScript', [sessionPrefix], [keys[1], keys[2]]), 0);
    const all = await entries();
    assert.equal(all.length, 6, 'crashed owners emit exactly one offline change');
    const expired = all.at(-1)!;
    assert.deepEqual(expired.split(',').sort(), [node, other].sort());

    // A live lease that is due in the index is re-armed at its real expiry
    // instead of being published.
    await run('kClaimScript');
    await redis.send('ZADD', [keys[1], '0', keys[0]]);
    assert.equal(await run('kExpireScript', [sessionPrefix], [keys[1], keys[2]]), 0);
    assert(Number(await redis.send('ZSCORE', [keys[1], keys[0]])) > Date.now(), 'live lease re-armed at its expiry');
    await redis.send('DEL', [otherKey]);
    console.log('PASS session ownership, renewal, disconnect and entity-level expiry changes');
} finally {
    await redis.send('DEL', keys);
    subscriber.close();
    redis.close();
}
