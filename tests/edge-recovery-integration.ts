import assert from 'node:assert/strict';
import { redisUrl } from './architecture-fixture';

// This test deliberately talks to Redis through the same stream, consumer
// group, lease, and registry keys used by the Edge projector.  It is intended
// for the disposable Redis fixture; never point it at a production instance.
const redis = new Bun.RedisClient(redisUrl);
const suffix = crypto.randomUUID();
const oldInstance = `edge-old-${suffix}`;
const activeInstance = `edge-active-${suffix}`;
const recoveryInstance = `edge-recovery-${suffix}`;
const oldWorker = '3';
const activeWorker = '4';
const group = 'iot-engine:edge-projector';
const registry = 'iot:v3:edge:projector:streams';
const oldStream = `iot:v3:edge:projector:${oldInstance}:${oldWorker}`;
const activeStream = `iot:v3:edge:projector:${activeInstance}:${activeWorker}`;
const oldLease = `iot:v3:edge:projector:lease:${oldInstance}:${oldWorker}`;
const activeLease = `iot:v3:edge:projector:lease:${activeInstance}:${activeWorker}`;
const wake = `iot:service:worker:${recoveryInstance}:${oldWorker}:wake`;
const oldToken = `${oldInstance}:${oldWorker}`;
const activeToken = `${activeInstance}:${activeWorker}`;
const recoveryToken = `${recoveryInstance}:recovery:${oldWorker}:${oldInstance}:${oldWorker}`;
const inputChannel = `iot:consumer:input:${oldStream}`;
const wakeChannel = `iot:consumer:input:${wake}`;
const pendingDeadlines = `iot:consumer:pending:${group}:${oldStream}`;
const deadlineChannel = `iot:consumer:deadline:${group}:${oldStream}`;
const recoveryDeadlines = `iot:v3:edge:projector:deadlines:${suffix}`;
const subscriber = new Bun.RedisClient(redisUrl);
const notices: { payload: string; channel: string }[] = [];
const barrierChannel = `iot:test:edge-recovery:${suffix}:barrier`;
let barrier: (() => void) | undefined;
for (const channel of [inputChannel, wakeChannel, deadlineChannel]) {
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

async function sourceScript(path: string, name: string): Promise<string> {
    const source = await Bun.file(path).text();
    const script = source.match(new RegExp(`\\b${name}\\s*=\\s*R"lua\\(([\\s\\S]*?)\\)lua"`))?.[1];
    assert(script, `missing Lua constant ${name} in ${path}`);
    return script;
}

async function waitForExpired(key: string) {
    for (let attempt = 0; attempt < 100; attempt++) {
        if (Number(await redis.send('EXISTS', [key])) === 0) return;
        await Bun.sleep(10);
    }
    throw new Error(`lease did not expire: ${key}`);
}

try {
    const projectionPath = 'service/features/edge/projection/projection.service.h';
    const publishPath = 'service/features/edge/edge.service.h';
    const claimRecovery = await sourceScript(projectionPath, 'kClaimLeaseScript');
    const renewLease = await sourceScript(projectionPath, 'kRenewLeaseScript');
    const dueStreams = await sourceScript(projectionPath, 'kDueRecoveryStreamsScript');
    const nextDeadline = await sourceScript(projectionPath, 'kNextRecoveryDeadlineScript');
    const fencedAck = await sourceScript(projectionPath, 'kAcknowledgeFencedScript');
    const eraseDead = await sourceScript(projectionPath, 'kEraseDeadStreamScript');
    const fencedPublish = await sourceScript(publishPath, 'kFencedPublishScript');

    // A discovered dead stream must not be recreated by group creation.
    const missingStream = `iot:v3:edge:projector:${suffix}:missing`;
    let missingRejected = false;
    try {
        await redis.send('XGROUP', ['CREATE', missingStream, group, '0']);
    } catch {
        missingRejected = true;
    }
    assert(missingRejected, 'discovered stream group creation recreated a missing stream');
    assert.equal(Number(await redis.send('EXISTS', [missingStream])), 0);

    await redis.send('SADD', [registry, oldStream, activeStream]);
    await redis.send('SET', [oldLease, oldToken, 'PX', '80', 'NX']);
    await redis.send('XGROUP', ['CREATE', oldStream, group, '0', 'MKSTREAM']);
    const oldMessage = String(await redis.send('XADD', [
        oldStream,
        '*',
        'kind',
        'ingress',
        'wire',
        'old-owner-entry',
        'received_at_ms',
        String(Date.now()),
    ]));
    const pending = await redis.send('XREADGROUP', [
        'GROUP',
        group,
        'old-owner-consumer',
        'COUNT',
        '1',
        'STREAMS',
        oldStream,
        '>',
    ]);
    const pendingRows = (pending as Record<string, unknown>)[oldStream] as unknown[];
    assert(
        Array.isArray(pendingRows) && pendingRows.length === 1,
        'old owner must leave a pending entry before crashing'
    );
    await redis.send('ZADD', [pendingDeadlines, String(Date.now() + 60000), oldMessage]);
    await waitForExpired(oldLease);

    // A live owner is never stolen, even when another worker is trying to
    // recover a different stream.
    await redis.send('SET', [activeLease, activeToken, 'PX', '5000', 'NX']);
    const activeClaim = await redis.send('EVAL', [
        claimRecovery,
        '2',
        activeLease,
        recoveryDeadlines,
        recoveryToken,
        '2000',
        activeStream,
    ]);
    assert.equal(Number(activeClaim), 0, 'active owner lease was stolen');
    assert.equal(await redis.send('GET', [activeLease]), activeToken);

    const claimed = await redis.send('EVAL', [
        claimRecovery,
        '2',
        oldLease,
        recoveryDeadlines,
        recoveryToken,
        '2000',
        oldStream,
    ]);
    assert.equal(Number(claimed), 1, 'dead owner recovery lease was not claimed');
    assert.equal(await redis.send('GET', [oldLease]), recoveryToken);
    const initialDeadline = Number(await redis.send('ZSCORE', [recoveryDeadlines, oldStream]));
    assert(initialDeadline > Date.now(), 'claim did not schedule lease expiry recovery');
    assert.deepEqual(await redis.send('EVAL', [dueStreams, '1', recoveryDeadlines]), []);
    assert(Number(await redis.send('EVAL', [nextDeadline, '1', recoveryDeadlines])) > 0);
    const staleRenew = await redis.send('EVAL', [
        renewLease, '2', oldLease, recoveryDeadlines, oldToken, '4000', oldStream,
    ]);
    assert.equal(Number(staleRenew), 0, 'stale owner renewed recovery lease');
    assert.equal(Number(await redis.send('ZSCORE', [recoveryDeadlines, oldStream])), initialDeadline);
    const renewed = await redis.send('EVAL', [
        renewLease, '2', oldLease, recoveryDeadlines, recoveryToken, '4000', oldStream,
    ]);
    assert.equal(Number(renewed), 1);
    assert(Number(await redis.send('ZSCORE', [recoveryDeadlines, oldStream])) > initialDeadline);

    // The deadline query returns only expired candidates, not the registry.
    await redis.send('ZADD', [recoveryDeadlines, String(Date.now() - 1), activeStream]);
    assert.deepEqual(await redis.send('EVAL', [dueStreams, '1', recoveryDeadlines]), [activeStream]);
    await redis.send('ZREM', [recoveryDeadlines, activeStream]);

    // The crashed gateway still has the old token.  Its ingress write must be
    // rejected atomically after recovery claims the lease.
    const staleWrite = await redis.send('EVAL', [
        fencedPublish,
        '6',
        oldLease,
        oldStream,
        registry,
        wake,
        inputChannel,
        wakeChannel,
        oldToken,
        '100000',
        '100000',
        'edge-projector',
        'kind',
        'ingress',
        'wire',
        'stale-write',
        'received_at_ms',
        String(Date.now()),
    ]);
    assert.equal(Number(staleWrite), 0, 'stale gateway write crossed the fence');
    assert.deepEqual(await drainNotices(), [], 'a fenced-out producer must not publish');
    assert.equal(Number(await redis.send('EVAL', [
        fencedAck, '4', oldLease, oldStream, pendingDeadlines, deadlineChannel,
        oldToken, group, oldMessage,
    ])), 0, 'stale owner crossed the acknowledgement fence');
    assert.notEqual(await redis.send('ZSCORE', [pendingDeadlines, oldMessage]), null);
    assert.deepEqual(await drainNotices(), [], 'stale acknowledgement published a deadline change');

    // Recovery claims the crashed consumer's pending entry and processes it
    // through the real stream before the fenced acknowledgement removes it.
    const claimedEntries = await redis.send('XAUTOCLAIM', [
        oldStream,
        group,
        'recovery-consumer',
        '0',
        '0-0',
        'COUNT',
        '10',
    ]) as unknown[];
    assert(Array.isArray(claimedEntries) && Array.isArray(claimedEntries[1]));
    const recoveredIds = (claimedEntries[1] as unknown[]).map((entry) =>
        Array.isArray(entry) ? String(entry[0]) : '');
    assert(recoveredIds.includes(oldMessage), 'old pending entry was not recovered');
    await redis.send('ZADD', [pendingDeadlines, String(Date.now()), oldMessage]);

    const acknowledged = await redis.send('EVAL', [
        fencedAck,
        '4',
        oldLease,
        oldStream,
        pendingDeadlines,
        deadlineChannel,
        recoveryToken,
        group,
        oldMessage,
    ]);
    assert.equal(Number(acknowledged), 1, 'recovery acknowledgement was rejected');
    assert.equal(Number(await redis.send('XLEN', [oldStream])), 0);
    const pendingSummary = await redis.send('XPENDING', [oldStream, group]) as unknown[];
    assert.equal(Number(pendingSummary[0]), 0);
    assert.equal(await redis.send('ZSCORE', [pendingDeadlines, oldMessage]), null);
    const ackNotices = await drainNotices();
    assert.equal(ackNotices.length, 1);
    assert.equal(ackNotices[0].channel, deadlineChannel);

    // A live, fenced producer must refuse a full data stream instead of
    // approximately trimming entries that have not reached the database.
    await redis.send('PEXPIRE', [oldLease, '5000']);
    const capacityFirst = await redis.send('EVAL', [
        fencedPublish,
        '6',
        oldLease,
        oldStream,
        registry,
        wake,
        inputChannel,
        wakeChannel,
        recoveryToken,
        '1',
        '100000',
        'edge-projector',
        'kind',
        'ingress',
        'wire',
        'capacity-first',
        'received_at_ms',
        String(Date.now()),
    ]);
    assert.notEqual(String(capacityFirst), '0');
    const [wakeEntry] = await redis.send('XRANGE', [wake, '-', '+']) as [string, string[]][];
    assert.deepEqual(await drainNotices(), [
        { payload: String(capacityFirst), channel: inputChannel },
        { payload: wakeEntry[0], channel: wakeChannel },
    ], 'durable data and task appends must each publish their actual ID');
    const capacitySecond = await redis.send('EVAL', [
        fencedPublish,
        '6',
        oldLease,
        oldStream,
        registry,
        wake,
        inputChannel,
        wakeChannel,
        recoveryToken,
        '1',
        '100000',
        'edge-projector',
        'kind',
        'ingress',
        'wire',
        'capacity-second',
        'received_at_ms',
        String(Date.now()),
    ]);
    assert.equal(Number(capacitySecond), 0, 'full data stream was trimmed instead of rejected');
    assert.equal(Number(await redis.send('XLEN', [oldStream])), 1);
    assert.deepEqual(await drainNotices(), [], 'a full stream must not emit input notifications');
    const retainedCapacityEntry = await redis.send('XRANGE', [oldStream, '-', '+']) as unknown[][];
    assert.equal(String(retainedCapacityEntry[0]?.[0]), String(capacityFirst));
    await redis.send('XDEL', [oldStream, String(capacityFirst)]);

    // Cleanup is atomic with the recovery token check.  It removes the stream,
    // lease, and registry entry only after the stream is fully drained.
    const erased = await redis.send('EVAL', [
        eraseDead,
        '4',
        oldLease,
        oldStream,
        registry,
        recoveryDeadlines,
        recoveryToken,
    ]);
    assert.equal(Number(erased), 1, 'drained dead stream was not removed');
    assert.equal(Number(await redis.send('EXISTS', [oldStream])), 0);
    assert.equal(Number(await redis.send('EXISTS', [oldLease])), 0);
    const registered = await redis.send('SISMEMBER', [registry, oldStream]);
    assert.equal(Number(registered), 0);
    assert.equal(await redis.send('ZSCORE', [recoveryDeadlines, oldStream]), null);

    console.log('PASS Edge owner recovery, live lease exclusion, and fencing');
} finally {
    await redis.send('DEL', [
        oldStream,
        activeStream,
        oldLease,
        activeLease,
        wake,
        pendingDeadlines,
        recoveryDeadlines,
        `iot:v3:edge:projector:${suffix}:missing`,
    ]);
    await redis.send('SREM', [registry, oldStream, activeStream]);
    subscriber.close();
    redis.close();
}
