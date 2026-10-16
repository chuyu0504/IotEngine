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

async function sourceScript(path: string, marker: string): Promise<string> {
    const source = await Bun.file(path).text();
    const start = source.indexOf(marker);
    assert(start >= 0, `missing Lua marker ${marker} in ${path}`);
    const script = source.slice(start).match(/R"lua\(([\s\S]*?)\)lua"/)?.[1];
    assert(script, `missing Lua body ${marker} in ${path}`);
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
    const runtimePath = 'service/features/edge/projection/projection.runtime.h';
    const publishPath = 'service/features/edge/edge.service.h';
    const redisPath = 'service/utils/redis.h';
    const claimRecovery = await sourceScript(runtimePath, 'kClaimLeaseScript =');
    const fencedAck = await sourceScript(runtimePath, 'kAcknowledgeFencedScript =');
    const eraseDead = await sourceScript(runtimePath, 'kEraseDeadStreamScript =');
    const fencedPublish = await sourceScript(publishPath, 'kFencedPublishScript =');

    assert(!fencedPublish.includes("local arguments = {'MAXLEN'"));
    for (const consumerRuntime of [
        'service/features/access/access.runtime.h',
        'service/features/telemetry/telemetry.runtime.h',
        'service/features/command/command.runtime.h',
        'service/features/configuration/configuration.runtime.h',
    ]) {
        const consumerSource = await Bun.file(consumerRuntime).text();
        assert(!consumerSource.includes('repairLegacyReadSet'), `${consumerRuntime} retains legacy read-set repair`);
        assert(consumerSource.includes('kPendingRecoveryInterval'), `${consumerRuntime} has no periodic pending recovery deadline`);
    }

    // A discovered dead stream must not be recreated after it was drained and
    // removed while recovery was being prepared.
    const redisSource = await Bun.file(redisPath).text();
    const groupHelper = redisSource.slice(redisSource.indexOf('ensureGroupIfPresent'));
    const groupCreate = groupHelper.match(/\{\s*"XGROUP"[\s\S]*?\}\s*\);/)?.[0];
    assert(groupCreate && !groupCreate.includes('MKSTREAM'));
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
    await waitForExpired(oldLease);

    // A live owner is never stolen, even when another worker is trying to
    // recover a different stream.
    await redis.send('SET', [activeLease, activeToken, 'PX', '5000', 'NX']);
    const activeClaim = await redis.send('EVAL', [
        claimRecovery,
        '1',
        activeLease,
        recoveryToken,
        '2000',
    ]);
    assert.equal(Number(activeClaim), 0, 'active owner lease was stolen');
    assert.equal(await redis.send('GET', [activeLease]), activeToken);

    const claimed = await redis.send('EVAL', [
        claimRecovery,
        '1',
        oldLease,
        recoveryToken,
        '2000',
    ]);
    assert.equal(Number(claimed), 1, 'dead owner recovery lease was not claimed');
    assert.equal(await redis.send('GET', [oldLease]), recoveryToken);

    // The crashed gateway still has the old token.  Its ingress write must be
    // rejected atomically after recovery claims the lease.
    const staleWrite = await redis.send('EVAL', [
        fencedPublish,
        '4',
        oldLease,
        oldStream,
        registry,
        wake,
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

    const acknowledged = await redis.send('EVAL', [
        fencedAck,
        '2',
        oldLease,
        oldStream,
        recoveryToken,
        group,
        oldMessage,
    ]);
    assert.equal(Number(acknowledged), 1, 'recovery acknowledgement was rejected');
    assert.equal(Number(await redis.send('XLEN', [oldStream])), 0);
    const pendingSummary = await redis.send('XPENDING', [oldStream, group]) as unknown[];
    assert.equal(Number(pendingSummary[0]), 0);

    // A live, fenced producer must refuse a full data stream instead of
    // approximately trimming entries that have not reached the database.
    await redis.send('PEXPIRE', [oldLease, '5000']);
    const capacityFirst = await redis.send('EVAL', [
        fencedPublish,
        '4',
        oldLease,
        oldStream,
        registry,
        wake,
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
    const capacitySecond = await redis.send('EVAL', [
        fencedPublish,
        '4',
        oldLease,
        oldStream,
        registry,
        wake,
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
    const retainedCapacityEntry = await redis.send('XRANGE', [oldStream, '-', '+']) as unknown[][];
    assert.equal(String(retainedCapacityEntry[0]?.[0]), String(capacityFirst));
    await redis.send('XDEL', [oldStream, String(capacityFirst)]);

    // Cleanup is atomic with the recovery token check.  It removes the stream,
    // lease, and registry entry only after the stream is fully drained.
    const erased = await redis.send('EVAL', [
        eraseDead,
        '3',
        oldLease,
        oldStream,
        registry,
        recoveryToken,
    ]);
    assert.equal(Number(erased), 1, 'drained dead stream was not removed');
    assert.equal(Number(await redis.send('EXISTS', [oldStream])), 0);
    assert.equal(Number(await redis.send('EXISTS', [oldLease])), 0);
    const registered = await redis.send('SISMEMBER', [registry, oldStream]);
    assert.equal(Number(registered), 0);

    console.log('PASS Edge owner recovery, live lease exclusion, and fencing');
} finally {
    await redis.send('DEL', [
        oldStream,
        activeStream,
        oldLease,
        activeLease,
        wake,
        `iot:v3:edge:projector:${suffix}:missing`,
    ]);
    await redis.send('SREM', [registry, oldStream, activeStream]);
    redis.close();
}
