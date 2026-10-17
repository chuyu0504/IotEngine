import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { apiBase, databaseUrl, redisUrl } from './architecture-fixture';
import { openSnapshotSubscription, type SnapshotSubscription } from './sse-fixture';

const sql = new Bun.SQL(databaseUrl);
const redis = new Bun.RedisClient(redisUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const tag = `sse_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}`;
function token(id: string, lifetime = 3600) {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
        iss: 'iot-engine', aud: 'iot-engine-web', sub: id, user_id: id,
        username: tag, token_type: 'access', iat: now, exp: now + lifetime,
    })}`;
    return `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
async function request(method: string, path: string, body?: unknown, bearer = token(admin), status = 200) {
    const response = await fetch(apiBase + path, {
        method, headers: { Accept: 'application/json', 'Content-Type': 'application/json', Authorization: `Bearer ${bearer}` },
        body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10000),
    });
    const result = await response.json();
    assert.equal(response.status, status, `${method} ${path}: ${JSON.stringify(result)}`);
    return result;
}

async function pubsubIds() {
    const clients = String(await redis.send('CLIENT', ['LIST', 'TYPE', 'PUBSUB']));
    return new Set(clients.split('\n').flatMap(line => {
        const id = line.match(/(?:^| )id=(\d+)(?: |$)/)?.[1];
        return id ? [id] : [];
    }));
}

async function waitForLock(relation: string) {
    const deadline = Date.now()+10000;
    for (;;) {
        const [row] = await sql`SELECT EXISTS (
            SELECT 1 FROM pg_locks WHERE relation=${relation}::regclass AND NOT granted
        ) AS waiting`;
        if (row.waiting) return;
        assert(Date.now()<deadline,`SSE query did not reach the ${relation} barrier`);
        await Bun.sleep(10);
    }
}

async function holdTable(table: 'device' | 'sys_role') {
    const locked = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const transaction = sql.begin(async connection => {
        await connection`SET LOCAL lock_timeout='10s'`;
        if (table === 'device') await connection`LOCK TABLE device IN ACCESS EXCLUSIVE MODE`;
        else await connection`LOCK TABLE sys_role IN ACCESS EXCLUSIVE MODE`;
        locked.resolve();
        await release.promise;
    });
    void transaction.catch(locked.reject);
    await locked.promise;
    return async () => { release.resolve(); await transaction; };
}

async function revokeDuringUnpositionedReload(roleId: string, events: SnapshotSubscription, clientId: string) {
    // This fault test has its own fixture and RUN_SERIAL: CLIENT PAUSE affects
    // the Redis server, not only this fixture's selected database.
    let releaseDevice: (() => Promise<void>) | undefined;
    let releaseRole: (() => Promise<void>) | undefined;
    let paused = false;
    let pauseStartedAt = 0;
    try {
        releaseDevice = await holdTable('device');
        await redis.send('PUBLISH',['iot:live:topic:auth','']);
        await waitForLock('device');
        // The first snapshot loaded its grant before waiting on device rows.
        // Gate the *later* groups permission loader, after the cache is cleared.
        releaseRole = await holdTable('sys_role');
        assert.equal(Number(await redis.send('CLIENT',['KILL','ID',clientId])),1);
        await redis.send('CLIENT',['PAUSE','10000','ALL']);
        paused = true;
        pauseStartedAt = Date.now();
        // Allow the owning Worker to observe loss and enter its one-second
        // reconnect backoff. The next SUBSCRIBE cannot confirm while paused.
        await Bun.sleep(1200);
        await releaseDevice(); releaseDevice = undefined;
        await waitForLock('sys_role');
        releaseDevice = await holdTable('device');
        await releaseRole(); releaseRole = undefined;
        // Groups are denied. Realtime then reloads the still-positive device
        // permission and waits on device rows: this proves the cache was
        // repopulated while the notification socket was unpositioned.
        await waitForLock('device');
        await sql.begin(async transaction => {
            await transaction`SET LOCAL session_replication_role=replica`;
            await transaction`UPDATE sys_role SET permissions='[]'::jsonb WHERE id=${roleId}`;
        });
        await releaseDevice(); releaseDevice = undefined;
        assert(Date.now()-pauseStartedAt<9000,'fault window expired before the unpositioned cache reload and revocation completed');
        await redis.send('CLIENT',['UNPAUSE']);
        paused = false;
        const recovered = new Map<string, any>();
        for (let count=0;count<4;count++) {
            const event = await events.next();
            recovered.set(event.event,JSON.parse(event.data));
        }
        assert.equal(recovered.get('devices').code,11007,'recovery reused a positive permission cached during the lost interval');
        assert.equal(recovered.get('realtime').code,11007);
        assert.equal(recovered.get('groups').code,11007);
        assert.equal(recovered.get('user').data.permissions.includes('iot:device:query'),false);
        await events.expectQuiet(1600);
    } finally {
        if (paused) await redis.send('CLIENT',['UNPAUSE']);
        await releaseRole?.();
        await releaseDevice?.();
    }
}
let stream: SnapshotSubscription | undefined;
try {
    await request('POST', '/v1/roles', { name: tag, code: tag, permissions: [] });
    const [role] = await sql`SELECT id FROM sys_role WHERE code=${tag}`;
    await request('POST', '/v1/users', { username: tag, password: 'local-sse-password', role_ids: [role.id] });
    const [user] = await sql`SELECT id FROM sys_user WHERE username=${tag}`;
    const login = (await request('POST', '/v1/auth/login', { username: tag, password: 'local-sse-password' })).data;
    assert.equal(login.user.id, user.id);
    assert.equal((await request('GET', '/v1/auth/me', undefined, login.token)).data.id, user.id);
    const refreshed = (await request('POST', '/v1/auth/refresh', { refresh_token: login.refresh_token })).data;
    assert.equal(refreshed.user.id, user.id);
    const previousSubscriptions = await pubsubIds();
    stream = await openSnapshotSubscription('/v1/device/events', refreshed.token, { includeUser: true });
    const initialChannels = new Map<string, any>();
    for (let count = 0; count < 4; count++) {
        const event = await stream.next(); initialChannels.set(event.event, JSON.parse(event.data));
    }
    assert.equal(initialChannels.get('user').data.id, user.id);
    assert.equal(initialChannels.get('user').code, 0);
    for (const event of ['devices', 'groups', 'realtime']) assert.equal(initialChannels.get(event).code, 11007);
    const ownedSubscriptions = [...await pubsubIds()].filter(id => !previousSubscriptions.has(id));
    assert.equal(ownedSubscriptions.length, 1, 'the SSE must own its Redis subscription');
    await request('PUT', `/v1/users/${user.id}`, { nickname: 'shared user channel' });
    const userChanged = await stream.next();
    assert.equal(userChanged.event, 'user');
    assert.equal(JSON.parse(userChanged.data).data.nickname, 'shared user channel');
    await request('PUT', `/v1/roles/${role.id}`, { name: tag, code: tag, permissions: ['iot:device:query'] });
    // An authorization change resends every channel whose payload changed; the
    // still-denied groups channel is identical and therefore stays silent.
    const grantedChannels = new Map<string, any>();
    for (let count = 0; count < 3; count++) {
        const event = await stream.next(); grantedChannels.set(event.event, JSON.parse(event.data));
    }
    assert.equal(grantedChannels.has('groups'), false);
    assert.equal(grantedChannels.get('devices').code, 0);
    assert.deepEqual(grantedChannels.get('devices').data.list,[],'fault fixture must not require Redis enrichment while commands are paused');
    assert.equal(grantedChannels.get('realtime').code, 0);
    assert(grantedChannels.get('user').data.permissions.includes('iot:device:query'));
    // If a telemetry notification incorrectly reloads the profile, this
    // deliberate unannounced fixture change will appear as a user event.
    // This disposable fixture database normally emits auth notifications from
    // live_users even for direct SQL. Suppress triggers only in this transaction
    // so the following telemetry event is the sole reason to execute a query.
    await sql.begin(async transaction => {
        await transaction`SET LOCAL session_replication_role = replica`;
        await transaction`UPDATE sys_user SET nickname='unannounced fixture change' WHERE id=${user.id}`;
    });
    // Telemetry of a device outside this user's scope must neither reach the
    // realtime channel nor reload any other channel.
    await redis.send('PUBLISH', ['iot:live:topic:device.realtime', crypto.randomUUID()]);
    await stream.expectQuiet();
    // Revoke while this very connection still holds a cached successful
    // permission answer. A separate connection's cache must not keep it alive.
    await request('PUT', `/v1/roles/${role.id}`, { name: tag, code: tag, permissions: [] });
    const revokedChannels = new Map<string, any>();
    for (let count = 0; count < 3; count++) {
        const event = await stream.next(); revokedChannels.set(event.event, JSON.parse(event.data));
    }
    assert.equal(revokedChannels.get('devices').code, 11007);
    assert.equal(revokedChannels.get('realtime').code, 11007);
    assert.equal(revokedChannels.get('user').data.permissions.includes('iot:device:query'), false);
    // Real notifications after revocation cannot resurrect cached access.
    await redis.send('PUBLISH', ['iot:live:topic:device.realtime', '']);
    await stream.expectQuiet(1000);
    await request('PUT', `/v1/roles/${role.id}`, { name: tag, code: tag, permissions: ['iot:device:query'] });
    for (let count = 0; count < 3; count++) await stream.next();
    await revokeDuringUnpositionedReload(role.id,stream,ownedSubscriptions[0]);
    await stream.close();
    console.log('PASS page SSE shares user profile, permission changes and scoped queries without an extra auth connection');
    stream = await openSnapshotSubscription('/v1/device/events', token(user.id, 2), { includeUser: true });
    for (let count = 0; count < 4; count++) assert.notEqual((await stream.next()).event, 'error');
    // The 15-second verifier tolerance plus the next 15-second check can
    // exceed 20 seconds. Comment heartbeats do not dispatch business events.
    const expired = await stream.next(40000);
    assert.equal(expired.event, 'error');
    assert.equal(JSON.parse(expired.data).code, 11005);
    await stream.close();
    stream = undefined;
    await request('POST', '/v1/auth/logout', undefined, refreshed.token);
    console.log('PASS HTTP login/refresh/me/logout, shared user SSE channel snapshot and token expiry without heartbeat events');
    // HTTP 认证先于权限：缺失、过期、伪造 Token 均为 401，且不会被权限校验改写为 403。
    const anonymous = await fetch(apiBase + '/v1/users', { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(10000) });
    assert.equal(anonymous.status, 401);
    assert.equal((await anonymous.json()).code, 11004);
    assert.equal((await request('GET', '/v1/users', undefined, token(user.id, -60), 401)).code, 11005);
    const forged = token(user.id).replace(/\.[^.]+$/, `.${createHmac('sha256', 'forged-signing-secret-forged-signing-secret').update(tag).digest('base64url')}`);
    assert.equal((await request('GET', '/v1/users', undefined, forged, 401)).code, 11006);
    // 已认证但无权限为 403；授权后同一 Token 立即成功；撤销后再次 403。
    assert.equal((await request('GET', '/v1/users', undefined, token(user.id), 403)).code, 11007);
    await request('PUT', `/v1/roles/${role.id}`, { name: tag, code: tag, permissions: ['iot:device:query', 'system:user:query'] });
    assert.equal((await request('GET', `/v1/users?keyword=${tag}`, undefined, token(user.id))).data.list[0].id, user.id);
    await request('PUT', `/v1/roles/${role.id}`, { name: tag, code: tag, permissions: ['iot:device:query'] });
    assert.equal((await request('GET', '/v1/users', undefined, token(user.id), 403)).code, 11007);
    console.log('PASS HTTP authentication precedes permission: missing/expired/forged 401, denied 403, grant and revoke apply immediately');
} finally {
    await stream?.close();
    await sql`DELETE FROM sys_user_role WHERE user_id IN (SELECT id FROM sys_user WHERE username=${tag})`;
    await sql`DELETE FROM sys_user WHERE username=${tag}`;
    await sql`DELETE FROM sys_role WHERE code=${tag}`;
    redis.close();
    await sql.close();
}
