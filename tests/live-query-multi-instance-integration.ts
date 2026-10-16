// Integration test for two independently running API instances.
// Run only against the disposable architecture fixture.
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { request, type IncomingMessage } from 'node:http';
import { databaseUrl } from './architecture-fixture';

const dbUrl = databaseUrl;
const ports = (Bun.env.LIVE_QUERY_API_PORTS ?? '')
    .split(',').map((value) => Number(value.trim())).filter((value) => Number.isInteger(value) && value > 0);
assert.equal(ports.length, 2, 'LIVE_QUERY_API_PORTS must contain exactly two ports');
const db = new Bun.SQL(dbUrl);
const admin = '00000000-0000-7000-8000-000000000002';
const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
function token(user = admin) {
    const now = Math.floor(Date.now() / 1000);
    const unsigned = `${encode({ alg: 'HS256', typ: 'JWT' })}.${encode({
        iss: 'iot-engine', aud: 'iot-engine-web', sub: user, user_id: user,
        username: 'live-multi-instance-test', token_type: 'access', iat: now, exp: now + 3600,
    })}`;
    return `${unsigned}.${createHmac('sha256', 'architecture-test-only-access-secret-000000000').update(unsigned).digest('base64url')}`;
}
const wait = (milliseconds: number) => Bun.sleep(milliseconds);
const frame = (text: string) => {
    const separator = text.includes('\r\n\r\n') ? '\r\n\r\n' : '\n\n';
    const index = text.indexOf(separator);
    if (index < 0) return undefined;
    const raw = text.slice(0, index);
    const data = raw.match(/^data:\s?(.*)$/m)?.[1];
    return data ? { value: JSON.parse(data), rest: text.slice(index + separator.length) } : { value: undefined, rest: text.slice(index + separator.length) };
};

async function open(port: number, path = '/v1/departments?page=1&pageSize=100') {
    const controller = new AbortController();
    const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        headers: { Authorization: `Bearer ${token()}`, Accept: 'text/event-stream' },
        signal: controller.signal,
    });
    assert.equal(response.status, 200, `API ${port} returned ${response.status}`);
    assert(response.body);
    const reader = response.body.getReader();
    let pending = '';
    return {
        async next() {
            const deadline = Date.now() + 10000;
            for (;;) {
                const parsed = frame(pending);
                if (parsed) { pending = parsed.rest; if (parsed.value !== undefined) return parsed.value; }
                if (Date.now() >= deadline) throw new Error(`timed out waiting for snapshot from ${port}`);
                const result = await Promise.race([
                    reader.read(),
                    wait(Math.max(1, deadline - Date.now())).then(() => ({ timedOut: true as const })),
                ]);
                if ('timedOut' in result) throw new Error(`timed out waiting for snapshot from ${port}`);
                if (result.done) throw new Error(`SSE ${port} ended before snapshot`);
                pending += new TextDecoder().decode(result.value, { stream: true });
            }
        },
        async close() {
            controller.abort();
            try { await reader.cancel(); }
            catch (error) { if (!(error instanceof Error) || error.name !== 'AbortError') throw error; }
        },
    };
}

async function rawPaused(port: number, path: string) {
    const req = request({ host: '127.0.0.1', port, path, agent: false,
        headers: { Authorization: `Bearer ${token()}`, Accept: 'text/event-stream' } });
    const opened = new Promise<IncomingMessage>((resolve, reject) => {
        req.once('error', reject);
        req.once('response', (incoming) => { incoming.pause(); resolve(incoming); });
        req.setTimeout(20000, () => req.destroy(new Error('paused SSE timed out')));
    });
    req.end();
    const response = await opened;
    assert.equal(response.statusCode, 200);
    // Node decodes HTTP chunks; pausing the bounded response buffer propagates
    // to the socket instead of storing an unbounded application backlog.
    return {
        async readLatest(marker: string) {
            let pending = '';
            let bytes = 0;
            const decoder = new TextDecoder();
            const timeout = setTimeout(() => req.destroy(new Error('latest snapshot timed out')), 20000);
            try {
                for await (const chunk of response) {
                    bytes += chunk.length;
                    assert(bytes <= 24 * 1024 * 1024, 'slow subscriber received an unbounded backlog');
                    pending += decoder.decode(chunk, { stream: true });
                    for (;;) {
                        const parsed = frame(pending);
                        if (!parsed) break;
                        pending = parsed.rest;
                        if (parsed.value?.data?.remark === marker) return { snapshot: parsed.value, bytes };
                    }
                }
                throw new Error('SSE ended before the latest complete snapshot');
            } finally { clearTimeout(timeout); }
        },
        close() { response.destroy(); req.destroy(); },
    };
}
const departmentIds = Array.from({ length: 100 }, () => crypto.randomUUID());
const marker = `multi-instance-${crypto.randomUUID()}`;
const queryPath = `/v1/departments?page=1&pageSize=100&keyword=${encodeURIComponent(marker)}`;
const protocolId = crypto.randomUUID();
let streams: Array<Awaited<ReturnType<typeof open>>> = [];
let raw: Awaited<ReturnType<typeof rawPaused>> | undefined;
try {
    streams = await Promise.all(ports.map((port) => open(port, queryPath)));
    const initial = await Promise.all(streams.map((stream) => stream.next()));
    assert(initial.every((snapshot) => Array.isArray(snapshot.data?.list)), 'both instances must return department snapshots');

    // sys_department.name is VARCHAR(100); burst updates below make the wire
    // payload large without violating the fixture schema.
    const names = departmentIds.map((_, index) => `${marker}-${String(index).padStart(3, '0')}-${'x'.repeat(40)}`);
    await db.begin(async (tx) => {
        for (let index = 0; index < departmentIds.length; index++)
            await tx`INSERT INTO sys_department(id,name,code) VALUES(${departmentIds[index]},${names[index]},${departmentIds[index]})`;
    });
    const updates = await Promise.all(streams.map((stream) => stream.next()));
    assert(updates.every((snapshot) => JSON.stringify(snapshot).includes(marker)), 'one committed SQL change must reach both API streams');
    console.log('PASS one external committed SQL change reached both API instances');

    await streams[0].close();
    streams[0] = await open(ports[1], queryPath);
    const reconnected = await streams[0].next();
    assert(JSON.stringify(reconnected).includes(marker), 'reconnect must receive current committed state');
    console.log('PASS reconnect receives latest committed snapshot');

    await Promise.all(streams.map((stream) => stream.close()));
    const padding = 'p'.repeat(4 * 1024 * 1024);
    const protocol = { padding, marker, storagePolicy: 'report', registers: [] };
    await db`INSERT INTO protocol_config(id,protocol,name,enabled,config,remark,created_by)
        VALUES(${protocolId},'Modbus',${`slow-${protocolId}`},false,${protocol}::jsonb,${marker},${admin})`;
    const detailPath = `/v1/protocol/configs/${protocolId}`;
    const complete = await open(ports[0], detailPath);
    const completeSnapshot = await complete.next();
    assert.equal(completeSnapshot.data?.remark, marker, 'large detail SSE must be complete and parseable');
    assert.equal(completeSnapshot.data?.config?.padding.length, padding.length);
    await complete.close();

    raw = await rawPaused(ports[0], detailPath);
    const latestMarker = `${marker}-latest`;
    await db`UPDATE protocol_config SET remark=${latestMarker}, config=${{ ...protocol, marker: latestMarker }}::jsonb WHERE id=${protocolId}`;
    // Four MiB exceeds the application receive buffer; kernel capacity varies.
    // Require a complete latest snapshot after resuming the paused response.
    await wait(Number(Bun.env.LIVE_QUERY_SLOW_PAUSE_MS ?? 1500));
    const { snapshot: rawSnapshot, bytes } = await raw.readLatest(latestMarker);
    assert.equal(rawSnapshot.data.remark, latestMarker);
    assert.equal(rawSnapshot.data.config.padding.length, padding.length);
    assert(bytes > 4 * 1024 * 1024, `paused detail snapshot produced only ${bytes} bytes`);
    console.log('PASS paused raw TCP receiver recovered latest large snapshot');
} finally {
    raw?.close();
    for (const stream of streams) await stream.close().catch(() => {});
    // Published revisions are immutable history, including in the fixture.
    await db`UPDATE protocol_config SET deleted_at=NOW() WHERE id=${protocolId}`;
    await db`DELETE FROM sys_department WHERE id IN ${db(departmentIds)}`;
    await db.close();
}
