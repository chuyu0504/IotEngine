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
interface LinkItem { id: string; name: string; debug_enabled: boolean; endpoint?: { targets?: { name: string }[] } }
// A frame is either the full page or an entity delta of rows on that page.
interface LinkSnapshot { code: number; data?: { list?: LinkItem[] }; delta?: { upsert: LinkItem[]; remove: string[] } }
interface SnapshotStream { next(): Promise<LinkSnapshot>; close(): Promise<void> }
interface PausedStream { readLatest(latest: (item: LinkItem) => boolean): Promise<{ snapshot: LinkSnapshot; bytes: number }>; close(): void }
const frame = (text: string): { value: LinkSnapshot | undefined; rest: string } | undefined => {
    const separator = text.includes('\r\n\r\n') ? '\r\n\r\n' : '\n\n';
    const index = text.indexOf(separator);
    if (index < 0) return undefined;
    const raw = text.slice(0, index);
    const data = raw.match(/^data:\s?(.*)$/m)?.[1];
    return data ? { value: JSON.parse(data), rest: text.slice(index + separator.length) } : { value: undefined, rest: text.slice(index + separator.length) };
};

async function open(port: number, path: string): Promise<SnapshotStream> {
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

async function rawPaused(port: number, path: string): Promise<PausedStream> {
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
        async readLatest(latest: (item: LinkItem) => boolean) {
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
                        if ((parsed.value?.data?.list ?? parsed.value?.delta?.upsert)?.some(latest)) return { snapshot: parsed.value, bytes };
                    }
                }
                throw new Error('SSE ended before the latest complete snapshot');
            } finally { clearTimeout(timeout); }
        },
        close() { response.destroy(); req.destroy(); },
    };
}
async function api(port: number, method: string, path: string, body: unknown) {
    const response = await fetch(`http://127.0.0.1:${port}${path}`, {
        method, headers: { Authorization: `Bearer ${token()}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    });
    const text = await response.text();
    assert.equal(response.status, 200, `${method} ${path} on ${port}: ${text}`);
    assert.equal(JSON.parse(text).code, 0, text);
}
const linkView = (snapshot: LinkSnapshot) => {
    assert.equal(snapshot?.code, 0, JSON.stringify(snapshot).slice(0, 500));
    return (snapshot.data?.list ?? snapshot.delta?.upsert)?.find((item) => item.id === linkId);
};
// Link list SSE: a committed write to one link reaches every API instance as an
// entity delta of that row, published through the outbox and the live stream.
const linkId = crypto.randomUUID();
const marker = `multi-instance-${crypto.randomUUID()}`;
const eventsPath = `/v1/link/events?page=1&pageSize=10&keyword=${encodeURIComponent(marker)}`;
const target = (index: number) => ({ id: `target-${index}`, name: `${marker}-${String(index).padStart(5, '0')}`.padEnd(100, 'n'),
    ip: `127.0.0.${index % 250 + 1}`.padEnd(200, '0'), port: 502, status: 'enabled' });
const endpoint = (targets: number) => ({ transport: 'tcp', mode: 'TCP Client', targets: Array.from({ length: targets }, (_, index) => target(index)) });
let streams: SnapshotStream[] = [];
let raw: PausedStream | undefined;
try {
    await db`INSERT INTO link(id,name,protocol,endpoint,created_by,execution,status)
        VALUES(${linkId},${marker},'Modbus',${endpoint(1)}::jsonb,${admin},'collector','disabled')`;
    streams = await Promise.all(ports.map((port) => open(port, eventsPath)));
    const initial = await Promise.all(streams.map((stream) => stream.next()));
    assert(initial.every((snapshot) => linkView(snapshot)?.debug_enabled === false), 'both instances must return the link snapshot');

    // A write handled by the second instance must reach the streams of both.
    await api(ports[1], 'PUT', `/v1/link/${linkId}/debug`, { enabled: true });
    const updates = await Promise.all(streams.map((stream) => stream.next()));
    assert(updates.every((snapshot) => linkView(snapshot)?.debug_enabled === true), 'one committed write must reach both API streams');
    console.log('PASS one committed write reached the SSE streams of both API instances');

    await streams[0].close();
    streams[0] = await open(ports[1], eventsPath);
    assert.equal(linkView(await streams[0].next())?.debug_enabled, true, 'reconnect must receive current committed state');
    console.log('PASS reconnect receives latest committed snapshot');

    await Promise.all(streams.map((stream) => stream.close()));
    streams = [];
    // About 4 MiB of targets: one snapshot exceeds the application receive buffer.
    const targetCount = 10000;
    await db`UPDATE link SET endpoint=${endpoint(targetCount)}::jsonb WHERE id=${linkId}`;
    const complete = await open(ports[0], eventsPath);
    streams.push(complete);
    const completeView = linkView(await complete.next());
    assert.equal(completeView?.endpoint?.targets?.length, targetCount, 'large SSE snapshot must be complete and parseable');
    assert.equal(completeView.endpoint.targets.at(-1).name, target(targetCount - 1).name);

    raw = await rawPaused(ports[0], eventsPath);
    // Changes while the subscriber is paused: the stream must deliver the latest
    // state after resuming instead of an unbounded backlog of older snapshots.
    const latestName = `${marker}-latest`;
    await api(ports[1], 'PUT', `/v1/link/${linkId}/debug`, { enabled: false });
    await db`UPDATE link SET name=${latestName} WHERE id=${linkId}`;
    await api(ports[1], 'PUT', `/v1/link/${linkId}/debug`, { enabled: true });
    // Kernel capacity varies; require a complete latest snapshot after resuming.
    await wait(Number(Bun.env.LIVE_QUERY_SLOW_PAUSE_MS ?? 1500));
    // Each change re-queries committed state, so an earlier event may already
    // carry the rename; the latest state has both the rename and debug enabled.
    const { snapshot: rawSnapshot, bytes } = await raw.readLatest((item) => item.name === latestName && item.debug_enabled);
    assert.equal(linkView(rawSnapshot)?.endpoint?.targets?.length, targetCount, 'latest snapshot must be complete');
    assert(bytes > 4 * 1024 * 1024, `paused snapshot stream produced only ${bytes} bytes`);
    console.log('PASS paused raw TCP receiver recovered latest large snapshot');
} finally {
    raw?.close();
    for (const stream of streams) await stream.close().catch(() => {});
    await db`UPDATE link SET deleted_at=NOW(), debug_enabled=false WHERE id=${linkId}`;
    await db.close();
}
