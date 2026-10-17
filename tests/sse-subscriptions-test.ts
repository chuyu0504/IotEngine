import { getEdgeInventory, observeEdgeDetail, getLogs, getEdgeVpnState } from '../web/pages/edge_node/edge_node.api';
import { getDeviceList, getDeviceRealtimeSnapshot, getDeviceGroupsWithCount, getDeviceCommandStatuses, getDebugPackets as getDevicePackets } from '../web/pages/device/device.api';
import { expect, spyOn, test } from 'bun:test';
import { queryUrl } from '../web/lib/http';
import { type LiveSession, LiveSubscriptions } from '../web/lib/live_subscriptions';
import { SseTransport } from '../web/lib/sse_transport';
import { consumeServerSentEvents } from '../web/lib/sse';
import { getList as getLinks, getDebugPackets } from '../web/pages/link/link.api';
import { useAuthStore } from '../web/store/auth_store';

const tick = () => new Promise<void>(resolve => setTimeout(resolve, 0));
function sseRegistry(options: {
    session: () => LiveSession;
    refresh: () => Promise<boolean>;
    fetch: typeof fetch;
    retryDelay?: (attempt: number, signal: AbortSignal) => Promise<void>;
}) {
    const { fetch, retryDelay, ...session } = options;
    return new LiveSubscriptions({ ...session, transport: new SseTransport({ fetch, retryDelay }) });
}
function connection() {
    let writer!: ReadableStreamDefaultController<Uint8Array>;
    let cancelled = false;
    const stream = new ReadableStream<Uint8Array>({
        start(controller) { writer = controller; },
        cancel() { cancelled = true; },
    });
    return {
        response: new Response(stream, { headers: { 'Content-Type': 'text/event-stream' } }),
        send(event: string, data: unknown) {
            writer.enqueue(new TextEncoder().encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
        },
        keepalive() {
            writer.enqueue(new TextEncoder().encode(': keep'));
            writer.enqueue(new TextEncoder().encode('alive\n\n'));
        },
        close() { writer.close(); },
        error() { writer.error(new TypeError('upstream connection lost')); },
        cancelled: () => cancelled,
    };
}

test('one healthy SSE connection shares snapshots and cancels only after the last observer leaves', async () => {
    const wire = connection();
    let calls = 0;
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async (url, init) => {
            calls++;
            expect(String(url)).toBe('/v1/device/events?group=A%26B');
            expect(new Headers(init?.headers).get('Accept')).toBe('text/event-stream');
            expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer token');
            return wire.response;
        }) as typeof fetch,
    });
    const values: unknown[][] = [[], []];
    const errors: Error[] = [];
    const stream = subscriptions.create(queryUrl('/v1/device/events', { group: 'A&B' }), 'snapshot');
    const first = stream.subscribe({ next: value => values[0].push(value), error: error => errors.push(error) });
    const second = stream.subscribe({ next: value => values[1].push(value), error: error => errors.push(error) });
    await tick();
    wire.send('snapshot', { code: 0, data: { voltage: 12 } });
    wire.keepalive();
    wire.send('snapshot', { code: 0, data: { voltage: 13 } });
    await tick();
    expect(calls).toBe(1);
    expect(values[0]).toEqual([{ voltage: 12 }, { voltage: 13 }]);
    expect(values[1]).toEqual(values[0]);
    first();
    expect(wire.cancelled()).toBe(false);
    second();
    await tick();
    expect(wire.cancelled()).toBe(true);
    expect(errors).toEqual([]);
});

test('disconnect retries are bounded, and release cancels an open stream', async () => {
    let calls = 0, delays = 0;
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async () => { calls++; throw new TypeError('network failure'); }) as typeof fetch,
        retryDelay: async () => { delays++; },
    });
    const errors: Error[] = [];
    const release = subscriptions.create('/v1/link/events', 'snapshot').subscribe({ next: () => {}, error: error => errors.push(error) });
    await tick();
    expect(calls).toBe(9);
    expect(delays).toBe(8);
    expect(errors).toHaveLength(1);
    release();
});

test('account replacement aborts old subscriptions and discards a late response', async () => {
    let session = { token: 'old', userId: 'old-user' };
    const wire = connection();
    let finish!: (response: Response) => void;
    const subscriptions = sseRegistry({
        session: () => session, refresh: async () => false,
        fetch: (() => new Promise<Response>(resolve => { finish = resolve; })) as typeof fetch,
    });
    const values: unknown[] = [], errors: Error[] = [];
    subscriptions.create('/v1/link/events', 'snapshot').subscribe({ next: value => values.push(value), error: error => errors.push(error) });
    await tick();
    session = { token: 'new', userId: 'new-user' };
    subscriptions.sessionChanged();
    finish(wire.response);
    await tick();
    expect(wire.cancelled()).toBe(true);
    expect(values).toEqual([]);
    expect(errors).toHaveLength(1);
});

test('expired stream refreshes its token and reconnects with a fresh snapshot', async () => {
    let session = { token: 'old', userId: 'user' };
    let calls = 0, refreshes = 0;
    const wire = connection();
    const subscriptions = sseRegistry({
        session: () => session,
        refresh: async () => { refreshes++; session = { ...session, token: 'fresh' }; return true; },
        fetch: (async (_url, init) => {
            calls++;
            if (calls === 1) return new Response(JSON.stringify({code: 11005, message: 'expired'}), {status: 401});
            expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer fresh');
            return wire.response;
        }) as typeof fetch,
    });
    const values: unknown[] = [], errors: Error[] = [];
    const release = subscriptions.create('/v1/link/events', 'snapshot').subscribe({ next: value => values.push(value), error: error => errors.push(error) });
    await tick();
    wire.send('snapshot', { code: 0, data: { id: 'user' } });
    await tick();
    expect([calls, refreshes]).toEqual([2, 1]);
    expect(values).toEqual([{ id: 'user' }]);
    expect(errors).toEqual([]);
    release();
});

test('a token still rejected after one refresh fails closed instead of refreshing again', async () => {
    let session = { token: 'old', userId: 'user' };
    let calls = 0, refreshes = 0;
    const subscriptions = sseRegistry({
        session: () => session,
        refresh: async () => { refreshes++; session = { ...session, token: `fresh-${refreshes}` }; return true; },
        fetch: (async () => {
            calls++;
            return new Response(JSON.stringify({ code: 11005, message: 'expired' }), { status: 401 });
        }) as typeof fetch,
    });
    const errors: Error[] = [];
    const release = subscriptions.create('/v1/link/events', 'snapshot').subscribe({ next: () => {}, error: error => errors.push(error) });
    await tick(); await tick();
    expect([calls, refreshes]).toEqual([2, 1]);
    expect(errors.map(error => error.message)).toEqual(['expired']);
    release();
});

test('business denial ends the subscription without a retry loop', async () => {
    const wire = connection();
    let calls = 0;
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async () => { calls++; return wire.response; }) as typeof fetch,
    });
    const errors: Error[] = [];
    const release = subscriptions.create('/v1/link/events', 'snapshot').subscribe({ next: () => {}, error: error => errors.push(error) });
    await tick();
    wire.send('error', { code: 11007, message: 'denied' });
    await tick();
    expect(errors[0]?.message).toBe('denied');
    expect(calls).toBe(1);
    expect(wire.cancelled()).toBe(true);
    release();
});

test('device channels share one connection, replay their own snapshots and isolate permission errors', async () => {
    const wire = connection();
    let calls = 0;
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async (url) => { expect(String(url)).toBe('/v1/device/events'); calls++; return wire.response; }) as typeof fetch,
    });
    const values: Record<string, unknown[]> = { devices: [], groups: [], realtime: [] };
    const errors: Record<string, Error[]> = { devices: [], groups: [], realtime: [] };
    const streams = ['devices', 'groups', 'realtime'].map(name => subscriptions.create('/v1/device/events', name));
    const releases = streams.map((stream, index) => {
        const name = ['devices', 'groups', 'realtime'][index];
        return stream.subscribe({ next: value => values[name].push(value), error: error => errors[name].push(error) });
    });
    await tick();
    wire.send('devices', { code: 0, data: ['device'] });
    wire.send('groups', { code: 0, data: ['group'] });
    wire.send('realtime', { code: 0, data: { temperature: 10 } });
    wire.send('realtime', { code: 0, data: { temperature: 11 } });
    await tick();
    expect(calls).toBe(1);
    expect(values.devices).toEqual([['device']]);
    expect(values.groups).toEqual([['group']]);
    expect(values.realtime).toEqual([{ temperature: 10 }, { temperature: 11 }]);
    expect(await streams[0].first()).toEqual(['device']);
    expect(await streams[2].first()).toEqual({ temperature: 11 });
    wire.send('groups', { code: 11007, message: 'group access revoked' });
    wire.send('realtime', { code: 0, data: { temperature: 12 } });
    await tick();
    expect(errors.groups[0]?.message).toBe('group access revoked');
    expect(errors.devices).toEqual([]);
    expect(errors.realtime).toEqual([]);
    await expect(streams[1].first()).rejects.toThrow('group access revoked');
    expect(values.realtime.at(-1)).toEqual({ temperature: 12 });
    wire.send('groups', { code: 0, data: ['restored-group'] });
    await tick();
    expect(await streams[1].first()).toEqual(['restored-group']);
    releases[0]();
    releases[1]();
    expect(wire.cancelled()).toBe(false);
    releases[2]();
    await tick();
    expect(wire.cancelled()).toBe(true);
    expect(calls).toBe(1);
});

test('entity deltas merge into the page value, keep untouched rows and leave the shared user channel alone', async () => {
    const wire = connection();
    let calls = 0;
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async () => { calls++; return wire.response; }) as typeof fetch,
    });
    type Row = { id: string; online: boolean };
    type Page = { list: Row[]; total: number; page: number };
    const users: unknown[] = [], pages: Page[] = [], deltas: unknown[] = [], merged: Page[] = [], errors: Error[] = [];
    const releaseUser = subscriptions.createShared('user').subscribe({ next: value => users.push(value), error: error => errors.push(error) });
    const devices = subscriptions.create<Page>('/v1/device/events', 'devices');
    const releasePlain = devices.subscribe({ next: value => pages.push(value), error: error => errors.push(error) });
    const releaseDelta = devices.subscribe({
        next: value => merged.push(value),
        delta: (delta, value) => { deltas.push(delta); merged.push(value); },
        error: error => errors.push(error),
    });
    await tick();
    wire.send('user', { code: 0, data: { id: 'user' } });
    wire.send('devices', { code: 0, data: { list: [{ id: 'a', online: false }, { id: 'b', online: false }, { id: 'c', online: false }], total: 30, page: 1 } });
    wire.send('devices', { code: 0, message: 'ok', delta: { upsert: [{ id: 'b', online: true }, { id: 'unknown', online: true }], remove: [] } });
    wire.send('devices', { code: 0, message: 'ok', delta: { upsert: [], remove: ['c', 'missing'] } });
    wire.send('devices', { code: 0, message: 'ok', delta: { upsert: [{ id: 'unknown', online: false }], remove: ['missing'] } });
    await tick();
    expect(calls).toBe(1);
    expect(pages).toHaveLength(3);
    const [initial, updated, removed] = pages;
    expect(updated.list.map(row => row.online)).toEqual([false, true, false]);
    expect(updated.list[0]).toBe(initial.list[0]);
    expect(updated.list[2]).toBe(initial.list[2]);
    expect(updated.total).toBe(30);
    expect(removed).toEqual({ list: [{ id: 'a', online: false }, { id: 'b', online: true }], total: 29, page: 1 });
    expect(removed.list[1]).toBe(updated.list[1]);
    expect(deltas).toEqual([
        { upsert: [{ id: 'b', online: true }, { id: 'unknown', online: true }], remove: [] },
        { upsert: [], remove: ['c', 'missing'] },
    ]);
    expect(merged).toEqual(pages);
    expect(await devices.first()).toBe(removed);
    expect(users).toEqual([{ id: 'user' }]);
    wire.send('devices', { code: 0, data: { list: [{ id: 'd', online: true }], total: 1, page: 1 } });
    await tick();
    expect(pages.at(-1)).toEqual({ list: [{ id: 'd', online: true }], total: 1, page: 1 });
    expect(merged.at(-1)).toBe(pages.at(-1));
    expect(errors).toEqual([]);
    releasePlain(); releaseDelta(); releaseUser();
    await tick();
    expect(wire.cancelled()).toBe(true);
});

test('a delta without a base snapshot rejects only its channel, while a malformed delta ends the source', async () => {
    const wires = [connection(), connection()];
    let calls = 0;
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async () => wires[calls++].response) as typeof fetch,
    });
    const errors: Record<string, Error[]> = { devices: [], realtime: [] };
    const values: Record<string, unknown[]> = { devices: [], realtime: [] };
    const releases = ['devices', 'realtime'].map(name => subscriptions.create('/v1/device/events', name)
        .subscribe({ next: value => values[name].push(value), error: error => errors[name].push(error) }));
    await tick();
    wires[0].send('realtime', { code: 0, data: { list: [{ id: 'a' }], total: 1 } });
    wires[0].send('devices', { code: 0, message: 'ok', delta: { upsert: [{ id: 'a' }], remove: [] } });
    await tick();
    expect(errors.devices.map(error => error.message)).toEqual(['实时增量缺少基准快照']);
    expect(errors.realtime).toEqual([]);
    wires[0].send('devices', { code: 0, data: { list: [{ id: 'a' }], total: 1 } });
    wires[0].send('realtime', { code: 0, message: 'ok', delta: { upsert: [{ id: 'a' }], remove: [7] } });
    await tick();
    expect(values.devices).toEqual([{ list: [{ id: 'a' }], total: 1 }]);
    expect(errors.realtime.map(error => error.message)).toEqual(['实时增量格式无效']);
    expect(errors.devices.map(error => error.message)).toEqual(['实时增量缺少基准快照', '实时增量格式无效']);
    expect(calls).toBe(1);
    releases.forEach(release => release());
});

test('link list and selected debug packets share one wire and isolate packet updates', async () => {
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
    Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
        getItem: () => null, setItem: () => {}, removeItem: () => {},
    } });
    const session = useAuthStore.getState();
    const wire = connection();
    const requests: string[] = [];
    const fetch = spyOn(globalThis, 'fetch').mockImplementation(async url => {
        requests.push(String(url));
        return wire.response;
    });
    const releases: Array<() => void> = [];
    try {
        useAuthStore.getState().setAuth('link-token', 'refresh', {
            id: 'user', username: 'tester', status: 'enabled', roles: [], permissions: [],
        });
        const id = '00000000-0000-7000-8000-000000000123';
        const query = { page: 1, pageSize: 10, keyword: 'A&B' };
        const links: unknown[] = [], packets: unknown[] = [], errors: Error[] = [];
        releases.push(getLinks(query, id).subscribe({ next: value => links.push(value), error: error => errors.push(error) }));
        releases.push(getDebugPackets(id, query).subscribe({ next: value => packets.push(value), error: error => errors.push(error) }));
        await tick();
        expect(requests).toHaveLength(1);
        expect(requests[0]).toContain('/v1/link/events?');
        expect(requests[0]).toContain(`debugLinkId=${id}`);
        expect(requests[0]).toContain('keyword=A%26B');
        wire.send('links', { code: 0, data: { list: [{ id }], total: 1 } });
        wire.send('packets', { code: 0, data: [] });
        wire.send('packets', { code: 0, data: [{ id: 'acquisition' }] });
        await tick();
        expect(links).toHaveLength(1);
        expect(packets).toEqual([[], [{ id: 'acquisition' }]]);
        releases.pop()?.();
        expect(wire.cancelled()).toBe(false);
        releases.pop()?.();
        await tick();
        expect(wire.cancelled()).toBe(true);
        expect(errors).toEqual([]);
    } finally {
        releases.forEach(release => release());
        await tick();
        fetch.mockRestore();
        useAuthStore.setState(session);
        if (previousStorage) Object.defineProperty(globalThis, 'sessionStorage', previousStorage);
        else Reflect.deleteProperty(globalThis, 'sessionStorage');
    }
});

test('device page API channels share one wire including debug packets and 256 command results', async () => {
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
    Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
        getItem: () => null, setItem: () => {}, removeItem: () => {},
    } });
    const session = useAuthStore.getState();
    const wire = connection();
    const requests: string[] = [];
    const fetch = spyOn(globalThis, 'fetch').mockImplementation(async url => {
        requests.push(String(url));
        return wire.response;
    });
    const releases: Array<() => void> = [];
    try {
        useAuthStore.getState().setAuth('device-token', 'refresh', {
            id: 'user', username: 'tester', status: 'enabled', roles: [], permissions: [],
        });
        const id = '00000000-0000-7000-8000-000000000123';
        const commandIds = Array.from({ length: 256 }, () => crypto.randomUUID());
        const commands: unknown[] = [];
        const links: unknown[] = [], packets: unknown[] = [], errors: Error[] = [];
        releases.push(getDeviceList(id, commandIds).subscribe({ next: value => links.push(value), error: error => errors.push(error) }));
        releases.push(getDevicePackets(id, commandIds).subscribe({ next: value => packets.push(value), error: error => errors.push(error) }));
        releases.push(getDeviceRealtimeSnapshot(id, commandIds).subscribe({ next: () => {}, error: error => errors.push(error) }));
        releases.push(getDeviceGroupsWithCount(id, commandIds).subscribe({ next: () => {}, error: error => errors.push(error) }));
        releases.push(getDeviceCommandStatuses(commandIds, id).subscribe({ next: value => commands.push(value), error: error => errors.push(error) }));
        await tick();
        expect(requests).toHaveLength(1);
        expect(requests[0]).toContain('/v1/device/events?');
        expect(requests[0]).toContain(`debugDeviceId=${id}`);
        expect(new URL(requests[0], 'http://localhost').searchParams.get('commandIds')?.split(',')).toHaveLength(256);
        wire.send('devices', { code: 0, data: { list: [{ id }], total: 1 } });
        wire.send('commands', { code: 0, data: { complete: false, statuses: [] } });
        wire.send('commands', { code: 0, data: { complete: true, statuses: [] } });
        wire.send('packets', { code: 0, data: [] });
        wire.send('packets', { code: 0, data: [{ id: 'acquisition' }] });
        await tick();
        expect(commands).toHaveLength(2);
        expect(links).toHaveLength(1);
        expect(packets).toEqual([[], [{ id: 'acquisition' }]]);
        releases.pop()?.();
        expect(wire.cancelled()).toBe(false);
        releases.forEach(release => release());
        await tick();
        expect(wire.cancelled()).toBe(true);
        expect(errors).toEqual([]);
    } finally {
        releases.forEach(release => release());
        await tick();
        fetch.mockRestore();
        useAuthStore.setState(session);
        if (previousStorage) Object.defineProperty(globalThis, 'sessionStorage', previousStorage);
        else Reflect.deleteProperty(globalThis, 'sessionStorage');
    }
});

test('shared user information observes the page connection without a fallback after leaving', async () => {
    const requests: string[] = [];
    const wires: ReturnType<typeof connection>[] = [];
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async (url, init) => {
            if (wires.length) expect(wires.at(-1)?.cancelled()).toBe(true);
            const path = String(url);
            expect(new Headers(init?.headers).get('X-SSE-User')).toBe('1');
            requests.push(path);
            const wire = connection(); wires.push(wire); return wire.response;
        }) as typeof fetch,
    });
    const users: unknown[] = [], pages: unknown[] = [], errors: Error[] = [];
    const userStream = subscriptions.createShared('user');
    const user = userStream.subscribe({ next: value => users.push(value), error: error => errors.push(error) });
    const page = subscriptions.create('/v1/device/events', 'devices').subscribe({ next: value => pages.push(value), error: error => errors.push(error) });
    await tick();
    expect(requests).toEqual(['/v1/device/events']);
    wires[0].send('devices', { code: 0, data: ['device'] });
    wires[0].send('user', { code: 0, data: { id: 'user', nickname: 'first' } });
    await tick();
    expect(users).toEqual([{ id: 'user', nickname: 'first' }]);
    expect(pages).toEqual([['device']]);
    expect(await userStream.first()).toEqual(users[0]);
    await tick(); expect(requests).toHaveLength(1);
    page(); await tick(); await tick();
    expect(requests).toEqual(['/v1/device/events']);
    expect(wires[0].cancelled()).toBe(true);
    user(); await tick();
    expect(requests).toHaveLength(1);
    expect(errors).toEqual([]);
});

test('a replacement waits until the cancelled response reader has finished releasing', async () => {
    let finishCancellation!: () => void;
    const cancelled = new Promise<void>(resolve => { finishCancellation = resolve; });
    const calls: string[] = [];
    const firstResponse = new Response(new ReadableStream<Uint8Array>({
        cancel: () => cancelled,
    }), { headers: { 'Content-Type': 'text/event-stream' } });
    const replacement = connection();
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async url => {
            calls.push(String(url));
            return calls.length === 1 ? firstResponse : replacement.response;
        }) as typeof fetch,
    });
    const observer = { next: () => {}, error: () => {} };
    const leave = subscriptions.create('/v1/link/events', 'snapshot').subscribe(observer);
    await tick();
    leave();
    const release = subscriptions.create(queryUrl('/v1/link/events', { debugLinkId: 'link' }), 'snapshot').subscribe(observer);
    await tick();
    expect(calls).toEqual(['/v1/link/events']);
    finishCancellation();
    await tick();
    expect(calls).toEqual(['/v1/link/events', '/v1/link/events?debugLinkId=link']);
    release();
    await tick();
    expect(replacement.cancelled()).toBe(true);
});

test('page transitions cancel the previous wire without opening an intermediate user connection', async () => {
    const requests: string[] = [], wires: ReturnType<typeof connection>[] = [];
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async url => { requests.push(String(url)); const wire = connection(); wires.push(wire); return wire.response; }) as typeof fetch,
    });
    const user = subscriptions.createShared('user').subscribe({ next: () => {}, error: () => {} });
    await tick(); expect(requests).toEqual([]);
    const first = subscriptions.create('/v1/alert/events', 'records').subscribe({ next: () => {}, error: () => {} });
    await tick(); expect(wires[0].cancelled()).toBe(false);
    first();
    const second = subscriptions.create('/v1/link/events', 'links').subscribe({ next: () => {}, error: () => {} });
    await tick();
    expect(requests).toEqual(['/v1/alert/events', '/v1/link/events']);
    expect(wires[0].cancelled()).toBe(true);
    user(); await tick(); expect(wires[1].cancelled()).toBe(false);
    second(); await tick(); expect(wires[1].cancelled()).toBe(true);
    expect(requests).toHaveLength(2);
});

test('a denied page does not turn its permission error into a user-profile failure', async () => {
    const requests: string[] = [], wires: ReturnType<typeof connection>[] = [];
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async url => { requests.push(String(url)); const wire = connection(); wires.push(wire); return wire.response; }) as typeof fetch,
    });
    const userErrors: Error[] = [], pageErrors: Error[] = [], users: unknown[] = [];
    const user = subscriptions.createShared('user').subscribe({ next: value => users.push(value), error: error => userErrors.push(error) });
    const page = subscriptions.create('/v1/edge/events', 'snapshot').subscribe({ next: () => {}, error: error => pageErrors.push(error) });
    await tick(); wires[0].send('snapshot', { code: 11007, message: 'page denied' });
    await tick(); await tick();
    expect(requests).toEqual(['/v1/edge/events']);
    expect(wires[0].cancelled()).toBe(true);
    expect(pageErrors[0]?.message).toBe('page denied');
    expect(userErrors).toEqual([]);
    expect(users).toEqual([]);
    user(); page(); await tick();
});

test('shared user snapshots clear on token refresh and never leak across account replacement', async () => {
    let session = { token: 'old', userId: 'first' };
    const wires: ReturnType<typeof connection>[] = [], tokens: string[] = [];
    const subscriptions = sseRegistry({
        session: () => session, refresh: async () => false,
        fetch: (async (_url, init) => {
            tokens.push(new Headers(init?.headers).get('Authorization') ?? '');
            const wire = connection(); wires.push(wire); return wire.response;
        }) as typeof fetch,
    });
    const users: unknown[] = [], errors: Error[] = [];
    const shared = subscriptions.createShared('user');
    const firstUser = shared.subscribe({ next: value => users.push(value), error: error => errors.push(error) });
    const firstPage = subscriptions.create('/v1/device/events', 'devices').subscribe({ next: () => {}, error: () => {} });
    await tick(); wires[0].send('user', { code: 0, data: { id: 'first', nickname: 'before' } });
    await tick();
    session = { ...session, token: 'fresh' }; subscriptions.sessionChanged();
    await tick(); expect(wires[0].cancelled()).toBe(true);
    wires[1].send('user', { code: 0, data: { id: 'first', nickname: 'after' } });
    await tick();
    expect(users).toEqual([{ id: 'first', nickname: 'before' }, { id: 'first', nickname: 'after' }]);
    expect(tokens).toEqual(['Bearer old', 'Bearer fresh']);
    session = { token: 'second-token', userId: 'second' }; subscriptions.sessionChanged();
    await tick(); expect(wires[1].cancelled()).toBe(true);
    expect(errors).toHaveLength(1);
    const replacement: unknown[] = [];
    const secondUser = shared.subscribe({ next: value => replacement.push(value), error: () => {} });
    const secondPage = subscriptions.create('/v1/device/events', 'devices').subscribe({ next: () => {}, error: () => {} });
    await tick(); expect(replacement).toEqual([]);
    wires[2].send('user', { code: 0, data: { id: 'second' } });
    await tick(); expect(replacement).toEqual([{ id: 'second' }]);
    expect(users).toHaveLength(2);
    firstUser(); firstPage(); secondUser(); secondPage(); await tick();
    expect(wires[2].cancelled()).toBe(true);
});


test('API-prefixed paths can share the user channel without changing their URLs', async () => {
    const wire = connection();
    const urls: string[] = [];
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async url => { urls.push(String(url)); return wire.response; }) as typeof fetch,
    });
    const errors: Error[] = [], values: unknown[] = [];
    const user = subscriptions.createShared('user').subscribe({ next: () => {}, error: error => errors.push(error) });
    const page = subscriptions.create('/api/example/events', 'snapshot').subscribe({ next: value => values.push(value), error: error => errors.push(error) });
    await tick(); expect(urls).toEqual(['/api/example/events']);
    wire.send('snapshot', { code: 0, data: [] });
    wire.send('user', { code: 0, data: { id: 'user' } });
    await tick(); expect(values).toEqual([[]]); expect(errors).toEqual([]);
    user(); page(); await tick(); expect(wire.cancelled()).toBe(true);
});

test('manual retry after exhaustion restores the existing live observer beyond the first snapshot', async () => {
    let online = false, calls = 0;
    const wire = connection();
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        retryDelay: async () => {},
        fetch: (async () => { calls++; if (!online) throw new TypeError('offline'); return wire.response; }) as typeof fetch,
    });
    const values: unknown[] = [], errors: Error[] = [];
    const stream = subscriptions.create('/v1/device/events', 'devices');
    const release = stream.subscribe({ next: value => values.push(value), error: error => errors.push(error) });
    try {
        await tick(); expect(calls).toBe(9); expect(errors).toHaveLength(1);
        online = true;
        const restored = stream.first(undefined, { fresh: true });
        await tick(); wire.send('devices', { code: 0, data: ['restored'] });
        expect(await restored).toEqual(['restored']);
        await tick(); expect(wire.cancelled()).toBe(false);
        wire.send('devices', { code: 0, data: ['later update'] });
        await tick(); expect(values).toEqual([['restored'], ['later update']]);
        expect(calls).toBe(10);
    } finally { release(); await tick(); }
    expect(wire.cancelled()).toBe(true);
});


test('a late query reader reuses a healthy channel snapshot without reconnecting', async () => {
    const wires: ReturnType<typeof connection>[] = [];
    const subscriptions = sseRegistry({
        session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
        fetch: (async () => { const wire = connection(); wires.push(wire); return wire.response; }) as typeof fetch,
    });
    const list = subscriptions.create('/v1/device/events', 'devices');
    const metrics = subscriptions.create('/v1/device/events', 'realtime');
    const release = list.subscribe({ next: () => {}, error: () => {} });
    try {
        await tick();
        wires[0].send('devices', { code: 0, data: [] });
        wires[0].send('realtime', { code: 0, data: ['current'] });
        await tick();
        const result = metrics.first(AbortSignal.timeout(100), { fresh: true });
        await tick();
        expect(wires).toHaveLength(1);
        expect(await result).toEqual(['current']);
        expect(wires[0].cancelled()).toBe(false);
    } finally { release(); await tick(); }
});


test('edge page shares inventory, detail, logs and VPN with isolated named events', async () => {
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
    Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
        getItem: () => null, setItem: () => {}, removeItem: () => {},
    } });
    const session = useAuthStore.getState(), wire = connection();
    const requests: string[] = [], errors: Error[] = [], nodes: unknown[] = [], logs: unknown[] = [];
    const fetch = spyOn(globalThis, 'fetch').mockImplementation(async url => {
        requests.push(String(url)); return wire.response;
    });
    const releases: Array<() => void> = [];
    try {
        useAuthStore.getState().setAuth('edge-token', 'refresh', {
            id: 'user', username: 'tester', status: 'enabled', roles: [], permissions: [],
        });
        const scope = { nodeId: crypto.randomUUID(), logs: { limit: 48 }, vpn: true };
        const error = (value: Error) => errors.push(value);
        releases.push(getEdgeInventory(scope).subscribe({ next: value => nodes.push(value), error }));
        releases.push(observeEdgeDetail(scope).subscribe({ next: () => {}, error }));
        releases.push(getLogs(scope).subscribe({ next: value => logs.push(value), error }));
        releases.push(getEdgeVpnState(scope).subscribe({ next: () => {}, error }));
        await tick(); expect(requests).toHaveLength(1);
        expect(requests[0]).toContain('/v1/edge/events?');
        expect(requests[0]).toContain(`nodeId=${scope.nodeId}`);
        wire.send('nodes', { code: 0, data: Array.from({length: 201}, (_, id) => ({id: String(id), name: 'node'})) });
        wire.send('detail', { code: 0, data: { id: scope.nodeId } });
        wire.send('vpn', { code: 0, data: { peers: [], routes: [] } });
        wire.send('logs', { code: 0, data: { lines: ['first'] } });
        wire.send('logs', { code: 0, data: { lines: ['later'] } });
        wire.send('nodes', { code: 0, message: 'ok', delta: { upsert: [{ id: '7', name: 'renamed' }], remove: ['200'] } });
        await tick(); expect(nodes).toHaveLength(2); expect(logs).toHaveLength(2);
        const [initial, merged] = nodes as Array<Array<{ id: string; name: string }>>;
        expect(merged).toHaveLength(200);
        expect(merged[7]).toEqual({ id: '7', name: 'renamed' });
        expect(merged[6]).toBe(initial[6]);
        expect(await getEdgeInventory(scope).first()).toBe(merged);
        releases.pop()?.(); await tick(); expect(wire.cancelled()).toBe(false);
        expect(errors).toEqual([]);
    } finally {
        releases.forEach(release => release()); await tick();
        expect(wire.cancelled()).toBe(true);
        fetch.mockRestore(); useAuthStore.setState(session);
        if (previousStorage) Object.defineProperty(globalThis, 'sessionStorage', previousStorage);
        else Reflect.deleteProperty(globalThis, 'sessionStorage');
    }
});


test('video status reads use HTTP while device changes keep one SSE', async () => {
    const { getHealth, getRecording, getDevices } = await import('../web/pages/gb28181/gb28181.api');
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
    Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
        getItem: () => null, setItem: () => {}, removeItem: () => {},
    } });
    const session = useAuthStore.getState(), wire = connection();
    const requests: Array<{url:string; accept:string|null; method:string}> = [];
    const fetch = spyOn(globalThis, 'fetch').mockImplementation(async (url, init) => {
        const accept = new Headers(init?.headers).get('Accept');
        requests.push({url:String(url), accept, method:init?.method ?? 'GET'});
        return accept === 'text/event-stream' ? wire.response
            : new Response(JSON.stringify({code:0,data:{recording:true,enabled:true}}), {headers:{'Content-Type':'application/json'}});
    });
    let release: (() => void) | undefined;
    try {
        useAuthStore.getState().setAuth('video-token', 'refresh', {
            id: 'user', username: 'tester', status: 'enabled', roles: [], permissions: [],
        });
        const changes: unknown[] = [];
        release = getDevices().subscribe({next:value => changes.push(value),error:error=>{throw error;}});
        expect((await getHealth()).enabled).toBe(true);
        expect((await getRecording({streamId:'camera/1'})).recording).toBe(true);
        await tick();
        expect(requests.filter(item=>item.accept==='text/event-stream').map(item=>item.url)).toEqual(['/v1/gb28181/devices/events']);
        expect(requests.filter(item=>item.accept!=='text/event-stream').map(item=>item.url)).toEqual(['/v1/gb28181/health','/v1/gb28181/streams/camera%2F1/recording']);
        expect(requests.every(item=>item.method==='GET')).toBe(true);
        wire.send('snapshot',{code:0,data:{items:[{id:'camera',online:true}]}});
        await tick(); expect(changes).toHaveLength(1);
        expect(requests).toHaveLength(3);
    } finally {
        release?.(); await tick(); expect(wire.cancelled()).toBe(true);
        fetch.mockRestore(); useAuthStore.setState(session);
        if (previousStorage) Object.defineProperty(globalThis, 'sessionStorage', previousStorage);
        else Reflect.deleteProperty(globalThis, 'sessionStorage');
    }
});

test('a silent SSE has no idle timer and stays open until explicitly aborted', async () => {
    const wire = connection(), controller = new AbortController();
    const timer = spyOn(globalThis, 'setTimeout');
    let ended = false;
    const consumed = consumeServerSentEvents(wire.response.body!, () => {}, controller.signal)
        .finally(() => { ended = true; });
    try {
        await Promise.resolve();
        await Promise.resolve();
        expect(timer).not.toHaveBeenCalled();
        expect(ended).toBe(false);
        expect(wire.cancelled()).toBe(false);
    } finally {
        timer.mockRestore();
        controller.abort();
        await consumed;
    }
    expect(wire.cancelled()).toBe(true);
});

for (const termination of ['close', 'error'] as const) {
    test(`upstream ${termination} reconnects once and delivers each recovery snapshot once`, async () => {
        const wires = [connection(), connection()];
        let calls = 0;
        const attempts: number[] = [];
        const subscriptions = sseRegistry({
            session: () => ({ token: 'token', userId: 'user' }), refresh: async () => false,
            fetch: (async () => wires[calls++].response) as typeof fetch,
            retryDelay: async attempt => { attempts.push(attempt); },
        });
        const values: unknown[][] = [[], []], errors: Error[] = [];
        const stream = subscriptions.create('/v1/device/events', 'snapshot');
        const releases = values.map(received => stream.subscribe({
            next: value => received.push(value), error: error => errors.push(error),
        }));
        try {
            await tick();
            wires[0].send('snapshot', { code: 0, data: ['initial'] });
            await tick();
            wires[0][termination]();
            await tick();
            expect(calls).toBe(2);
            expect(attempts).toEqual([1]);
            wires[1].send('snapshot', { code: 0, data: ['recovered'] });
            wires[1].send('snapshot', { code: 0, data: ['later'] });
            await tick();
            expect(values[0]).toEqual([['initial'], ['recovered'], ['later']]);
            expect(values[1]).toEqual(values[0]);
            expect(errors).toEqual([]);
            expect(calls).toBe(2);
        } finally {
            releases.forEach(release => release());
            await tick();
        }
        expect(wires[1].cancelled()).toBe(true);
    });
}
