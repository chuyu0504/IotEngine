import { expect, spyOn, test } from 'bun:test';
import { QueryClient, QueryObserver } from '@tanstack/react-query';
import { snapshotCacheObserver } from '../web/hooks/use_snapshot_query';
import { applySnapshotDelta } from '../web/lib/snapshot_stream';
import { getDeviceList } from '../web/pages/device/device.api';
import { useAuthStore } from '../web/store/auth_store';

const tick = () => new Promise<void>(resolve => setTimeout(resolve, 0));

test('row arrays replace named rows in place and keep every other row object', () => {
    const rows = [{ id: 'a', name: 'A' }, { id: 'b', name: 'B' }, { id: 'c', name: 'C' }];
    const merged = applySnapshotDelta(rows, { upsert: [{ id: 'b', name: 'B2' }], remove: ['c'] });
    expect(merged).toEqual([{ id: 'a', name: 'A' }, { id: 'b', name: 'B2' }]);
    expect(merged[0]).toBe(rows[0]);
    expect(rows).toHaveLength(3);
});

test('pages drop removed rows and decrement the total only by rows actually removed', () => {
    const page = { list: [{ id: 'a' }, { id: 'b' }], total: 12, page: 2, pageSize: 2 };
    const merged = applySnapshotDelta(page, { upsert: [], remove: ['a', 'not-on-page'] });
    expect(merged).toEqual({ list: [{ id: 'b' }], total: 11, page: 2, pageSize: 2 });
    expect(merged.list[0]).toBe(page.list[1]);
});

test('upserts of rows outside the snapshot are ignored and an unchanged snapshot keeps its identity', () => {
    const page = { list: [{ id: 'a' }], total: 1 };
    expect(applySnapshotDelta(page, { upsert: [{ id: 'new' }], remove: ['gone'] })).toBe(page);
    const rows = [{ id: 'a' }];
    expect(applySnapshotDelta(rows, { upsert: [rows[0]], remove: [] })).toBe(rows);
});

test('a row both upserted and removed is removed', () => {
    const rows = [{ id: 'a', v: 1 }, { id: 'b', v: 1 }];
    expect(applySnapshotDelta(rows, { upsert: [{ id: 'a', v: 2 }], remove: ['a'] })).toEqual([{ id: 'b', v: 1 }]);
});

test('deltas against payloads that are not row collections are rejected', () => {
    expect(() => applySnapshotDelta({ total: 3 }, { upsert: [], remove: ['a'] })).toThrow('实时增量不适用于当前快照');
    expect(() => applySnapshotDelta([{ name: 'no id' }], { upsert: [], remove: [] })).toThrow('实时增量行缺少 id');
    expect(() => applySnapshotDelta([{ id: 'a' }], { upsert: [{ id: 1 }], remove: [] })).toThrow('实时增量行缺少 id');
});

test('a device delta changes only that row reference in the query cache', async () => {
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
    Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
        getItem: () => null, setItem: () => {}, removeItem: () => {},
    } });
    const session = useAuthStore.getState();
    let writer!: ReadableStreamDefaultController<Uint8Array>;
    let cancelled = false;
    const fetch = spyOn(globalThis, 'fetch').mockImplementation(async () => new Response(new ReadableStream<Uint8Array>({
        start(controller) { writer = controller; },
        cancel() { cancelled = true; },
    }), { headers: { 'Content-Type': 'text/event-stream' } }));
    const send = (data: unknown) => writer.enqueue(new TextEncoder().encode(`event: devices\ndata: ${JSON.stringify(data)}\n\n`));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    const queryKey = ['devices', 'list', undefined, undefined];
    const errors: Error[] = [];
    let received = 0;
    let release: (() => void) | undefined;
    let unobserve: (() => void) | undefined;
    try {
        useAuthStore.getState().setAuth('cache-token', 'refresh', {
            id: 'user', username: 'tester', status: 'enabled', roles: [], permissions: [],
        });
        type Row = { id: string; name: string; online: boolean; values: { temperature: number } };
        type Page = { list: Row[]; total: number };
        const notifications: Page[] = [];
        unobserve = new QueryObserver<Page>(client, { queryKey, enabled: false })
            .subscribe(result => { if (result.data) notifications.push(result.data); });
        release = getDeviceList().subscribe(snapshotCacheObserver<Page>(client, queryKey, {
            received: () => { received++; },
            error: error => errors.push(error),
        }));
        await tick();
        const rows: Row[] = ['a', 'b', 'c'].map(id => ({ id, name: id, online: false, values: { temperature: 20 } }));
        send({ code: 0, data: { list: rows, total: 3 } });
        await tick();
        const initial = client.getQueryData<Page>(queryKey);
        if (!initial) throw new Error('missing initial snapshot');
        send({ code: 0, message: 'ok', delta: { upsert: [{ ...rows[1], online: true, values: { temperature: 21 } }], remove: [] } });
        await tick();
        const updated = client.getQueryData<Page>(queryKey);
        if (!updated) throw new Error('missing merged snapshot');
        expect(updated).not.toBe(initial);
        expect(updated.list[0]).toBe(initial.list[0]);
        expect(updated.list[2]).toBe(initial.list[2]);
        expect(updated.list[1]).not.toBe(initial.list[1]);
        expect(updated.list[1]).toEqual({ id: 'b', name: 'b', online: true, values: { temperature: 21 } });
        expect(updated.total).toBe(3);
        send({ code: 0, message: 'ok', delta: { upsert: [{ ...rows[0] }], remove: [] } });
        await tick();
        // A row upserted with identical content keeps the cached object through structural sharing.
        expect(client.getQueryData<Page>(queryKey)?.list[0]).toBe(initial.list[0]);
        send({ code: 0, message: 'ok', delta: { upsert: [], remove: ['c'] } });
        await tick();
        const removed = client.getQueryData<Page>(queryKey);
        expect(removed?.total).toBe(2);
        expect(removed?.list.map(row => row.id)).toEqual(['a', 'b']);
        expect(removed?.list[1]).toBe(updated.list[1]);
        expect(notifications.at(-1)).toBe(removed);
        expect(received).toBe(4);
        expect(errors).toEqual([]);
    } finally {
        unobserve?.();
        release?.();
        await tick();
        expect(cancelled).toBe(true);
        client.clear();
        fetch.mockRestore();
        useAuthStore.setState(session);
        if (previousStorage) Object.defineProperty(globalThis, 'sessionStorage', previousStorage);
        else Reflect.deleteProperty(globalThis, 'sessionStorage');
    }
});
