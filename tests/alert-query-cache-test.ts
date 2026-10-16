import { afterEach, expect, spyOn, test } from 'bun:test';
import { QueryClient, QueryClientProvider, QueryObserver } from '@tanstack/react-query';
import { App } from 'antd';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import request from '../web/lib/http';
import { getRecords, getStats } from '../web/pages/alert/alert.api';
import { useAuthStore } from '../web/store/auth_store';
import {
    alertKeys,
    useAlertAcknowledge,
    useAlertBatchAcknowledge,
    useAlertRuleSave,
    useAlertTemplateLoader,
    useAlertTemplateSave,
} from '../web/pages/alert/alert.service';

const clients: QueryClient[] = [];
const disposals: Array<() => void> = [];
afterEach(() => {
    for (const dispose of disposals.splice(0)) dispose();
    for (const client of clients.splice(0)) client.clear();
});

function fixture() {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
    clients.push(client);
    const messages = spyOn(App, 'useApp').mockReturnValue({
        message: { success: () => {} },
    } as ReturnType<typeof App.useApp>);
    disposals.push(() => messages.mockRestore());
    const requests: string[] = [];
    const keys = {
        templates: alertKeys.templates({ page: 1 }),
        rules: alertKeys.rules({ page: 1 }),
        records: alertKeys.records({ status: 'active' }),
        stats: alertKeys.stats(),
    };
    for (const [name, queryKey] of Object.entries(keys)) {
        client.setQueryData(queryKey, {});
        const observer = new QueryObserver(client, {
            queryKey,
            queryFn: async () => { requests.push(name); return {}; },
        });
        disposals.push(observer.subscribe(() => {}));
    }
    let actions!: {
        template: ReturnType<typeof useAlertTemplateSave>;
        rule: ReturnType<typeof useAlertRuleSave>;
        acknowledge: ReturnType<typeof useAlertAcknowledge>;
        acknowledgeBatch: ReturnType<typeof useAlertBatchAcknowledge>;
        load: ReturnType<typeof useAlertTemplateLoader>;
    };
    function Capture() {
        actions = {
            template: useAlertTemplateSave(),
            rule: useAlertRuleSave(),
            acknowledge: useAlertAcknowledge(),
            acknowledgeBatch: useAlertBatchAcknowledge(),
            load: useAlertTemplateLoader(),
        };
        return null;
    }
    renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(Capture)));
    return { client, actions, requests };
}

test('template and rule writes refresh only their own HTTP lists', async () => {
    const { actions, requests } = fixture();
    const put = spyOn(request, 'put').mockResolvedValue(undefined);
    disposals.push(() => put.mockRestore());
    const values = { id: 'template-id', name: 'template', conditions: [], severity: 'warning' as const,
        logic: 'and' as const, silence_duration: 300 };
    await actions.template.mutateAsync(values);
    expect(requests).toEqual(['templates']);
    await actions.rule.mutateAsync({ ...values, id: 'rule-id', device_id: 'device-id' });
    expect(requests).toEqual(['templates', 'rules']);
});

test('acknowledgements do not restart realtime queries or reload configuration', async () => {
    const { actions, requests } = fixture();
    const post = spyOn(request, 'post').mockResolvedValue(undefined);
    disposals.push(() => post.mockRestore());
    await actions.acknowledge.mutateAsync('record-id');
    await actions.acknowledgeBatch.mutateAsync(['record-id']);
    expect(requests).toEqual([]);
    expect(post).toHaveBeenCalledTimes(2);
});

test('template detail loads coalesce concurrent readers but reload on the next edit', async () => {
    const { actions, client } = fixture();
    let resolveDetail!: (value: unknown) => void;
    const pending = new Promise(resolve => { resolveDetail = resolve; });
    const get = spyOn(request, 'get').mockImplementation(() => pending);
    disposals.push(() => get.mockRestore());
    const first = actions.load('template-id');
    const second = actions.load('template-id');
    expect(get).toHaveBeenCalledTimes(1);
    resolveDetail({ id: 'template-id', name: 'first' });
    expect(await first).toEqual(await second);
    expect(client.getQueryData(alertKeys.templateDetail('template-id'))).toEqual({ id: 'template-id', name: 'first' });
    get.mockResolvedValue({ id: 'template-id', name: 'updated' });
    expect(await actions.load('template-id')).toEqual({ id: 'template-id', name: 'updated' });
    expect(get).toHaveBeenCalledTimes(2);
});

test('alert records and statistics share the filtered SSE connection and release it on scope changes', async () => {
    const previousStorage = Object.getOwnPropertyDescriptor(globalThis, 'sessionStorage');
    Object.defineProperty(globalThis, 'sessionStorage', { configurable: true, value: {
        getItem: () => null, setItem: () => {}, removeItem: () => {},
    } });
    const originalSession = useAuthStore.getState();
    const connections: Array<{ url: string; writer: ReadableStreamDefaultController<Uint8Array>; closed: boolean }> = [];
    const fetch = spyOn(globalThis, 'fetch').mockImplementation(async (url) => {
        const entry = { url: String(url), writer: undefined as unknown as ReadableStreamDefaultController<Uint8Array>, closed: false };
        connections.push(entry);
        return new Response(new ReadableStream<Uint8Array>({
            start(writer) { entry.writer = writer; },
            cancel() { entry.closed = true; },
        }), { headers: { 'Content-Type': 'text/event-stream' } });
    });
    const releases: Array<() => void> = [];
    const tick = () => new Promise(resolve => setTimeout(resolve, 0));
    try {
        useAuthStore.getState().setAuth('alert-token', 'refresh', {
            id: 'alert-user', username: 'tester', status: 'enabled', roles: [], permissions: [],
        });
        const records: unknown[] = [], stats: unknown[] = [], errors: Error[] = [];
        const params = { page: 1, pageSize: 20, status: 'active', severity: undefined };
        releases.push(getRecords(params).subscribe({ next: value => records.push(value), error: error => errors.push(error) }));
        releases.push(getStats(params).subscribe({ next: value => stats.push(value), error: error => errors.push(error) }));
        await tick();
        expect(connections).toHaveLength(1);
        expect(connections[0].url).toBe('/v1/alert/events?page=1&pageSize=20&status=active');
        const first = { id: 'record-1', status: 'active', level: 'warning' };
        const second = { id: 'record-2', status: 'active', level: 'warning' };
        connections[0].writer.enqueue(new TextEncoder().encode(
            `event: records\ndata: ${JSON.stringify({ code: 0, data: { list: [first, second], total: 21 } })}\n\n` +
            'event: stats\ndata: {"code":0,"data":{"total":4}}\n\n' +
            'event: stats\ndata: {"code":0,"data":{"total":5}}\n\n' +
            `event: records\ndata: ${JSON.stringify({ code: 0, message: 'ok', delta: { upsert: [{ ...second, level: 'critical' }], remove: [] } })}\n\n` +
            `event: records\ndata: ${JSON.stringify({ code: 0, message: 'ok', delta: { upsert: [], remove: ['record-1'] } })}\n\n`));
        await tick();
        expect(records).toEqual([
            { list: [first, second], total: 21 },
            { list: [first, { ...second, level: 'critical' }], total: 21 },
            { list: [{ ...second, level: 'critical' }], total: 20 },
        ]);
        const [initial, escalated] = records as Array<{ list: unknown[] }>;
        expect(escalated.list[0]).toBe(initial.list[0]);
        expect(await getRecords(params).first()).toEqual({ list: [{ ...second, level: 'critical' }], total: 20 });
        expect(stats).toEqual([{ total: 4 }, { total: 5 }]);
        releases.splice(0).forEach(release => release());
        await tick();
        expect(connections[0].closed).toBe(true);
        const next = { ...params, page: 2 };
        releases.push(getRecords(next).subscribe({ next: () => {}, error: error => errors.push(error) }));
        releases.push(getStats(next).subscribe({ next: () => {}, error: error => errors.push(error) }));
        await tick();
        expect(connections).toHaveLength(2);
        expect(connections.filter(entry => !entry.closed)).toHaveLength(1);
        expect(connections[1].url).toContain('page=2');
        expect(errors).toEqual([]);
    } finally {
        releases.forEach(release => release());
        await tick();
        fetch.mockRestore();
        useAuthStore.setState(originalSession);
        if (previousStorage) Object.defineProperty(globalThis, 'sessionStorage', previousStorage);
        else Reflect.deleteProperty(globalThis, 'sessionStorage');
    }
});
