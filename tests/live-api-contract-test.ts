import { expect, test } from 'bun:test';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';

async function files(directory: string): Promise<string[]> {
    const entries = await readdir(directory, { withFileTypes: true });
    return (await Promise.all(entries.map((entry) => entry.isDirectory()
        ? files(join(directory, entry.name)) : [join(directory, entry.name)]))).flat();
}

test('business controllers expose HTTP and reserve WS events for edge debugging', async () => {
    const debugEvents: string[] = [];
    for (const path of await files('service/modules')) {
        if (!path.endsWith('.controller.h')) continue;
        const source = await readFile(path, 'utf8');
        expect(source).not.toContain('"/channel"');
        for (const match of source.matchAll(/registry\.add<[^\n]+>\("([^\"]+)"/g)) {
            expect(path.replaceAll('\\', '/')).toBe('service/modules/edge_node/edge_node.controller.h');
            debugEvents.push(match[1]);
        }
    }
    expect(debugEvents.sort()).toEqual([
        'edge.debug.authenticate', 'edge.serial.close', 'edge.serial.command',
        'edge.serial.events.subscribe', 'edge.serial.open', 'edge.terminal.close',
        'edge.terminal.events.subscribe', 'edge.terminal.keepalive', 'edge.terminal.open',
        'edge.terminal.output.ack', 'edge.terminal.resize', 'edge.terminal.write',
    ]);
    const device = await readFile('service/modules/device/device.controller.h', 'utf8');
    expect(device).toContain('RUVIA_GET("/", list, service::middleware::PermissionMiddleware<"iot:device:query">)');
    expect(device).toContain('RUVIA_PUT("/:id", update');
    expect(device).toContain('RUVIA_GET_SSE("/events", deviceEvents, ruvia::query_model<DeviceEventsQuery>)');
    expect(device).not.toContain('RUVIA_GET_SSE("/realtime/events"');
    expect(device).not.toContain('RUVIA_GET_SSE("/groups/tree-count/events"');
    const auth = await readFile('service/modules/system/auth/auth.controller.h', 'utf8');
    expect(auth).toContain('RUVIA_POST("/login", login');
    expect(auth).toContain('RUVIA_POST("/refresh", refresh');
});

test('frontend allows ordinary GET but disables scheduled refetch and universal WS requests', async () => {
    for (const path of await files('web')) {
        if (!/\.[jt]sx?$/.test(path)) continue;
        const source = await readFile(path, 'utf8');
        for (const match of source.matchAll(/refetchInterval\s*:\s*([^,}\n]+)/g))
            expect(match[1].trim()).toBe('false');
        expect(source).not.toContain('/v1/channel');
        expect(source).not.toContain('requestEvent(');
    }
    const api = await readFile('web/pages/device/device.api.ts', 'utf8');
    expect(api).toMatch(/request\.get[<(]/);
    expect(api).toContain('createSseSnapshotStream');
});
