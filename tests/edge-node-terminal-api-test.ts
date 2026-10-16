import { afterAll, afterEach, expect, spyOn, test } from 'bun:test';
import { edgeDebugConnection } from '../web/pages/edge_node/edge_node.api';
import {
    acknowledgeTerminalOutput,
    closeTerminal,
    keepTerminalAlive,
    openTerminal,
    resizeTerminal,
    writeTerminal,
} from '../web/pages/edge_node/edge_node.api';

const node = '00000000-0000-7000-8000-000000000001';
const sessionId = '00000000-0000-7000-8000-000000000002';
const send = spyOn(edgeDebugConnection, 'request');
afterEach(() => send.mockReset());
afterAll(() => send.mockRestore());

test('terminal binary input survives JSON transport and shares the native event channel', async () => {
    const calls: { event: string; data: unknown }[] = [];
    send.mockImplementation(async (event, data) => {
        calls.push({ event, data });
        return event === 'edge.terminal.open' ? { id: sessionId } : null;
    });
    expect(await openTerminal(node, 120, 30)).toEqual({ id: sessionId });
    const bytes = Uint8Array.from({ length: 16384 }, (_, index) => index % 256);
    await writeTerminal(node, sessionId, bytes);
    await resizeTerminal(node, sessionId, 80, 24);
    await acknowledgeTerminalOutput(node, sessionId, 3);
    await keepTerminalAlive(node, sessionId);
    await closeTerminal(node, sessionId);
    expect(calls.map(call => call.event)).toEqual([
        'edge.terminal.open', 'edge.terminal.write', 'edge.terminal.resize',
        'edge.terminal.output.ack', 'edge.terminal.keepalive', 'edge.terminal.close',
    ]);
    const input = calls[1].data as { id: string; sessionId: string; content: string };
    expect(input.id).toBe(node);
    expect(input.sessionId).toBe(sessionId);
    expect(Buffer.from(input.content, 'base64')).toEqual(Buffer.from(bytes));
    expect(Object.keys(input).sort()).toEqual(['content', 'id', 'sessionId']);
});

test('invalid terminal input and geometry are rejected before any write is sent', async () => {
    expect(() => writeTerminal(node, sessionId, new Uint8Array())).toThrow();
    expect(() => writeTerminal(node, sessionId, new Uint8Array(16385))).toThrow();
    expect(() => resizeTerminal(node, sessionId, 0, 24)).toThrow();
    expect(() => resizeTerminal(node, sessionId, 80, 1001)).toThrow();
    expect(() => closeTerminal(node, 'invalid')).toThrow();
    expect(send).not.toHaveBeenCalled();
});
