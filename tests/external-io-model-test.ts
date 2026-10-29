import { describe, it, expect } from 'bun:test';
import { externalIoCompatible } from '../web/pages/protocol/protocol.service';
import type { ExternalIoConfig } from '../web/pages/protocol/protocol.types';
import type { Edge } from '../web/pages/edge_node/edge_node.types';
import { ioControlSchema } from '../web/pages/edge_node/edge_node.schema';
const config: ExternalIoConfig = {
    edgeNodeModel: 'Ebyte T113-I ECM30',
    storagePolicy: 'report',
    points: [{ id: crypto.randomUUID(), name: 'output', kind: 'DO', channel: 'do0' }],
};
const node = {
    model: 'Ebyte T113-I ECM30',
    capability: {
        externalIo: {
            supported: true,
            channels: [{ channel: 'do0', kind: 'DO', available: true, controllable: true }],
        },
    },
} as Pick<Edge.Node, 'model' | 'capability'>;
describe('external IO model selection', () => {
    it('matches both model and channel capability', () => {
        expect(externalIoCompatible(config, node)).toBe(true);
        expect(externalIoCompatible(config, { ...node, model: 'another board' })).toBe(false);
        expect(
            externalIoCompatible(config, {
                ...node,
                capability: { ...node.capability, externalIo: { supported: true, channels: [] } },
            })
        ).toBe(false);
    });
    it('rejects missing model and missing node', () => {
        expect(externalIoCompatible({ ...config, edgeNodeModel: undefined }, node)).toBe(false);
        expect(externalIoCompatible(config)).toBe(false);
    });
    it('requires an explicit value and valid operation identity', () => {
        expect(
            ioControlSchema.safeParse({
                channel: 'do0',
                value: true,
                idempotencyKey: crypto.randomUUID(),
            }).success
        ).toBe(true);
        expect(ioControlSchema.safeParse({ channel: 'do0', idempotencyKey: 'bad' }).success).toBe(
            false
        );
    });
});
