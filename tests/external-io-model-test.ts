import { describe, it, expect } from 'bun:test';
import { externalIoAvailableChannels, externalIoCompatible } from '../web/pages/protocol/protocol.service';
import type { ExternalIoConfig, ExternalIoHardwareModel } from '../web/pages/protocol/protocol.types';
import { protocolCreateSchema } from '../web/pages/protocol/protocol.schema';
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
    const hardware: ExternalIoHardwareModel = {
        model: config.edgeNodeModel!,
        channels: [
            { kind: 'DI', channel: 'di0', controllable: false },
            { kind: 'DO', channel: 'do0', controllable: true },
            { kind: 'AI', channel: 'AI0', controllable: false },
        ],
    };
    it('creates an empty scheme before adding channel points', () => {
        expect(protocolCreateSchema.safeParse({name:'IO scheme',protocol:'ExternalIO',config:{storagePolicy:'report',edgeNodeModel:hardware.model,points:[]}}).success).toBe(true);
        expect(externalIoCompatible({...config,points:[]},node)).toBe(false);
    });
    it('offers only unused model channels and preserves the channel while editing', () => {
        expect(externalIoAvailableChannels(hardware, config.points).map(channel=>channel.channel)).toEqual(['di0','AI0']);
        expect(externalIoAvailableChannels(hardware, config.points, config.points[0].id)).toEqual(hardware.channels);
        const points = hardware.channels.map(channel=>({id:crypto.randomUUID(),name:channel.channel,kind:channel.kind,channel:channel.channel}));
        expect(externalIoAvailableChannels(hardware, points)).toEqual([]);
        expect(externalIoAvailableChannels(undefined, [])).toEqual([]);
        expect(externalIoAvailableChannels({...hardware,channels:[{kind:'DO',channel:'do0',controllable:false}]},[])).toEqual([]);
        expect(hardware.channels).toHaveLength(3);
        expect(config.points[0].channel).toBe('do0');
    });
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
