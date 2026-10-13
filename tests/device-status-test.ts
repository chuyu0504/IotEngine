import { describe, expect, test } from 'bun:test';
import { getDeviceCardElements, isDeviceOnline } from '../web/pages/device/device.service';
import { type Device } from '../web/pages/device/device.types';

const device = (values: Partial<Device.Overview>): Device.Overview =>
    ({
        id: 'device-1',
        name: '测试设备',
        device_code: 'TEST-1',
        link_id: 'link-1',
        protocol_config_id: 'protocol-1',
        status: 'enabled',
        ...values,
    }) as Device.Overview;

describe('device online state', () => {
    const now = Date.parse('2026-08-13T12:00:00.000Z');

    test('uses the latest valid data time', () => {
        expect(
            isDeviceOnline(
                device({ reportTime: '2026-08-13T11:59:31.000Z', online_timeout: 30 }),
                now
            )
        ).toBeTrue();
    });

    test('does not treat a connected TCP link as online without data', () => {
        expect(
            isDeviceOnline(device({ connected: true, connectionState: 'connected' }), now)
        ).toBeFalse();
    });

    test('does not treat an edge connection as online after data becomes stale', () => {
        expect(
            isDeviceOnline(
                device({
                    connected: true,
                    edge_node_id: 'edge-1',
                    edge_transport: 'tcp',
                    edgeStatus: { state: 'connected' },
                    reportTime: '2026-08-13T11:54:59.000Z',
                    online_timeout: 300,
                }),
                now
            )
        ).toBeFalse();
    });
});

test('MQTT cards exclude control-only values even when they have cached telemetry', () => {
    const value = device({
        cardPointIds: ['report-a', 'report-b', 'derived'],
        elements: [
            { id: 'report-a', name: '测量', value: 12 },
            { id: 'control', name: '控制参数', value: 99 },
            { id: 'report-b', name: '状态', value: 1 },
            { id: 'derived', name: '派生值', value: 24 },
            { id: 'hidden', name: '隐藏原始值', value: 120, visible: false },
        ],
    });
    expect(getDeviceCardElements(value).map((item) => item.id)).toEqual([
        'report-a', 'report-b', 'derived',
    ]);
    expect(getDeviceCardElements({ ...value, cardPointIds: [] })).toEqual([]);
    expect(getDeviceCardElements({ ...value, cardPointIds: undefined })).toHaveLength(4);
});
