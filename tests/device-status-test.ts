import { describe, expect, test } from 'bun:test';
import {
    getDeviceCardElements,
    getExternalIoDerivedStart,
    isDeviceOnline,
} from '../web/pages/device/device.service';
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
    test('follows the server-computed runtime state', () => {
        expect(isDeviceOnline(device({ runtimeState: 'online' }))).toBeTrue();
        expect(
            isDeviceOnline(device({ runtimeState: 'offline', runtimeReason: 'data_stale' }))
        ).toBeFalse();
    });

    test('ignores client clocks and stale report times when the server says online', () => {
        expect(
            isDeviceOnline(
                device({
                    runtimeState: 'online',
                    reportTime: '2000-01-01T00:00:00.000Z',
                    online_timeout: 1,
                })
            )
        ).toBeTrue();
    });

    test('treats a device without runtime state as offline', () => {
        expect(isDeviceOnline(device({}))).toBeFalse();
    });

    test('does not treat a connected TCP link as online without data', () => {
        expect(
            isDeviceOnline(
                device({ connected: true, connectionState: 'connected', runtimeState: 'offline' })
            )
        ).toBeFalse();
    });

    test('does not treat an edge connection as online after the server marks data stale', () => {
        expect(
            isDeviceOnline(
                device({
                    connected: true,
                    edge_node_id: 'edge-1',
                    edge_transport: 'tcp',
                    edgeStatus: { state: 'connected' },
                    runtimeState: 'offline',
                    runtimeReason: 'data_stale',
                })
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

test('external IO cards locate derived values after all configured native points', () => {
    const value = device({
        protocol_type: 'ExternalIO',
        element_count: 3,
        elements: [
            { id: 'raw-a', name: 'DI0', value: 0 },
            { id: 'raw-hidden', name: 'DI1', value: 0, visible: false },
            { id: 'raw-b', name: 'DO0', value: 0 },
            { id: 'derived-a', name: '派生一', value: 1 },
            { id: 'derived-b', name: '派生二', value: 2 },
        ],
    });
    const visible = getDeviceCardElements(value);
    expect(visible.map((element) => element.id)).toEqual(['raw-a', 'raw-b', 'derived-a', 'derived-b']);
    expect(getExternalIoDerivedStart(value, visible)).toBe(2);
    expect(getExternalIoDerivedStart({ ...value, protocol_type: 'Modbus' }, visible)).toBe(-1);
    expect(getExternalIoDerivedStart({ ...value, element_count: undefined }, visible)).toBe(-1);
    expect(getExternalIoDerivedStart({ ...value, element_count: 9 }, visible)).toBe(-1);
});
