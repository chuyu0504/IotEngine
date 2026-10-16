import { describe, expect, test } from 'bun:test';
import { summarizeDeviceCommandResult } from '../web/pages/device/device.service';
import type { Device } from '../web/pages/device/device.types';
import { validateValue } from '../web/pages/device/index';

const status = (
    values: Partial<Device.CommandStatusesResult['statuses'][number]>
): Device.CommandStatusesResult['statuses'][number] => ({
    command_id: 'command-1',
    device_id: 'device-1',
    device_code: 'DEV-1',
    protocol: 'Modbus',
    status: 'SUCCEEDED',
    ...values,
});

describe('device command result summary', () => {
    test('reports readback values on success', () => {
        expect(
            summarizeDeviceCommandResult({
                complete: true,
                statuses: [
                    status({
                        actual_values: [
                            {
                                element_id: 'level',
                                name: '水位',
                                kind: 'DOUBLE',
                                value: '1.5',
                                unit: 'm',
                            },
                        ],
                    }),
                ],
            })
        ).toEqual({ failed: false, message: '设备执行成功，回读：水位=1.5m' });
    });

    test('warns against resending when the write acknowledgement is missing', () => {
        const summary = summarizeDeviceCommandResult({
            complete: true,
            statuses: [status({ status: 'UNKNOWN', reason: 'write_ack_missing' })],
        });
        expect(summary.failed).toBeTrue();
        expect(summary.message).toContain('未收到写入应答，指令可能已执行');
        expect(summary.message).toContain('勿直接重发');
    });

    test('warns against resending when readback cannot confirm the final state', () => {
        const summary = summarizeDeviceCommandResult({
            complete: true,
            statuses: [status({ status: 'UNKNOWN', reason: 'readback_timeout' })],
        });
        expect(summary.failed).toBeTrue();
        expect(summary.message).toContain('回读未能确认最终设备状态');
        expect(summary.message).toContain('勿直接重发');
    });
});

describe('device command validation', () => {
    test('rejects negative BCD values because the wire format has no sign', () => {
        expect(
            validateValue({
                _key: 'level',
                elementId: 'level',
                name: '水位',
                value: '-12.34',
                encode: 'BCD',
                length: 2,
                digits: 2,
            })
        ).not.toBeNull();
    });

    test('measures STRING limits in UTF-8 bytes', () => {
        expect(
            validateValue({
                _key: 'name',
                elementId: 'name',
                name: '名称',
                value: '水位',
                dataType: 'STRING',
                size: 2,
            })
        ).not.toBeNull();
    });

    test('rejects JavaScript-only numeric literals', () => {
        expect(
            validateValue({
                _key: 'bcd',
                elementId: 'bcd',
                name: 'BCD',
                value: '0x10',
                encode: 'BCD',
                length: 2,
            })
        ).not.toBeNull();
        expect(
            validateValue({
                _key: 'float',
                elementId: 'float',
                name: '浮点数',
                value: '0b10',
                dataType: 'FLOAT32',
            })
        ).not.toBeNull();
    });
});
