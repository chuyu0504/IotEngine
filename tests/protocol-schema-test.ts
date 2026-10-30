import { expect, test } from 'bun:test';

import {
    defaultConfig,
    getModbusDeviceTypeFormValues,
    getS7DeviceTypeFormValues,
    numberOrDefault,
} from '../web/pages/protocol/protocol.service';
import { protocolCreateSchema } from '../web/pages/protocol/protocol.schema';
import { derivedPointSchema } from '../web/utils/derived_point';

test('外部 IO 与采集协议平级，校验点位类型、单位和唯一标识', () => {
    const point = { id: '00000000-0000-4000-8000-000000000001', name: '电流', kind: 'AI', channel: 'AI0', unit: 'uA' };
    const input = { name: '外部 IO', protocol: 'ExternalIO', config: { points: [point] } };
    expect(protocolCreateSchema.safeParse(input).success).toBe(true);
    expect(protocolCreateSchema.safeParse({ ...input, config: { points: [point, point] } }).success).toBe(false);
    expect(protocolCreateSchema.safeParse({ ...input, config: { points: [{ ...point, channel: 'di0' }] } }).success).toBe(false);
    expect(protocolCreateSchema.safeParse({ ...input, config: { points: [point, { ...point, id: '00000000-0000-4000-8000-000000000002' }] } }).success).toBe(false);
    expect(protocolCreateSchema.safeParse({ ...input, config: { points: [{ ...point, unit: 'mA' }] } }).success).toBe(true);
    expect(protocolCreateSchema.safeParse({ ...input, config: { points: [{ ...point, unit: 'A' }] } }).success).toBe(false);
    expect(protocolCreateSchema.safeParse({ ...input, config: { points: [{ ...point, kind: 'DO' }] } }).success).toBe(false);
});

test('读取上报缺省为五分钟，显式间隔与命令快速读取保持不变', () => {
    expect(defaultConfig().readInterval).toBe(300);
    for (const form of [getModbusDeviceTypeFormValues, getS7DeviceTypeFormValues]) {
        expect(form().readInterval).toBe(300);
        expect(form().commandFastReadInterval).toBe(1);
        for (const interval of [1, 5, 60, 300, 3600]) {
            const value = form({
                id: 'test', name: 'test', protocol: 'Modbus', enabled: true,
                config: { readInterval: interval, commandFastReadInterval: 2 },
            });
            expect(value.readInterval).toBe(interval);
            expect(value.commandFastReadInterval).toBe(2);
        }
    }
});

test('派生值结构校验把缺失公式、重复变量、窗口输入和单位条件定位到字段', () => {
    const point = {
        id: '00000000-0000-4000-8000-000000000001',
        name: '压力',
        kind: 'expression',
        valueType: 'number',
        unitMode: 'conditional',
        expression: 'x / 1000',
        unitRules: [{ condition: 'x > 1000', unit: 'MPa' }],
        inputs: [{ alias: 'x', pointId: '00000000-0000-4000-8000-000000000002' }],
        maxAgeSeconds: 300,
        visible: false,
    };
    expect(derivedPointSchema.safeParse(point).success).toBe(true);
    const issuePaths = (value: unknown) => {
        const result = derivedPointSchema.safeParse(value);
        return result.success ? [] : result.error.issues.map((issue) => issue.path);
    };
    expect(issuePaths({ ...point, expression: ' ' })).toEqual([['expression']]);
    expect(
        issuePaths({ ...point, inputs: [point.inputs[0], { ...point.inputs[0] }] })
    ).toEqual([['inputs']]);
    expect(issuePaths({ ...point, unitRules: [] })).toEqual([['unitRules']]);
    expect(
        issuePaths({ ...point, kind: 'average', expression: undefined, windowSeconds: 60 })
    ).toEqual([['sourceAlias']]);
    expect(
        issuePaths({
            ...point,
            kind: 'average',
            expression: undefined,
            windowSeconds: 60,
            sourceAlias: 'x',
        })
    ).toEqual([]);
});

test('S7 protocol schema rejects malformed area fields', () => {
    const result = protocolCreateSchema.safeParse({
        name: 's7-test',
        protocol: 'S7',
        config: {
            plcModel: 'S7-1200',
            connection: {},
            areas: [
                {
                    id: 'temperature',
                    name: 'Temperature',
                    area: 'DB',
                    dataType: 'INT16',
                    dbNumber: 1,
                    start: 'bad',
                    size: 2,
                },
            ],
        },
    });

    expect(result.success).toBe(false);
});

test('Modbus protocol schema rejects fractional register addresses', () => {
    const result = protocolCreateSchema.safeParse({
        name: 'modbus-test',
        protocol: 'Modbus',
        config: {
            byteOrder: 'BIG_ENDIAN',
            registers: [
                {
                    id: 'r1',
                    name: 'Register 1',
                    registerType: 'HOLDING_REGISTER',
                    dataType: 'INT16',
                    address: 1.5,
                    quantity: 1,
                },
            ],
        },
    });

    expect(result.success).toBe(false);
});

test('Modbus helpers normalize legacy numeric strings before save', () => {
    expect(numberOrDefault('5', 1)).toBe(5);
    expect(numberOrDefault('bad', 1)).toBe(1);
});
