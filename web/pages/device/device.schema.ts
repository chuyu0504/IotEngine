import { z } from 'zod';
import { formatJson } from '@/utils/json';
import type { DeviceParameterDefinition } from '../protocol/protocol.types';

const statusSchema = z.enum(['enabled', 'disabled']);
const timezoneSchema = z.string().regex(/^[+-](0\d|1[0-4]):[0-5]\d$/, '设备时区格式必须为 ±HH:MM');
const packetSchema = z
    .object({
        mode: z.enum(['OFF', 'HEX', 'ASCII']),
        content: z.string().optional(),
    })
    .superRefine(({ mode, content }, context) => {
        if (mode === 'OFF') return;
        if (!content?.length) {
            context.addIssue({
                code: 'custom',
                path: ['content'],
                message: '报文内容不能为空',
            });
            return;
        }
        if (mode === 'ASCII' && content.length > 256) {
            context.addIssue({
                code: 'custom',
                path: ['content'],
                message: 'ASCII 报文不能超过 256 字节',
            });
            return;
        }
        if (mode !== 'HEX') return;
        const normalized = content.replace(/\s/g, '');
        if (!/^(?:[0-9A-Fa-f]{2})+$/.test(normalized)) {
            context.addIssue({
                code: 'custom',
                path: ['content'],
                message: 'HEX 内容必须由完整字节组成',
            });
        } else if (normalized.length / 2 > 256) {
            context.addIssue({
                code: 'custom',
                path: ['content'],
                message: 'HEX 报文不能超过 256 字节',
            });
        }
    });
export const deviceParametersSchema = z.preprocess(
    (value, context) => {
        if (typeof value !== 'string') return value;
        if (!value.trim()) return {};
        try {
            return JSON.parse(formatJson(value));
        } catch (error) {
            context.addIssue({ code: 'custom', message: (error as Error).message });
            return z.NEVER;
        }
    },
    z
        .record(
            z
                .string()
                .min(1)
                .refine(
                    (name) =>
                        name === name.trim() &&
                        !/[\r\n\t]/.test(name) &&
                        new TextEncoder().encode(name).length <= 100,
                    '设备参数名须为 1–100 字节且不能有首尾空格'
                ),
            z.unknown()
        )
        .superRefine((value, context) => {
            if (
                Object.keys(value).length > 256 ||
                new TextEncoder().encode(JSON.stringify(value)).length > 262144
            )
                context.addIssue({ code: 'custom', message: '设备参数最多 256 项、256 KiB' });
        })
);
export function deviceParameterInputSchema(type: DeviceParameterDefinition['dataType']) {
    if (type === 'BOOL')
        return z
            .enum(['true', 'false'])
            .transform((value) => value === 'true')
            .optional();
    if (type === 'DOUBLE')
        return z
            .number()
            .finite()
            .nullish()
            .transform((value) => value ?? undefined);
    if (type === 'JSON')
        return z
            .string()
            .transform((value, context) => {
                if (!value.trim()) return undefined;
                try {
                    return JSON.parse(formatJson(value));
                } catch (error) {
                    context.addIssue({ code: 'custom', message: (error as Error).message });
                    return z.NEVER;
                }
            })
            .optional();
    return z.string().optional();
}
export function deviceParameterValues(
    definitions: DeviceParameterDefinition[],
    inputs: Record<string, unknown>,
    current: Record<string, unknown> = {}
) {
    const values = { ...current };
    for (const definition of definitions) {
        const value = deviceParameterInputSchema(definition.dataType).parse(
            inputs[definition.name]
        );
        if (value === undefined) {
            if (current[definition.name] !== null) delete values[definition.name];
        } else values[definition.name] = value;
    }
    return values;
}
export const saveDeviceSchema = z
    .object({
        name: z.string().trim().min(1, '设备名称不能为空').max(100, '设备名称最多100个字符'),
        device_code: z
            .string()
            .trim()
            .min(1, '设备编码不能为空')
            .max(100, '设备编码最多100个字符')
            .regex(/^[A-Za-z0-9_-]+$/, '设备编码只能包含字母、数字、连字符和下划线'),
        link_id: z.uuid({ error: '请选择有效本地链路' }).optional(),
        edge_connection: z
            .object({
                name: z.string().min(1).max(100),
                edge_node_id: z.uuid(),
                protocol: z.enum(['Modbus', 'SL651', 'S7', 'MC', 'FINS', 'DLT645']),
                endpoint: z.object({
                    transport: z.enum(['serial', 'tcp']),
                    interface: z.string().min(1).max(96),
                    mode: z.enum(['TCP Client', 'TCP Server']),
                    ip: z.string(),
                    port: z.number().int(),
                    targets: z.array(z.never()),
                    baud_rate: z.number().int().min(300).max(4000000).optional(),
                    data_bits: z.number().int().min(5).max(8).optional(),
                    stop_bits: z.number().int().min(1).max(2).optional(),
                    parity: z.enum(['none', 'even', 'odd']).optional(),
                }),
                status: statusSchema,
            })
            .optional(),
        target_id: z.string().trim().max(100).optional(),
        protocol_config_id: z.uuid({ error: '请选择有效设备类型' }),
        group_id: z.uuid({ error: '请选择有效设备分组' }).nullish(),
        status: statusSchema.default('enabled'),
        online_timeout: z.coerce.number().int().min(1).max(86400).default(300),
        remote_control: z.boolean().default(true),
        modbus_mode: z.enum(['TCP', 'RTU']).nullish(),
        slave_id: z.coerce.number().int().min(1).max(247).nullish(),
        timezone: timezoneSchema.default('+08:00'),
        heartbeat: packetSchema.default({ mode: 'OFF' }),
        registration: packetSchema.default({ mode: 'OFF' }),
        parameters: deviceParametersSchema.optional(),
        remark: z.string().optional(),
    })
    .refine((value) => Boolean(value.link_id) !== Boolean(value.edge_connection), {
        message: '请选择本地链路或配置边缘设备连接',
        path: ['link_id'],
    });
export const saveDeviceGroupSchema = z.object({
    name: z.string().trim().min(1, '分组名称不能为空').max(100, '分组名称最多100个字符'),
    parent_id: z.uuid({ error: '请选择有效上级分组' }).nullish(),
    status: statusSchema.default('enabled'),
    sort_order: z.coerce.number().int().min(0).default(0),
    remark: z.string().optional(),
});
export const deviceIdSchema = z.uuid({ error: 'id 必须是 UUID' });
const commandElementSchema = z.object({
    elementId: z.uuid({ error: '下发要素 ID 必须是 UUID' }),
    value: z.string().trim().min(1, '下发要素值不能为空').max(4096, '下发要素值最多 4096 个字符'),
});
export const deviceCommandSchema = z
    .object({
        idempotency_key: z.uuid().optional(),
        mqttMessageId: z.uuid().optional(),
        elements: z
            .array(commandElementSchema)
            .min(1, '请至少选择一个下发要素')
            .max(256, '单次最多下发 256 个要素'),
    })
    .superRefine(({ elements }, context) => {
        const ids = new Set<string>();
        elements.forEach((element, index) => {
            if (!ids.has(element.elementId)) {
                ids.add(element.elementId);
                return;
            }
            context.addIssue({
                code: 'custom',
                path: ['elements', index, 'elementId'],
                message: '一次下发不能包含重复要素',
            });
        });
    });
const deviceShareSchema = z.object({
    subject_type: z.enum(['user', 'department']),
    subject_id: z.uuid({ error: '分享对象 ID 必须是 UUID' }),
    access_level: z.enum(['view', 'operate']),
});
export const replaceDeviceSharesSchema = z
    .object({
        shares: z.array(deviceShareSchema).max(500, '单次最多设置 500 个分享对象'),
    })
    .superRefine(({ shares }, context) => {
        const uniqueSubjects = new Set<string>();
        shares.forEach((share, index) => {
            const key = `${share.subject_type}:${share.subject_id}`;
            if (!uniqueSubjects.has(key)) {
                uniqueSubjects.add(key);
                return;
            }
            context.addIssue({
                code: 'custom',
                path: ['shares', index, 'subject_id'],
                message: '分享对象不能重复',
            });
        });
    });

export const deviceDebugSchema = z.object({ enabled: z.boolean() });
