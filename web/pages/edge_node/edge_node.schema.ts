import { z } from 'zod';

export const terminalSessionSchema = z.object({ id: z.uuid() });
export const terminalSizeSchema = z.object({
    columns: z.number().int().min(1).max(1000),
    rows: z.number().int().min(1).max(1000),
});
export const terminalEventsSchema = z.object({
    events: z
        .array(
            z.discriminatedUnion('kind', [
                z.object({ kind: z.literal('ready') }),
                z.object({
                    kind: z.literal('data'),
                    content: z.string().max(21848),
                    sequence: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
                }),
                z.object({ kind: z.literal('close'), reason: z.string().max(4096) }),
            ])
        )
        .max(256),
});

import { pageParamsSchema } from '@/utils/pagination';

export const serialSettingsSchema = z.object({
    baudRate: z
        .number()
        .int()
        .refine(
            (value) =>
                [
                    300, 600, 1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200, 230400, 460800,
                ].includes(value),
            '请选择支持的波特率'
        ),
    dataBits: z.number().int().min(5).max(8),
    stopBits: z.union([z.literal(1), z.literal(2)]),
    parity: z.enum(['none', 'even', 'odd']),
    rs485: z.boolean(),
});
export const serialDebugOpenSchema = z.object({
    path: z
        .string()
        .min(1)
        .max(96)
        .regex(/^\/dev\/[^\r\n]+$/, '串口路径无效'),
});
export const serialDebugEventSchema = z.object({
    kind: z.enum(['state', 'data', 'sent', 'error', 'closed']),
    requestId: z.number().int().nonnegative().optional(),
    manual: z.boolean().optional(),
    direction: z.enum(['RX', 'TX', '']).optional(),
    hex: z
        .string()
        .max(2048)
        .regex(/^(?:[0-9a-fA-F]{2})*$/)
        .optional(),
    timestamp: z.number().optional(),
    sequence: z.number().int().nonnegative().optional(),
    droppedBytes: z.number().int().nonnegative().optional(),
    message: z.string().optional(),
    settings: z
        .object({
            path: z.string(),
            baudRate: z.number(),
            dataBits: z.number(),
            stopBits: z.number(),
            parity: z.string(),
            rs485: z.boolean(),
        })
        .optional(),
});

const enrollmentStatusSchema = z.enum(['pending', 'approved']);
function parseIpv4(value: string) {
    const parts = value.split('.');
    if (parts.length !== 4) return null;
    const numbers = parts.map((part) => {
        if (!/^(0|[1-9]\d{0,2})$/.test(part)) return Number.NaN;
        return Number(part);
    });
    if (numbers.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) return null;
    return numbers.reduce((result, part) => (result * 256 + part) >>> 0, 0);
}
const ipv4TextSchema = z
    .string()
    .refine((value) => parseIpv4(value) !== null, '请输入有效的 IPv4 地址');
const networkDeviceSchema = z
    .string()
    .min(1, '请选择网卡')
    .max(32)
    .regex(/^[A-Za-z0-9_.:-]+$/, '网卡名称包含非法字符');
const logicalInterfaceNameSchema = z
    .string()
    .min(1, '逻辑接口名称不能为空')
    .max(15, '逻辑接口名称不能超过 15 个字符')
    .regex(/^[A-Za-z0-9_]+$/, '逻辑接口名称只能包含字母、数字和下划线')
    .refine((value) => value !== 'loopback', 'loopback 接口不允许远程修改');
export const networkInterfaceSchema = z
    .object({
        operation: z.enum(['upsert', 'delete']),
        name: logicalInterfaceNameSchema,
        previousName: logicalInterfaceNameSchema.optional(),
        mode: z.enum(['dhcp', 'static']).optional(),
        device: z.union([z.literal(''), networkDeviceSchema]).optional(),
        bridge: z.boolean().optional(),
        bridgePorts: z.array(networkDeviceSchema).max(8).optional(),
        ip: z.union([z.literal(''), ipv4TextSchema]).optional(),
        prefixLength: z.number().int().min(0).max(30).optional(),
        gateway: z.union([z.literal(''), ipv4TextSchema]).optional(),
    })
    .superRefine((value, context) => {
        if (value.previousName) {
            if (value.operation !== 'upsert' || value.previousName === value.name) {
                context.addIssue({
                    code: 'custom',
                    path: ['previousName'],
                    message: '原逻辑接口名称无效',
                });
            }
        }
        if (value.operation === 'delete') return;
        if (!value.mode) {
            context.addIssue({
                code: 'custom',
                path: ['mode'],
                message: '请选择 DHCP 或静态 IPv4',
            });
        }
        if (value.bridge) {
            if (value.name.length > 12) {
                context.addIssue({
                    code: 'custom',
                    path: ['name'],
                    message: '网桥逻辑名称不能超过 12 个字符',
                });
            }
            if (!value.bridgePorts?.length) {
                context.addIssue({
                    code: 'custom',
                    path: ['bridgePorts'],
                    message: '请至少选择一个网桥成员',
                });
            }
        } else if (!value.device) {
            context.addIssue({
                code: 'custom',
                path: ['device'],
                message: '请选择一个网卡',
            });
        }
        if (value.mode === 'dhcp') {
            if (value.ip || value.gateway || (value.prefixLength ?? 0) !== 0) {
                context.addIssue({
                    code: 'custom',
                    path: ['ip'],
                    message: 'DHCP 接口不能填写静态 IPv4',
                });
            }
            return;
        }
        if (value.mode !== 'static') return;
        const address = value.ip ? parseIpv4(value.ip) : null;
        const prefix = value.prefixLength ?? 0;
        if (address === null) {
            context.addIssue({ code: 'custom', path: ['ip'], message: '请输入有效的 IPv4 地址' });
            return;
        }
        if (prefix < 1 || prefix > 30) {
            context.addIssue({
                code: 'custom',
                path: ['prefixLength'],
                message: 'IPv4 前缀必须在 1 - 30 之间',
            });
            return;
        }
        const mask = (0xffffffff << (32 - prefix)) >>> 0;
        const hostMask = ~mask >>> 0;
        const host = address & hostMask;
        if (host === 0 || host === hostMask) {
            context.addIssue({
                code: 'custom',
                path: ['ip'],
                message: 'IP 不能是网络地址或广播地址',
            });
        }
        if (value.gateway) {
            const gateway = parseIpv4(value.gateway);
            if (
                gateway === null ||
                (gateway & mask) !== (address & mask) ||
                gateway === address ||
                (gateway & hostMask) === 0 ||
                (gateway & hostMask) === hostMask
            ) {
                context.addIssue({
                    code: 'custom',
                    path: ['gateway'],
                    message: '网关必须是同网段内不同的合法主机地址',
                });
            }
        }
    });
export const networkSchema = z
    .object({
        interfaces: z.array(networkInterfaceSchema).min(1).max(8),
        rollbackTimeoutSec: z.number().int().min(30).max(300),
    })
    .superRefine((value, context) => {
        const names = new Set<string>();
        const previousNames = new Set<string>();
        const devices = new Set<string>();
        value.interfaces.forEach((item, index) => {
            if (names.has(item.name)) {
                context.addIssue({
                    code: 'custom',
                    path: ['interfaces', index, 'name'],
                    message: '同一请求不能重复配置逻辑接口',
                });
            }
            names.add(item.name);
            if (item.previousName) {
                if (previousNames.has(item.previousName)) {
                    context.addIssue({
                        code: 'custom',
                        path: ['interfaces', index, 'previousName'],
                        message: '同一请求不能重复修改原逻辑接口',
                    });
                }
                previousNames.add(item.previousName);
            }
            if (item.operation === 'delete') return;
            const selected = item.bridge
                ? (item.bridgePorts ?? [])
                : item.device
                  ? [item.device]
                  : [];
            selected.forEach((device) => {
                if (devices.has(device)) {
                    context.addIssue({
                        code: 'custom',
                        path: ['interfaces', index, item.bridge ? 'bridgePorts' : 'device'],
                        message: '同一网卡不能重复分配',
                    });
                }
                devices.add(device);
            });
        });
    });
export const firmwareUpgradeSchema = z.object({
    file: z
        .instanceof(File)
        .refine(
            (file) => file.size > 0 && file.size <= 128 * 1024 * 1024,
            '固件必须在 1 B 到 128 MiB 之间'
        ),
    keepSettings: z.boolean(),
});
export const nodeNameSchema = z.object({
    name: z.string().min(1, '节点名称不能为空').max(100, '节点名称不能超过 100 个字符'),
});
export const nodeGroupSchema = z.object({
    groupId: z.union([z.literal(''), z.uuid('节点分组 ID 无效')]),
});
export const edgeGroupSchema = z.object({
    name: z.string().trim().min(1, '分组名称不能为空').max(100, '分组名称不能超过 100 个字符'),
    parentId: z.union([z.literal(''), z.uuid('上级分组 ID 无效')]).optional(),
    status: z.enum(['enabled', 'disabled']),
    sortOrder: z.number().int().min(0, '分组排序不能小于 0'),
    remark: z.string().max(500, '分组备注不能超过 500 个字符').optional(),
});
export const edgeListQuerySchema = pageParamsSchema.extend({
    status: enrollmentStatusSchema.optional(),
    keyword: z.string().optional(),
    groupId: z.union([z.literal('ungrouped'), z.uuid()]).optional(),
});
export const logsQuerySchema = z.object({
    limit: z.number().int().min(1).max(48).optional(),
    level: z.enum(['debug', 'info', 'warn', 'error']).optional(),
    source: z.string().max(16).optional(),
});
export const logLevelSchema = z.object({
    level: z.enum(['silent', 'debug', 'info', 'warn', 'error']),
});
export const edgeIdSchema = z.uuid('节点 ID 必须是 UUID');

export const dtuAsciiPacketSchema = z
    .string()
    .max(256, '报文最多 256 字节')
    .refine(
        (value) => Array.from(value).every((char) => char.charCodeAt(0) <= 127),
        'ASCII 不支持中文或其他非 ASCII 字符'
    )
    .transform((value) =>
        Array.from(value, (char) => char.charCodeAt(0).toString(16).padStart(2, '0'))
            .join('')
            .toUpperCase()
    );

export const dtuChannelSchema = z
    .object({
        channelId: z.uuid(),
        name: z.string().trim().min(1, '请输入通道名称').max(100),
        enabled: z.boolean(),
        southMode: z.enum(['serial', 'tcp_client', 'tcp_server']),
        southHost: z.string().trim().max(253).optional(),
        southPort: z.number().int().min(1).max(65535).optional(),
        northHost: z.string().trim().min(1, '请输入北向服务器').max(253),
        northPort: z.number().int().min(1).max(65535),
        serialPath: z.string().max(96).optional(),
        baudRate: z.number().int().optional(),
        dataBits: z.number().int().min(5).max(8).optional(),
        stopBits: z.number().int().min(1).max(2).optional(),
        parity: z.enum(['none', 'even', 'odd']).optional(),
        rs485: z.boolean().optional(),
        maxClients: z.number().int().min(1).max(16),
        queueBytes: z.number().int().min(4096).max(65536),
        serialFrameMs: z.number().int().min(0).max(1000),
        uplinkOnly: z.boolean().optional(),
        debugEnabled: z.boolean().optional(),
        registrationHex: z
            .string()
            .max(512)
            .regex(/^(?:[0-9a-fA-F]{2})*$/, '请输入完整的 HEX 字节')
            .optional(),
        heartbeatHex: z
            .string()
            .max(512)
            .regex(/^(?:[0-9a-fA-F]{2})*$/, '请输入完整的 HEX 字节')
            .optional(),
        heartbeatIntervalSec: z.number().int().min(0).max(86400).optional(),
    })
    .superRefine((value, context) => {
        if ((value.heartbeatIntervalSec ?? 0) > 0 && !value.heartbeatHex)
            context.addIssue({ code: 'custom', path: ['heartbeatHex'], message: '请输入心跳包' });
        if (value.southMode === 'serial') {
            if (!value.serialPath?.startsWith('/dev/'))
                context.addIssue({
                    code: 'custom',
                    path: ['serialPath'],
                    message: '请选择节点串口',
                });
        } else if (!value.southHost || !value.southPort) {
            context.addIssue({
                code: 'custom',
                path: ['southHost'],
                message: '请输入南向地址和端口',
            });
        }
    });

export const ioControlSchema = z.object({
    channel: z.string().min(3).max(16),
    value: z.boolean(),
    idempotencyKey: z.uuid(),
});
