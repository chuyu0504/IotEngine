import { z } from 'zod';
import { pageParamsSchema } from '@/utils/pagination';
import { PROTOCOL_TYPES } from '../protocol/protocol.types';

const ipv4Schema = z.ipv4({ error: '请输入有效的 IPv4 地址' });
const modeSchema = z.enum(['TCP Server', 'TCP Client'], { error: '链路模式无效' });
const protocolSchema = z.enum(PROTOCOL_TYPES, { error: '协议无效' });
const statusSchema = z.enum(['enabled', 'disabled'], { error: '状态无效' });
const targetSchema = z.object({
    id: z.string().min(1, '目标 ID 不能为空').max(64, '目标 ID 不能超过 64 个字符'),
    name: z.string().min(1, '目标名称不能为空').max(100, '目标名称不能超过 100 个字符'),
    ip: z.string().min(1).max(253),
    port: z.number().int().min(1, '端口必须在 1 - 65535 之间').max(65535),
    status: statusSchema,
    mqtt: z
        .object({
            clientId: z.string().min(1, '请输入 MQTT Client ID').max(128),
            username: z.string().max(1024).optional(),
            password: z.string().max(1024).optional(),
            keepAliveSeconds: z.number().int().min(1).max(65535).optional(),
        })
        .optional(),
});
export const saveLinkSchema = z
    .object({
        execution: z.literal('collector').default('collector'),
        name: z.string().min(1, '链路名称不能为空').max(100, '链路名称不能超过 100 个字符'),
        protocol: protocolSchema,
        endpoint: z.object({
            mode: modeSchema,
            ip: z.string(),
            port: z.number().int().min(0).max(65535),
            targets: z.array(targetSchema).max(100, '单条链路最多配置 100 个目标'),
        }),
        status: statusSchema,
    })
    .superRefine((value, context) => {
        const brokerHostSchema = z.union([
            ipv4Schema,
            z.ipv6(),
            z
                .string()
                .regex(
                    /^(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)*[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.?$/
                ),
        ]);
        value.endpoint.targets.forEach((target, index) => {
            if (
                !(value.protocol === 'MQTT' ? brokerHostSchema : ipv4Schema).safeParse(target.ip)
                    .success
            )
                context.addIssue({
                    code: 'custom',
                    path: ['endpoint', 'targets', index, 'ip'],
                    message:
                        value.protocol === 'MQTT'
                            ? '请输入 Broker 域名或 IP'
                            : '请输入有效的 IPv4 地址',
                });
        });
        if (value.protocol === 'MQTT') {
            if (value.endpoint.mode !== 'TCP Client')
                context.addIssue({
                    code: 'custom',
                    path: ['endpoint', 'mode'],
                    message: 'MQTT 只支持 TCP Client',
                });
            value.endpoint.targets.forEach((target, index) => {
                if (!target.mqtt)
                    context.addIssue({
                        code: 'custom',
                        path: ['endpoint', 'targets', index, 'mqtt'],
                        message: '请配置 MQTT Client ID',
                    });
                if (target.mqtt?.password && !target.mqtt.username)
                    context.addIssue({
                        code: 'custom',
                        path: ['endpoint', 'targets', index, 'mqtt', 'username'],
                        message: '密码需要同时配置用户名',
                    });
            });
        }
        if (value.protocol === 'SL651' && value.endpoint.mode !== 'TCP Server')
            context.addIssue({
                code: 'custom',
                path: ['endpoint', 'mode'],
                message: 'SL651 只支持 TCP Server 模式',
            });
        if (value.endpoint.mode === 'TCP Server') {
            if (value.endpoint.ip !== '0.0.0.0')
                context.addIssue({
                    code: 'custom',
                    path: ['endpoint', 'ip'],
                    message: 'TCP Server 监听 IP 必须是 0.0.0.0',
                });
            if (value.endpoint.port < 1)
                context.addIssue({
                    code: 'custom',
                    path: ['endpoint', 'port'],
                    message: '端口必须在 1 - 65535 之间',
                });
            if (value.endpoint.targets.length)
                context.addIssue({
                    code: 'custom',
                    path: ['endpoint', 'targets'],
                    message: 'TCP Server 不能配置目标地址',
                });
        } else if (!value.endpoint.targets.length) {
            context.addIssue({
                code: 'custom',
                path: ['endpoint', 'targets'],
                message: 'TCP Client 至少需要一个目标地址',
            });
        }
    });
export const linkListQuerySchema = pageParamsSchema.extend({
    mode: modeSchema.optional(),
    protocol: protocolSchema.optional(),
    status: statusSchema.optional(),
});
export const linkIdSchema = z.uuid({ error: 'id 必须是 UUID' });

export const linkDebugSchema = z.object({ enabled: z.boolean() });
