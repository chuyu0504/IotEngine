import { z } from 'zod';
import { pageParamsSchema } from '@/utils/pagination';
export const openStatusSchema = z.enum(['enabled', 'disabled']);
export const openScopeSchema = z.enum([
    'device:realtime',
    'device:history',
    'device:command',
    'alert:read',
]);
export const webhookEventSchema = z.enum([
    'device.data.reported',
    'device.image.reported',
    'device.command.accepted',
    'device.command.updated',
    'device.alert.triggered',
    'device.alert.resolved',
]);
const uuidSchema = z.uuid({ error: '必须是 UUID' });
export const accessIdSchema = uuidSchema;
const nullableString = z.string().nullable();
export const keySaveSchema = z.object({
    name: z.string().trim().min(1, '请输入调用配置名称').max(64),
    status: openStatusSchema,
    scopes: z.array(openScopeSchema).min(1, '至少选择一个开放权限'),
    deviceIds: z.array(uuidSchema).min(1, '至少选择一台设备').max(10000),
    expiresAt: z
        .union([z.literal(''), z.iso.datetime({ offset: true }), z.null()])
        .optional()
        .transform((value) => value || null),
    remark: z.string().trim().max(200).nullable().optional(),
});
export const webhookSaveSchema = z.object({
    accessKeyId: uuidSchema,
    name: z.string().trim().min(1, '请输入 Webhook 名称').max(64),
    url: z.url({ protocol: /^https?$/ }).max(2048),
    status: openStatusSchema,
    timeoutSeconds: z.coerce.number().int().min(1).max(30),
    skipTlsVerify: z.boolean().default(false),
    headers: z.record(z.string(), z.string()),
    eventTypes: z.array(webhookEventSchema).min(1, '至少选择一个事件'),
    secret: z.string().trim().max(255).nullable().optional(),
});
export const logQuerySchema = pageParamsSchema.extend({
    accessKeyId: uuidSchema.optional(),
    webhookId: uuidSchema.optional(),
    mqttId: uuidSchema.optional(),
    direction: z.enum(['pull', 'push']).optional(),
    status: z.enum(['success', 'failed']).optional(),
    action: z.string().max(50).optional(),
});
export const deviceOptionSchema = z.object({
    id: uuidSchema,
    name: z.string(),
    deviceCode: z.string(),
    canCommand: z.boolean(),
});
export const keyItemSchema = z.object({
    id: uuidSchema,
    name: z.string(),
    accessKeyPrefix: z.string(),
    status: openStatusSchema,
    scopes: z.array(openScopeSchema),
    deviceIds: z.array(uuidSchema),
    expiresAt: nullableString,
    lastUsedAt: nullableString,
    lastUsedIp: nullableString,
    remark: nullableString,
    webhookCount: z.number().int(),
    createdAt: z.string(),
    updatedAt: z.string(),
});
export const keySecretSchema = z.object({
    id: uuidSchema,
    name: z.string(),
    accessKey: z.string(),
    accessKeyPrefix: z.string(),
});
export const webhookItemSchema = z.object({
    id: uuidSchema,
    accessKeyId: uuidSchema,
    accessKeyName: z.string(),
    name: z.string(),
    url: z.string(),
    status: openStatusSchema,
    timeoutSeconds: z.number().int(),
    skipTlsVerify: z.boolean(),
    headers: z.record(z.string(), z.string()),
    eventTypes: z.array(webhookEventSchema),
    deviceIds: z.array(uuidSchema),
    hasSecret: z.boolean(),
    lastTriggeredAt: nullableString,
    lastSuccessAt: nullableString,
    lastFailureAt: nullableString,
    lastHttpStatus: z.number().int().nullable(),
    lastError: nullableString,
    createdAt: z.string(),
    updatedAt: z.string(),
});
export const logItemSchema = z.object({
    id: uuidSchema,
    accessKeyId: uuidSchema.nullable(),
    accessKeyName: nullableString,
    webhookId: uuidSchema.nullable(),
    webhookName: nullableString,
    mqttId: uuidSchema.nullable(),
    mqttName: nullableString,
    direction: z.enum(['pull', 'push']),
    action: z.string(),
    eventType: nullableString,
    status: z.enum(['success', 'failed']),
    httpMethod: nullableString,
    target: nullableString,
    requestIp: nullableString,
    httpStatus: z.number().int().nullable(),
    deviceId: uuidSchema.nullable(),
    deviceCode: nullableString,
    message: nullableString,
    requestPayload: z.record(z.string(), z.unknown()),
    responsePayload: z.record(z.string(), z.unknown()),
    createdAt: z.string(),
});
export const logPageSchema = z.object({
    list: z.array(logItemSchema),
    total: z.number().int(),
    page: z.number().int(),
    pageSize: z.number().int(),
    totalPages: z.number().int(),
});

const mqttBrokerSchema = z
    .string({ error: '请输入 Broker 地址' })
    .max(2048)
    .refine((value) => {
        try {
            const url = new URL(value);
            return (
                ['mqtt:', 'mqtts:'].includes(url.protocol) &&
                !!url.hostname &&
                !url.username &&
                !url.password &&
                !url.search &&
                !url.hash &&
                (!url.pathname || url.pathname === '/') &&
                (!url.port || Number(url.port) > 0)
            );
        } catch {
            return false;
        }
    }, '请输入 mqtt:// 或 mqtts:// Broker 地址，不含凭据、路径或查询参数');
export const mqttSaveSchema = z
    .object({
        accessKeyId: uuidSchema,
        name: z
            .string({ error: '请输入 MQTT 配置名称' })
            .trim()
            .min(1, '请输入 MQTT 配置名称')
            .max(64),
        brokerUrl: mqttBrokerSchema,
        status: openStatusSchema,
        timeoutSeconds: z.coerce.number().int().min(1).max(30),
        eventTypes: z.array(webhookEventSchema).min(1, '至少选择一个事件'),
        username: z
            .string()
            .max(65535)
            .refine((value) => !value.includes('\0'), '用户名不能包含 NUL')
            .nullable()
            .optional(),
        password: z.string().max(65535).nullable().optional(),
    })
    .refine((value) => !value.password || !!value.username, {
        path: ['username'],
        message: '设置密码时必须提供用户名',
    });
export const mqttItemSchema = z.object({
    id: uuidSchema,
    accessKeyId: uuidSchema,
    accessKeyName: z.string(),
    name: z.string(),
    brokerUrl: z.string(),
    status: openStatusSchema,
    timeoutSeconds: z.number().int(),
    eventTypes: z.array(webhookEventSchema),
    username: nullableString,
    hasPassword: z.boolean(),
    lastTriggeredAt: nullableString,
    lastSuccessAt: nullableString,
    lastFailureAt: nullableString,
    lastError: nullableString,
    createdAt: z.string(),
    updatedAt: z.string(),
});
