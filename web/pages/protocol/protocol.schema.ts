import { z } from 'zod';
import { formatJson, type JsonValue as MqttJson, parseJson } from '@/utils/json';
import type { Modbus, MqttConfig, MqttPoint, MqttTemplateDraft } from './protocol.types';
export const protocolIdSchema = z.uuid({ error: 'id 必须是 UUID' });
export const protocolTypeSchema = z.enum(['SL651', 'Modbus', 'S7', 'MC', 'FINS', 'DLT645', 'MQTT']);

const mqttTopicSchema = z
    .string()
    .min(1)
    .max(1024)
    .refine(
        (value) =>
            !value.includes('\0') &&
            value
                .split('/')
                .every(
                    (level, index, levels) =>
                        !/[+#]/.test(level) ||
                        level === '+' ||
                        (level === '#' && index === levels.length - 1)
                ),
        'Topic 通配符须独占一层，# 只能位于最后一层'
    );
const mqttCommandTopicSchema = mqttTopicSchema.refine(
    (value) => !/[+#]/.test(value),
    '指令 Topic 不允许通配符'
);
const mqttPathSchema = z
    .string()
    .max(256)
    .refine(
        (value) =>
            !value.startsWith('/') || (!/~(?![01])/.test(value) && value.split('/').length <= 33),
        'JSON Pointer 转义无效或超过 32 层'
    );

const mqttPointer = (parts: readonly string[]) =>
    `/${parts.map((part) => part.replace(/~/g, '~0').replace(/\//g, '~1')).join('/')}`;
const mqttTemplateError = (template: string, path: string, message: string, offset = 0): never => {
    throw Object.assign(new Error(`${template}${path || '（根对象）'}: ${message}`), { offset });
};

function parseMqttTemplate(text: string, template: string): { [key: string]: MqttJson } {
    if (new TextEncoder().encode(text).length > 16384)
        mqttTemplateError(template, '', '模板不能超过 16384 UTF-8 字节');
    let value: MqttJson;
    try {
        value = parseJson(text);
    } catch (error) {
        const path =
            error instanceof Error && 'path' in error && typeof error.path === 'string'
                ? error.path
                : '';
        const offset =
            error instanceof Error && 'offset' in error && typeof error.offset === 'number'
                ? error.offset
                : 0;
        return mqttTemplateError(
            template,
            path,
            error instanceof SyntaxError
                ? '模板须为有效 JSON'
                : error instanceof Error
                  ? error.message
                  : 'JSON 无效',
            offset
        );
    }
    if (!value || typeof value !== 'object' || Array.isArray(value))
        return mqttTemplateError(template, '', '模板根节点须为 JSON 对象');
    return value;
}

export function formatMqttTemplate(text: string): string {
    parseMqttTemplate(text, '消息模板');
    return formatJson(text);
}

function visitMqttTemplate(
    value: MqttJson,
    visit: (value: string, path: string) => void,
    parts: string[] = []
): void {
    if (typeof value === 'string') visit(value, mqttPointer(parts));
    else if (value && typeof value === 'object')
        Object.entries(value).forEach(([key, child]) => {
            visitMqttTemplate(child, visit, [...parts, key]);
        });
}

function mqttPointPlaceholder(value: string, template: string, path: string): string | undefined {
    if (!value.startsWith('$point:')) return undefined;
    const name = value.slice('$point:'.length);
    if (!name || name !== name.trim() || name.length > 100)
        mqttTemplateError(template, path, '点位占位符名称须为 1–100 个字符且不能有首尾空格');
    return name;
}

function mqttReportMappings(text: string) {
    const value = parseMqttTemplate(text, 'reportTemplate');
    const points = new Map<string, string>();
    let deviceCodeField: string | undefined;
    let timeField: string | undefined;
    visitMqttTemplate(value, (marker, path) => {
        const name = mqttPointPlaceholder(marker, 'reportTemplate', path);
        if (name !== undefined) {
            if (points.has(name))
                mqttTemplateError('reportTemplate', path, `点位占位符重复：${name}`);
            points.set(name, path);
        } else if (marker === '$deviceCode') {
            if (deviceCodeField)
                mqttTemplateError('reportTemplate', path, '$deviceCode 只能出现一次');
            deviceCodeField = path;
        } else if (marker === '$time') {
            if (timeField) mqttTemplateError('reportTemplate', path, '$time 只能出现一次');
            timeField = path;
        } else if (marker.startsWith('$')) {
            mqttTemplateError('reportTemplate', path, `未知占位符：${marker}`);
        }
    });
    return { points, deviceCodeField, timeField };
}

function mqttCommandReferences(text: string, names: ReadonlySet<string>, legacyValues = false) {
    const value = parseMqttTemplate(text, 'commandTemplate');
    const references = new Set<string>();
    visitMqttTemplate(value, (marker, path) => {
        const name = mqttPointPlaceholder(marker, 'commandTemplate', path);
        if (name !== undefined) {
            if (!names.has(name)) mqttTemplateError('commandTemplate', path, `未知点位：${name}`);
            references.add(name);
        } else if (
            marker.startsWith('$') &&
            marker !== '$deviceCode' &&
            marker !== '$commandId' &&
            !marker.startsWith('$param:') &&
            !(legacyValues && marker === '$values')
        ) {
            mqttTemplateError('commandTemplate', path, `未知占位符：${marker}`);
        }
    });
    return references;
}
function mqttCommandTopicReferences(topic: string, names: ReadonlySet<string>): Set<string> {
    const references = new Set<string>();
    for (const level of topic.split('/')) {
        if (level === '{deviceCode}') continue;
        if (level.startsWith('{point:') && level.endsWith('}')) {
            const name = level.slice(7, -1);
            if (!names.has(name)) throw new Error(`控制 Topic 引用了未定义的属性：${name}`);
            references.add(name);
        } else if (/[{}]/.test(level)) {
            throw new Error('Topic 占位符须独占一层，仅支持 {deviceCode} 和 {point:属性名称}');
        }
    }
    return references;
}

export function mqttCommandPoints(
    template: string,
    points: readonly MqttPoint[],
    topic = ''
): MqttPoint[] {
    const names = mqttCommandReferences(template, new Set(points.map((point) => point.name)));
    mqttCommandTopicReferences(topic, new Set(points.map((point) => point.name))).forEach(
        (name) => {
            names.add(name);
        }
    );
    return points.filter((point) => names.has(point.name));
}
const mqttJsonInput = (value: unknown) => {
    if (typeof value !== 'string') return value;
    if (!value.trim()) return undefined;
    try {
        return JSON.parse(value);
    } catch {
        return value;
    }
};
export const mqttConfigSchema = z
    .object({
        storagePolicy: z.enum(['report', 'change']),
        topic: mqttTopicSchema,
        recordsPath: mqttPathSchema.optional(),
        deviceCodeField: mqttPathSchema.optional(),
        identitySource: z.enum(['bound', 'payload', 'topic']).optional(),
        topicDeviceSegment: z.number().int().min(0).max(1024).optional(),
        payloadFormat: z.enum(['json', 'text', 'binary']).optional(),
        delimiter: z.string().min(1).max(8).optional(),
        recordDelimiter: z.string().min(1).max(8).optional(),
        recordLength: z.number().int().min(0).max(1048576).optional(),
        timeField: mqttPathSchema.optional(),
        timeFormat: z.enum(['unix_ms', 'unix_s', 'iso8601']).optional(),
        commandTopic: z.union([z.literal(''), mqttCommandTopicSchema]).optional(),
        commandTemplate: z.string().max(16384).optional(),
        deviceParameters: z.preprocess(
            mqttJsonInput,
            z
                .array(
                    z.object({
                        deviceCode: z.string().min(1).max(100),
                        name: z.string().trim().min(1).max(100),
                        value: z.unknown(),
                    })
                )
                .max(10000)
                .optional()
        ),
        commands: z
            .array(
                z.object({
                    id: z.uuid(),
                    name: z.string().trim().min(1).max(100),
                    topic: mqttCommandTopicSchema,
                    template: z.string().min(1).max(16384),
                    requiredPointIds: z.array(z.uuid()).max(256),
                    response: z.preprocess(
                        mqttJsonInput,
                        z
                            .object({
                                topic: mqttTopicSchema.refine(
                                    (topic) => !/[+#]/.test(topic) && !topic.includes('{point:'),
                                    '回执须为具体设备 Topic'
                                ),
                                matchFields: z.array(mqttPathSchema).min(1).max(16),
                                codeField: mqttPathSchema.min(1),
                                successCode: z
                                    .number()
                                    .int()
                                    .min(-9007199254740991)
                                    .max(9007199254740991),
                                timeoutMs: z.number().int().min(1000).max(120000).optional(),
                            })
                            .optional()
                    ),
                })
            )
            .max(32)
            .optional(),
        reportTemplate: z.string().max(16384).optional(),
        qos: z.union([z.literal(0), z.literal(1), z.literal(2)]),
        points: z
            .array(
                z.object({
                    id: z.uuid(),
                    name: z.string().trim().min(1).max(100),
                    field: mqttPathSchema,
                    dataType: z.enum(['BOOL', 'STRING', 'DOUBLE']),
                    unit: z.string().max(32).optional(),
                    writable: z.boolean().optional(),
                    scale: z.number().optional(),
                    offset: z.number().optional(),
                    enumValues: z
                        .array(
                            z.object({ input: z.string().max(256), output: z.string().max(256) })
                        )
                        .max(64)
                        .optional(),
                })
            )
            .max(256),
    })
    .superRefine((config, context) => {
        const format = config.payloadFormat ?? 'json';
        const identity = config.identitySource ?? (config.deviceCodeField ? 'payload' : 'bound');
        const issue = (path: (string | number)[], message: string) =>
            context.addIssue({ code: 'custom', path, message });
        const parameterKeys = new Set<string>();
        const parameterNames = new Set<string>();
        config.deviceParameters?.forEach((parameter, index) => {
            const key = JSON.stringify([parameter.deviceCode, parameter.name]);
            if (
                parameterKeys.has(key) ||
                parameter.value === undefined ||
                JSON.stringify(parameter.value).length > 16384
            )
                issue(['deviceParameters', index], '设备参数重复或值无效');
            parameterKeys.add(key);
            parameterNames.add(parameter.name);
        });
        const selector = (value: string, path: (string | number)[]) => {
            if (format === 'text' && (!/^(0|[1-9]\d*)$/.test(value) || Number(value) > 4096))
                issue(path, '文本字段填写从 0 开始的列号，最多 4096');
            if (format === 'binary') {
                const match =
                    /^(0|[1-9]\d*):(1|[1-9]\d*):(UINT|INT|FLOAT|UTF8|HEX)(?::(LE|BE))?$/.exec(
                        value
                    );
                if (!match) {
                    issue(path, '格式：偏移:长度:编码:字节序，例如 0:2:UINT:LE');
                    return;
                }
                const offset = Number(match[1]),
                    length = Number(match[2]),
                    encoding = match[3];
                if (
                    offset + length > 1048576 ||
                    (config.recordLength && offset + length > config.recordLength) ||
                    ((encoding === 'UINT' || encoding === 'INT') && ![1, 2, 4].includes(length)) ||
                    (encoding === 'FLOAT' && ![4, 8].includes(length))
                )
                    issue(path, '二进制字段长度无效或超出记录边界');
            }
        };
        if (identity === 'payload') {
            if (!config.deviceCodeField) issue(['deviceCodeField'], '请输入设备标识字段');
            else selector(config.deviceCodeField, ['deviceCodeField']);
        }
        if (identity === 'bound' && /[+#]/.test(config.topic))
            issue(['identitySource'], '通配 Topic 需要从 Topic 或负载识别设备');
        if (format !== 'json' && config.recordsPath) issue(['recordsPath'], '记录路径仅用于 JSON');
        if (config.timeField) selector(config.timeField, ['timeField']);
        const ids = new Set<string>();
        const fields = new Set<string>();
        let namedTemplate = config.reportTemplate !== undefined;
        if (config.reportTemplate !== undefined) {
            if (format !== 'json') issue(['reportTemplate'], '上报模板仅用于 JSON');
            try {
                mqttReportMappings(config.reportTemplate);
            } catch (error) {
                issue(['reportTemplate'], (error as Error).message);
            }
        }
        if (config.commandTemplate) {
            if (format !== 'json') issue(['commandTemplate'], '指令模板仅用于 JSON');
            try {
                const references = mqttCommandReferences(
                    config.commandTemplate,
                    new Set(config.points.map((point) => point.name)),
                    true
                );
                namedTemplate ||= references.size > 0;
                if (references.size && !config.commandTopic)
                    issue(['commandTopic'], '点位指令模板需要指令 Topic');
            } catch (error) {
                issue(['commandTemplate'], (error as Error).message);
            }
        }
        const names = new Set<string>();
        const commandIds = new Set<string>();
        const commandNames = new Set<string>();
        const commandPoints = new Set<string>();
        if (config.commands !== undefined && (config.commandTopic || config.commandTemplate))
            issue(['commands'], '控制消息列表不能与旧版单条控制配置同时使用');
        config.commands?.forEach((command, index) => {
            namedTemplate = true;
            if (format !== 'json') issue(['commands', index], '多条控制消息仅用于 JSON');
            if (commandIds.has(command.id) || commandNames.has(command.name))
                issue(['commands', index], '控制消息标识和名称必须唯一');
            commandIds.add(command.id);
            commandNames.add(command.name);
            try {
                const template = parseMqttTemplate(command.template, 'commandTemplate');
                let hasCommandId = false;
                visitMqttTemplate(template, (marker) => {
                    if (marker === '$commandId') hasCommandId = true;
                    if (marker.startsWith('$param:') && !parameterNames.has(marker.slice(7)))
                        throw new Error(`设备参数未定义：${marker.slice(7)}`);
                });
                if (command.response) {
                    const valueAt = (path: string): unknown => {
                        const parts = path.startsWith('/')
                            ? path
                                  .slice(1)
                                  .split('/')
                                  .map((part) => part.replace(/~1/g, '/').replace(/~0/g, '~'))
                            : [path];
                        return parts.reduce<unknown>(
                            (value, part) =>
                                value && typeof value === 'object'
                                    ? (value as Record<string, unknown>)[part]
                                    : undefined,
                            template
                        );
                    };
                    if (
                        !hasCommandId ||
                        !command.response.matchFields.some(
                            (field) => valueAt(field) === '$commandId'
                        )
                    )
                        throw new Error('回执关联字段须包含 $commandId 所在路径');
                    if (
                        new Set(command.response.matchFields).size !==
                            command.response.matchFields.length ||
                        command.response.matchFields.some(
                            (field) => !field || valueAt(field) === undefined
                        )
                    )
                        throw new Error('回执关联字段无效、重复或不在指令中');
                }
                const referenced = mqttCommandPoints(
                    command.template,
                    config.points,
                    command.topic
                );
                if (!referenced.length) throw new Error('控制消息至少需要一个点位占位符');
                referenced.forEach((point) => {
                    commandPoints.add(point.id);
                    if (!point.writable) throw new Error(`控制消息引用不可写点位：${point.name}`);
                });
                if (
                    new Set(command.requiredPointIds).size !== command.requiredPointIds.length ||
                    command.requiredPointIds.some(
                        (id) => !referenced.some((point) => point.id === id)
                    )
                )
                    throw new Error('必填控制点必须属于当前消息且不能重复');
                const mandatoryNames = mqttCommandTopicReferences(
                    command.topic,
                    new Set(config.points.map((point) => point.name))
                );
                const scanArray = (value: MqttJson): void => {
                    if (Array.isArray(value)) {
                        value.forEach((child) => {
                            if (typeof child === 'string' && child.startsWith('$point:'))
                                mandatoryNames.add(child.slice(7));
                            scanArray(child);
                        });
                    } else if (value && typeof value === 'object')
                        Object.values(value).forEach(scanArray);
                };
                scanArray(parseMqttTemplate(command.template, 'commandTemplate'));
                referenced.forEach((point) => {
                    if (
                        mandatoryNames.has(point.name) &&
                        !command.requiredPointIds.includes(point.id)
                    )
                        throw new Error(`Topic 或数组中的属性必须设为必填：${point.name}`);
                });
            } catch (error) {
                issue(['commands', index], (error as Error).message);
            }
        });
        config.points.forEach((point, index) => {
            const inputs = new Set<string>(),
                outputs = new Set<string>();
            point.enumValues?.forEach((value, valueIndex) => {
                const output =
                    point.dataType === 'DOUBLE'
                        ? value.output !== '' && Number.isFinite(Number(value.output))
                            ? String(Number(value.output))
                            : undefined
                        : point.dataType === 'BOOL'
                          ? ['true', '1'].includes(value.output)
                              ? 'true'
                              : ['false', '0'].includes(value.output)
                                ? 'false'
                                : undefined
                          : value.output;
                if (inputs.has(value.input) || output === undefined || outputs.has(output))
                    issue(
                        ['points', index, 'enumValues', valueIndex],
                        '枚举原值、转换值须唯一且符合点位类型'
                    );
                inputs.add(value.input);
                if (output !== undefined) outputs.add(output);
            });
            if (point.field) selector(point.field, ['points', index, 'field']);
            else if (format !== 'json' || config.reportTemplate === undefined)
                issue(['points', index, 'field'], '未绑定上报的属性需要 JSON 上报模板');
            if (namedTemplate && names.has(point.name))
                issue(['points', index, 'name'], '使用名称占位符时点位名称必须唯一');
            names.add(point.name);
            if (point.writable && point.scale === 0)
                issue(['points', index, 'scale'], '可写点位倍率不能为零');
            if (
                ids.has(point.id) ||
                (point.field && (fields.has(point.field) || point.field === config.deviceCodeField))
            )
                context.addIssue({
                    code: 'custom',
                    path: ['points', index],
                    message: '点位标识和字段必须唯一，不能占用设备标识字段',
                });
            if (point.writable && !config.commandTopic && !commandPoints.has(point.id))
                context.addIssue({
                    code: 'custom',
                    path: ['commandTopic'],
                    message: '可写点位需要指令 Topic',
                });
            ids.add(point.id);
            if (point.field) fields.add(point.field);
        });
    });

function mqttCanonicalField(field: string): string {
    if (!field.startsWith('/')) return mqttPointer([field]);
    if (/~(?![01])/.test(field) || field.split('/').length > 33)
        throw new Error(`字段路径无效：${field}`);
    return field;
}

export function compileMqttTemplate(draft: MqttTemplateDraft): MqttConfig {
    const report = mqttReportMappings(draft.reportTemplate);
    const attributeNames = new Set(draft.points.map((point) => point.name));
    for (const name of report.points.keys()) {
        if (!attributeNames.has(name)) throw new Error(`reportTemplate: 请先定义属性：${name}`);
    }
    const levels = draft.topic.split('/');
    const markers = levels.filter((level) => level === '{deviceCode}').length;
    if (
        markers > 1 ||
        levels.some((level) => level.includes('{deviceCode}') && level !== '{deviceCode}')
    )
        throw new Error('topic: {deviceCode} 须独占一层且只能出现一次');
    if (/[+#]/.test(draft.topic) && !report.deviceCodeField && !markers)
        throw new Error(
            'topic: 通配 Topic 须在上报模板中包含 $deviceCode，或将设备对应的 + 改为 {deviceCode}'
        );
    const commandTopic = draft.commandTopic?.trim() ? draft.commandTopic : undefined;
    const commandTemplate = draft.commandTemplate?.trim() ? draft.commandTemplate : undefined;
    if (!!commandTopic !== !!commandTemplate)
        throw new Error('commandTopic/commandTemplate: 指令 Topic 和指令模板须同时填写或同时清空');
    const references = commandTemplate
        ? mqttCommandReferences(commandTemplate, attributeNames)
        : new Set<string>();
    draft.commands?.forEach((command) => {
        mqttCommandPoints(command.template, draft.points, command.topic).forEach((point) => {
            references.add(point.name);
        });
    });
    if (commandTopic) {
        const commandMarkers = commandTopic.split('/').filter((level) => level === '{deviceCode}');
        if (
            commandMarkers.length > 1 ||
            commandTopic
                .split('/')
                .some((level) => level.includes('{deviceCode}') && level !== '{deviceCode}')
        )
            throw new Error('commandTopic: {deviceCode} 须独占一层且只能出现一次');
    }
    const byName = new Map<string, MqttPoint>();
    draft.points.forEach((point, index) => {
        if (byName.has(point.name))
            throw new Error(`points/${index}/name: 点位名称必须唯一：${point.name}`);
        byName.set(point.name, point);
    });
    if (!draft.points.length) throw new Error('请先定义至少一个属性');
    const points = draft.points.map(
        (point) =>
            ({
                ...point,
                field: report.points.get(point.name) ?? '',
                writable: references.has(point.name),
            }) satisfies MqttPoint
    );
    const result = mqttConfigSchema.safeParse({
        topic: draft.topic,
        storagePolicy: draft.storagePolicy,
        qos: draft.qos,
        payloadFormat: 'json',
        identitySource: markers ? 'bound' : report.deviceCodeField ? 'payload' : 'bound',
        topicDeviceSegment: 0,
        recordsPath: '',
        deviceCodeField: report.deviceCodeField ?? '',
        timeField: report.timeField ?? '',
        ...(draft.timeFormat ? { timeFormat: draft.timeFormat } : {}),
        commandTopic: commandTopic ?? '',
        commandTemplate: commandTemplate ?? '',
        ...(draft.commands !== undefined ? { commands: draft.commands } : {}),
        ...(draft.deviceParameters !== undefined
            ? { deviceParameters: draft.deviceParameters }
            : {}),
        reportTemplate: draft.reportTemplate,
        points,
    });
    if (!result.success) {
        const issue = result.error.issues[0];
        throw new Error(`${issue.path.join('/') || 'config'}: ${issue.message}`);
    }
    return result.data;
}

export function mqttTemplateDraft(config: MqttConfig): MqttTemplateDraft {
    const unsupported = (reason: string): never => {
        throw new Error(
            `此 MQTT 配置无法使用 JSON 模板编辑：${reason}；请通过原配置/API 保留并处理，不能自动丢弃映射`
        );
    };
    if ((config.payloadFormat ?? 'json') !== 'json') unsupported('文本和二进制格式不受支持');
    if (config.recordsPath) unsupported('recordsPath 批量记录路径不受单记录模板支持');
    let topic = config.topic;
    const identity = config.identitySource ?? (config.deviceCodeField ? 'payload' : 'bound');
    if (identity === 'topic') {
        const levels = topic.split('/');
        const segment = config.topicDeviceSegment ?? 1;
        if (levels[segment] !== '+' || config.deviceCodeField)
            unsupported('Topic 设备层不是明确的 +，或同时配置了负载设备字段');
        levels[segment] = '{deviceCode}';
        topic = levels.join('/');
    }
    let reportTemplate = config.reportTemplate;
    if (reportTemplate === undefined) {
        const root: { [key: string]: MqttJson } = Object.create(null);
        const insert = (field: string, marker: string) => {
            const path = mqttCanonicalField(field);
            const parts = path
                .slice(1)
                .split('/')
                .map((part) => part.replace(/~1/g, '/').replace(/~0/g, '~'));
            if (parts.some((part) => /^(0|[1-9]\d*)$/.test(part)))
                unsupported(`字段 ${field} 无法区分数组下标与数字对象键`);
            let object = root;
            parts.forEach((part, index) => {
                if (index === parts.length - 1) {
                    if (Object.hasOwn(object, part)) unsupported(`字段 ${field} 与其他字段重叠`);
                    object[part] = marker;
                } else {
                    if (!Object.hasOwn(object, part)) object[part] = Object.create(null);
                    const child = object[part];
                    if (!child || typeof child !== 'object' || Array.isArray(child))
                        unsupported(`字段 ${field} 与其他字段重叠`);
                    object = child as { [key: string]: MqttJson };
                }
            });
        };
        if (config.deviceCodeField) insert(config.deviceCodeField, '$deviceCode');
        if (config.timeField) insert(config.timeField, '$time');
        config.points.forEach((point) => {
            if (point.field) insert(point.field, `$point:${point.name}`);
        });
        reportTemplate = JSON.stringify(root, null, 2);
    }
    const draft: MqttTemplateDraft = {
        topic,
        reportTemplate,
        qos: config.qos,
        storagePolicy: config.storagePolicy,
        ...(config.timeFormat ? { timeFormat: config.timeFormat } : {}),
        ...(config.commandTopic ? { commandTopic: config.commandTopic } : {}),
        ...(config.commandTemplate ? { commandTemplate: config.commandTemplate } : {}),
        ...(config.deviceParameters
            ? { deviceParameters: config.deviceParameters.map((parameter) => ({ ...parameter })) }
            : {}),
        ...(config.commands !== undefined
            ? {
                  commands: config.commands.map((command) => ({
                      ...command,
                      requiredPointIds: [...command.requiredPointIds],
                  })),
              }
            : {}),
        points: config.points.map((point) => ({
            ...point,
            ...(point.enumValues
                ? { enumValues: point.enumValues.map((value) => ({ ...value })) }
                : {}),
        })),
    };
    let compiled: MqttConfig;
    try {
        compiled = compileMqttTemplate(draft);
    } catch (error) {
        return unsupported((error as Error).message);
    }
    const expectedIdentity = identity === 'topic' ? 'bound' : identity;
    if (
        compiled.identitySource !== expectedIdentity ||
        compiled.deviceCodeField !==
            (config.deviceCodeField ? mqttCanonicalField(config.deviceCodeField) : '') ||
        compiled.timeField !== (config.timeField ? mqttCanonicalField(config.timeField) : '')
    )
        unsupported('上报模板与现有设备标识或时间字段不一致');
    if (
        compiled.points.length !== config.points.length ||
        config.points.some((point) => {
            const next = compiled.points.find((item) => item.id === point.id);
            return (
                !next ||
                next.name !== point.name ||
                next.field !== (point.field ? mqttCanonicalField(point.field) : '') ||
                !!next.writable !== !!point.writable
            );
        })
    )
        unsupported('模板与现有点位映射或可写状态不一致');
    return draft;
}

// 与服务端 Expression 使用相同语法和复杂度上限；仅解析，不执行用户输入。
export function validatePointExpression(text: string, aliases: readonly string[]): void {
    let position = 0;
    let nodes = 0;
    function fail(message: string): never {
        throw new Error(`${message}（第 ${position + 1} 个字符）`);
    }
    if (!text.trim()) fail('请输入公式');
    if (text.length > 512) fail('公式不能超过 512 个字符');
    const space = () => {
        while (position < text.length && /[ \t\r\n]/.test(text[position])) position++;
    };
    const take = (token: string) => {
        space();
        if (!text.startsWith(token, position)) return false;
        position += token.length;
        return true;
    };
    const add = () => {
        if (++nodes > 128) fail('公式过于复杂');
    };
    const functions: Record<string, number> = { if: 3, min: 2, max: 2, abs: 1, sqrt: 1, round: 1 };
    const operators: [string, number][] = [
        ['||', 0],
        ['&&', 1],
        ['==', 2],
        ['!=', 2],
        ['<=', 3],
        ['>=', 3],
        ['<', 3],
        ['>', 3],
        ['+', 4],
        ['-', 4],
        ['*', 5],
        ['/', 5],
        ['%', 5],
    ];
    function atom(depth: number): void {
        if (depth > 24) fail('公式嵌套过深');
        if (take('(')) {
            parse(0, depth + 1);
            if (!take(')')) fail('缺少右括号');
            return;
        }
        for (const operator of ['!', '-', '+']) {
            if (take(operator)) {
                atom(depth + 1);
                add();
                return;
            }
        }
        space();
        const name = /^[a-zA-Z_][a-zA-Z0-9_]*/.exec(text.slice(position))?.[0];
        if (name) {
            position += name.length;
            if (name !== 'true' && name !== 'false') {
                if (take('(')) {
                    if (!Object.hasOwn(functions, name)) fail(`不支持函数 ${name}`);
                    let count = 1;
                    parse(0, depth + 1);
                    while (take(',')) {
                        if (++count > 3) fail('函数参数过多');
                        parse(0, depth + 1);
                    }
                    if (!take(')')) fail('缺少右括号');
                    if (count !== functions[name]) fail(`${name} 需要 ${functions[name]} 个参数`);
                } else if (!aliases.includes(name)) fail(`未绑定变量 ${name}`);
            }
            add();
            return;
        }
        const number = /^(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/.exec(text.slice(position))?.[0];
        if (!number || !Number.isFinite(Number(number))) fail('需要有效数值、变量或子表达式');
        if (Number(number) === 0 && /[1-9]/.test(number.split(/[eE]/)[0])) fail('数值超出范围');
        position += number.length;
        add();
    }
    function parse(minimum: number, depth: number): void {
        atom(depth);
        for (;;) {
            space();
            const operator = operators.find(([token]) => text.startsWith(token, position));
            if (!operator || operator[1] < minimum) return;
            position += operator[0].length;
            parse(operator[1] + 1, depth + 1);
            add();
        }
    }
    parse(0, 0);
    space();
    if (position !== text.length) fail('存在多余字符或缺少运算符');
}

export const derivedPointSchema = z
    .object({
        id: z.uuid(),
        name: z.string().trim().min(1).max(100),
        unit: z.string().max(32).optional(),
        kind: z.enum(['expression', 'average', 'minimum', 'maximum']),
        unitMode: z.enum(['fixed', 'conditional']),
        unitRules: z
            .array(
                z.object({ condition: z.string().trim().min(1).max(512), unit: z.string().max(32) })
            )
            .max(8)
            .optional(),
        sourceAlias: z.string().optional(),
        valueType: z.enum(['number', 'boolean']),
        expression: z.string().max(512).optional(),
        inputs: z
            .array(
                z.object({
                    alias: z
                        .string()
                        .regex(/^[a-zA-Z_][a-zA-Z0-9_]{0,31}$/)
                        .refine(
                            (value) => value !== 'true' && value !== 'false',
                            '变量名不能使用 true 或 false'
                        ),
                    pointId: z.uuid(),
                })
            )
            .min(1)
            .max(16),
        windowSeconds: z.number().int().min(1).max(86400).optional(),
        maxAgeSeconds: z.number().int().min(1).max(86400),
        visible: z.boolean(),
    })
    .superRefine((point, context) => {
        const validate = (expression: string, path: (string | number)[]) => {
            try {
                validatePointExpression(
                    expression,
                    point.inputs.map((input) => input.alias)
                );
            } catch (error) {
                context.addIssue({ code: 'custom', path, message: (error as Error).message });
            }
        };
        if (new Set(point.inputs.map((input) => input.alias)).size !== point.inputs.length)
            context.addIssue({ code: 'custom', path: ['inputs'], message: '输入变量名不能重复' });
        if (point.kind === 'expression') validate(point.expression ?? '', ['expression']);
        if (point.unitMode === 'conditional')
            point.unitRules?.forEach((rule, index) => {
                validate(rule.condition, ['unitRules', index, 'condition']);
            });
        if (
            point.kind !== 'expression' &&
            (!point.windowSeconds ||
                !point.inputs.some((input) => input.alias === point.sourceAlias) ||
                point.valueType !== 'number')
        )
            context.addIssue({
                code: 'custom',
                path: ['sourceAlias'],
                message: '请选择窗口计算的输入变量，并设置窗口时长和数值结果类型',
            });
        if (point.unitMode === 'conditional' && !point.unitRules?.length)
            context.addIssue({ code: 'custom', path: ['unitRules'], message: '请添加单位条件' });
    });

export const dlt645PointCreateSchema = z.object({
    name: z.string().trim().min(1).max(100),
    unit: z.string().max(32).optional(),
    dataType: z.enum(['BCD', 'BCD_SIGNED', 'HEX']),
    identifier: z.string().regex(/^(?:[\dA-Fa-f]{4}|[\dA-Fa-f]{8})$/),
    length: z.number().int().min(1).max(200),
    digits: z.number().int().min(0).max(8).optional(),
    writable: z.boolean().optional(),
    visible: z.boolean().optional(),
});

export function industrialConfigSchema(protocol: 'MC' | 'FINS' | 'DLT645') {
    const integer = (max: number, min = 0) => z.number().int().min(min).max(max).optional();
    const connection =
        protocol === 'MC'
            ? z.object({
                  frame: z.enum(['3E', '4E']).optional(),
                  network: integer(255),
                  station: integer(255),
                  moduleIo: integer(65535),
                  multidrop: integer(255),
                  monitoringTimer: integer(65535, 1),
              })
            : protocol === 'FINS'
              ? z.object({
                    sourceNetwork: integer(127),
                    sourceNode: integer(254),
                    sourceUnit: integer(255),
                    destinationNetwork: integer(127),
                    destinationNode: integer(254),
                    destinationUnit: integer(255),
                })
              : z.object({
                    version: z.enum(['1997', '2007']).optional(),
                    wakeupBytes: integer(4),
                    writePassword: z
                        .string()
                        .regex(/^(?:[\da-fA-F]{8})?$/)
                        .optional(),
                    operatorCode: z
                        .string()
                        .regex(/^(?:[\da-fA-F]{8})?$/)
                        .optional(),
                });
    return z
        .object({
            storagePolicy: z.enum(['report', 'change']),
            readInterval: integer(3600, 1),
            commandFastReadDuration: integer(3600),
            commandFastReadInterval: integer(3600, 1),
            connection,
            points: z
                .array(
                    z.object({
                        id: z.uuid(),
                        name: z.string().trim().min(1).max(100),
                        unit: z.string().max(32).optional(),
                        writable: z.boolean().optional(),
                        dataType: z.enum([
                            'BOOL',
                            'INT16',
                            'UINT16',
                            'INT32',
                            'UINT32',
                            'FLOAT32',
                            'INT64',
                            'UINT64',
                            'DOUBLE',
                            'BCD',
                            'BCD_SIGNED',
                            'HEX',
                        ]),
                        area: z.string().optional(),
                        address: integer(protocol === 'MC' ? 16777215 : 65535),
                        bit: integer(15),
                        byteOrder: z
                            .enum([
                                'BIG_ENDIAN',
                                'LITTLE_ENDIAN',
                                'BIG_ENDIAN_BYTE_SWAP',
                                'LITTLE_ENDIAN_BYTE_SWAP',
                            ])
                            .optional(),
                        scale: z.number().finite().optional(),
                        decimals: integer(8, -1),
                        identifier: z.string().optional(),
                        length: integer(200, 1),
                        digits: integer(8),
                    })
                )
                .max(256),
        })
        .superRefine((config, context) => {
            const ids = new Set<string>();
            for (const [index, point] of config.points.entries()) {
                const invalid = (message: string) =>
                    context.addIssue({ code: 'custom', path: ['points', index], message });
                if (ids.has(point.id)) invalid('点位标识不能重复');
                ids.add(point.id);
                if (protocol === 'DLT645') {
                    const meter = config.connection as {
                        version?: string;
                        writePassword?: string;
                        operatorCode?: string;
                    };
                    const legacy = meter.version === '1997';
                    if (
                        !new RegExp(`^[0-9A-Fa-f]{${legacy ? 4 : 8}}$`).test(point.identifier ?? '')
                    )
                        invalid('数据标识与 DL/T645 版本不匹配');
                    if (
                        !['BCD', 'BCD_SIGNED', 'HEX'].includes(point.dataType) ||
                        !point.length ||
                        (point.dataType !== 'HEX' && point.length > 8)
                    )
                        invalid('电表数据类型或长度无效');
                    if (
                        point.writable &&
                        (!meter.writePassword ||
                            (!legacy && !meter.operatorCode) ||
                            (point.length ?? 0) > (legacy ? 44 : 38))
                    )
                        invalid('可写点位必须配置认证字段，长度不得超过写帧上限');
                    continue;
                }
                const bits = point.dataType === 'BOOL';
                if (['BCD', 'BCD_SIGNED', 'HEX'].includes(point.dataType))
                    invalid('PLC 数据类型无效');
                const width =
                    bits || ['INT16', 'UINT16'].includes(point.dataType)
                        ? 1
                        : ['INT64', 'UINT64', 'DOUBLE'].includes(point.dataType)
                          ? 4
                          : 2;
                if (point.address === undefined) invalid('请输入十进制地址');
                if (protocol === 'MC') {
                    const bitArea = ['M', 'X', 'Y', 'B', 'L', 'F', 'V', 'S', 'TS', 'CS'].includes(
                        point.area ?? ''
                    );
                    if (
                        (!bitArea &&
                            !['D', 'W', 'R', 'ZR', 'TN', 'CN'].includes(point.area ?? '')) ||
                        (bits && !bitArea)
                    )
                        invalid('软元件区域与数据类型不匹配');
                    if ((point.address ?? 0) + width * (bitArea && !bits ? 16 : 1) - 1 > 16777215)
                        invalid('地址范围越界');
                } else if (
                    !['D', 'CIO', 'W', 'H', 'A'].includes(point.area ?? '') ||
                    (!bits && (point.bit ?? 0) !== 0) ||
                    (point.address ?? 0) + (bits ? 0 : width - 1) > 65535
                )
                    invalid('FINS 区域、位地址或范围无效');
            }
        });
}
const sl651ElementSchema = z
    .object({
        guideHex: z.string().optional().default(''),
        positionMode: z.enum(['GUIDE', 'OFFSET']).optional().default('GUIDE'),
        byteOffset: z.number().int().min(0).max(8388607).optional().default(0),
        encode: z.enum(['BCD', 'HEX', 'DICT', 'JPEG', 'TIME_YYMMDDHHMMSS']),
        length: z.number().int().min(0).max(8388608),
        digits: z.number().int().min(0).max(7),
    })
    .superRefine((element, context) => {
        if (element.positionMode === 'OFFSET') {
            if (element.length < 1 || element.byteOffset + element.length > 8388608)
                context.addIssue({
                    code: 'custom',
                    message: '固定位置的长度须大于零，偏移加长度不能超出正文上限',
                });
            return;
        }
        const guide = element.guideHex;
        if (!/^(?:[0-9a-fA-F]{4}|[fF]{2}[0-9a-fA-F]{4})$/.test(guide)) {
            context.addIssue({ code: 'custom', message: '引导符须为两字节，FF 扩展须为三字节' });
            return;
        }
        const lead = Number.parseInt(guide.slice(0, 2), 16);
        const definition = Number.parseInt(guide.slice(-2), 16);
        let valid = (lead === 255) === (guide.length === 6);
        if (lead < 240 || lead === 255) {
            valid &&=
                element.length > 0 &&
                element.length === definition >> 3 &&
                (element.encode !== 'BCD' || element.digits === (definition & 7));
        } else {
            valid &&= definition === lead;
            if (lead === 240) valid &&= element.length === 5;
            else if (lead === 241) valid &&= element.length === 6;
            else if (lead !== 242 && lead !== 243) valid &&= element.length > 0;
        }
        if (!valid)
            context.addIssue({
                code: 'custom',
                message: '引导符、长度或小数位不符合 SL651 数据定义',
            });
    });
const sl651FunctionsSchema = z.array(
    z.object({
        funcCode: z.string().regex(/^[0-9a-fA-F]{2}$/, '功能码须为一个 HEX 字节'),
        dir: z.enum(['UP', 'DOWN']),
        elements: z.array(sl651ElementSchema),
        responseElements: z.array(sl651ElementSchema).optional(),
    })
);
const configSchema = z.record(z.string(), z.unknown()).superRefine((config, context) => {
    if (config.funcs === undefined && config.responseMode === undefined) return;
    if (
        config.responseMode !== undefined &&
        !['M1', 'M2', 'M3', 'M4'].includes(String(config.responseMode))
    )
        context.addIssue({ code: 'custom', path: ['responseMode'], message: '应答模式无效' });
    if (config.funcs === undefined) return;
    const result = sl651FunctionsSchema.safeParse(config.funcs);
    if (!result.success)
        for (const issue of result.error.issues)
            context.addIssue({
                code: 'custom',
                path: ['funcs', ...issue.path],
                message: issue.message,
            });
});
const modbusRegisterSchema = z.object({
    id: z.string().min(1, '寄存器 ID 不能为空'),
    name: z.string().min(1, '寄存器名称不能为空'),
    registerType: z.enum(['COIL', 'DISCRETE_INPUT', 'HOLDING_REGISTER', 'INPUT_REGISTER']),
    dataType: z.enum([
        'BOOL',
        'INT16',
        'UINT16',
        'INT32',
        'UINT32',
        'FLOAT32',
        'INT64',
        'UINT64',
        'DOUBLE',
    ]),
    address: z
        .number()
        .int('寄存器地址必须是整数')
        .min(0, '寄存器地址不能小于 0')
        .max(65535, '寄存器地址不能大于 65535'),
    quantity: z
        .number()
        .int('寄存器数量必须是整数')
        .min(1, '寄存器数量不能小于 1')
        .max(4, '寄存器数量不能大于 4'),
});
const s7AreaSchema = z
    .object({
        id: z.string().min(1, '寄存器 ID 不能为空'),
        name: z.string().min(1, '寄存器名称不能为空'),
        group: z.string().optional(),
        area: z.enum(['DB', 'V', 'MK', 'PE', 'PA', 'CT', 'TM']),
        dataType: z
            .enum([
                'BOOL',
                'INT8',
                'UINT8',
                'INT16',
                'UINT16',
                'INT32',
                'UINT32',
                'FLOAT',
                'LREAL',
                'STRING',
            ])
            .optional(),
        dbNumber: z.number().int().min(1, 'DB 编号不能小于 1').optional(),
        start: z.number().int().min(0, '起始偏移不能小于 0'),
        startBit: z.number().int().min(0, '位号不能小于 0').max(7, '位号只能是 0~7').optional(),
        size: z.number().int().min(1, '长度不能小于 1'),
        unit: z.string().optional(),
        decimals: z
            .number()
            .int()
            .min(-1, '小数位不能小于 -1')
            .max(8, '小数位不能大于 8')
            .optional(),
        writable: z.boolean().optional(),
        remark: z.string().optional(),
    })
    .superRefine((area, context) => {
        if (area.area === 'DB' && area.dbNumber === undefined)
            context.addIssue({
                code: 'custom',
                path: ['dbNumber'],
                message: 'DB 区域必须填写 DB 编号',
            });
    });
function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
const baseSchema = z.object({
    name: z.string().trim().min(1, '请输入配置名称').max(64, '配置名称最多64个字符'),
    enabled: z.boolean().optional(),
    config: configSchema,
    remark: z.string().optional(),
});
export const protocolCreateSchema = baseSchema
    .extend({ protocol: protocolTypeSchema })
    .superRefine((value, context) => {
        const config = value.config;
        if (value.protocol === 'MQTT') {
            const result = mqttConfigSchema.safeParse(config);
            if (!result.success)
                for (const issue of result.error.issues)
                    context.addIssue({
                        code: 'custom',
                        path: ['config', ...issue.path],
                        message: issue.message,
                    });
            return;
        }
        if (value.protocol === 'MC' || value.protocol === 'FINS' || value.protocol === 'DLT645') {
            const result = industrialConfigSchema(value.protocol).safeParse(config);
            if (!result.success)
                for (const issue of result.error.issues)
                    context.addIssue({
                        code: 'custom',
                        path: ['config', ...issue.path],
                        message: issue.message,
                    });
            return;
        }
        if (value.protocol === 'Modbus') {
            if (
                ![
                    'BIG_ENDIAN',
                    'LITTLE_ENDIAN',
                    'BIG_ENDIAN_BYTE_SWAP',
                    'LITTLE_ENDIAN_BYTE_SWAP',
                ].includes(String(config.byteOrder ?? ''))
            )
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'byteOrder'],
                    message: '字节序无效',
                });
            const registersResult = z.array(modbusRegisterSchema).safeParse(config.registers);
            if (!registersResult.success)
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'registers'],
                    message: registersResult.error.issues[0]?.message ?? '寄存器配置无效',
                });
            if (config.packet !== undefined && !isObject(config.packet))
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'packet'],
                    message: '组包配置必须是对象',
                });
        } else if (value.protocol === 'SL651') {
            if (!['M1', 'M2', 'M3', 'M4'].includes(String(config.responseMode ?? '')))
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'responseMode'],
                    message: '应答模式无效',
                });
            if (!Array.isArray(config.funcs))
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'funcs'],
                    message: '功能码必须是数组',
                });
        } else {
            if (
                !['S7-200', 'S7-300', 'S7-400', 'S7-1200', 'S7-1500'].includes(
                    String(config.plcModel ?? '')
                )
            )
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'plcModel'],
                    message: 'PLC型号无效',
                });
            if (!Array.isArray(config.areas)) {
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'areas'],
                    message: '寄存器必须是数组',
                });
            } else {
                const areasResult = z.array(s7AreaSchema).safeParse(config.areas);
                if (!areasResult.success)
                    context.addIssue({
                        code: 'custom',
                        path: ['config', 'areas'],
                        message: areasResult.error.issues[0]?.message ?? 'S7 寄存器配置无效',
                    });
            }
            if (!isObject(config.connection)) {
                context.addIssue({
                    code: 'custom',
                    path: ['config', 'connection'],
                    message: '连接配置必须是对象',
                });
            } else {
                const connection = config.connection;
                if (
                    !['STANDARD', 'COMPATIBLE', 'AUTO'].includes(
                        String(connection.probeMode ?? 'STANDARD')
                    )
                )
                    context.addIssue({
                        code: 'custom',
                        path: ['config', 'connection', 'probeMode'],
                        message: '连接探测模式无效',
                    });
                for (const [field, label] of [
                    ['handshakeTimeout', '握手超时'],
                    ['directProbeTimeout', '兼容探测超时'],
                ] as const) {
                    const value = connection[field] ?? 5000;
                    if (!Number.isInteger(value) || Number(value) < 1000 || Number(value) > 30000)
                        context.addIssue({
                            code: 'custom',
                            path: ['config', 'connection', field],
                            message: `${label}必须在 1000 - 30000 ms 之间`,
                        });
                }
            }
        }
    });
export const protocolUpdateSchema = baseSchema.partial();

export function parseProtocolImport(text: string, protocol: z.infer<typeof protocolTypeSchema>) {
    let rawItems: unknown;
    try {
        rawItems = JSON.parse(text);
    } catch {
        throw new Error('JSON 格式错误');
    }
    if (!Array.isArray(rawItems) || rawItems.length === 0) {
        throw new Error('文件内容为空或格式不正确');
    }
    const items: z.infer<typeof protocolCreateSchema>[] = [];
    for (let i = 0; i < rawItems.length; i++) {
        const rawItem = rawItems[i];
        if (typeof rawItem !== 'object' || rawItem === null || Array.isArray(rawItem)) {
            throw new Error(`第 ${i + 1} 项必须是对象`);
        }
        const itemProtocol = (rawItem as Record<string, unknown>).protocol;
        if (itemProtocol !== protocol) {
            const actualProtocol = typeof itemProtocol === 'string' ? itemProtocol : '未指定';
            throw new Error(
                `第 ${i + 1} 项协议类型为 ${actualProtocol}，不能导入到 ${protocol} 页面`
            );
        }
        const parsedItem = protocolCreateSchema.safeParse(rawItem);
        if (!parsedItem.success) {
            const issue = parsedItem.error.issues[0];
            const path = issue.path.length ? `${issue.path.map(String).join('.')}：` : '';
            throw new Error(`第 ${i + 1} 项 ${path}${issue.message}`);
        }
        items.push(parsedItem.data);
    }
    return items;
}

const normalizeTsapValue = (value?: string) => {
    if (!value) return undefined;
    const normalized = value
        .replace(/^0x/i, '')
        .replace(/[\s.:\-_]/g, '')
        .toUpperCase();
    return normalized || undefined;
};
export const formatTsapValue = (value?: string) => {
    const normalized = normalizeTsapValue(value);
    if (!normalized || !/^[0-9A-F]{1,4}$/.test(normalized)) {
        return undefined;
    }
    return normalized.padStart(4, '0');
};
export const validateTsapValue = async (_: unknown, value?: string) => {
    if (!formatTsapValue(value)) {
        throw new Error('请输入 1-4 位十六进制 TSAP，例如 4D57 或 0200');
    }
};

/** 检查寄存器地址是否冲突 */
export const checkAddressConflict = (
    registers: Modbus.Register[],
    newRegister: {
        registerType: Modbus.RegisterType;
        address: number;
        quantity: number;
    },
    excludeId?: string
): {
    conflict: boolean;
    conflictWith?: Modbus.Register;
} => {
    const newStart = newRegister.address;
    const newEnd = newRegister.address + newRegister.quantity - 1;
    for (const reg of registers) {
        // 跳过自身（编辑模式）
        if (excludeId && reg.id === excludeId) continue;
        // 只检查同类型寄存器
        if (reg.registerType !== newRegister.registerType) continue;
        const existStart = reg.address;
        const existEnd = reg.address + reg.quantity - 1;
        // 检查地址范围是否重叠
        if (!(newEnd < existStart || newStart > existEnd)) {
            return { conflict: true, conflictWith: reg };
        }
    }
    return { conflict: false };
};
