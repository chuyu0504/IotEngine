import { deviceParametersSchema, deviceParameterValues } from '../web/pages/device/device.schema';
import { expect, test } from 'bun:test';
import {
    compileMqttTemplate,
    formatMqttFieldPath,
    formatMqttTemplate,
    mqttCommandPoints,
    mqttConfigSchema,
    mqttTemplateDraft,
} from '../web/pages/protocol/protocol.schema';
import type { MqttTemplateDraft } from '../web/pages/protocol/protocol.types';
import { saveLinkSchema } from '../web/pages/link/link.schema';
import { deviceCommandSchema } from '../web/pages/device/device.schema';

const point = {
    id: '00000000-0000-7000-8000-000000000001',
    name: '温度',
    field: '/metrics/temperature',
    dataType: 'DOUBLE' as const,
};

test('MQTT message paths display JavaScript property access including arrays and escaped keys', () => {
    for (const [field, display] of [
        ['/body/water_level', 'body.water_level'],
        ['/data/0/value', 'data[0].value'],
        ['/body/sensor-name', 'body["sensor-name"]'],
        ['/body/a~1b/~0key', 'body["a/b"]["~key"]'],
        ['/body/01', 'body["01"]'],
        ['/body/9007199254740993', 'body["9007199254740993"]'],
        ['/body/水位', 'body.水位'],
        ['/body/a"b', 'body["a\\"b"]'],
        ['/', '[""]'],
        ['literal.key', '["literal.key"]'],
        ['temperature', 'temperature'],
        ['', ''],
    ]) expect(formatMqttFieldPath(field)).toBe(display);
});
const config = {
    identityProperty: 'serial',
    parameters: [{ name: 'serial', dataType: 'STRING' as const }],
    storagePolicy: 'report' as const,
    topic: 'devices/+/telemetry',
    identitySource: 'topic' as const,
    topicDeviceSegment: 1,
    qos: 1 as const,
    points: [point],
};

test('MQTT properties can be shared by multiple control messages or used only for control', () => {
    const enabled = { ...point, id: crypto.randomUUID(), name: '开关', field: '', dataType: 'BOOL' as const };
    const commands = [
        { id: crypto.randomUUID(), name: '设置温度', topic: 'devices/{param:serial}/set', template: '{"temperature":"$point:温度","enabled":"$point:开关"}', requiredPointIds: [point.id] },
        { id: crypto.randomUUID(), name: '切换开关', topic: 'devices/{param:serial}/switch', template: '{"switch":"$point:开关"}', requiredPointIds: [enabled.id] },
    ];
    const compiled = compileMqttTemplate(templateDraft({ points: [point, enabled], commands }));
    expect(compiled.commands).toEqual(commands);
    expect(compiled.points.map((point) => [point.name, point.field, point.writable])).toEqual([
        ['温度', '/metrics/temperature', true], ['开关', '', true],
    ]);
    const draft = mqttTemplateDraft(compiled);
    expect(compileMqttTemplate(draft)).toEqual(compiled);
    expect(draft.commands?.[0].requiredPointIds).not.toBe(compiled.commands?.[0].requiredPointIds);
    expect(mqttCommandPoints(commands[1].template, compiled.points).map((point) => point.id)).toEqual([enabled.id]);
    const controlOnly = compileMqttTemplate(templateDraft({ points: [enabled], reportTemplate: '{}', commands: [commands[1]] }));
    expect(controlOnly.points[0].field).toBe('');
    expect(deviceCommandSchema.parse({ mqttMessageId: commands[1].id, elements: [{ elementId: enabled.id, value: '0' }] }).mqttMessageId).toBe(commands[1].id);
    expect(deviceCommandSchema.safeParse({ mqttMessageId: 'invalid', elements: [{ elementId: enabled.id, value: '0' }] }).success).toBe(false);
});

test('MQTT control message names, IDs, membership and required flags are validated independently', () => {
    const command = { id: crypto.randomUUID(), name: '设置', topic: 'commands/{param:serial}', template: '{"value":"$point:温度"}', requiredPointIds: [point.id] };
    const draft = templateDraft({ commands: [command] });
    for (const commands of [
        [command, { ...command, name: '重复标识' }],
        [command, { ...command, id: crypto.randomUUID() }],
        [{ ...command, requiredPointIds: [crypto.randomUUID()] }],
        [{ ...command, requiredPointIds: [point.id, point.id] }],
        [{ ...command, template: '{"value":"$point:未知属性"}' }],
        [{ ...command, template: '{}' }],
        [{ ...command, topic: 'commands/+' }],
    ]) expect(() => compileMqttTemplate({ ...draft, commands })).toThrow();
    expect(() => compileMqttTemplate({ ...draft, commandTopic: 'legacy', commandTemplate: '{"value":"$point:温度"}' })).toThrow('同时使用');
    expect(compileMqttTemplate({ ...draft, commands: [{ ...command, requiredPointIds: [] }] }).commands?.[0].requiredPointIds).toEqual([]);
});

test('MQTT Topic and array references require a value without forcing unrelated properties', () => {
    const channel = { ...point, id: crypto.randomUUID(), name: '通道', field: '', dataType: 'STRING' as const };
    const command = { id: crypto.randomUUID(), name: '通道设置', topic: 'devices/{param:serial}/{point:通道}', template: '{"value":"$point:温度"}', requiredPointIds: [channel.id] };
    const draft = templateDraft({ points: [point, channel], commands: [command] });
    expect(compileMqttTemplate(draft).points.every((point) => point.writable)).toBe(true);
    expect(() => compileMqttTemplate({ ...draft, commands: [{ ...command, requiredPointIds: [] }] })).toThrow('Topic 或数组');
    expect(() => compileMqttTemplate({ ...draft, commands: [{ ...command, topic: 'commands/{point:未知}' }] })).toThrow('未定义');
    expect(() => compileMqttTemplate({ ...draft, commands: [{ ...command, topic: 'commands/prefix{point:通道}' }] })).toThrow('独占一层');
    expect(() => compileMqttTemplate({ ...draft, commands: [{ ...command, template: '{"array":["$point:温度"]}' }] })).toThrow('Topic 或数组');
    expect(compileMqttTemplate({ ...draft, commands: [{ ...command, template: '{"array":["$point:温度"]}', requiredPointIds: [point.id, channel.id] }] }).commands).toHaveLength(1);
});

test('MQTT formatting preserves exact JSON tokens and refuses invalid or duplicate keys', () => {
    const original = '{"big":9007199254740993123456789,"decimal":1.2300e+12,"text":"\\u4e2d,[]:{}","values":[{},[],true,null,"$point:温度"]}';
    const formatted = formatMqttTemplate(original);
    expect(formatted).toContain('\n    "big": 9007199254740993123456789,');
    expect(formatted).toContain('1.2300e+12');
    expect(formatted).toContain('"\\u4e2d,[]:{}"');
    expect(JSON.parse(formatted)).toEqual(JSON.parse(original));
    expect(formatMqttTemplate(formatted)).toBe(formatted);
    for (const invalid of ['{invalid}', '{"a":1,"a":2}', '{"a":1,"\\u0061":2}', '[]']) expect(() => formatMqttTemplate(invalid)).toThrow();
});

test('MQTT templates support wildcard topic identity and nested JSON batches', () => {
    expect(mqttConfigSchema.safeParse(config).success).toBe(true);
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            identitySource: 'payload',
            recordsPath: '/devices',
            deviceCodeField: '/meta/code',
        }).success
    ).toBe(true);
    expect(mqttConfigSchema.safeParse({ ...config, identitySource: 'bound' }).success).toBe(false);
    expect(mqttConfigSchema.safeParse({ ...config, topic: 'devices/a+/telemetry' }).success).toBe(
        false
    );
    expect(mqttConfigSchema.safeParse({ ...config, topic: 'devices/#/telemetry' }).success).toBe(
        false
    );
});

test('MQTT text and binary selectors validate against record boundaries', () => {
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            payloadFormat: 'text',
            points: [{ ...point, field: '1' }],
        }).success
    ).toBe(true);
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            payloadFormat: 'text',
            points: [{ ...point, field: '-1' }],
        }).success
    ).toBe(false);
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            payloadFormat: 'binary',
            recordLength: 6,
            points: [{ ...point, field: '4:2:UINT:LE' }],
        }).success
    ).toBe(true);
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            payloadFormat: 'binary',
            recordLength: 5,
            points: [{ ...point, field: '4:2:UINT:LE' }],
        }).success
    ).toBe(false);
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            payloadFormat: 'binary',
            points: [{ ...point, field: '0:8:UINT' }],
        }).success
    ).toBe(false);
});

test('MQTT writable mappings require command topics, raw values and unique enums', () => {
    expect(
        mqttConfigSchema.safeParse({ ...config, points: [{ ...point, writable: true }] }).success
    ).toBe(false);
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            commandTopic: 'commands/{param:serial}',
            points: [{ ...point, writable: true, scale: 0 }],
        }).success
    ).toBe(false);
    expect(
        mqttConfigSchema.safeParse({
            ...config,
            points: [
                {
                    ...point,
                    enumValues: [
                        { input: 'on', output: '1' },
                        { input: 'off', output: '1.0' },
                    ],
                },
            ],
        }).success
    ).toBe(false);
    expect(mqttConfigSchema.safeParse({ ...config, commandTemplate: '{invalid}' }).success).toBe(
        false
    );
});

test('Broker connections accept domains and keep other protocols restricted to IPv4', () => {
    const link = {
        name: 'Broker',
        protocol: 'MQTT',
        status: 'enabled',
        endpoint: {
            mode: 'TCP Client',
            ip: '',
            port: 0,
            targets: [
                {
                    id: 'broker',
                    name: 'Broker',
                    ip: 'mqtt.example.com',
                    port: 1883,
                    status: 'enabled',
                    mqtt: { clientId: 'test-client' },
                },
            ],
        },
    };
    expect(saveLinkSchema.safeParse(link).success).toBe(true);
    expect(saveLinkSchema.safeParse({ ...link, protocol: 'Modbus' }).success).toBe(false);
    expect(
        saveLinkSchema.safeParse({
            ...link,
            endpoint: {
                ...link.endpoint,
                targets: [{ ...link.endpoint.targets[0], mqtt: undefined }],
            },
        }).success
    ).toBe(false);
});

function templateDraft(overrides: Partial<MqttTemplateDraft> = {}): MqttTemplateDraft {
    return {
        identityProperty: 'serial',
        parameters: [{ name: 'serial', dataType: 'STRING' }],
        timeField: '/time',
        topic: 'devices/{param:serial}/telemetry',
        reportTemplate: '{"device":"$param:serial","time":"0","metrics":{"temperature":"$point:温度"}}',
        qos: 1,
        storagePolicy: 'report',
        timeFormat: 'unix_ms',
        points: [point],
        ...overrides,
    };
}

test('MQTT template compilation escapes paths, preserves constants and supports fixed arrays', () => {
    const reportTemplate = JSON.stringify({
        'a/b': { 'x~y': ['$point:温度', 42, true, null, 'cost $5'] },
        literal: { action: 'report', version: 1 },
        device: '$param:serial',
        time: '0',
    }, null, 2);
    const compiled = compileMqttTemplate(templateDraft({ reportTemplate }));
    expect(compiled.points[0]).toEqual({ ...point, field: '/a~1b/x~0y/0', writable: false });
    expect(compiled.identitySource).toBe('bound');
    expect(compiled.deviceCodeField).toBe('/device');
    expect(compiled.timeField).toBe('/time');
    expect(compiled.reportTemplate).toBe(reportTemplate);
    expect(mqttConfigSchema.parse(compiled).reportTemplate).toBe(reportTemplate);
    expect(mqttTemplateDraft(compiled).reportTemplate).toBe(reportTemplate);
});

test('MQTT properties retain their IDs and metadata when message bindings move or names change explicitly', () => {
    const metadata = {
        ...point,
        unit: '℃',
        enumValues: [{ input: 'on', output: '1' }],
    };
    const moved = compileMqttTemplate(templateDraft({
        reportTemplate: '{"other":{"value":"$point:温度"}}',
        points: [metadata],
    }));
    expect(moved.points[0]).toEqual({ ...metadata, field: '/other/value', writable: false });
    const renamed = compileMqttTemplate(templateDraft({
        reportTemplate: '{"metrics":{"temperature":"$point:室温"}}',
        points: [{ ...metadata, name: '室温' }],
    }));
    expect(renamed.points[0]).toEqual({ ...metadata, name: '室温', writable: false });
    expect(() => compileMqttTemplate(templateDraft({
        reportTemplate: '{"fresh":"$point:新点位"}', points: [],
    }))).toThrow('请先定义属性');
});

test('MQTT message editing never invents properties or transfers another property identity', () => {
    const second = {
        ...point,
        id: '00000000-0000-7000-8000-000000000002',
        name: '湿度',
        field: '/humidity',
        dataType: 'STRING' as const,
    };
    expect(() => compileMqttTemplate(templateDraft({
        reportTemplate: '{"humidity":"$point:新名称","moved":"$point:湿度"}',
        points: [point, second],
    }))).toThrow('请先定义属性：新名称');
    const compiled = compileMqttTemplate(templateDraft({
        reportTemplate: '{"moved":"$point:湿度"}', points: [point, second],
    }));
    expect(compiled.points[0]).toEqual({ ...point, field: '', writable: false });
    expect(compiled.points[1]).toEqual({ ...second, field: '/moved', writable: false });
});

test('MQTT report placeholders and duplicate JSON keys cannot silently lose mappings', () => {
    for (const reportTemplate of [
        '{"a":"$point:温度","b":"$point:温度"}',
        '{"a":"$param:serial","b":"$param:serial","c":"$point:温度"}',
        '{"a":"$time","b":"$time","c":"$point:温度"}',
        '{"a":"$unknown","b":"$point:温度"}',
        '{"a":"$values","b":"$point:温度"}',
        '{"a":"$point:"}',
        '{"a":"$point: 温度"}',
        '{"a":"$point:温度","a":1}',
        '{"a":"$point:温度","\\u0061":1}',
    ]) {
        expect(() => compileMqttTemplate(templateDraft({ reportTemplate }))).toThrow('reportTemplate');
    }
    expect(() => compileMqttTemplate(templateDraft({
        reportTemplate: '{"nested":{"value":"$unknown"},"valid":"$point:温度"}',
    }))).toThrow('reportTemplate/nested/value');
    expect(() => compileMqttTemplate(templateDraft({
        points: [point, { ...point, id: crypto.randomUUID(), field: '/other' }],
    }))).toThrow('points/1/name');
});

test('MQTT JSON templates reject malformed roots, excessive depth and UTF-8 size', () => {
    for (const reportTemplate of ['{invalid}', 'null', '[]', '"$point:温度"']) {
        expect(() => compileMqttTemplate(templateDraft({ reportTemplate }))).toThrow('reportTemplate');
    }
    let value: unknown = '$point:温度';
    for (let index = 0; index < 32; index++) value = { child: value };
    expect(compileMqttTemplate(templateDraft({ reportTemplate: JSON.stringify(value) })).points)
        .toHaveLength(1);
    value = { child: value };
    expect(() => compileMqttTemplate(templateDraft({ reportTemplate: JSON.stringify(value) })))
        .toThrow('32 层');
    const large = JSON.stringify({ point: '$point:温度', literal: '中'.repeat(6000) });
    expect(() => compileMqttTemplate(templateDraft({ reportTemplate: large }))).toThrow('UTF-8');
    expect(mqttConfigSchema.safeParse({
        ...config,
        reportTemplate: large,
    }).success).toBe(false);
    expect(() => compileMqttTemplate(templateDraft({
        commandTopic: 'commands/{param:serial}',
        commandTemplate: '{"literal":"' + '中'.repeat(6000) + '"}',
    }))).toThrow('UTF-8');
});

test('MQTT command references alone determine writable points and keep literal constants', () => {
    const commandTemplate = '{ "action":"set", "device":"$param:serial", "a":"$point:温度", "b":["$point:温度",7] }';
    const compiled = compileMqttTemplate(templateDraft({
        reportTemplate: '{"temperature":"$point:温度","humidity":"$point:湿度"}',
        commandTopic: 'commands/{param:serial}',
        commandTemplate,
        points: [
            { ...point, writable: false },
            { ...point, id: crypto.randomUUID(), name: '湿度', field: '/humidity', writable: true },
        ],
    }));
    expect(compiled.commandTemplate).toBe(commandTemplate);
    expect(compiled.points.map((item) => item.writable)).toEqual([true, false]);
    expect(mqttConfigSchema.safeParse(compiled).success).toBe(true);
    for (const commandTemplate of [
        '{"value":"$point:未知点位"}',
        '{"value":"$time"}',
        '{"value":"$values"}',
        '{"value":"$unknown"}',
        '{"value":"$point:"}',
        '{"value":"$point:温度","value":1}',
        '[]',
        '{broken}',
    ]) {
        expect(() => compileMqttTemplate(templateDraft({
            commandTopic: 'commands/{param:serial}',
            commandTemplate,
        }))).toThrow('commandTemplate');
    }
});

test('MQTT templates require paired control settings and unambiguous topic identity', () => {
    expect(() => compileMqttTemplate(templateDraft({ commandTopic: 'commands/device' })))
        .toThrow('commandTopic/commandTemplate');
    expect(() => compileMqttTemplate(templateDraft({ commandTemplate: '{"value":"$point:温度"}' })))
        .toThrow('commandTopic/commandTemplate');
    expect(() => compileMqttTemplate(templateDraft({
        commandTopic: 'commands/+',
        commandTemplate: '{"value":"$point:温度"}',
    }))).toThrow('commandTopic');
    for (const topic of [
        'devices/prefix{param:serial}/telemetry',
        'devices/{param:serial}/{param:serial}',
        'devices/+/telemetry',
    ]) {
        expect(() => compileMqttTemplate(templateDraft({
            topic,
            reportTemplate: '{"value":"$point:温度"}',
        }))).toThrow('topic');
    }
    const payload = compileMqttTemplate(templateDraft({
        topic: 'devices/+/telemetry',
    }));
    expect(payload.identitySource).toBe('payload');
    expect(compileMqttTemplate(templateDraft()).identitySource).toBe('bound');
    expect(compileMqttTemplate(templateDraft({
        topic: 'devices/fixed/telemetry',
        reportTemplate: '{"value":"$point:温度"}',
    })).identitySource).toBe('bound');
});

test('MQTT template drafts roundtrip metadata and remove stale commands cleanly', () => {
    const compiled = compileMqttTemplate(templateDraft({
        commandTopic: 'commands/{param:serial}',
        commandTemplate: '{"value":"$point:温度"}',
        points: [{ ...point, unit: '℃' }],
    }));
    const draft = mqttTemplateDraft(compiled);
    expect(compileMqttTemplate(draft)).toEqual(compiled);
    expect(draft.points[0]).not.toBe(compiled.points[0]);
    const cleared = compileMqttTemplate({
        ...draft,
        commandTopic: '',
        commandTemplate: ' \n ',
    });
    expect(cleared.commandTopic).toBe('');
    expect(cleared.commandTemplate).toBe('');
    expect(cleared.recordsPath).toBe('');
    expect(cleared.topicDeviceSegment).toBe(0);
    const clearedSelectors = compileMqttTemplate({
        ...draft,
        reportTemplate: '{"value":"$point:温度"}',
        commandTopic: '',
        commandTemplate: '',
    });
    expect(clearedSelectors.deviceCodeField).toBe('');
    expect(clearedSelectors.timeField).toBe('/time');
    expect(clearedSelectors.identitySource).toBe('bound');
    expect(cleared.points[0].writable).toBe(false);
    expect(compileMqttTemplate(mqttTemplateDraft(cleared))).toEqual(cleared);
});

test('MQTT legacy JSON drafts reconstruct escaped literal fields and explicit topic identity', () => {
    const draft = mqttTemplateDraft({
        ...config,
        points: [{ ...point, field: 'literal.name/with~escape' }],
        timeField: '/meta/time',
        timeFormat: 'iso8601',
    });
    expect(draft.topic).toBe('devices/{param:serial}/telemetry');
    expect(JSON.parse(draft.reportTemplate)).toEqual({
        meta: { time: 0 },
        'literal.name/with~escape': '$point:温度',
    });
    const compiled = compileMqttTemplate(draft);
    expect(compiled.points[0].id).toBe(point.id);
    expect(compiled.points[0].field).toBe('/literal.name~1with~0escape');
    expect(compiled.timeField).toBe('/meta/time');
    expect(compiled.timeFormat).toBe('iso8601');
});

test('MQTT legacy or stale configurations fail explicitly instead of discarding mappings', () => {
    const compiled = compileMqttTemplate(templateDraft());
    for (const invalid of [
        { ...config, payloadFormat: 'text' as const, points: [{ ...point, field: '0' }] },
        { ...config, payloadFormat: 'binary' as const, points: [{ ...point, field: '0:1:UINT' }] },
        { ...config, recordsPath: '/devices' },
        { ...config, points: [{ ...point, field: '/values/0' }] },
        { ...config, points: [point, { ...point, id: crypto.randomUUID(), name: '其他', field: '/metrics' }] },
        { ...config, commandTopic: 'commands/device' },
        { ...config, commandTopic: 'commands/device', commandTemplate: '{"values":"$values"}' },
        { ...compiled, reportTemplate: '{"other":"$point:温度"}' },
        { ...compiled, reportTemplate: '{"device":"$param:serial","time":"0","other":"$point:其他"}' },
        { ...compiled, points: [{ ...compiled.points[0], writable: true }] },
    ]) {
        expect(() => mqttTemplateDraft(invalid)).toThrow('不能自动丢弃映射');
    }
});

test('MQTT device identity and parameter references survive editing', () => {
    const draft = {
        identityProperty: 'serial', parameters: [{name:'serial',dataType:'STRING' as const},{name:'address',dataType:'STRING' as const},{name:'uuid',dataType:'STRING' as const}],
        topic: 'devices/{param:serial}/up', reportTemplate: '{"value":"$point:温度"}',
        storagePolicy: 'report' as const, qos: 1 as const, points: [point],
        commands: [{id: crypto.randomUUID(), name: 'Set', topic: 'devices/{param:serial}/down',
            template: '{"id":"$point:温度","address":"$param:address","deviceUuid":"$param:uuid","deviceCode":"$param:serial","value":"$point:温度"}', requiredPointIds: [point.id],
            response: {topic: 'devices/{param:serial}/result', matchFields: ['/id','/address'], codeField: '/result/code', successCode: 0, timeoutMs: 10000}}],
    };
    const compiled = compileMqttTemplate(draft);
    expect(compileMqttTemplate(mqttTemplateDraft(compiled))).toEqual(compiled);
    for (const changes of [
        {commands: [{...draft.commands[0], response: {...draft.commands[0].response, matchFields: ['/address']}}]},
        {commands: [{...draft.commands[0], response: {...draft.commands[0].response, topic: 'devices/+/result'}}]},
        {commands: [{...draft.commands[0], response: {...draft.commands[0].response, timeoutMs: 0}}]},
    ]) expect(() => compileMqttTemplate({...draft, ...changes})).toThrow();
});

test('MQTT named commands validate user fields and reject built-in aggregate variables', () => {
    expect(mqttConfigSchema.safeParse({
        ...config,
        commandTopic: 'commands/device',
        commandTemplate: '{"values":"$values","device":"$param:serial"}',
    }).success).toBe(false);
    expect(mqttConfigSchema.safeParse({
        ...config,
        commandTopic: 'commands/device',
        commandTemplate: '{"value":"$point:未知"}',
    }).success).toBe(false);
    const duplicate = { ...point, id: crypto.randomUUID(), field: '/other' };
    expect(mqttConfigSchema.safeParse({ ...config, points: [point, duplicate] }).success).toBe(true);
    const named = mqttConfigSchema.safeParse({
        ...config,
        commandTopic: 'commands/device',
        commandTemplate: '{"value":"$point:温度"}',
        points: [point, duplicate],
    });
    expect(named.success).toBe(false);
    if (!named.success) expect(named.error.issues.some((issue) =>
        issue.path.join('/') === 'points/1/name'
    )).toBe(true);
});


test('MQTT editing retains shared derived calculations and visibility', () => {
    const derived = {
        id: crypto.randomUUID(), name: '换算温度', kind: 'expression' as const,
        valueType: 'number' as const, expression: 'x * 0.1 - 1',
        inputs: [{alias:'x',pointId:point.id}], unitMode:'fixed' as const,
        unit:'℃',maxAgeSeconds:300,visible:true,
    };
    const compiled = compileMqttTemplate({...templateDraft(),derivedPoints:[derived],pointVisibility:{[point.id]:false}});
    expect(compiled.derivedPoints).toEqual([derived]);
    expect(compileMqttTemplate(mqttTemplateDraft(compiled))).toEqual(compiled);
    for (const conversion of [{scale:1},{offset:0},{scale:0.1,offset:-1}]) {
        expect(mqttConfigSchema.safeParse({...config,points:[{...point,...conversion}]}).success).toBe(false);
    }
});


test('multiple MQTT reports keep independent topics and mappings and can be removed', () => {
    const reports = [
        {id:crypto.randomUUID(),name:'温度',topic:'devices/{param:serial}/temperature',qos:1 as const,reportTemplate:'{"temperature":"$point:温度"}'},
        {id:crypto.randomUUID(),name:'状态',topic:'devices/{param:serial}/state',qos:2 as const,reportTemplate:'{"nested":{"value":"$point:温度"}}'},
    ];
    const draft = {...templateDraft(),reports};
    const compiled = compileMqttTemplate(draft);
    expect(compiled.reports?.map(report=>report.pointFields)).toEqual([
        [{pointId:point.id,field:'/temperature'}], [{pointId:point.id,field:'/nested/value'}],
    ]);
    expect(compileMqttTemplate(mqttTemplateDraft(compiled))).toEqual(compiled);
    const after = compileMqttTemplate({...mqttTemplateDraft(compiled),reports:[reports[1]]});
    expect(after.reports).toHaveLength(1);
    expect(after.reports?.[0].topic).toBe(reports[1].topic);
    expect(after.points[0].id).toBe(point.id);
    expect(after.points[0].field).toBe('/nested/value');
    expect(()=>compileMqttTemplate({...draft,reports:[]})).toThrow();
    expect(()=>compileMqttTemplate({...draft,reports:[reports[0],reports[0]]})).toThrow();
    expect(()=>compileMqttTemplate({...draft,reports:[{...reports[0],reportTemplate:'{}'}]})).toThrow();
});


test('device parameters are object values owned by each device', () => {
    const values = {address:'A001', channel:2, enabled:true, metadata:{x:null}, array:[1,'a']};
    expect(deviceParametersSchema.parse(JSON.stringify(values))).toEqual(values);
    expect(deviceParametersSchema.parse('')).toEqual({});
    expect(deviceParametersSchema.parse('{}')).toEqual({});
    for (const value of ['null','[]','1','{"a":1,"a":2}','{" a":1}','{bad']) expect(deviceParametersSchema.safeParse(value).success).toBe(false);
    expect(mqttConfigSchema.safeParse({...config, deviceParameters: []}).success).toBe(false);
});


test('MQTT parameter definitions survive editing and validate names and types', () => {
    const configured={...config,parameters:[{name:'serial',dataType:'STRING' as const},{name:'stationAddress',dataType:'STRING' as const},{name:'channel',dataType:'DOUBLE' as const}]};
    expect(compileMqttTemplate(mqttTemplateDraft(configured)).parameters).toEqual(configured.parameters);
    expect(mqttConfigSchema.safeParse({...config,parameters:[{name:'same',dataType:'STRING'},{name:'same',dataType:'BOOL'}]}).success).toBe(false);
    expect(mqttConfigSchema.safeParse({...config,parameters:[{name:'x',dataType:'unknown'}]}).success).toBe(false);
});
test('device parameter fields keep JSON types, clear one value and retain unrelated values',()=>{
    const definitions=[{name:'address',dataType:'STRING' as const},{name:'channel',dataType:'DOUBLE' as const},{name:'enabled',dataType:'BOOL' as const},{name:'extra',dataType:'JSON' as const}];
    expect(deviceParameterValues(definitions,{address:'A001',channel:2,enabled:'false',extra:'{"nested":null}'},{retained:7})).toEqual({address:'A001',channel:2,enabled:false,extra:{nested:null},retained:7});
    expect(deviceParameterValues(definitions,{}, {address:'old',retained:7})).toEqual({retained:7});
    expect(()=>deviceParameterValues(definitions,{channel:'bad'})).toThrow();
    expect(()=>deviceParameterValues(definitions,{extra:'{bad'})).toThrow();
});

test('an untouched empty field preserves an existing explicit null',()=>{expect(deviceParameterValues([{name:'nullable',dataType:'STRING'}],{},{nullable:null})).toEqual({nullable:null});});

test('clearing an individual JSON parameter removes its null value',()=>{expect(deviceParameterValues([{name:'value',dataType:'JSON'}],{value:''},{value:null})).toEqual({});});

test('MQTT identifiers and every template reference are defined by the user', () => {
    const draft = templateDraft({
        identityProperty:'硬件序列号', parameters:[{name:'硬件序列号',dataType:'STRING'}],
        topic:'factory/up', reportTemplate:'{"my_identifier":"$param:硬件序列号","my_clock":0,"reading":"$point:温度"}',
        timeField:'/my_clock',
    });
    const compiled=compileMqttTemplate(draft);
    expect(compiled.deviceCodeField).toBe('/my_identifier');
    expect(compiled.identitySource).toBe('payload');
    expect(compiled.timeField).toBe('/my_clock');
    expect(compileMqttTemplate(mqttTemplateDraft(compiled))).toEqual(compiled);
    expect(()=>compileMqttTemplate({...draft,identityProperty:undefined})).toThrow('设备标识符');
    expect(()=>compileMqttTemplate({...draft,parameters:[]})).toThrow('设备标识符');
    expect(()=>compileMqttTemplate({...draft,parameters:[{name:'硬件序列号',dataType:'DOUBLE'}]})).toThrow('文本类型');
    for(const token of ['$deviceCode','$deviceId','$commandId','$time','$values']) {
        expect(()=>compileMqttTemplate({...draft,reportTemplate:JSON.stringify({value:token})})).toThrow('未知占位符');
        expect(()=>compileMqttTemplate({...draft,commandTopic:'factory/down',commandTemplate:JSON.stringify({value:token,reading:'$point:温度'})})).toThrow('未知占位符');
    }
    expect(()=>compileMqttTemplate({...draft,topic:'factory/{deviceCode}'})).toThrow('用户定义');
    expect(()=>compileMqttTemplate({...draft,commandTopic:'factory/down',commandTemplate:'{"value":"$param:未定义"}'})).toThrow('未定义固有属性');
});
