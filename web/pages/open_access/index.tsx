import {
    DeleteOutlined,
    PlusOutlined,
    QuestionCircleOutlined,
    ReloadOutlined,
} from '@ant-design/icons';
import {
    Alert,
    App,
    Button,
    Card,
    DatePicker,
    Descriptions,
    Drawer,
    Form,
    Grid,
    Input,
    InputNumber,
    Modal,
    Popover,
    Result,
    Select,
    Space,
    Switch,
    Table,
    Tabs,
    Tag,
    Tooltip,
    Typography,
} from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import type { Dayjs } from 'dayjs';
import dayjs from 'dayjs';
import { useMemo, useState } from 'react';
import { FormModal } from '@/components/form_modal';
import { PageContainer } from '@/components/page_container';
import { StatusTag } from '@/components/status_tag';
import { usePermissions } from '@/hooks/use_permission';
import { formatDateTime } from '@/utils/date_time';
import { validateForm } from '@/utils/validation';
import { keySaveSchema, webhookSaveSchema, mqttSaveSchema } from './open_access.schema';
import {
    useOpenMqtt,
    useMqttCreate,
    useMqttUpdate,
    useMqttDelete,
    useKeyCreate,
    useKeyDelete,
    useKeyRotate,
    useKeyUpdate,
    useOpenDevices,
    useOpenKeys,
    useOpenLogs,
    useOpenWebhooks,
    useWebhookCreate,
    useWebhookDelete,
    useWebhookUpdate,
} from './open_access.service';
import type { Access } from './open_access.types';

const { Search, TextArea } = Input;
const TAB_WEBHOOK = 'webhook';
const TAB_LOG = 'access-log';
const TAB_MQTT = 'mqtt';
const SCOPE_OPTIONS: Array<{
    label: string;
    shortLabel: string;
    value: Access.Scope;
    color: string;
}> = [
    { label: '读取实时数据', shortLabel: '实时', value: 'device:realtime', color: 'blue' },
    { label: '读取历史数据', shortLabel: '历史', value: 'device:history', color: 'gold' },
    { label: '下发设备命令', shortLabel: '控制', value: 'device:command', color: 'purple' },
    { label: '读取告警', shortLabel: '告警', value: 'alert:read', color: 'red' },
];
const EVENT_TYPE_OPTIONS: Array<{
    label: string;
    value: Access.EventType;
    color: string;
}> = [
    { label: '数据上报', value: 'device.data.reported', color: 'blue' },
    { label: '图片上报', value: 'device.image.reported', color: 'cyan' },
    { label: '命令已接受', value: 'device.command.accepted', color: 'orange' },
    { label: '命令状态更新', value: 'device.command.updated', color: 'geekblue' },
    { label: '告警触发', value: 'device.alert.triggered', color: 'red' },
    { label: '告警恢复', value: 'device.alert.resolved', color: 'green' },
];
const STATUS_OPTIONS: Array<{
    label: string;
    value: Access.Status;
}> = [
    { label: '启用', value: 'enabled' },
    { label: '禁用', value: 'disabled' },
];
const LOG_DIRECTION_OPTIONS = [
    { label: '主动调用', value: 'pull' },
    { label: '事件推送', value: 'push' },
];
const LOG_STATUS_OPTIONS = [
    { label: '成功', value: 'success' },
    { label: '失败', value: 'failed' },
];
const LOG_ACTION_OPTIONS = [
    { label: '设备列表', value: 'device-list' },
    { label: '实时查询', value: 'realtime' },
    { label: '历史查询', value: 'history' },
    { label: '控制下发', value: 'command' },
    { label: '告警查询', value: 'alert' },
    { label: 'Webhook 推送', value: 'webhook' },
    { label: 'MQTT 推送', value: 'mqtt' },
];
const EXAMPLE_DEVICE_ID = '018f8ad8-7b9a-7ef1-bd92-6a90127d5d44';
const AUTH_HEADER_EXAMPLE = 'X-Access-Key: <你的AccessKey>';
const DEVICE_LIST_EXAMPLE = [
    'curl -X GET "https://your-host/open-api/device/list?page=1&pageSize=50" \\',
    '  -H "X-Access-Key: <你的AccessKey>"',
    '',
    '# 返回授权范围内设备的 id / code / name',
].join('\n');
const REALTIME_EXAMPLE = [
    `curl -X GET "https://your-host/open-api/device/realtime?deviceId=${EXAMPLE_DEVICE_ID}" \\`,
    '  -H "X-Access-Key: <你的AccessKey>"',
    '',
    '# 返回 device + points，未采集点位的 value 为 null',
].join('\n');
const HISTORY_EXAMPLE = [
    `curl -X GET "https://your-host/open-api/device/history?deviceId=${EXAMPLE_DEVICE_ID}&startTime=2026-07-22T00:00:00Z&endTime=2026-07-22T23:59:59Z&page=1&pageSize=20" \\`,
    '  -H "X-Access-Key: <你的AccessKey>"',
    '',
    '# startTime / endTime 必填，时间统一使用带时区的 ISO 8601',
].join('\n');
const COMMAND_EXAMPLE = [
    `curl -X POST "https://your-host/open-api/device/command" \\`,
    '  -H "Content-Type: application/json" \\',
    '  -H "X-Access-Key: <你的AccessKey>" \\',
    `  -d '{"deviceId":"${EXAMPLE_DEVICE_ID}","idempotency_key":"019f0000-0000-7000-8000-000000000005","elements":[{"elementId":"019f0000-0000-7000-8000-000000000004","value":"1"}]}'`,
    '',
    '# 接口受理成功不等于设备已经执行，最终结果以命令响应为准',
].join('\n');
const ALERT_EXAMPLE = [
    'curl -X GET "https://your-host/open-api/device/alert?status=active&severity=critical&page=1&pageSize=20" \\',
    '  -H "X-Access-Key: <你的AccessKey>"',
    '',
    '# 可追加 deviceId，仅能查询当前 AccessKey 授权范围',
].join('\n');
const WEBHOOK_HEADER_EXAMPLE = [
    'X-IOT-Event: device.data.reported',
    'X-IOT-Timestamp: 2026-07-22T08:00:00Z',
    'X-IOT-Delivery: 019f891e-4d2f-77c3-9d23-222dbd8383ee',
    'X-IOT-Signature: sha256=<lowercase-hmac-hex>',
    '',
    '# X-IOT-Signature 仅在配置签名密钥时发送',
].join('\n');
const WEBHOOK_PAYLOAD_EXAMPLE = JSON.stringify(
    {
        event: 'device.data.reported',
        time: '2026-07-22T08:00:00Z',
        deliveryId: '019f891e-4d2f-77c3-9d23-222dbd8383ee',
        data: {
            device: { id: EXAMPLE_DEVICE_ID, code: 'ST001', name: '泵站一号' },
            points: [
                {
                    id: '019f0000-0000-7000-8000-000000000003',
                    access: 'ro',
                    name: '水位',
                    value: 12.34,
                    unit: 'm',
                    time: '2026-07-22T07:59:58Z',
                },
                {
                    id: '019f0000-0000-7000-8000-000000000004',
                    name: '水泵开关',
                    access: 'rw',
                    value: 0,
                    unit: '',
                    time: '2026-07-22T07:59:58Z',
                },
            ],
        },
    },
    null,
    2
);
interface AccessKeyFormValues {
    name: string;
    status: Access.Status;
    allowRealtime: boolean;
    allowHistory: boolean;
    allowCommand: boolean;
    allowAlert: boolean;
    deviceIds: string[];
    expiresAt?: Dayjs | null;
    remark?: string;
}
interface WebhookHeaderFormItem {
    key: string;
    value: string;
}
interface WebhookFormValues {
    accessKeyId: string;
    name: string;
    url: string;
    status: Access.Status;
    timeoutSeconds: number;
    skipTlsVerify: boolean;
    eventTypes: Access.EventType[];
    secret?: string;
    headers?: WebhookHeaderFormItem[];
}
function eventMeta(eventType?: string | null) {
    return EVENT_TYPE_OPTIONS.find((item) => item.value === eventType);
}
function actionLabel(action: string) {
    return LOG_ACTION_OPTIONS.find((item) => item.value === action)?.label ?? action;
}
function formatPayload(payload?: Record<string, unknown>) {
    if (!payload || Object.keys(payload).length === 0) return '';
    return JSON.stringify(payload, null, 2);
}
function renderEventTypes(eventTypes: Access.EventType[]) {
    return (
        <Space size={[4, 4]} wrap>
            {eventTypes.map((eventType) => {
                const meta = eventMeta(eventType);
                return (
                    <Tag key={eventType} color={meta?.color ?? 'default'}>
                        {meta?.label ?? eventType}
                    </Tag>
                );
            })}
        </Space>
    );
}
function UsageCodeBlock({
    title,
    description,
    code,
}: {
    title: string;
    description: string;
    code: string;
}) {
    return (
        <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <div className="mb-2 text-sm font-medium text-slate-800">{title}</div>
            <div className="mb-3 text-xs leading-5 text-slate-500">{description}</div>
            <Typography.Paragraph
                copyable={{ text: code, tooltips: ['复制示例', '已复制'] }}
                className="!mb-0 whitespace-pre-wrap rounded-lg bg-slate-950 p-3 font-mono text-xs !text-slate-100"
            >
                {code}
            </Typography.Paragraph>
        </div>
    );
}
function JsonPayloadCard({
    title,
    content,
    emptyText = '暂无内容',
}: {
    title: string;
    content?: string;
    emptyText?: string;
}) {
    return (
        <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
            <div className="mb-3 text-sm font-medium text-slate-800">{title}</div>
            {content ? (
                <Typography.Paragraph
                    copyable={{ text: content, tooltips: ['复制内容', '已复制'] }}
                    className="!mb-0 max-h-[360px] overflow-auto whitespace-pre-wrap rounded-lg bg-slate-950 p-3 font-mono text-xs !text-slate-100"
                >
                    {content}
                </Typography.Paragraph>
            ) : (
                <div className="rounded-lg bg-white px-3 py-6 text-center text-sm text-slate-400">
                    {emptyText}
                </div>
            )}
        </div>
    );
}
function AccessKeyFormModal({
    open,
    editing,
    loading,
    devices,
    devicesLoading,
    onCancel,
    onFinish,
}: {
    open: boolean;
    editing: Access.KeyItem | null;
    loading: boolean;
    devices: Access.DeviceOption[];
    devicesLoading: boolean;
    onCancel: () => void;
    onFinish: (values: Access.KeySaveDto) => void;
}) {
    const [form] = Form.useForm<AccessKeyFormValues>();
    const selectedDeviceIds = Form.useWatch('deviceIds', form) ?? [];
    const allowCommand = Form.useWatch('allowCommand', form) ?? false;
    const selectableDeviceIds = useMemo(
        () =>
            devices
                .filter((device) => !allowCommand || device.canCommand)
                .map((device) => device.id),
        [allowCommand, devices]
    );
    const allDevicesSelected =
        selectableDeviceIds.length > 0 &&
        selectableDeviceIds.every((id) => selectedDeviceIds.includes(id));
    const handleOpenChange = (isOpen: boolean) => {
        if (!isOpen) return;
        if (editing) {
            form.setFieldsValue({
                name: editing.name,
                status: editing.status,
                allowRealtime: editing.scopes.includes('device:realtime'),
                allowHistory: editing.scopes.includes('device:history'),
                allowCommand: editing.scopes.includes('device:command'),
                allowAlert: editing.scopes.includes('alert:read'),
                deviceIds: editing.deviceIds,
                expiresAt: editing.expiresAt ? dayjs(editing.expiresAt) : null,
                remark: editing.remark ?? undefined,
            });
            return;
        }
        form.resetFields();
        form.setFieldsValue({
            status: 'enabled',
            allowRealtime: true,
            allowHistory: true,
            allowCommand: false,
            allowAlert: false,
            deviceIds: [],
            expiresAt: null,
        });
    };
    return (
        <FormModal
            open={open}
            title={editing ? '编辑主动调用配置' : '新建主动调用配置'}
            onCancel={() => {
                onCancel();
                form.resetFields();
            }}
            onOk={() => form.submit()}
            confirmLoading={loading}
            destroyOnHidden
            afterOpenChange={handleOpenChange}
        >
            <Form<AccessKeyFormValues>
                form={form}
                layout="vertical"
                onFinish={(values) => {
                    const scopes: Access.Scope[] = [];
                    if (values.allowRealtime) scopes.push('device:realtime');
                    if (values.allowHistory) scopes.push('device:history');
                    if (values.allowCommand) scopes.push('device:command');
                    if (values.allowAlert) scopes.push('alert:read');
                    const payload = {
                        name: values.name,
                        status: values.status,
                        scopes,
                        deviceIds: values.deviceIds,
                        expiresAt: values.expiresAt?.toISOString() ?? null,
                        remark: values.remark?.trim() || null,
                    };
                    const validated = validateForm(form, keySaveSchema, payload);
                    if (validated) onFinish(validated);
                }}
            >
                <Form.Item label="配置名称" name="name" required>
                    <Input maxLength={64} placeholder="例如：第三方数据平台" />
                </Form.Item>

                <Form.Item
                    label={
                        <div className="flex w-full items-center justify-between gap-3">
                            <span>可访问设备</span>
                            <Space size={8}>
                                <Button
                                    type="link"
                                    size="small"
                                    className="!h-auto !p-0"
                                    disabled={!selectableDeviceIds.length || allDevicesSelected}
                                    onClick={(event) => {
                                        event.preventDefault();
                                        event.stopPropagation();
                                        form.setFieldsValue({ deviceIds: selectableDeviceIds });
                                    }}
                                >
                                    选择全部
                                </Button>
                                <Button
                                    type="link"
                                    size="small"
                                    className="!h-auto !p-0"
                                    disabled={!selectedDeviceIds.length}
                                    onClick={(event) => {
                                        event.preventDefault();
                                        event.stopPropagation();
                                        form.setFieldsValue({ deviceIds: [] });
                                    }}
                                >
                                    清空
                                </Button>
                            </Space>
                        </div>
                    }
                    name="deviceIds"
                    required
                    extra="该设备范围同时用于主动调用、Webhook 和 MQTT 推送"
                >
                    <Select
                        mode="multiple"
                        allowClear
                        showSearch
                        loading={devicesLoading}
                        placeholder="选择允许访问的设备"
                        optionFilterProp="label"
                        options={devices.map((device) => ({
                            label: `${device.name} (${device.deviceCode || device.id})`,
                            value: device.id,
                            disabled: allowCommand && !device.canCommand,
                        }))}
                    />
                </Form.Item>

                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
                    <Form.Item label="实时查询" name="allowRealtime" valuePropName="checked">
                        <Switch checkedChildren="允许" unCheckedChildren="关闭" />
                    </Form.Item>
                    <Form.Item label="历史查询" name="allowHistory" valuePropName="checked">
                        <Switch checkedChildren="允许" unCheckedChildren="关闭" />
                    </Form.Item>
                    <Form.Item label="控制下发" name="allowCommand" valuePropName="checked">
                        <Switch checkedChildren="允许" unCheckedChildren="关闭" />
                    </Form.Item>
                    <Form.Item label="告警查询" name="allowAlert" valuePropName="checked">
                        <Switch checkedChildren="允许" unCheckedChildren="关闭" />
                    </Form.Item>
                </div>

                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <Form.Item label="状态" name="status" required>
                        <Select options={STATUS_OPTIONS} />
                    </Form.Item>
                    <Form.Item label="过期时间" name="expiresAt">
                        <DatePicker
                            showTime
                            format="YYYY-MM-DD HH:mm:ss"
                            allowClear
                            className="!w-full"
                            placeholder="为空表示不过期"
                        />
                    </Form.Item>
                </div>

                <Form.Item label="备注" name="remark">
                    <TextArea
                        rows={3}
                        maxLength={200}
                        placeholder="记录用途、对接方、联系人等信息"
                    />
                </Form.Item>
            </Form>
        </FormModal>
    );
}
function WebhookFormModal({
    open,
    editing,
    loading,
    accessKeys,
    onCancel,
    onFinish,
}: {
    open: boolean;
    editing: Access.WebhookItem | null;
    loading: boolean;
    accessKeys: Access.KeyItem[];
    onCancel: () => void;
    onFinish: (values: Access.WebhookSaveDto) => void;
}) {
    const [form] = Form.useForm<WebhookFormValues>();
    const selectedAccessKeyId = Form.useWatch('accessKeyId', form);
    const selectedAccessKey = accessKeys.find((item) => item.id === selectedAccessKeyId);
    const handleOpenChange = (isOpen: boolean) => {
        if (!isOpen) return;
        if (editing) {
            form.setFieldsValue({
                accessKeyId: editing.accessKeyId,
                name: editing.name,
                url: editing.url,
                status: editing.status,
                eventTypes: editing.eventTypes,
                timeoutSeconds: editing.timeoutSeconds,
                skipTlsVerify: editing.skipTlsVerify,
                secret: '',
                headers: Object.entries(editing.headers).map(([key, value]) => ({ key, value })),
            });
            return;
        }
        form.resetFields();
        form.setFieldsValue({
            accessKeyId: accessKeys.length === 1 ? accessKeys[0]?.id : undefined,
            status: 'enabled',
            timeoutSeconds: 5,
            skipTlsVerify: false,
            eventTypes: ['device.data.reported'],
            headers: [],
        });
    };
    return (
        <FormModal
            open={open}
            title={editing ? '编辑 Webhook' : '新建 Webhook'}
            onCancel={() => {
                onCancel();
                form.resetFields();
            }}
            onOk={() => form.submit()}
            confirmLoading={loading}
            destroyOnHidden
            afterOpenChange={handleOpenChange}
        >
            <Form<WebhookFormValues>
                form={form}
                layout="vertical"
                onFinish={(values) => {
                    const headers = Object.fromEntries(
                        (values.headers ?? [])
                            .map((item) => ({ key: item.key.trim(), value: item.value.trim() }))
                            .filter((item) => item.key.length > 0)
                            .map((item) => [item.key, item.value])
                    );
                    const secret = values.secret?.trim();
                    const payload: Access.WebhookSaveDto = {
                        accessKeyId: values.accessKeyId,
                        name: values.name,
                        url: values.url,
                        status: values.status,
                        timeoutSeconds: values.timeoutSeconds,
                        skipTlsVerify: values.skipTlsVerify,
                        headers,
                        eventTypes: values.eventTypes,
                        secret: secret || undefined,
                    };
                    const validated = validateForm(form, webhookSaveSchema, payload);
                    if (validated) onFinish(validated);
                }}
            >
                <Alert
                    type="info"
                    showIcon
                    className="mb-4"
                    title="设备范围说明"
                    description="Webhook 不单独配置设备，推送范围完全继承所绑定调用配置的设备列表。"
                />

                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <Form.Item
                        label="绑定调用配置"
                        name="accessKeyId"
                        required
                        extra={
                            selectedAccessKey
                                ? `当前继承 ${selectedAccessKey.deviceIds.length} 台设备`
                                : '请选择一个已配置设备范围的调用配置'
                        }
                    >
                        <Select
                            showSearch
                            placeholder="选择调用配置"
                            optionFilterProp="label"
                            options={accessKeys.map((item) => ({
                                label: `${item.name} (${item.accessKeyPrefix})`,
                                value: item.id,
                                disabled: item.deviceIds.length === 0,
                            }))}
                        />
                    </Form.Item>
                    <Form.Item label="状态" name="status" required>
                        <Select options={STATUS_OPTIONS} />
                    </Form.Item>
                </div>

                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <Form.Item label="名称" name="name" required>
                        <Input maxLength={64} placeholder="例如：运营平台回调" />
                    </Form.Item>
                    <Form.Item label="超时秒数" name="timeoutSeconds" required>
                        <InputNumber min={1} max={30} className="!w-full" placeholder="1-30 秒" />
                    </Form.Item>
                </div>

                <Form.Item label="回调地址" name="url" required>
                    <Input placeholder="https://example.com/webhook" />
                </Form.Item>
                <Form.Item
                    label="跳过 TLS 证书验证"
                    name="skipTlsVerify"
                    valuePropName="checked"
                    extra="仅用于自签名证书等受控环境；开启后无法验证目标服务器身份。"
                >
                    <Switch checkedChildren="已跳过" unCheckedChildren="严格验证" />
                </Form.Item>
                <Form.Item label="订阅事件" name="eventTypes" required>
                    <Select
                        mode="multiple"
                        options={EVENT_TYPE_OPTIONS.map((item) => ({
                            label: item.label,
                            value: item.value,
                        }))}
                        placeholder="选择需要推送的事件"
                    />
                </Form.Item>
                <Form.Item
                    label="签名密钥"
                    name="secret"
                    extra={editing ? '留空表示保持原密钥不变' : '可选，用于生成 X-IOT-Signature'}
                >
                    <Input.Password maxLength={255} placeholder="不填则不启用签名密钥" />
                </Form.Item>

                <Form.List name="headers">
                    {(fields, { add, remove }) => (
                        <div className="rounded-lg border border-dashed border-slate-300 p-4">
                            <div className="mb-3 flex items-center justify-between gap-2">
                                <div>
                                    <div className="font-medium text-slate-800">自定义 Header</div>
                                    <div className="text-xs text-slate-500">
                                        用于透传租户标识、来源系统等额外信息
                                    </div>
                                </div>
                                <Button type="dashed" icon={<PlusOutlined />} onClick={() => add()}>
                                    添加 Header
                                </Button>
                            </div>
                            {fields.length === 0 && (
                                <div className="rounded-md bg-slate-50 px-3 py-2 text-sm text-slate-500">
                                    当前没有额外 Header
                                </div>
                            )}
                            {fields.map((field) => (
                                <div
                                    key={field.key}
                                    className="mb-3 flex items-start gap-2 last:mb-0"
                                >
                                    <Form.Item
                                        className="mb-0 flex-1"
                                        name={[field.name, 'key']}
                                        rules={[{ required: true, message: '请输入 Header 名称' }]}
                                    >
                                        <Input placeholder="X-Tenant-Id" />
                                    </Form.Item>
                                    <Form.Item
                                        className="mb-0 flex-1"
                                        name={[field.name, 'value']}
                                        rules={[{ required: true, message: '请输入 Header 值' }]}
                                    >
                                        <Input placeholder="tenant-a" />
                                    </Form.Item>
                                    <Button
                                        danger
                                        type="text"
                                        icon={<DeleteOutlined />}
                                        onClick={() => remove(field.name)}
                                    />
                                </div>
                            ))}
                        </div>
                    )}
                </Form.List>
            </Form>
        </FormModal>
    );
}
function MqttFormModal({
    open,
    editing,
    loading,
    accessKeys,
    onCancel,
    onFinish,
}: {
    open: boolean;
    editing: Access.MqttItem | null;
    loading: boolean;
    accessKeys: Access.KeyItem[];
    onCancel: () => void;
    onFinish: (values: Access.MqttSaveDto) => void;
}) {
    const [form] = Form.useForm<Access.MqttSaveDto & { clearPassword?: boolean }>();
    const keyId = Form.useWatch('accessKeyId', form);
    const selected = accessKeys.find((key) => key.id === keyId);
    const availableEvents = EVENT_TYPE_OPTIONS.filter((event) =>
        selected?.scopes.includes(
            event.value.startsWith('device.alert.')
                ? 'alert:read'
                : event.value.startsWith('device.command.')
                  ? 'device:command'
                  : 'device:realtime'
        )
    );
    return (
        <FormModal
            open={open}
            title={editing ? '编辑 MQTT 推送' : '新建 MQTT 推送'}
            onCancel={onCancel}
            onOk={() => form.submit()}
            confirmLoading={loading}
            destroyOnHidden
            afterOpenChange={(visible) => {
                if (!visible) return;
                form.resetFields();
                form.setFieldsValue(
                    editing
                        ? { ...editing, password: '', clearPassword: false }
                        : {
                              accessKeyId: accessKeys.length === 1 ? accessKeys[0]?.id : undefined,
                              status: 'enabled',
                              timeoutSeconds: 5,
                              eventTypes: [],
                          }
                );
            }}
        >
            <Form
                form={form}
                layout="vertical"
                onFinish={(values) => {
                    const { clearPassword, ...fields } = values;
                    const payload = {
                        ...fields,
                        username: fields.username || null,
                        password: clearPassword ? null : fields.password || undefined,
                    };
                    const validated = validateForm(form, mqttSaveSchema, payload);
                    if (validated) onFinish(validated);
                }}
            >
                <Alert
                    className="mb-4"
                    type="info"
                    showIcon
                    title="按授权范围推送"
                    description="继承调用配置的设备和权限。Topic 固定为 iot/{deviceId}，通过 JSON 的 event 字段区分事件。使用 MQTT 3.1.1 / QoS 1；接收方请按 deliveryId 去重。"
                />
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <Form.Item
                        label="绑定调用配置"
                        name="accessKeyId"
                        required
                        extra={
                            selected
                                ? `当前继承 ${selected.deviceIds.length} 台设备`
                                : '先选择调用配置'
                        }
                    >
                        <Select
                            options={accessKeys.map((key) => ({ label: key.name, value: key.id }))}
                            onChange={() => form.setFieldValue('eventTypes', [])}
                        />
                    </Form.Item>
                    <Form.Item label="状态" name="status" required>
                        <Select options={STATUS_OPTIONS} />
                    </Form.Item>
                    <Form.Item label="名称" name="name" required>
                        <Input maxLength={64} />
                    </Form.Item>
                    <Form.Item label="投递超时（秒）" name="timeoutSeconds" required>
                        <InputNumber min={1} max={30} className="w-full" />
                    </Form.Item>
                </div>
                <Form.Item
                    label="Broker 地址"
                    name="brokerUrl"
                    required
                    extra="mqtt://host:1883 或 mqtts://host:8883；TLS 会验证服务器证书和主机名。"
                >
                    <Input placeholder="mqtts://broker.example.com:8883" />
                </Form.Item>
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                    <Form.Item label="用户名" name="username">
                        <Input autoComplete="off" />
                    </Form.Item>
                    <Form.Item
                        label="密码"
                        name="password"
                        extra={editing?.hasPassword ? '已保存密码；留空保留原值' : '可选'}
                    >
                        <Input.Password autoComplete="new-password" />
                    </Form.Item>
                </div>
                {editing?.hasPassword && (
                    <Form.Item label="清除已保存密码" name="clearPassword" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                )}
                <Form.Item label="推送事件" name="eventTypes" required>
                    <Select
                        mode="multiple"
                        options={availableEvents}
                        placeholder="选择调用配置授权的事件"
                    />
                </Form.Item>
            </Form>
        </FormModal>
    );
}

function AccessContent() {
    const screens = Grid.useBreakpoint();
    const { modal } = App.useApp();
    const { has } = usePermissions();
    const [activeTab, setActiveTab] = useState(TAB_WEBHOOK);
    const [webhookKeyword, setWebhookKeyword] = useState('');
    const [mqttKeyword, setMqttKeyword] = useState('');
    const [mqttModalOpen, setMqttModalOpen] = useState(false);
    const [editingMqtt, setEditingMqtt] = useState<Access.MqttItem | null>(null);
    const [logKeyword, setLogKeyword] = useState('');
    const [logQuery, setLogQuery] = useState<Access.LogQuery>({ page: 1, pageSize: 20 });
    const [accessKeyModalOpen, setAccessKeyModalOpen] = useState(false);
    const [webhookModalOpen, setWebhookModalOpen] = useState(false);
    const [usageModalOpen, setUsageModalOpen] = useState(false);
    const [editingAccessKey, setEditingAccessKey] = useState<Access.KeyItem | null>(null);
    const [editingWebhook, setEditingWebhook] = useState<Access.WebhookItem | null>(null);
    const [selectedLog, setSelectedLog] = useState<Access.LogItem | null>(null);
    const {
        data: accessKeys = [],
        isLoading: loadingAccessKeys,
        isFetching: refreshingKeys,
        refetch: refreshKeys,
    } = useOpenKeys();
    const {
        data: webhooks = [],
        isLoading: loadingWebhooks,
        isFetching: refreshingWebhooks,
        refetch: refreshWebhooks,
    } = useOpenWebhooks();
    const { data: devices = [], isLoading: loadingDevices } = useOpenDevices();
    const {
        data: logPage,
        isLoading: loadingLogs,
        isFetching: refreshingLogs,
        refetch: refreshLogs,
    } = useOpenLogs(logQuery);
    const keyCreate = useKeyCreate();
    const keyUpdate = useKeyUpdate();
    const keyRotate = useKeyRotate();
    const keyDelete = useKeyDelete();
    const webhookCreate = useWebhookCreate();
    const webhookUpdate = useWebhookUpdate();
    const webhookDelete = useWebhookDelete();
    const {
        data: mqtt = [],
        isLoading: loadingMqtt,
        isFetching: refreshingMqtt,
        refetch: refreshMqtt,
    } = useOpenMqtt();
    const mqttCreate = useMqttCreate();
    const mqttUpdate = useMqttUpdate();
    const mqttDelete = useMqttDelete();
    const canAdd = has('iot:open-access:add');
    const canEdit = has('iot:open-access:edit');
    const canDelete = has('iot:open-access:delete');
    const accessLogs = logPage?.list ?? [];
    const logTotal = logPage?.total ?? 0;
    const deviceMap = useMemo(
        () => new Map(devices.map((device) => [device.id, device])),
        [devices]
    );
    const accessKeyEnabledCount = accessKeys.filter((item) => item.status === 'enabled').length;
    const webhookEnabledCount = webhooks.filter((item) => item.status === 'enabled').length;
    const coveredDeviceCount = new Set(accessKeys.flatMap((item) => item.deviceIds)).size;
    const deviceSearchText = (deviceId: string) => {
        const device = deviceMap.get(deviceId);
        return device ? `${device.name} ${device.deviceCode} ${device.id}` : deviceId;
    };
    const renderDeviceIdentity = (deviceId: string, fallbackCode?: string | null) => {
        const device = deviceMap.get(deviceId);
        const name = device?.name || deviceId;
        const code = device?.deviceCode || fallbackCode;
        return (
            <span className="inline-flex min-w-0 flex-col leading-4">
                <span className="truncate">{name}</span>
                {code && <span className="font-mono text-[11px] text-slate-400">{code}</span>}
            </span>
        );
    };
    const renderDeviceScope = (deviceIds: string[]) => {
        if (deviceIds.length === 0) return '--';
        const visible = deviceIds.slice(0, 2);
        const hidden = deviceIds.slice(2);
        return (
            <div className="max-w-[300px]">
                <div className="mb-1 text-xs text-slate-500">{deviceIds.length} 台设备</div>
                <Space size={[4, 4]} wrap>
                    {visible.map((deviceId) => (
                        <Tag key={deviceId} className="!py-1">
                            {renderDeviceIdentity(deviceId)}
                        </Tag>
                    ))}
                    {hidden.length > 0 && (
                        <Popover
                            trigger="hover"
                            title="设备明细"
                            content={
                                <div className="max-w-[360px]">
                                    <Space size={[4, 4]} wrap>
                                        {deviceIds.map((deviceId) => (
                                            <Tag key={deviceId} className="!py-1">
                                                {renderDeviceIdentity(deviceId)}
                                            </Tag>
                                        ))}
                                    </Space>
                                </div>
                            }
                        >
                            <Tag color="processing">+{hidden.length}</Tag>
                        </Popover>
                    )}
                </Space>
            </div>
        );
    };
    const renderScopes = (scopes: Access.Scope[]) => (
        <Space size={[4, 4]} wrap>
            {SCOPE_OPTIONS.filter((item) => scopes.includes(item.value)).map((item) => (
                <Tag key={item.value} color={item.color}>
                    {item.shortLabel}
                </Tag>
            ))}
        </Space>
    );
    const filteredWebhooks = (() => {
        const keyword = webhookKeyword.trim().toLowerCase();
        if (!keyword) return webhooks;
        return webhooks.filter((item) =>
            [
                item.name,
                item.url,
                item.accessKeyName,
                item.lastError ?? '',
                item.deviceIds.map(deviceSearchText).join(' '),
            ]
                .join(' ')
                .toLowerCase()
                .includes(keyword)
        );
    })();
    const filteredLogs = (() => {
        const keyword = logKeyword.trim().toLowerCase();
        if (!keyword) return accessLogs;
        return accessLogs.filter((item) =>
            [
                item.accessKeyName ?? '',
                item.webhookName ?? '',
                item.action,
                item.eventType ?? '',
                item.target ?? '',
                item.deviceCode ?? '',
                item.message ?? '',
                item.deviceId ? deviceSearchText(item.deviceId) : '',
            ]
                .join(' ')
                .toLowerCase()
                .includes(keyword)
        );
    })();
    const showSecret = (secret: Access.KeySecret, title: string) => {
        modal.info({
            title,
            width: 640,
            okText: '我已保存',
            content: (
                <div className="space-y-3">
                    <Alert
                        type="warning"
                        showIcon
                        title="该认证 Key 只会展示这一次"
                        description="请立即复制保存；后续页面只保留前缀，无法再次查看完整明文。"
                    />
                    <div className="rounded-lg bg-slate-50 p-3">
                        <div className="mb-2 text-sm text-slate-500">完整认证 Key</div>
                        <Typography.Paragraph
                            copyable={{ text: secret.accessKey }}
                            className="!mb-0 break-all font-mono text-sm"
                        >
                            {secret.accessKey}
                        </Typography.Paragraph>
                    </div>
                </div>
            ),
        });
    };
    const accessKeyColumns: ColumnsType<Access.KeyItem> = [
        { title: '名称', dataIndex: 'name', width: 180 },
        {
            title: '认证 Key 前缀',
            dataIndex: 'accessKeyPrefix',
            width: 180,
            render: (value: string) => (
                <Typography.Text code className="font-mono text-xs">
                    {value}
                </Typography.Text>
            ),
        },
        {
            title: '状态',
            dataIndex: 'status',
            width: 90,
            render: (value: Access.Status) => <StatusTag status={value} />,
        },
        {
            title: '访问权限',
            dataIndex: 'scopes',
            width: 240,
            render: (value: Access.Scope[]) => renderScopes(value),
        },
        {
            title: '设备范围',
            dataIndex: 'deviceIds',
            width: 320,
            render: (value: string[]) => renderDeviceScope(value),
        },
        { title: 'Webhook 数', dataIndex: 'webhookCount', width: 110 },
        { title: '过期时间', dataIndex: 'expiresAt', width: 180, render: formatDateTime },
        {
            title: '最近使用',
            key: 'lastUsed',
            width: 220,
            render: (_, record) =>
                record.lastUsedAt ? (
                    <div className="leading-5">
                        <div>{formatDateTime(record.lastUsedAt)}</div>
                        <div className="text-xs text-slate-500">{record.lastUsedIp || '--'}</div>
                    </div>
                ) : (
                    '--'
                ),
        },
        {
            title: '备注',
            dataIndex: 'remark',
            width: 220,
            ellipsis: true,
            render: (value: string | null) => value || '--',
        },
        {
            title: '操作',
            key: 'actions',
            width: 220,
            fixed: screens.md ? 'right' : undefined,
            render: (_, record) => (
                <Space wrap>
                    {canEdit && (
                        <Button
                            type="link"
                            onClick={() => {
                                setEditingAccessKey(record);
                                setAccessKeyModalOpen(true);
                            }}
                        >
                            编辑
                        </Button>
                    )}
                    {canEdit && (
                        <Button
                            type="link"
                            icon={<ReloadOutlined />}
                            onClick={() =>
                                modal.confirm({
                                    title: `确认轮换「${record.name}」的认证 Key 吗？`,
                                    content: '旧 Key 会立即失效，第三方系统需要同步更新新 Key。',
                                    okText: '确认轮换',
                                    onOk: () =>
                                        keyRotate
                                            .mutateAsync(record.id)
                                            .then((secret) =>
                                                showSecret(secret, '认证 Key 已轮换')
                                            ),
                                })
                            }
                        >
                            轮换
                        </Button>
                    )}
                    {canDelete && (
                        <Button
                            type="link"
                            danger
                            onClick={() =>
                                modal.confirm({
                                    title: `确认删除「${record.name}」吗？`,
                                    content:
                                        '删除后主动调用、关联 Webhook 和 MQTT 推送都会失效，此操作不可撤销。',
                                    okText: '确定删除',
                                    okButtonProps: { danger: true },
                                    onOk: () => keyDelete.mutateAsync(record.id),
                                })
                            }
                        >
                            删除
                        </Button>
                    )}
                </Space>
            ),
        },
    ];
    const webhookColumns: ColumnsType<Access.WebhookItem> = [
        { title: '名称', dataIndex: 'name', width: 180 },
        { title: '绑定调用配置', dataIndex: 'accessKeyName', width: 180 },
        {
            title: '回调地址',
            dataIndex: 'url',
            width: 280,
            render: (value: string) => (
                <Tooltip title={value}>
                    <span className="inline-block max-w-[260px] truncate">{value}</span>
                </Tooltip>
            ),
        },
        {
            title: '事件',
            dataIndex: 'eventTypes',
            width: 260,
            render: (value: Access.EventType[]) => renderEventTypes(value),
        },
        {
            title: '继承设备',
            dataIndex: 'deviceIds',
            width: 320,
            render: (value: string[]) => renderDeviceScope(value),
        },
        {
            title: '超时',
            dataIndex: 'timeoutSeconds',
            width: 90,
            render: (value: number) => `${value}s`,
        },
        {
            title: 'TLS',
            dataIndex: 'skipTlsVerify',
            width: 110,
            render: (value: boolean) =>
                value ? <Tag color="warning">跳过验证</Tag> : <Tag color="success">严格验证</Tag>,
        },
        {
            title: '状态',
            dataIndex: 'status',
            width: 90,
            render: (value: Access.Status) => <StatusTag status={value} />,
        },
        {
            title: '最近触发',
            dataIndex: 'lastTriggeredAt',
            width: 180,
            render: formatDateTime,
        },
        {
            title: '最近结果',
            key: 'lastResult',
            width: 220,
            render: (_, record) => {
                if (record.lastFailureAt) {
                    return (
                        <Tooltip title={record.lastError || '推送失败'}>
                            <div className="leading-5">
                                <Tag color="error" className="!mr-0">
                                    失败
                                </Tag>
                                <div className="text-xs text-slate-500">
                                    {formatDateTime(record.lastFailureAt)}
                                    {record.lastHttpStatus ? ` / ${record.lastHttpStatus}` : ''}
                                </div>
                            </div>
                        </Tooltip>
                    );
                }
                if (record.lastSuccessAt) {
                    return (
                        <div className="leading-5">
                            <Tag color="success" className="!mr-0">
                                成功
                            </Tag>
                            <div className="text-xs text-slate-500">
                                {formatDateTime(record.lastSuccessAt)}
                            </div>
                        </div>
                    );
                }
                return '--';
            },
        },
        {
            title: '签名',
            key: 'secret',
            width: 90,
            render: (_, record) =>
                record.hasSecret ? <Tag color="purple">已配置</Tag> : <Tag>未配置</Tag>,
        },
        {
            title: '操作',
            key: 'actions',
            width: 160,
            fixed: screens.md ? 'right' : undefined,
            render: (_, record) => (
                <Space wrap>
                    {canEdit && (
                        <Button
                            type="link"
                            onClick={() => {
                                setEditingWebhook(record);
                                setWebhookModalOpen(true);
                            }}
                        >
                            编辑
                        </Button>
                    )}
                    {canDelete && (
                        <Button
                            type="link"
                            danger
                            onClick={() =>
                                modal.confirm({
                                    title: `确认删除「${record.name}」吗？`,
                                    content: '删除后将停止该回调地址的事件推送，此操作不可撤销。',
                                    okText: '确定删除',
                                    okButtonProps: { danger: true },
                                    onOk: () => webhookDelete.mutateAsync(record.id),
                                })
                            }
                        >
                            删除
                        </Button>
                    )}
                </Space>
            ),
        },
    ];
    const renderLogPayload = (title: string, payload: Record<string, unknown>) => {
        const content = formatPayload(payload);
        if (!content) return null;
        return (
            <Popover
                trigger="click"
                title={title}
                content={
                    <Typography.Paragraph
                        copyable={{ text: content, tooltips: ['复制内容', '已复制'] }}
                        className="!mb-0 max-h-[360px] max-w-[520px] overflow-auto whitespace-pre-wrap rounded-lg bg-slate-950 p-3 font-mono text-xs !text-slate-100"
                    >
                        {content}
                    </Typography.Paragraph>
                }
            >
                <Button size="small" type="link" className="!px-0">
                    {title}
                </Button>
            </Popover>
        );
    };
    const mqttColumns: ColumnsType<Access.MqttItem> = [
        {
            title: '名称 / 调用配置',
            key: 'name',
            width: 210,
            render: (_, row) => (
                <>
                    <div>{row.name}</div>
                    <Typography.Text type="secondary">{row.accessKeyName}</Typography.Text>
                </>
            ),
        },
        {
            title: 'Broker / Topic',
            key: 'broker',
            width: 320,
            render: (_, row) => (
                <>
                    <div className="break-all">{row.brokerUrl}</div>
                    <Typography.Text type="secondary">{'iot/{deviceId}（固定）'}</Typography.Text>
                </>
            ),
        },
        {
            title: '状态',
            dataIndex: 'status',
            width: 100,
            render: (value: Access.Status) => <StatusTag status={value} />,
        },
        { title: '事件', dataIndex: 'eventTypes', width: 240, render: renderEventTypes },
        { title: '最近成功', dataIndex: 'lastSuccessAt', width: 180, render: formatDateTime },
        { title: '最近失败', dataIndex: 'lastFailureAt', width: 180, render: formatDateTime },
        {
            title: '错误',
            dataIndex: 'lastError',
            width: 260,
            render: (value: string | null) => value || '--',
        },
        {
            title: '操作',
            key: 'actions',
            width: 160,
            fixed: screens.md ? 'right' : undefined,
            render: (_, row) => (
                <Space>
                    {canEdit && (
                        <Button
                            type="link"
                            onClick={() => {
                                setEditingMqtt(row);
                                setMqttModalOpen(true);
                            }}
                        >
                            编辑
                        </Button>
                    )}
                    {canDelete && (
                        <Button
                            type="link"
                            danger
                            onClick={() =>
                                modal.confirm({
                                    title: '删除 MQTT 推送？',
                                    content: row.name,
                                    okButtonProps: { danger: true },
                                    onOk: () => mqttDelete.mutateAsync(row.id),
                                })
                            }
                        >
                            删除
                        </Button>
                    )}
                </Space>
            ),
        },
    ];
    const logColumns: ColumnsType<Access.LogItem> = [
        { title: '时间', dataIndex: 'createdAt', width: 180, render: formatDateTime },
        {
            title: '调用配置',
            key: 'accessKey',
            width: 220,
            render: (_, record) => (
                <div className="leading-5">
                    <div>{record.accessKeyName ?? '--'}</div>
                    <div className="text-xs text-slate-500">
                        {record.mqttName
                            ? `MQTT：${record.mqttName}`
                            : record.webhookName
                              ? `Webhook：${record.webhookName}`
                              : '--'}
                    </div>
                </div>
            ),
        },
        {
            title: '来源',
            dataIndex: 'direction',
            width: 120,
            render: (value: 'pull' | 'push') => (
                <Tag color={value === 'pull' ? 'blue' : 'purple'}>
                    {value === 'pull' ? '主动调用' : '事件推送'}
                </Tag>
            ),
        },
        {
            title: '动作 / 事件',
            key: 'action',
            width: 220,
            render: (_, record) => {
                const meta = eventMeta(record.eventType);
                return (
                    <div className="leading-5">
                        <div>{actionLabel(record.action)}</div>
                        <div className="text-xs text-slate-500">
                            {record.eventType ? (
                                <Tag color={meta?.color ?? 'default'} className="!mr-0">
                                    {meta?.label ?? record.eventType}
                                </Tag>
                            ) : (
                                '--'
                            )}
                        </div>
                    </div>
                );
            },
        },
        {
            title: '设备',
            key: 'device',
            width: 260,
            render: (_, record) =>
                record.deviceId || record.deviceCode ? (
                    <div className="leading-5">
                        {record.deviceId ? (
                            renderDeviceIdentity(record.deviceId, record.deviceCode)
                        ) : (
                            <div>{record.deviceCode}</div>
                        )}
                    </div>
                ) : (
                    '--'
                ),
        },
        {
            title: '目标',
            key: 'target',
            width: 260,
            render: (_, record) => (
                <Tooltip title={record.target ?? '--'}>
                    <div className="leading-5">
                        <div className="truncate">{record.target ?? '--'}</div>
                        <div className="text-xs text-slate-500">
                            {record.httpMethod?.toUpperCase() || '--'}
                        </div>
                    </div>
                </Tooltip>
            ),
        },
        {
            title: '结果',
            key: 'status',
            width: 130,
            render: (_, record) => (
                <div className="leading-5">
                    <Tag
                        color={record.status === 'success' ? 'success' : 'error'}
                        className="!mr-0"
                    >
                        {record.status === 'success' ? '成功' : '失败'}
                    </Tag>
                    <div className="text-xs text-slate-500">
                        {record.action === 'mqtt'
                            ? 'MQTT / QoS 1'
                            : `HTTP ${record.httpStatus ?? '--'}`}
                    </div>
                </div>
            ),
        },
        {
            title: '摘要',
            dataIndex: 'message',
            width: 220,
            render: (value: string | null) => value || '--',
        },
        {
            title: '载荷',
            key: 'payload',
            width: 150,
            render: (_, record) => {
                const request = renderLogPayload('请求体', record.requestPayload);
                const response = renderLogPayload('响应体', record.responsePayload);
                return request || response ? (
                    <Space size={8} wrap>
                        {request}
                        {response}
                    </Space>
                ) : (
                    '--'
                );
            },
        },
        {
            title: '操作',
            key: 'actions',
            width: 90,
            fixed: screens.md ? 'right' : undefined,
            render: (_, record) => (
                <Button type="link" onClick={() => setSelectedLog(record)}>
                    详情
                </Button>
            ),
        },
    ];
    const updateLogQuery = (patch: Partial<Access.LogQuery>) => {
        setLogQuery((previous) => ({ ...previous, ...patch, page: 1 }));
    };
    const handleLogTableChange = (pagination: TablePaginationConfig) => {
        setLogQuery((previous) => ({
            ...previous,
            page: pagination.current ?? Number(previous.page ?? 1),
            pageSize: pagination.pageSize ?? Number(previous.pageSize ?? 20),
        }));
    };
    const selectedRequest = formatPayload(selectedLog?.requestPayload);
    const selectedResponse = formatPayload(selectedLog?.responsePayload);
    const selectedEvent = eventMeta(selectedLog?.eventType);
    return (
        <PageContainer
            header={
                <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="flex flex-wrap items-center gap-2">
                        <h3 className="m-0 text-base font-medium">开放接入</h3>
                        {activeTab === TAB_WEBHOOK && (
                            <Tag color="processing">设备范围继承调用配置，不单独配置</Tag>
                        )}
                    </div>
                    <Space wrap>
                        <Search
                            allowClear
                            value={
                                activeTab === TAB_WEBHOOK
                                    ? webhookKeyword
                                    : activeTab === TAB_MQTT
                                      ? mqttKeyword
                                      : logKeyword
                            }
                            placeholder={
                                activeTab === TAB_WEBHOOK
                                    ? '搜索名称 / 回调地址 / 调用配置 / 设备 / 错误信息'
                                    : activeTab === TAB_MQTT
                                      ? '搜索名称 / Broker / Topic / 调用配置'
                                      : '搜索调用配置 / 设备 / 目标 / 事件 / 摘要'
                            }
                            onChange={(event) => {
                                if (activeTab === TAB_WEBHOOK) {
                                    setWebhookKeyword(event.target.value);
                                } else if (activeTab === TAB_MQTT) {
                                    setMqttKeyword(event.target.value);
                                } else {
                                    setLogKeyword(event.target.value);
                                }
                            }}
                            className="w-full sm:w-[320px]"
                        />
                        <Button
                            icon={<ReloadOutlined />}
                            loading={
                                refreshingKeys ||
                                refreshingWebhooks ||
                                refreshingMqtt ||
                                refreshingLogs
                            }
                            onClick={() => {
                                void refreshKeys();
                                if (activeTab === TAB_WEBHOOK) void refreshWebhooks();
                                else if (activeTab === TAB_MQTT) void refreshMqtt();
                                else void refreshLogs();
                            }}
                        >
                            刷新
                        </Button>
                        <Button
                            icon={<QuestionCircleOutlined />}
                            onClick={() => setUsageModalOpen(true)}
                        >
                            使用说明
                        </Button>
                        {canAdd && (
                            <Button
                                type="primary"
                                icon={<PlusOutlined />}
                                onClick={() => {
                                    setEditingAccessKey(null);
                                    setAccessKeyModalOpen(true);
                                }}
                            >
                                新建调用配置
                            </Button>
                        )}
                        {activeTab === TAB_MQTT && canAdd && (
                            <Button
                                icon={<PlusOutlined />}
                                disabled={!accessKeys.length}
                                onClick={() => {
                                    setEditingMqtt(null);
                                    setMqttModalOpen(true);
                                }}
                            >
                                新建 MQTT 推送
                            </Button>
                        )}
                        {activeTab === TAB_WEBHOOK && canAdd && (
                            <Button
                                icon={<PlusOutlined />}
                                disabled={!accessKeys.length}
                                onClick={() => {
                                    setEditingWebhook(null);
                                    setWebhookModalOpen(true);
                                }}
                            >
                                新建 Webhook
                            </Button>
                        )}
                    </Space>
                </div>
            }
        >
            <div className="h-full overflow-y-auto pr-1">
                <div className="mb-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                    <Card size="small" className="border-slate-200">
                        <div className="text-xs uppercase tracking-[0.2em] text-slate-400">
                            调用配置
                        </div>
                        <div className="mt-2 text-3xl font-semibold text-slate-900">
                            {accessKeys.length}
                        </div>
                        <div className="mt-2 text-sm text-slate-500">
                            启用 {accessKeyEnabledCount} / 总数 {accessKeys.length}
                        </div>
                    </Card>
                    <Card size="small" className="border-slate-200">
                        <div className="text-xs uppercase tracking-[0.2em] text-slate-400">
                            Webhook
                        </div>
                        <div className="mt-2 text-3xl font-semibold text-slate-900">
                            {webhooks.length}
                        </div>
                        <div className="mt-2 text-sm text-slate-500">
                            启用 {webhookEnabledCount} / 总数 {webhooks.length}
                        </div>
                    </Card>
                    <Card size="small" className="border-slate-200">
                        <div className="text-xs uppercase tracking-[0.2em] text-slate-400">
                            设备覆盖
                        </div>
                        <div className="mt-2 text-3xl font-semibold text-slate-900">
                            {coveredDeviceCount}
                        </div>
                        <div className="mt-2 text-sm text-slate-500">
                            当前所有调用配置共覆盖 {coveredDeviceCount} 台设备
                        </div>
                    </Card>
                    <Card size="small" className="border-slate-200">
                        <div className="text-xs uppercase tracking-[0.2em] text-slate-400">
                            调用记录
                        </div>
                        <div className="mt-2 text-3xl font-semibold text-slate-900">{logTotal}</div>
                        <div className="mt-2 text-sm text-slate-500">
                            包含主动调用、Webhook 与 MQTT 推送结果
                        </div>
                    </Card>
                </div>

                <div className="mb-4 space-y-4">
                    <Alert
                        type="info"
                        showIcon
                        title="调用配置说明"
                        description="支持创建多个 AccessKey；每个调用配置可单独设置设备范围与访问权限，Webhook 和 MQTT 推送再绑定到对应配置。"
                    />
                    <Table<Access.KeyItem>
                        rowKey="id"
                        columns={accessKeyColumns}
                        dataSource={accessKeys}
                        loading={loadingAccessKeys}
                        pagination={false}
                        sticky
                        scroll={{ x: 1760, y: 280 }}
                        locale={{
                            emptyText: canAdd
                                ? '暂无调用配置，请点击右上角“新建调用配置”'
                                : '暂无调用配置',
                        }}
                    />
                </div>

                <div className="rounded-xl border border-slate-200 bg-white">
                    <Tabs
                        activeKey={activeTab}
                        onChange={setActiveTab}
                        className="px-4 pt-3"
                        items={[
                            {
                                key: TAB_WEBHOOK,
                                label: `Webhook (${webhooks.length})`,
                                children: (
                                    <div className="space-y-4">
                                        <Alert
                                            type="info"
                                            showIcon
                                            title="推送规则"
                                            description="Webhook 按事件类型分发，设备范围继承绑定调用配置；若还没有调用配置，请先创建调用配置。"
                                        />
                                        <Table<Access.WebhookItem>
                                            rowKey="id"
                                            columns={webhookColumns}
                                            dataSource={filteredWebhooks}
                                            loading={loadingWebhooks}
                                            pagination={false}
                                            sticky
                                            scroll={{ x: 1950, y: 340 }}
                                        />
                                    </div>
                                ),
                            },
                            {
                                key: TAB_MQTT,
                                label: `MQTT (${mqtt.length})`,
                                children: (
                                    <div className="space-y-4">
                                        <Alert
                                            type="info"
                                            showIcon
                                            title="MQTT 事件推送"
                                            description="推送授权设备的数据、图片、命令状态和告警。QoS 1 确认成功后完成投递，失败自动重试；可手动刷新查看结果。"
                                        />
                                        <Table<Access.MqttItem>
                                            rowKey="id"
                                            columns={mqttColumns}
                                            loading={loadingMqtt}
                                            dataSource={mqtt.filter((row) =>
                                                [row.name, row.brokerUrl, row.accessKeyName].some(
                                                    (value) =>
                                                        value
                                                            .toLowerCase()
                                                            .includes(
                                                                mqttKeyword.trim().toLowerCase()
                                                            )
                                                )
                                            )}
                                            pagination={false}
                                            sticky
                                            scroll={{ x: 1650, y: 340 }}
                                        />
                                    </div>
                                ),
                            },
                            {
                                key: TAB_LOG,
                                label: `调用记录 (${logTotal})`,
                                children: (
                                    <div className="space-y-4">
                                        <Alert
                                            type="info"
                                            showIcon
                                            title="记录说明"
                                            description="主动调用记录第三方查询与控制请求；Webhook 和 MQTT 推送记录平台投递事件的结果。"
                                        />
                                        <div className="flex flex-wrap gap-3">
                                            <Select
                                                allowClear
                                                value={logQuery.accessKeyId}
                                                placeholder="筛选调用配置"
                                                className="min-w-[180px]"
                                                options={accessKeys.map((item) => ({
                                                    label: item.name,
                                                    value: item.id,
                                                }))}
                                                onChange={(accessKeyId) =>
                                                    updateLogQuery({ accessKeyId })
                                                }
                                            />
                                            <Select
                                                allowClear
                                                value={logQuery.direction}
                                                placeholder="筛选来源"
                                                className="min-w-[160px]"
                                                options={LOG_DIRECTION_OPTIONS}
                                                onChange={(direction) =>
                                                    updateLogQuery({ direction })
                                                }
                                            />
                                            <Select
                                                allowClear
                                                value={logQuery.action}
                                                placeholder="筛选动作"
                                                className="min-w-[160px]"
                                                options={LOG_ACTION_OPTIONS}
                                                onChange={(action) => updateLogQuery({ action })}
                                            />
                                            <Select
                                                allowClear
                                                value={logQuery.status}
                                                placeholder="筛选结果"
                                                className="min-w-[160px]"
                                                options={LOG_STATUS_OPTIONS}
                                                onChange={(status) => updateLogQuery({ status })}
                                            />
                                            <Button
                                                onClick={() => {
                                                    setLogKeyword('');
                                                    setLogQuery({
                                                        page: 1,
                                                        pageSize: Number(logQuery.pageSize ?? 20),
                                                    });
                                                }}
                                            >
                                                重置筛选
                                            </Button>
                                        </div>
                                        <Table<Access.LogItem>
                                            rowKey="id"
                                            columns={logColumns}
                                            dataSource={filteredLogs}
                                            loading={loadingLogs}
                                            onChange={handleLogTableChange}
                                            pagination={{
                                                current:
                                                    logPage?.page ?? Number(logQuery.page ?? 1),
                                                pageSize:
                                                    logPage?.pageSize ??
                                                    Number(logQuery.pageSize ?? 20),
                                                total: logTotal,
                                                showSizeChanger: true,
                                                showTotal: (total) => `共 ${total} 条`,
                                            }}
                                            sticky
                                            scroll={{ x: 1960, y: 340 }}
                                        />
                                    </div>
                                ),
                            },
                        ]}
                    />
                </div>
            </div>

            <AccessKeyFormModal
                open={accessKeyModalOpen}
                editing={editingAccessKey}
                loading={keyCreate.isPending || keyUpdate.isPending}
                devices={devices}
                devicesLoading={loadingDevices}
                onCancel={() => {
                    setAccessKeyModalOpen(false);
                    setEditingAccessKey(null);
                }}
                onFinish={(values) => {
                    if (editingAccessKey) {
                        keyUpdate.mutate(
                            { id: editingAccessKey.id, data: values },
                            {
                                onSuccess: () => {
                                    setAccessKeyModalOpen(false);
                                    setEditingAccessKey(null);
                                },
                            }
                        );
                        return;
                    }
                    keyCreate.mutate(values, {
                        onSuccess: (secret) => {
                            setAccessKeyModalOpen(false);
                            showSecret(secret, '认证 Key 已创建');
                        },
                    });
                }}
            />

            <MqttFormModal
                open={mqttModalOpen}
                editing={editingMqtt}
                accessKeys={accessKeys}
                loading={mqttCreate.isPending || mqttUpdate.isPending}
                onCancel={() => {
                    setMqttModalOpen(false);
                    setEditingMqtt(null);
                }}
                onFinish={(data) => {
                    const options = {
                        onSuccess: () => {
                            setMqttModalOpen(false);
                            setEditingMqtt(null);
                        },
                    };
                    if (editingMqtt) mqttUpdate.mutate({ id: editingMqtt.id, data }, options);
                    else mqttCreate.mutate(data, options);
                }}
            />
            <WebhookFormModal
                open={webhookModalOpen}
                editing={editingWebhook}
                loading={webhookCreate.isPending || webhookUpdate.isPending}
                accessKeys={accessKeys}
                onCancel={() => {
                    setWebhookModalOpen(false);
                    setEditingWebhook(null);
                }}
                onFinish={(values) => {
                    if (editingWebhook) {
                        webhookUpdate.mutate(
                            { id: editingWebhook.id, data: values },
                            {
                                onSuccess: () => {
                                    setWebhookModalOpen(false);
                                    setEditingWebhook(null);
                                },
                            }
                        );
                        return;
                    }
                    webhookCreate.mutate(values, {
                        onSuccess: () => setWebhookModalOpen(false),
                    });
                }}
            />

            <Modal
                open={usageModalOpen}
                title="开放接入使用说明"
                footer={null}
                width={980}
                destroyOnHidden
                onCancel={() => setUsageModalOpen(false)}
            >
                <Tabs
                    size="small"
                    items={[
                        {
                            key: 'usage-call',
                            label: '数据查询与控制',
                            children: (
                                <div className="space-y-4">
                                    <Alert
                                        type="info"
                                        showIcon
                                        title="主动调用说明"
                                        description="一个调用配置可统一管理实时、历史、控制和告警权限。建议先获取授权设备 UUID，后续请求统一使用设备 ID。"
                                    />
                                    <div className="mx-auto max-w-4xl space-y-4">
                                        <UsageCodeBlock
                                            title="1. 鉴权方式"
                                            description="AccessKey 只通过请求头传递，避免密钥出现在 URL 和代理日志中。"
                                            code={AUTH_HEADER_EXAMPLE}
                                        />
                                        <UsageCodeBlock
                                            title="2. 设备列表"
                                            description="先获取当前 AccessKey 可访问设备的 UUID、编码和名称。"
                                            code={DEVICE_LIST_EXAMPLE}
                                        />
                                        <UsageCodeBlock
                                            title="3. 实时数据查询"
                                            description="按设备 UUID 查询当前点位数据。"
                                            code={REALTIME_EXAMPLE}
                                        />
                                        <UsageCodeBlock
                                            title="4. 历史数据查询"
                                            description="指定 UTC 时间范围，并可使用分页参数。"
                                            code={HISTORY_EXAMPLE}
                                        />
                                        <UsageCodeBlock
                                            title="5. 控制下发"
                                            description="传入 elementId 和字符串值；受理结果与设备实际执行结果是两个阶段。"
                                            code={COMMAND_EXAMPLE}
                                        />
                                        <UsageCodeBlock
                                            title="6. 告警查询"
                                            description="可按设备、状态和严重级别筛选授权范围内的告警。"
                                            code={ALERT_EXAMPLE}
                                        />
                                    </div>
                                </div>
                            ),
                        },
                        {
                            key: 'usage-mqtt',
                            label: 'MQTT 推送',
                            children: (
                                <div className="space-y-4">
                                    <Alert
                                        type="info"
                                        showIcon
                                        title="接收 MQTT 事件"
                                        description="先创建调用配置，再在 MQTT 页签绑定该配置并填写 Broker 和需要的事件。Topic 固定为 iot/{deviceId}，订阅端使用自己的 Broker 账号。MQTT 推送体与 Webhook 相同，按 deliveryId 去重；所有时间为 UTC。"
                                    />
                                    <UsageCodeBlock
                                        title="订阅示例"
                                        description="Topic 固定为 iot/{deviceId}；按设备 UUID 或通配符订阅，通过 JSON 的 event 字段区分事件。"
                                        code={
                                            "mosquitto_sub -h broker.example.com -p 8883 --capath /etc/ssl/certs -u subscriber -t 'iot/+' -q 1"
                                        }
                                    />
                                    <UsageCodeBlock
                                        title="事件体示例"
                                        description="QoS 1 可能重复投递；Broker 确认表示已接收，不代表下游业务已处理。"
                                        code={WEBHOOK_PAYLOAD_EXAMPLE}
                                    />
                                </div>
                            ),
                        },
                        {
                            key: 'usage-webhook',
                            label: 'Webhook 推送',
                            children: (
                                <div className="space-y-4">
                                    <Alert
                                        type="info"
                                        showIcon
                                        title="推送说明"
                                        description={
                                            <>
                                                Webhook 按事件类型过滤，设备范围继承绑定调用配置。
                                                <br />
                                                事件包括：
                                                {renderEventTypes(
                                                    EVENT_TYPE_OPTIONS.map((item) => item.value)
                                                )}
                                            </>
                                        }
                                    />
                                    <div className="mx-auto max-w-4xl space-y-4">
                                        <UsageCodeBlock
                                            title="1. 接收方需要关注的请求头"
                                            description="配置 secret 时，以原始请求体计算 HMAC-SHA256 签名。"
                                            code={WEBHOOK_HEADER_EXAMPLE}
                                        />
                                        <UsageCodeBlock
                                            title="2. Webhook 推送体示例"
                                            description="使用 deliveryId 去重，所有时间均为 UTC ISO 8601。"
                                            code={WEBHOOK_PAYLOAD_EXAMPLE}
                                        />
                                    </div>
                                </div>
                            ),
                        },
                    ]}
                />
            </Modal>

            <Drawer
                title="调用记录详情"
                open={selectedLog !== null}
                size={880}
                destroyOnHidden
                onClose={() => setSelectedLog(null)}
            >
                {selectedLog && (
                    <div className="space-y-4">
                        <Descriptions
                            bordered
                            size="small"
                            column={2}
                            items={[
                                {
                                    key: 'createdAt',
                                    label: '时间',
                                    children: formatDateTime(selectedLog.createdAt),
                                },
                                {
                                    key: 'status',
                                    label: '结果',
                                    children: (
                                        <Space size={8} wrap>
                                            <Tag
                                                color={
                                                    selectedLog.status === 'success'
                                                        ? 'success'
                                                        : 'error'
                                                }
                                            >
                                                {selectedLog.status === 'success' ? '成功' : '失败'}
                                            </Tag>
                                            <span>
                                                {selectedLog.action === 'mqtt'
                                                    ? 'MQTT / QoS 1'
                                                    : `HTTP ${selectedLog.httpStatus ?? '--'}`}
                                            </span>
                                        </Space>
                                    ),
                                },
                                {
                                    key: 'accessKey',
                                    label: '调用配置',
                                    children: selectedLog.accessKeyName ?? '--',
                                },
                                {
                                    key: 'webhook',
                                    label: '推送配置',
                                    children:
                                        selectedLog.mqttName ?? selectedLog.webhookName ?? '--',
                                },
                                {
                                    key: 'direction',
                                    label: '来源',
                                    children: (
                                        <Tag
                                            color={
                                                selectedLog.direction === 'pull' ? 'blue' : 'purple'
                                            }
                                        >
                                            {selectedLog.direction === 'pull'
                                                ? '主动调用'
                                                : '事件推送'}
                                        </Tag>
                                    ),
                                },
                                {
                                    key: 'action',
                                    label: '动作',
                                    children: actionLabel(selectedLog.action),
                                },
                                {
                                    key: 'eventType',
                                    label: '事件',
                                    children: selectedLog.eventType ? (
                                        <Tag color={selectedEvent?.color ?? 'default'}>
                                            {selectedEvent?.label ?? selectedLog.eventType}
                                        </Tag>
                                    ) : (
                                        '--'
                                    ),
                                },
                                {
                                    key: 'httpMethod',
                                    label: '请求方法',
                                    children: selectedLog.httpMethod?.toUpperCase() ?? '--',
                                },
                                {
                                    key: 'target',
                                    label: '目标',
                                    span: 2,
                                    children: selectedLog.target ?? '--',
                                },
                                {
                                    key: 'device',
                                    label: '设备',
                                    children: selectedLog.deviceId
                                        ? renderDeviceIdentity(
                                              selectedLog.deviceId,
                                              selectedLog.deviceCode
                                          )
                                        : selectedLog.deviceCode || '--',
                                },
                                {
                                    key: 'requestIp',
                                    label: '请求 IP',
                                    children: selectedLog.requestIp ?? '--',
                                },
                                {
                                    key: 'message',
                                    label: '摘要',
                                    span: 2,
                                    children: selectedLog.message ?? '--',
                                },
                            ]}
                        />
                        <div className="grid gap-4 xl:grid-cols-2">
                            <JsonPayloadCard
                                title="请求体"
                                content={selectedRequest}
                                emptyText="当前记录没有请求体"
                            />
                            <JsonPayloadCard
                                title="响应体"
                                content={selectedResponse}
                                emptyText="当前记录没有响应体"
                            />
                        </div>
                    </div>
                )}
            </Drawer>
        </PageContainer>
    );
}
export function AccessPage() {
    const { has } = usePermissions();
    if (!has('iot:open-access:query')) {
        return (
            <PageContainer>
                <Result
                    status="403"
                    title="无权限"
                    subTitle="您没有开放接入查询权限，请联系管理员分配 iot:open-access:query"
                />
            </PageContainer>
        );
    }
    return <AccessContent />;
}

export default AccessPage;
