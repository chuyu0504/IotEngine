import { CloseOutlined, PlusOutlined } from '@ant-design/icons';
import type { UseMutationResult } from '@tanstack/react-query';
import {
    App,
    AutoComplete,
    Button,
    Divider,
    Form,
    Input,
    InputNumber,
    Modal,
    Result,
    Select,
    Skeleton,
    Space,
    Spin,
    Table,
    Tag,
} from 'antd';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import { useEffect, useMemo, useState } from 'react';
import { FormModal } from '@/components/form_modal';
import { LiveQueryError } from '@/components/live_query_error';
import { PageContainer } from '@/components/page_container';
import { StatusTag } from '@/components/status_tag';
import { useDebounceFn } from '@/hooks/use_debounce_fn';
import { usePermission } from '@/hooks/use_permission';
import { formatDateTime } from '@/utils/date_time';
import { createUuid } from '@/utils/uuid';
import { useDeviceConfigurationList, useDeviceOptions } from '../device/device.service';
import {
    protocolPointOptions,
    useProtocolConfigDetail,
    useProtocolConfigOptions,
} from '../protocol/protocol.service';
import {
    type Modbus,
    PROTOCOL_TYPES,
    type Protocol,
    type S7,
    type SL651,
} from '../protocol/protocol.types';
import {
    useAlertAcknowledge,
    useAlertApplyTemplate,
    useAlertBatchAcknowledge,
    useAlertRecordList,
    useAlertRuleBatchDelete,
    useAlertRuleDelete,
    useAlertRuleList,
    useAlertRuleSave,
    useAlertStats,
    useAlertTemplateDelete,
    useAlertTemplateList,
    useAlertTemplateLoader,
    useAlertTemplateSave,
} from './alert.service';
import type { Alert } from './alert.types';

const CONDITION_TYPE_OPTIONS = [
    { label: '阈值', value: 'threshold' },
    { label: '离线检测', value: 'offline' },
    { label: '变化率', value: 'rate_of_change' },
];
const OPERATOR_OPTIONS = [
    { label: '>', value: '>' },
    { label: '>=', value: '>=' },
    { label: '<', value: '<' },
    { label: '<=', value: '<=' },
    { label: '==', value: '==' },
    { label: '!=', value: '!=' },
];
const DIRECTION_OPTIONS = [
    { label: '任意', value: 'any' },
    { label: '上升', value: 'rise' },
    { label: '下降', value: 'fall' },
];
/** 字典映射项（简化版，同时适用 SL651 和 Modbus） */
export interface DictItem {
    key: string;
    label: string;
    /** 触发值（仅 BIT 模式，"0"或"1"） */
    value?: string;
}
export interface ElementOption {
    value: string;
    label: string;
    /** 字典映射类型：BIT=位映射 VALUE=值映射（仅 SL651 DICT 要素） */
    dictMapType?: 'VALUE' | 'BIT';
    /** 字典映射项 */
    dictItems?: DictItem[];
}
interface ConditionEditorProps {
    value: Alert.Condition;
    elementOptions: ElementOption[];
    onChange: (value: Alert.Condition) => void;
    onRemove: () => void;
}
export function ConditionEditor({
    value,
    elementOptions,
    onChange,
    onRemove,
}: ConditionEditorProps) {
    const update = (patch: Partial<Alert.Condition>) => {
        onChange({ ...value, ...patch });
    };
    const hasElements = elementOptions.length > 0;
    // 当前选中要素的字典信息
    const selectedElement = elementOptions.find((e) => e.value === value.elementKey);
    const isBitDict = selectedElement?.dictMapType === 'BIT' && !!selectedElement.dictItems?.length;
    const isValueDict =
        !!selectedElement?.dictItems?.length && selectedElement.dictMapType !== 'BIT';
    // 要素选择器（threshold 和 rate_of_change 共用）
    const renderElementSelect = () => (
        <div className="flex-1 min-w-0">
            {hasElements ? (
                <Select
                    value={value.elementKey || undefined}
                    onChange={(v: string) => update({ elementKey: v, bitIndex: undefined })}
                    options={elementOptions}
                    showSearch
                    optionFilterProp="label"
                    placeholder="选择要素"
                    className="w-full"
                />
            ) : (
                <Input
                    value={value.elementKey}
                    onChange={(e) => update({ elementKey: e.target.value })}
                    placeholder="要素标识"
                />
            )}
        </div>
    );
    return (
        <div className="border border-gray-200 rounded-md p-3 mb-2 relative">
            <Button
                type="text"
                size="small"
                danger
                icon={<CloseOutlined />}
                onClick={onRemove}
                className="absolute top-1 right-1"
            />

            <div className="flex items-start gap-2">
                <div className="w-[100px] shrink-0">
                    <Select
                        value={value.type}
                        onChange={(type: Alert.ConditionType) => {
                            if (type === 'threshold') {
                                onChange({ type, elementKey: '', operator: '>', value: '' });
                            } else if (type === 'offline') {
                                onChange({ type, duration: 300 });
                            } else {
                                onChange({
                                    type,
                                    elementKey: '',
                                    changeRate: '',
                                    changeDirection: 'any',
                                });
                            }
                        }}
                        options={CONDITION_TYPE_OPTIONS}
                        placeholder="条件类型"
                        className="w-full"
                    />
                </div>

                {value.type === 'threshold' && (
                    <>
                        {renderElementSelect()}
                        {isBitDict && (
                            <div className="w-[120px] shrink-0">
                                <Select
                                    value={
                                        value.bitIndex != null ? String(value.bitIndex) : undefined
                                    }
                                    onChange={(v: string) => update({ bitIndex: Number(v) })}
                                    options={selectedElement?.dictItems?.map((d) => ({
                                        value: d.key,
                                        label: `${d.label}(${d.key})`,
                                    }))}
                                    placeholder="选择位"
                                    className="w-full"
                                />
                            </div>
                        )}
                        <div className="w-[70px] shrink-0">
                            <Select
                                value={value.operator}
                                onChange={(operator: Alert.Operator) => update({ operator })}
                                options={OPERATOR_OPTIONS}
                                className="w-full"
                            />
                        </div>
                        <div className="w-[130px] shrink-0">
                            {isValueDict ? (
                                <Select
                                    value={value.value || undefined}
                                    onChange={(v: string) => update({ value: v })}
                                    options={selectedElement?.dictItems?.map((d) => ({
                                        value: d.key,
                                        label: d.label,
                                    }))}
                                    placeholder="选择值"
                                    className="w-full"
                                />
                            ) : (
                                <Input
                                    value={value.value}
                                    onChange={(e) => update({ value: e.target.value })}
                                    placeholder="阈值"
                                />
                            )}
                        </div>
                    </>
                )}

                {value.type === 'offline' && (
                    <div className="flex-1 min-w-0">
                        <InputNumber
                            value={value.duration}
                            onChange={(v) => update({ duration: v ?? 300 })}
                            addonAfter="秒"
                            min={60}
                            max={86400}
                            placeholder="离线超时时间"
                            className="w-full"
                        />
                    </div>
                )}

                {value.type === 'rate_of_change' && (
                    <>
                        {renderElementSelect()}
                        {isBitDict && (
                            <div className="w-[120px] shrink-0">
                                <Select
                                    value={
                                        value.bitIndex != null ? String(value.bitIndex) : undefined
                                    }
                                    onChange={(v: string) => update({ bitIndex: Number(v) })}
                                    options={selectedElement?.dictItems?.map((d) => ({
                                        value: d.key,
                                        label: `${d.label}(${d.key})`,
                                    }))}
                                    placeholder="选择位"
                                    className="w-full"
                                />
                            </div>
                        )}
                        <div className="w-[100px] shrink-0">
                            <Input
                                value={value.changeRate}
                                onChange={(e) => update({ changeRate: e.target.value })}
                                placeholder="变化率%"
                            />
                        </div>
                        <div className="w-[100px] shrink-0">
                            <Select
                                value={value.changeDirection}
                                onChange={(changeDirection: Alert.ChangeDirection) =>
                                    update({ changeDirection })
                                }
                                options={DIRECTION_OPTIONS}
                                className="w-full"
                            />
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}

/** 告警按遥测要素求值，与协议无关，所有协议类型均可配置。 */
const PROTOCOL_TYPE_OPTIONS = PROTOCOL_TYPES.map((value) => ({ label: value, value }));
const EMPTY_ELEMENT_OPTIONS: ElementOption[] = [];

/** 从协议配置中提取可用于告警条件的要素。 */
function protocolElementOptions(item: Protocol.Item, type: string): ElementOption[] {
    if (type === 'SL651') {
        const config = item.config as SL651.Config;
        return (config.funcs ?? []).flatMap((f) =>
            (f.elements ?? []).map((e) => {
                const option: ElementOption = { value: e.id, label: e.name };
                if (e.encode === 'DICT' && e.dictConfig) {
                    option.dictMapType = e.dictConfig.mapType;
                    option.dictItems = e.dictConfig.items.map((d) => ({
                        key: d.key,
                        label: d.label,
                        value: d.value,
                    }));
                }
                return option;
            })
        );
    }
    if (type === 'Modbus') {
        const config = item.config as Modbus.Config;
        return (config.registers ?? []).map((r) => {
            const option: ElementOption = { value: r.id, label: r.name };
            if (r.dictConfig?.items?.length) {
                option.dictMapType = 'VALUE';
                option.dictItems = r.dictConfig.items.map((d) => ({ key: d.key, label: d.label }));
            }
            return option;
        });
    }
    if (type === 'S7') {
        const config = item.config as S7.Config;
        return (config.areas ?? []).map((area) => ({ value: area.id, label: area.name }));
    }
    return protocolPointOptions(item);
}

interface AlertRuleFormValues {
    id?: string;
    name: string;
    device_id: string;
    severity: Alert.Severity;
    conditions: Alert.Condition[];
    logic: 'and' | 'or';
    silence_duration: number;
    recovery_condition: string;
    recovery_wait_seconds: number;
    status: 'enabled' | 'disabled';
    remark?: string;
}
type EditableCondition = Alert.Condition & {
    _key: string;
};
const editableCondition = (condition: Alert.Condition): EditableCondition => ({
    ...condition,
    _key: createUuid(),
});
interface AlertRuleFormModalProps {
    open: boolean;
    editing: Alert.RuleItem | null;
    saveMutation: UseMutationResult<
        void,
        Error,
        Alert.RuleDto & {
            id?: string;
        },
        unknown
    >;
    onClose: () => void;
}
export function AlertRuleFormModal({
    open,
    editing,
    saveMutation,
    onClose,
}: AlertRuleFormModalProps) {
    const [form] = Form.useForm<AlertRuleFormValues>();
    const [conditions, setConditions] = useState<EditableCondition[]>([]);
    const [protocolType, setProtocolType] = useState<Protocol.Type | ''>('');
    // 获取带协议类型的设备列表（useMemo 稳定引用，避免 ?? [] 每次创建新数组）
    const { data: deviceStaticData } = useDeviceConfigurationList({ enabled: open });
    const allDevices = useMemo(() => deviceStaticData?.list ?? [], [deviceStaticData?.list]);
    // 按协议类型过滤设备选项
    const filteredDevices = useMemo(
        () =>
            protocolType ? allDevices.filter((d) => d.protocol_type === protocolType) : allDevices,
        [allDevices, protocolType]
    );
    // 监听选中的设备 ID
    const selectedDeviceId = Form.useWatch('device_id', form);
    // 从设备静态数据中获取协议配置 ID
    const selectedDevice = allDevices.find((d) => d.id === selectedDeviceId);
    const protocolConfigId = selectedDevice?.protocol_config_id;
    const { data: protocolConfig, isLoading: isLoadingConfig } = useProtocolConfigDetail(
        protocolConfigId,
        { enabled: open && Boolean(protocolConfigId) }
    );
    // 从协议配置中提取要素选项
    const elementOptions = useMemo(
        () =>
            protocolConfig?.config
                ? protocolElementOptions(
                      protocolConfig,
                      selectedDevice?.protocol_type || protocolConfig.protocol
                  )
                : EMPTY_ELEMENT_OPTIONS,
        [protocolConfig, selectedDevice?.protocol_type]
    );
    useEffect(() => {
        if (open) {
            if (editing) {
                // 编辑模式：从已有设备推导协议类型
                const editDevice = allDevices.find((d) => d.id === editing.device_id);
                setProtocolType((editDevice?.protocol_type as Protocol.Type) || '');
                form.setFieldsValue({
                    id: editing.id,
                    name: editing.name,
                    device_id: editing.device_id,
                    severity: editing.severity,
                    logic: editing.logic,
                    silence_duration: editing.silence_duration,
                    recovery_condition: editing.recovery_condition,
                    recovery_wait_seconds: editing.recovery_wait_seconds,
                    status: editing.status,
                    remark: editing.remark ?? '',
                });
                setConditions((editing.conditions || []).map(editableCondition));
            } else {
                form.resetFields();
                form.setFieldsValue({
                    severity: 'warning',
                    logic: 'and',
                    silence_duration: 300,
                    recovery_condition: 'reverse',
                    recovery_wait_seconds: 60,
                    status: 'enabled',
                });
                setConditions([]);
                setProtocolType('');
            }
        }
    }, [open, editing, form, allDevices]);
    const addCondition = () => {
        setConditions((prev) => [
            ...prev,
            editableCondition({
                type: 'threshold',
                elementKey: '',
                operator: '>',
                value: '',
            }),
        ]);
    };
    const updateCondition = (index: number, value: Alert.Condition) => {
        setConditions((prev) =>
            prev.map((condition, current) =>
                current === index ? { ...value, _key: condition._key } : condition
            )
        );
    };
    const removeCondition = (index: number) => {
        setConditions((prev) => prev.filter((_, i) => i !== index));
    };
    const onFinish = (values: AlertRuleFormValues) => {
        saveMutation.mutate(
            {
                ...values,
                conditions: conditions.map(({ _key: _unused, ...condition }) => condition),
            },
            {
                onSuccess: () => {
                    onClose();
                },
            }
        );
    };
    return (
        <FormModal
            open={open}
            title={editing ? '编辑规则' : '新建规则'}
            onCancel={onClose}
            onOk={() => form.submit()}
            confirmLoading={saveMutation.isPending}
            destroyOnHidden
        >
            <Form<AlertRuleFormValues> form={form} layout="vertical" onFinish={onFinish}>
                <Form.Item name="id" hidden>
                    <Input />
                </Form.Item>

                <Form.Item
                    label="规则名称"
                    name="name"
                    rules={[{ required: true, message: '请输入规则名称' }]}
                >
                    <Input placeholder="如：水位超高告警" />
                </Form.Item>

                <div className="flex gap-4">
                    <Form.Item label="协议类型" className="w-[140px]">
                        <Select
                            value={protocolType || undefined}
                            onChange={(v) => {
                                setProtocolType(v);
                                form.setFieldValue('device_id', undefined);
                            }}
                            allowClear
                            placeholder="全部协议"
                            options={PROTOCOL_TYPE_OPTIONS}
                        />
                    </Form.Item>

                    <Form.Item
                        label="关联设备"
                        name="device_id"
                        rules={[{ required: true, message: '请选择关联设备' }]}
                        className="flex-1"
                    >
                        <Select
                            showSearch
                            optionFilterProp="label"
                            placeholder="搜索并选择设备"
                            options={filteredDevices.map((d) => ({ label: d.name, value: d.id }))}
                        />
                    </Form.Item>
                </div>

                <div className="flex gap-4">
                    <Form.Item label="严重级别" name="severity" className="flex-1">
                        <Select>
                            <Select.Option value="critical">严重</Select.Option>
                            <Select.Option value="warning">警告</Select.Option>
                            <Select.Option value="info">信息</Select.Option>
                        </Select>
                    </Form.Item>

                    <Form.Item label="条件逻辑" name="logic" className="flex-1">
                        <Select>
                            <Select.Option value="and">全部满足 (AND)</Select.Option>
                            <Select.Option value="or">任一满足 (OR)</Select.Option>
                        </Select>
                    </Form.Item>
                </div>

                <div className="mb-4">
                    <div className="flex items-center justify-between mb-2">
                        <span className="font-medium">
                            告警条件
                            {isLoadingConfig && selectedDeviceId && (
                                <Spin size="small" className="ml-2" />
                            )}
                        </span>
                        <Button
                            type="dashed"
                            size="small"
                            icon={<PlusOutlined />}
                            onClick={addCondition}
                        >
                            添加条件
                        </Button>
                    </div>
                    {conditions.length === 0 && (
                        <div className="text-gray-400 text-center py-4 border border-dashed rounded">
                            暂无条件，请点击"添加条件"
                        </div>
                    )}
                    {conditions.map((cond, index) => (
                        <ConditionEditor
                            key={cond._key}
                            value={cond}
                            elementOptions={elementOptions}
                            onChange={(v) => updateCondition(index, v)}
                            onRemove={() => removeCondition(index)}
                        />
                    ))}
                </div>

                <div className="flex gap-4">
                    <Form.Item label="冷却时间(秒)" name="silence_duration" className="flex-1">
                        <InputNumber min={0} max={86400} className="w-full" />
                    </Form.Item>

                    <Form.Item label="状态" name="status" className="flex-1">
                        <Select>
                            <Select.Option value="enabled">启用</Select.Option>
                            <Select.Option value="disabled">禁用</Select.Option>
                        </Select>
                    </Form.Item>
                </div>

                <div className="flex gap-4">
                    <Form.Item label="恢复策略" name="recovery_condition" className="flex-1">
                        <Select>
                            <Select.Option value="reverse">条件反向恢复</Select.Option>
                            <Select.Option value="auto_60">自动恢复 (60秒)</Select.Option>
                            <Select.Option value="auto_300">自动恢复 (5分钟)</Select.Option>
                            <Select.Option value="auto_900">自动恢复 (15分钟)</Select.Option>
                            <Select.Option value="auto_3600">自动恢复 (1小时)</Select.Option>
                        </Select>
                    </Form.Item>

                    <Form.Item label="恢复等待(秒)" name="recovery_wait_seconds" className="flex-1">
                        <InputNumber min={0} max={86400} className="w-full" />
                    </Form.Item>
                </div>

                <Form.Item label="备注" name="remark">
                    <Input.TextArea rows={2} placeholder="可选备注" />
                </Form.Item>
            </Form>
        </FormModal>
    );
}

interface TemplateFormValues {
    id?: string;
    name: string;
    category?: string;
    description?: string;
    severity: Alert.Severity;
    logic: 'and' | 'or';
    silence_duration: number;
    recovery_condition: string;
    recovery_wait_seconds: number;
    protocol_config_id?: string;
}
interface AlertTemplateFormModalProps {
    open: boolean;
    editing: Alert.TemplateItem | null;
    editingDetail?: Alert.TemplateDetail | null;
    saveMutation: UseMutationResult<
        void,
        Error,
        Alert.TemplateDto & {
            id?: string;
        },
        unknown
    >;
    onClose: () => void;
}
export function AlertTemplateFormModal({
    open,
    editing,
    editingDetail,
    saveMutation,
    onClose,
}: AlertTemplateFormModalProps) {
    const [form] = Form.useForm<TemplateFormValues>();
    const [conditions, setConditions] = useState<EditableCondition[]>([]);
    const [protocolType, setProtocolType] = useState<Protocol.Type | ''>('');
    // 根据协议类型获取配置选项列表
    const { data: configOptionsData } = useProtocolConfigOptions(protocolType as Protocol.Type, {
        enabled: open && !!protocolType,
    });
    const configOptions = configOptionsData?.list ?? [];
    // 监听表单中选中的协议配置 ID
    const selectedConfigId = Form.useWatch('protocol_config_id', form);
    // 获取协议配置详情以提取要素
    const { data: protocolConfig, isLoading: isLoadingConfig } = useProtocolConfigDetail(
        selectedConfigId,
        { enabled: open && !!selectedConfigId }
    );
    // 从协议配置中提取要素选项
    const elementOptions = useMemo(
        () =>
            protocolConfig?.config
                ? protocolElementOptions(protocolConfig, protocolType || protocolConfig.protocol)
                : EMPTY_ELEMENT_OPTIONS,
        [protocolConfig, protocolType]
    );
    useEffect(() => {
        if (open) {
            if (editing && editingDetail) {
                // 编辑模式：从详情中恢复协议类型
                if (editingDetail.protocol_config_id) {
                    // 协议类型将在 protocolConfig 加载后自动确定
                    // 先尝试从 TemplateItem 的 protocol_type 获取
                    if (editing.protocol_type) {
                        setProtocolType(editing.protocol_type as Protocol.Type);
                    }
                } else {
                    setProtocolType('');
                }
                form.setFieldsValue({
                    id: editingDetail.id,
                    name: editingDetail.name,
                    category: editingDetail.category ?? '',
                    description: editingDetail.description ?? '',
                    severity: editingDetail.severity,
                    logic: editingDetail.logic,
                    silence_duration: editingDetail.silence_duration,
                    recovery_condition: editingDetail.recovery_condition,
                    recovery_wait_seconds: editingDetail.recovery_wait_seconds,
                    protocol_config_id: editingDetail.protocol_config_id || undefined,
                });
                setConditions((editingDetail.conditions || []).map(editableCondition));
            } else {
                form.resetFields();
                form.setFieldsValue({
                    severity: 'warning',
                    logic: 'and',
                    silence_duration: 300,
                    recovery_condition: 'reverse',
                    recovery_wait_seconds: 60,
                });
                setConditions([]);
                setProtocolType('');
            }
        }
    }, [open, editing, editingDetail, form]);
    const addCondition = () => {
        setConditions((prev) => [
            ...prev,
            editableCondition({
                type: 'threshold',
                elementKey: '',
                operator: '>',
                value: '',
            }),
        ]);
    };
    const updateCondition = (index: number, value: Alert.Condition) => {
        setConditions((prev) =>
            prev.map((condition, current) =>
                current === index ? { ...value, _key: condition._key } : condition
            )
        );
    };
    const removeCondition = (index: number) => {
        setConditions((prev) => prev.filter((_, i) => i !== index));
    };
    const onFinish = (values: TemplateFormValues) => {
        // 从选中的协议配置推导 applicable_protocols
        const applicableProtocols = protocolType ? [protocolType] : [];
        saveMutation.mutate(
            {
                ...values,
                conditions: conditions.map(({ _key: _unused, ...condition }) => condition),
                applicable_protocols: applicableProtocols,
            },
            {
                onSuccess: () => {
                    onClose();
                },
            }
        );
    };
    return (
        <FormModal
            open={open}
            title={editing ? '编辑模板' : '新建模板'}
            onCancel={onClose}
            onOk={() => form.submit()}
            confirmLoading={saveMutation.isPending}
            destroyOnHidden
        >
            <Form<TemplateFormValues> form={form} layout="vertical" onFinish={onFinish}>
                <Form.Item name="id" hidden>
                    <Input />
                </Form.Item>

                <Form.Item
                    label="模板名称"
                    name="name"
                    rules={[{ required: true, message: '请输入模板名称' }]}
                >
                    <Input placeholder="如：高温告警模板" />
                </Form.Item>

                <div className="flex gap-4">
                    <Form.Item label="分类" name="category" className="flex-1">
                        <AutoComplete
                            options={['温度', '水位', '流量', '压力', '湿度', '通用'].map((v) => ({
                                value: v,
                            }))}
                            placeholder="选择或输入分类"
                            filterOption
                        />
                    </Form.Item>

                    <Form.Item label="协议类型" className="flex-1">
                        <Select
                            value={protocolType || undefined}
                            onChange={(v) => {
                                setProtocolType(v);
                                form.setFieldValue('protocol_config_id', undefined);
                            }}
                            allowClear
                            placeholder="选择协议"
                            options={PROTOCOL_TYPE_OPTIONS}
                        />
                    </Form.Item>
                </div>

                <Form.Item
                    label="设备类型（协议配置）"
                    name="protocol_config_id"
                    rules={[{ required: true, message: '请选择设备类型' }]}
                >
                    <Select
                        showSearch
                        optionFilterProp="label"
                        placeholder={protocolType ? '选择设备类型' : '请先选择协议类型'}
                        disabled={!protocolType}
                        options={configOptions.map((c) => ({ label: c.name, value: c.id }))}
                    />
                </Form.Item>

                <Form.Item label="描述" name="description">
                    <Input.TextArea rows={2} placeholder="模板描述（可选）" />
                </Form.Item>

                <div className="flex gap-4">
                    <Form.Item label="严重级别" name="severity" className="flex-1">
                        <Select>
                            <Select.Option value="critical">严重</Select.Option>
                            <Select.Option value="warning">警告</Select.Option>
                            <Select.Option value="info">信息</Select.Option>
                        </Select>
                    </Form.Item>

                    <Form.Item label="条件逻辑" name="logic" className="flex-1">
                        <Select>
                            <Select.Option value="and">全部满足 (AND)</Select.Option>
                            <Select.Option value="or">任一满足 (OR)</Select.Option>
                        </Select>
                    </Form.Item>
                </div>

                <div className="mb-4">
                    <div className="flex items-center justify-between mb-2">
                        <span className="font-medium">
                            告警条件
                            {isLoadingConfig && selectedConfigId && (
                                <Spin size="small" className="ml-2" />
                            )}
                        </span>
                        <Button
                            type="dashed"
                            size="small"
                            icon={<PlusOutlined />}
                            onClick={addCondition}
                        >
                            添加条件
                        </Button>
                    </div>
                    {conditions.length === 0 && (
                        <div className="text-gray-400 text-center py-4 border border-dashed rounded">
                            {selectedConfigId
                                ? '暂无条件，请点击"添加条件"'
                                : '请先选择设备类型，再添加条件'}
                        </div>
                    )}
                    {conditions.map((cond, index) => (
                        <ConditionEditor
                            key={cond._key}
                            value={cond}
                            elementOptions={elementOptions}
                            onChange={(v) => updateCondition(index, v)}
                            onRemove={() => removeCondition(index)}
                        />
                    ))}
                </div>

                <div className="flex gap-4">
                    <Form.Item label="冷却时间(秒)" name="silence_duration" className="flex-1">
                        <InputNumber min={0} max={86400} className="w-full" />
                    </Form.Item>

                    <Form.Item label="恢复策略" name="recovery_condition" className="flex-1">
                        <Select>
                            <Select.Option value="reverse">条件反向恢复</Select.Option>
                            <Select.Option value="auto_60">自动恢复 (60秒)</Select.Option>
                            <Select.Option value="auto_300">自动恢复 (5分钟)</Select.Option>
                            <Select.Option value="auto_900">自动恢复 (15分钟)</Select.Option>
                            <Select.Option value="auto_3600">自动恢复 (1小时)</Select.Option>
                        </Select>
                    </Form.Item>
                </div>

                <Form.Item label="恢复等待(秒)" name="recovery_wait_seconds">
                    <InputNumber min={0} max={86400} className="w-full" />
                </Form.Item>
            </Form>
        </FormModal>
    );
}

const { Search } = Input;
const DATE_TIME_COLUMN_WIDTH = 180;
const SEVERITY_OPTIONS = [
    { label: '全部级别', value: '' },
    { label: '严重', value: 'critical' },
    { label: '警告', value: 'warning' },
    { label: '信息', value: 'info' },
];
const STATUS_OPTIONS = [
    { label: '全部状态', value: '' },
    { label: '活跃', value: 'active' },
    { label: '已确认', value: 'acknowledged' },
    { label: '已恢复', value: 'resolved' },
];
const SEVERITY_COLORS: Record<string, string> = {
    critical: 'red',
    warning: 'orange',
    info: 'blue',
};
const STATUS_COLORS: Record<string, string> = {
    active: 'error',
    acknowledged: 'warning',
    resolved: 'success',
};
const STATUS_LABELS: Record<string, string> = {
    active: '活跃',
    acknowledged: '已确认',
    resolved: '已恢复',
};
const SEVERITY_LABELS: Record<string, string> = {
    critical: '严重',
    warning: '警告',
    info: '信息',
};
// ==================== 规则配置弹窗（规则 + 模板上下布局）====================
function RuleConfigModal({ open, onClose }: { open: boolean; onClose: () => void }) {
    // ---- 规则状态 ----
    const [ruleKeyword, setRuleKeyword] = useState('');
    const [ruleSeverity, setRuleSeverity] = useState('');
    const [rulePagination, setRulePagination] = useState({ page: 1, pageSize: 5 });
    const [ruleFormVisible, setRuleFormVisible] = useState(false);
    const [ruleEditing, setRuleEditing] = useState<Alert.RuleItem | null>(null);
    const [ruleSelectedKeys, setRuleSelectedKeys] = useState<string[]>([]);
    // ---- 模板状态 ----
    const [tplPagination, setTplPagination] = useState({ page: 1, pageSize: 5 });
    const [tplFormVisible, setTplFormVisible] = useState(false);
    const [tplEditing, setTplEditing] = useState<Alert.TemplateItem | null>(null);
    const [tplEditingDetail, setTplEditingDetail] = useState<Alert.TemplateDetail | null>(null);
    const [applyingTemplate, setApplyingTemplate] = useState<Alert.TemplateItem | null>(null);
    const [selectedDeviceIds, setSelectedDeviceIds] = useState<string[]>([]);
    const { modal } = App.useApp();
    const canAdd = usePermission('iot:alert:add');
    const canEdit = usePermission('iot:alert:edit');
    const canDelete = usePermission('iot:alert:delete');
    // ---- 规则数据 ----
    const doRuleSearch = (value: string) => {
        setRuleKeyword(value);
        setRulePagination((prev) => ({ ...prev, page: 1 }));
    };
    const { run: debouncedRuleSearch } = useDebounceFn(doRuleSearch, 300);
    const { data: rulePage, isLoading: ruleLoading } = useAlertRuleList({
        page: rulePagination.page,
        pageSize: rulePagination.pageSize,
        keyword: ruleKeyword || undefined,
        severity: ruleSeverity || undefined,
    });
    const ruleSaveMutation = useAlertRuleSave();
    const ruleDeleteMutation = useAlertRuleDelete();
    const ruleBatchDeleteMutation = useAlertRuleBatchDelete();
    // ---- 模板数据 ----
    const { data: templatePage, isLoading: tplLoading } = useAlertTemplateList({
        page: tplPagination.page,
        pageSize: tplPagination.pageSize,
    });
    const tplSaveMutation = useAlertTemplateSave();
    const loadTemplate = useAlertTemplateLoader();
    const tplDeleteMutation = useAlertTemplateDelete();
    const applyMutation = useAlertApplyTemplate();
    // ---- 规则操作 ----
    const openRuleCreate = () => {
        setRuleEditing(null);
        setRuleFormVisible(true);
    };
    const openRuleEdit = (record: Alert.RuleItem) => {
        setRuleEditing(record);
        setRuleFormVisible(true);
    };
    const onRuleDelete = (record: Alert.RuleItem) => {
        modal.confirm({
            title: `确认删除规则「${record.name}」吗？`,
            content: '删除后该规则将停止告警检测。此操作不可撤销。',
            okText: '确定删除',
            okButtonProps: { danger: true },
            onOk: () => ruleDeleteMutation.mutate(record.id),
        });
    };
    const onRuleBatchDelete = () => {
        if (ruleSelectedKeys.length === 0) return;
        modal.confirm({
            title: '批量删除',
            content: `确认删除选中的 ${ruleSelectedKeys.length} 条规则？此操作不可撤销。`,
            okText: '确定删除',
            okButtonProps: { danger: true },
            onOk: () =>
                ruleBatchDeleteMutation.mutate(ruleSelectedKeys, {
                    onSuccess: () => setRuleSelectedKeys([]),
                }),
        });
    };
    // ---- 模板操作 ----
    const openTplCreate = () => {
        setTplEditing(null);
        setTplEditingDetail(null);
        setTplFormVisible(true);
    };
    const openTplEdit = async (record: Alert.TemplateItem) => {
        try {
            const detail = await loadTemplate(record.id);
            setTplEditing(record);
            setTplEditingDetail(detail);
            setTplFormVisible(true);
        } catch {
            // 请求层统一显示错误。
        }
    };
    const onTplDelete = (record: Alert.TemplateItem) => {
        modal.confirm({
            title: `确认删除模板「${record.name}」吗？`,
            content: '此操作不可撤销。',
            okText: '确定删除',
            okButtonProps: { danger: true },
            onOk: () => tplDeleteMutation.mutate(record.id),
        });
    };
    const openApplyModal = (record: Alert.TemplateItem) => {
        setApplyingTemplate(record);
        setSelectedDeviceIds([]);
    };
    const onApply = () => {
        if (!applyingTemplate || selectedDeviceIds.length === 0) return;
        applyMutation.mutate(
            { template_id: applyingTemplate.id, device_ids: selectedDeviceIds },
            {
                onSuccess: () => {
                    setApplyingTemplate(null);
                },
            }
        );
    };
    // ---- 表格列定义 ----
    const ruleColumns: ColumnsType<Alert.RuleItem> = [
        { title: '规则名称', dataIndex: 'name', ellipsis: true },
        { title: '关联设备', dataIndex: 'device_name', ellipsis: true },
        {
            title: '严重级别',
            dataIndex: 'severity',
            width: 100,
            render: (v: Alert.Severity) => (
                <Tag color={SEVERITY_COLORS[v]}>{SEVERITY_LABELS[v] || v}</Tag>
            ),
        },
        {
            title: '条件数',
            dataIndex: 'conditions',
            width: 80,
            render: (conditions: Alert.Condition[]) => conditions?.length ?? 0,
        },
        {
            title: '逻辑',
            dataIndex: 'logic',
            width: 80,
            render: (v: string) => <Tag>{v === 'and' ? '全部满足' : '任一满足'}</Tag>,
        },
        {
            title: '状态',
            dataIndex: 'status',
            width: 80,
            render: (v: 'enabled' | 'disabled') => <StatusTag status={v} />,
        },
        {
            title: '操作',
            key: 'actions',
            width: 150,
            fixed: 'right' as const,
            render: (_, record) => (
                <Space>
                    {canEdit && (
                        <Button type="link" onClick={() => openRuleEdit(record)}>
                            编辑
                        </Button>
                    )}
                    {canDelete && (
                        <Button type="link" danger onClick={() => onRuleDelete(record)}>
                            删除
                        </Button>
                    )}
                </Space>
            ),
        },
    ];
    const tplColumns: ColumnsType<Alert.TemplateItem> = [
        { title: '模板名称', dataIndex: 'name', ellipsis: true },
        {
            title: '设备类型',
            dataIndex: 'config_name',
            width: 140,
            render: (v: string, record) =>
                v ? (
                    <span>
                        {v}
                        <Tag className="ml-1" bordered={false}>
                            {record.protocol_type}
                        </Tag>
                    </span>
                ) : (
                    '-'
                ),
        },
        { title: '分类', dataIndex: 'category', width: 100 },
        {
            title: '严重级别',
            dataIndex: 'severity',
            width: 100,
            render: (v: Alert.Severity) => (
                <Tag color={SEVERITY_COLORS[v]}>{SEVERITY_LABELS[v] || v}</Tag>
            ),
        },
        {
            title: '操作',
            key: 'actions',
            width: 200,
            fixed: 'right' as const,
            render: (_, record) => (
                <Space>
                    {canAdd && (
                        <Button type="link" onClick={() => openApplyModal(record)}>
                            应用
                        </Button>
                    )}
                    {canEdit && (
                        <Button type="link" onClick={() => openTplEdit(record)}>
                            编辑
                        </Button>
                    )}
                    {canDelete && (
                        <Button type="link" danger onClick={() => onTplDelete(record)}>
                            删除
                        </Button>
                    )}
                </Space>
            ),
        },
    ];
    return (
        <Modal
            open={open}
            title="规则配置"
            onCancel={onClose}
            footer={null}
            width={1000}
            destroyOnClose
        >
            {/* ---- 规则模板 ---- */}
            <div className="flex items-center justify-between flex-wrap gap-2 mb-2">
                <span className="font-medium text-base">规则模板</span>
                {canAdd && (
                    <Button type="primary" onClick={openTplCreate}>
                        新建模板
                    </Button>
                )}
            </div>
            <Table<Alert.TemplateItem>
                rowKey="id"
                columns={tplColumns}
                dataSource={templatePage?.list || []}
                loading={tplLoading}
                pagination={{
                    current: tplPagination.page,
                    pageSize: tplPagination.pageSize,
                    total: templatePage?.total || 0,
                    showSizeChanger: true,
                    showTotal: (total, range) => `${range[0]}-${range[1]} / 共 ${total} 条`,
                }}
                onChange={(p) =>
                    setTplPagination({ page: p.current || 1, pageSize: p.pageSize || 5 })
                }
                size="small"
                scroll={{ x: 'max-content' }}
            />

            <Divider className="my-3" />

            {/* ---- 告警规则 ---- */}
            <div className="flex items-center justify-between flex-wrap gap-2 mb-2">
                <Space wrap>
                    <span className="font-medium text-base">告警规则</span>
                    <Search
                        allowClear
                        placeholder="规则名称 / 备注"
                        onChange={(e) => debouncedRuleSearch(e.target.value)}
                        onSearch={doRuleSearch}
                        className="w-[200px]"
                    />
                    <Select
                        value={ruleSeverity}
                        onChange={(v) => {
                            setRuleSeverity(v);
                            setRulePagination((prev) => ({ ...prev, page: 1 }));
                        }}
                        options={SEVERITY_OPTIONS}
                        className="w-[120px]"
                    />
                </Space>
                <Space>
                    {canDelete && ruleSelectedKeys.length > 0 && (
                        <Button
                            danger
                            onClick={onRuleBatchDelete}
                            loading={ruleBatchDeleteMutation.isPending}
                        >
                            批量删除 ({ruleSelectedKeys.length})
                        </Button>
                    )}
                    {canAdd && (
                        <Button type="primary" onClick={openRuleCreate}>
                            新建规则
                        </Button>
                    )}
                </Space>
            </div>
            <Table<Alert.RuleItem>
                rowKey="id"
                columns={ruleColumns}
                dataSource={rulePage?.list || []}
                loading={ruleLoading}
                rowSelection={
                    canDelete
                        ? {
                              selectedRowKeys: ruleSelectedKeys,
                              onChange: (keys) => setRuleSelectedKeys(keys as string[]),
                          }
                        : undefined
                }
                pagination={{
                    current: rulePagination.page,
                    pageSize: rulePagination.pageSize,
                    total: rulePage?.total || 0,
                    showSizeChanger: true,
                    showTotal: (total, range) => `${range[0]}-${range[1]} / 共 ${total} 条`,
                }}
                onChange={(p) =>
                    setRulePagination({ page: p.current || 1, pageSize: p.pageSize || 5 })
                }
                size="small"
                scroll={{ x: 'max-content' }}
            />

            {/* 子弹窗 */}
            <AlertRuleFormModal
                open={ruleFormVisible}
                editing={ruleEditing}
                saveMutation={ruleSaveMutation}
                onClose={() => {
                    setRuleFormVisible(false);
                    setRuleEditing(null);
                }}
            />
            <AlertTemplateFormModal
                open={tplFormVisible}
                editing={tplEditing}
                editingDetail={tplEditingDetail}
                saveMutation={tplSaveMutation}
                onClose={() => {
                    setTplFormVisible(false);
                    setTplEditing(null);
                    setTplEditingDetail(null);
                }}
            />
            <ApplyTemplateModal
                open={!!applyingTemplate}
                templateName={applyingTemplate?.name ?? ''}
                selectedDeviceIds={selectedDeviceIds}
                onDeviceIdsChange={setSelectedDeviceIds}
                onOk={onApply}
                onCancel={() => setApplyingTemplate(null)}
                loading={applyMutation.isPending}
            />
        </Modal>
    );
}
// ==================== 应用模板弹窗 ====================
function ApplyTemplateModal({
    open,
    templateName,
    selectedDeviceIds,
    onDeviceIdsChange,
    onOk,
    onCancel,
    loading,
}: {
    open: boolean;
    templateName: string;
    selectedDeviceIds: string[];
    onDeviceIdsChange: (ids: string[]) => void;
    onOk: () => void;
    onCancel: () => void;
    loading: boolean;
}) {
    const { data: deviceOptionsData } = useDeviceOptions({ enabled: open });
    const deviceOptions = useMemo(
        () => (deviceOptionsData ?? []).map((d) => ({ label: d.name, value: d.id })),
        [deviceOptionsData]
    );
    return (
        <FormModal
            open={open}
            title={`应用模板「${templateName}」`}
            onCancel={onCancel}
            onOk={onOk}
            confirmLoading={loading}
            okButtonProps={{ disabled: selectedDeviceIds.length === 0 }}
            destroyOnHidden
        >
            <Form layout="vertical">
                <Form.Item
                    label="目标设备"
                    required
                    extra={
                        selectedDeviceIds.length > 0
                            ? `已选择 ${selectedDeviceIds.length} 个设备`
                            : undefined
                    }
                >
                    <Select
                        mode="multiple"
                        value={selectedDeviceIds}
                        onChange={onDeviceIdsChange}
                        options={deviceOptions}
                        showSearch
                        optionFilterProp="label"
                        placeholder="搜索并选择设备"
                        className="w-full"
                        maxTagCount="responsive"
                    />
                </Form.Item>
            </Form>
        </FormModal>
    );
}
// ==================== 主页面 ====================
const AlertPage = () => {
    const canQuery = usePermission('iot:alert:query');
    const canAck = usePermission('iot:alert:ack');
    const [severity, setSeverity] = useState('');
    const [status, setStatus] = useState('');
    const [pagination, setPagination] = useState({ page: 1, pageSize: 20 });
    const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
    const [configModalOpen, setConfigModalOpen] = useState(false);
    const { modal } = App.useApp();
    const recordParams = {
        page: pagination.page,
        pageSize: pagination.pageSize,
        severity: severity || undefined,
        status: status || undefined,
    };
    const {
        data: stats,
        isLoading: statsLoading,
        error: statsError,
        connectionError: statsConnectionError,
        refetch: retryStats,
    } = useAlertStats(recordParams, {
        enabled: canQuery,
    });
    const {
        data: recordPage,
        isLoading,
        error: recordsError,
        connectionError: recordsConnectionError,
        refetch: retryRecords,
    } = useAlertRecordList(recordParams, { enabled: canQuery });
    const ackMutation = useAlertAcknowledge();
    const batchAckMutation = useAlertBatchAcknowledge();
    if (!canQuery) {
        return (
            <PageContainer>
                <Result status="403" title="无权限" subTitle="您没有查询告警的权限，请联系管理员" />
            </PageContainer>
        );
    }
    const onAcknowledge = (record: Alert.RecordItem) => {
        modal.confirm({
            title: '确认告警',
            content: `确认告警「${record.message}」？`,
            okText: '确认',
            onOk: () => ackMutation.mutate(record.id),
        });
    };
    const onBatchAcknowledge = () => {
        if (selectedRowKeys.length === 0) return;
        modal.confirm({
            title: '批量确认',
            content: `确认选中的 ${selectedRowKeys.length} 条告警？`,
            okText: '确认',
            onOk: () =>
                batchAckMutation.mutate(selectedRowKeys, {
                    onSuccess: () => setSelectedRowKeys([]),
                }),
        });
    };
    const handleTableChange = (paginationConfig: TablePaginationConfig) => {
        setPagination({
            page: paginationConfig.current || 1,
            pageSize: paginationConfig.pageSize || 20,
        });
    };
    const recordColumns: ColumnsType<Alert.RecordItem> = [
        { title: '设备', dataIndex: 'device_name', ellipsis: true, width: 120 },
        {
            title: '级别',
            dataIndex: 'severity',
            width: 80,
            render: (v: Alert.Severity) => (
                <Tag color={SEVERITY_COLORS[v]}>{SEVERITY_LABELS[v] || v}</Tag>
            ),
        },
        { title: '告警消息', dataIndex: 'message', ellipsis: true },
        {
            title: '触发时间',
            dataIndex: 'triggered_at',
            width: DATE_TIME_COLUMN_WIDTH,
            render: (value: string) => formatDateTime(value),
        },
        {
            title: '状态',
            dataIndex: 'status',
            width: 90,
            render: (v: Alert.RecordStatus) => (
                <Tag color={STATUS_COLORS[v]}>{STATUS_LABELS[v] || v}</Tag>
            ),
        },
        {
            title: '确认时间',
            dataIndex: 'acknowledged_at',
            width: DATE_TIME_COLUMN_WIDTH,
            render: (value?: string) => formatDateTime(value),
        },
        {
            title: '操作',
            key: 'actions',
            width: 80,
            fixed: 'right' as const,
            render: (_, record) =>
                canAck && record.status === 'active' ? (
                    <Button type="link" onClick={() => onAcknowledge(record)}>
                        确认
                    </Button>
                ) : null,
        },
    ];
    return (
        <PageContainer>
            <LiveQueryError
                error={recordsError ?? statsError ?? recordsConnectionError ?? statsConnectionError}
                retry={
                    recordsError || statsError
                        ? () => Promise.all([retryRecords(), retryStats()])
                        : recordsConnectionError
                          ? retryRecords
                          : retryStats
                }
            />
            {/* 概要统计 + 管理入口 */}
            <div className="flex items-center justify-between flex-wrap gap-2 mb-4">
                {statsLoading ? (
                    <Skeleton.Input active size="small" style={{ width: 500 }} />
                ) : (
                    <Space size="middle" wrap>
                        <span className="font-medium">
                            活跃告警
                            <span className="text-lg font-semibold ml-1">{stats?.total ?? 0}</span>
                        </span>
                        <Tag color="red">严重 {stats?.critical ?? 0}</Tag>
                        <Tag color="orange">警告 {stats?.warning ?? 0}</Tag>
                        <Tag color="blue">信息 {stats?.info ?? 0}</Tag>
                        <Tag color="cyan">已确认 {stats?.acknowledged ?? 0}</Tag>
                        <Tag color="green">今日恢复 {stats?.today_resolved ?? 0}</Tag>
                        <Tag>今日新增 {stats?.today_new ?? 0}</Tag>
                        <Tag>涉及设备 {stats?.affected_devices ?? 0}</Tag>
                    </Space>
                )}
                <Button onClick={() => setConfigModalOpen(true)}>规则配置</Button>
            </div>

            {/* 记录筛选 + 操作 */}
            <div className="flex items-center justify-between flex-wrap gap-2 mb-4">
                <Space wrap>
                    <Select
                        value={severity}
                        onChange={(v) => {
                            setSeverity(v);
                            setPagination((prev) => ({ ...prev, page: 1 }));
                        }}
                        options={SEVERITY_OPTIONS}
                        className="w-[120px]"
                    />
                    <Select
                        value={status}
                        onChange={(v) => {
                            setStatus(v);
                            setPagination((prev) => ({ ...prev, page: 1 }));
                        }}
                        options={STATUS_OPTIONS}
                        className="w-[120px]"
                    />
                </Space>
                <Space>
                    {canAck && selectedRowKeys.length > 0 && (
                        <Button
                            type="primary"
                            onClick={onBatchAcknowledge}
                            loading={batchAckMutation.isPending}
                        >
                            批量确认 ({selectedRowKeys.length})
                        </Button>
                    )}
                </Space>
            </div>

            {/* 告警记录表 */}
            <Table<Alert.RecordItem>
                rowKey="id"
                columns={recordColumns}
                dataSource={recordPage?.list || []}
                loading={isLoading}
                rowSelection={
                    canAck
                        ? {
                              selectedRowKeys,
                              onChange: (keys) => setSelectedRowKeys(keys as string[]),
                              getCheckboxProps: (record) => ({
                                  disabled: record.status !== 'active',
                              }),
                          }
                        : undefined
                }
                pagination={{
                    current: pagination.page,
                    pageSize: pagination.pageSize,
                    total: recordPage?.total || 0,
                    showSizeChanger: true,
                    showTotal: (total, range) => `${range[0]}-${range[1]} / 共 ${total} 条`,
                }}
                onChange={handleTableChange}
                size="middle"
                scroll={{ x: 'max-content' }}
                sticky
            />

            {/* 规则配置弹窗 */}
            <RuleConfigModal open={configModalOpen} onClose={() => setConfigModalOpen(false)} />
        </PageContainer>
    );
};

export default AlertPage;
