import {
    ApartmentOutlined,
    CopyOutlined,
    DeleteOutlined,
    EditOutlined,
    HistoryOutlined,
    PlusOutlined,
    ReloadOutlined,
    SendOutlined,
    ShareAltOutlined,
} from '@ant-design/icons';
import { replaceEqualDeep } from '@tanstack/react-query';
import {
    Alert,
    App,
    Button,
    Card,
    Checkbox,
    ConfigProvider,
    DatePicker,
    Divider,
    Drawer,
    Empty,
    Flex,
    Form,
    Input,
    InputNumber,
    Modal,
    Pagination,
    Popconfirm,
    Popover,
    Radio,
    Result,
    Select,
    Skeleton,
    Space,
    Switch,
    Table,
    Tag,
    Tooltip,
    Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { Dayjs } from 'dayjs';
import dayjs from 'dayjs';
import type { CSSProperties, Dispatch, ReactNode, SetStateAction } from 'react';
import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import type { DeviceCardItem } from '@/components/device_card';
import DeviceCard from '@/components/device_card';
import { JsonEditor } from '@/components/json_editor';
import { FormModal } from '@/components/form_modal';
import type { GroupFormValues } from '@/components/group_form_modal';
import { type GroupTreeAction, GroupTreePanel } from '@/components/group_tree_panel';
import { PacketDebugPanel } from '@/components/packet_debug_panel';
import { PageContainer } from '@/components/page_container';
import { VirtualGrid } from '@/components/virtual_grid';
import { usePermissions } from '@/hooks/use_permission';
import { useAuthStore } from '@/store/auth_store';
import { formatDateTime } from '@/utils/date_time';
import { useEdgeConfigurationDetail, useEdgeSelectionList } from '../edge_node/edge_node.service';
import type { Edge } from '../edge_node/edge_node.types';
import { useLinkOptions } from '../link/link.service';
import type { Link } from '../link/link.types';
import { useProtocolConfigOptions } from '../protocol/protocol.service';
import {
    deviceParameterInputSchema,
    deviceParameterValues,
    saveDeviceSchema,
} from './device.schema';
import {
    getDeviceCardElements,
    useDeviceDetailLoader,
    isDeviceOnline,
    summarizeDeviceCommandResult,
    useDeviceCommand,
    useDeviceCommandResults,
    useDeviceDebug,
    useDeviceDelete,
    useDeviceGroupDelete,
    useDeviceGroupSave,
    useDeviceGroupShares,
    useDeviceGroupShareTargets,
    useDeviceGroupTreeWithCount,
    useDeviceHistory,
    useDeviceList,
    useDeviceRealtimeSnapshot,
    useDeviceSave,
    useDeviceShares,
    useDeviceShareTargets,
    useReplaceDeviceGroupShares,
    useReplaceDeviceShares,
} from './device.service';
import type { Device, DeviceGroup, EdgeConnection, EdgeStatus } from './device.types';

interface CommandElement {
    required?: boolean;
    _key: string;
    elementId: string;
    name: string;
    value: string;
    unit?: string;
    options?: Device.CommandOperationElement['options'];
    dataType?: string;
    size?: number;
    encode?: string;
    length?: number;
    digits?: number;
}
interface CommandPopoverProps {
    pending: boolean;
    onSubmit: (deviceId: string, data: Device.Command) => void;
    device: Device.Overview;
    /** 服务端推送的实时在线状态；`device` 为打开弹窗时加载的详情快照。 */
    online: boolean;
    func: Device.CommandOperation;
    onClose: () => void;
}
const INTEGER_RANGES: Record<string, [bigint, bigint]> = {
    INT8: [-128n, 127n],
    UINT8: [0n, 255n],
    INT16: [-32768n, 32767n],
    UINT16: [0n, 65535n],
    INT32: [-2147483648n, 2147483647n],
    UINT32: [0n, 4294967295n],
    INT64: [-9223372036854775808n, 9223372036854775807n],
    UINT64: [0n, 18446744073709551615n],
};
const HEX_VALUE_PATTERN = /^[0-9a-fA-F]+$/;
const INTEGER_VALUE_PATTERN = /^[+-]?\d+$/;
const DECIMAL_VALUE_PATTERN = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
const parseBigIntStrict = (value: string): bigint | null => {
    if (!INTEGER_VALUE_PATTERN.test(value)) return null;
    try {
        return BigInt(value);
    } catch {
        return null;
    }
};
export const validateValue = (element: CommandElement): string | null => {
    const value = element.value.trim();
    if (!value) return `「${element.name}」值不能为空`;
    if (element.dataType === 'BOOL') {
        return value === '0' || value === '1'
            ? null
            : `「${element.name}」BOOL 类型只能输入 0 或 1`;
    }
    if (element.dataType) {
        if (element.dataType === 'STRING') {
            if (
                typeof element.size === 'number' &&
                element.size > 0 &&
                new TextEncoder().encode(value).byteLength > element.size
            )
                return `「${element.name}」STRING 长度不能超过 ${element.size} 字节`;
            return null;
        }
        const range = INTEGER_RANGES[element.dataType];
        if (range) {
            const parsed = parseBigIntStrict(value);
            if (parsed === null) return `「${element.name}」请输入有效整数`;
            if (parsed < range[0] || parsed > range[1])
                return `「${element.name}」${element.dataType} 范围 ${range[0]} ~ ${range[1]}`;
            return null;
        }
        const number = DECIMAL_VALUE_PATTERN.test(value) ? Number(value) : Number.NaN;
        if (!Number.isFinite(number)) return `「${element.name}」请输入有效数字`;
        if (
            (element.dataType === 'FLOAT32' || element.dataType === 'FLOAT') &&
            !Number.isFinite(Math.fround(number))
        )
            return `「${element.name}」${element.dataType} 值超出范围`;
        return null;
    }
    if (element.encode === 'BCD') {
        const number = DECIMAL_VALUE_PATTERN.test(value) ? Number(value) : Number.NaN;
        if (!Number.isFinite(number)) return `「${element.name}」BCD 编码只能输入数字`;
        if (number < 0) return `「${element.name}」BCD 编码不支持负数`;
        const digits = Math.max(0, Math.min(8, element.digits ?? 0));
        const length = Math.max(1, element.length ?? 1);
        if (Math.round(Math.abs(number) * 10 ** digits) >= 10 ** (length * 2))
            return `「${element.name}」BCD 编码长度超出 ${length} 字节`;
        return null;
    }
    if (element.encode) {
        if (!HEX_VALUE_PATTERN.test(value))
            return `「${element.name}」${element.encode} 编码只能输入十六进制字符`;
        if (
            typeof element.length === 'number' &&
            element.length > 0 &&
            value.length > element.length * 2
        )
            return `「${element.name}」${element.encode} 编码长度不能超过 ${element.length} 字节`;
        return null;
    }
    return DECIMAL_VALUE_PATTERN.test(value) && Number.isFinite(Number(value))
        ? null
        : `「${element.name}」请输入有效数字`;
};
const CommandPopover = ({
    device,
    online,
    func,
    onClose,
    pending,
    onSubmit,
}: CommandPopoverProps) => {
    const { message } = App.useApp();
    const isSl651CompleteCommand = device.protocol_type === 'SL651';
    const isMqttCommand = device.protocol_type === 'MQTT';
    const [elements, setElements] = useState<CommandElement[]>(() =>
        (func.elements || []).map((element) => ({
            ...element,
            _key: String(element.elementId ?? element.name),
            value: element.value ?? '',
        }))
    );
    const [selectedKeys, setSelectedKeys] = useState<string[]>(() =>
        isSl651CompleteCommand
            ? (func.elements || []).map((element) => String(element.elementId ?? element.name))
            : []
    );
    const checkOnline = useCallback(() => {
        if (online) return true;
        message.warning('设备离线');
        return false;
    }, [online, message]);
    const handleSend = useCallback(() => {
        const selected = elements.filter((element) =>
            isMqttCommand
                ? element.required || element.value.trim().length > 0
                : selectedKeys.includes(element._key)
        );
        if (!selected.length) {
            message.warning('请至少选择一个要素');
            return;
        }
        for (const element of selected) {
            const error = validateValue(element);
            if (error) {
                message.error(error);
                return;
            }
        }
        if (!checkOnline()) return;
        if (pending) return;
        onSubmit(device.id, {
            ...(func.mqttMessageId ? { mqttMessageId: func.mqttMessageId } : {}),
            elements: selected.map((element) => ({
                elementId: element.elementId,
                value: element.value.trim(),
            })),
        });
    }, [
        checkOnline,
        pending,
        onSubmit,
        device.id,
        elements,
        message,
        selectedKeys,
        isMqttCommand,
        func.mqttMessageId,
    ]);
    const handlePresetClick = useCallback(
        (element: CommandElement, value: string) => {
            if (isSl651CompleteCommand || isMqttCommand) {
                setElements((current) =>
                    current.map((item) => (item._key === element._key ? { ...item, value } : item))
                );
                setSelectedKeys(elements.map((item) => item._key));
                return;
            }
            if (!checkOnline()) return;
            if (pending) return;
            onSubmit(device.id, { elements: [{ elementId: element.elementId, value }] });
        },
        [checkOnline, pending, onSubmit, device.id, elements, isSl651CompleteCommand, isMqttCommand]
    );
    if (!elements.length) return <div className="p-3">暂无可下发要素</div>;
    return (
        <div className="max-w-[360px]">
            <div className="mb-2">
                <div>
                    设备：{device.name}（{device.device_code}）
                </div>
                <div className="text-xs text-gray-400">指令：{func.name}</div>
            </div>
            <div className="mb-2 max-h-[260px] overflow-y-auto pr-1">
                {elements.map((element) => {
                    const checked = selectedKeys.includes(element._key);
                    const hasOptions = !!element.options?.length;
                    return (
                        <div
                            key={element._key}
                            className={`mb-2 pb-2 ${hasOptions ? 'border-b border-gray-100' : ''}`}
                        >
                            <Flex align="center" className={hasOptions ? 'mb-1.5' : ''}>
                                {!isMqttCommand && (
                                    <Checkbox
                                        checked={checked}
                                        disabled={isSl651CompleteCommand}
                                        onChange={(event) =>
                                            setSelectedKeys((current) =>
                                                event.target.checked
                                                    ? [...current, element._key]
                                                    : current.filter((key) => key !== element._key)
                                            )
                                        }
                                    />
                                )}
                                <span className="mx-1.5 flex-1">
                                    {element.name}
                                    {isMqttCommand && (
                                        <span
                                            className={
                                                element.required
                                                    ? 'ml-1 text-red-500'
                                                    : 'ml-1 text-gray-400'
                                            }
                                        >
                                            {element.required ? '必填' : '选填'}
                                        </span>
                                    )}
                                    {element.unit ? `（${element.unit}）` : ''}
                                </span>
                                <Input
                                    size="small"
                                    className="!w-[120px]"
                                    value={element.value}
                                    placeholder={
                                        isMqttCommand && !element.required
                                            ? '留空不发送'
                                            : hasOptions
                                              ? '或手动输入'
                                              : ''
                                    }
                                    onChange={(event) =>
                                        setElements((current) =>
                                            current.map((item) =>
                                                item._key === element._key
                                                    ? { ...item, value: event.target.value }
                                                    : item
                                            )
                                        )
                                    }
                                />
                            </Flex>
                            {hasOptions && (
                                <Flex wrap gap={6} className="ml-[26px]">
                                    <span className="mr-1 text-xs text-gray-400">预设值：</span>
                                    {element.options?.map((option) => (
                                        <Button
                                            key={option.value}
                                            size="small"
                                            type="primary"
                                            ghost
                                            loading={pending}
                                            onClick={() => handlePresetClick(element, option.value)}
                                        >
                                            {option.label}
                                        </Button>
                                    ))}
                                </Flex>
                            )}
                        </div>
                    );
                })}
            </div>
            <Flex justify="flex-end" gap={8}>
                <Button size="small" onClick={onClose}>
                    取消
                </Button>
                <Button
                    size="small"
                    type="primary"
                    loading={pending}
                    disabled={!isMqttCommand && !selectedKeys.length}
                    onClick={handleSend}
                >
                    下发
                </Button>
            </Flex>
        </div>
    );
};

export type DeviceFormValues = Omit<Device.CreateDto, 'parameters'> & {
    parameterInputs?: Record<string, unknown>;
    parameters?: Record<string, unknown>;
} & EdgeConnection & {
        id?: string;
        edge_protocol?: Link.Protocol;
    };
const EMPTY_EDGE_NODES: Edge.Node[] = [];
interface Props {
    open: boolean;
    editing: Device.Overview | null;
    loading: boolean;
    linkOptions: Link.Option[];
    groups: DeviceGroup.TreeItem[];
    onCancel: () => void;
    onFinish: (values: DeviceFormValues) => void;
}
export function DeviceFormModal({
    open,
    editing,
    loading,
    linkOptions,
    groups,
    onCancel,
    onFinish,
}: Props) {
    const [form] = Form.useForm<DeviceFormValues>();
    const [source, setSource] = useState<'collector' | 'edge'>('collector');
    const [nodeId, setNodeId] = useState<string>();
    const { data: node } = useEdgeConfigurationDetail(
        open && source === 'edge' ? nodeId : undefined
    );
    const edgeProtocol = Form.useWatch('edge_protocol', form);
    const edgeTransport = Form.useWatch('edge_transport', form);
    const edgeMode = Form.useWatch('edge_mode', form);
    const { data: nodes = EMPTY_EDGE_NODES, isLoading: loadingNodes } = useEdgeSelectionList(
        open && source === 'edge'
    );
    const clearChannel = () =>
        form.setFieldsValue({
            link_id: undefined,
            protocol_config_id: undefined,
            target_id: undefined,
        });
    const availableLinks = linkOptions.filter((link) => link.execution !== 'edge');
    const nodeOptions = nodes.map((node) => ({ value: node.id, label: node.name || node.imei }));
    if (editing?.edge_node_id && !nodeOptions.some((node) => node.value === editing.edge_node_id)) {
        nodeOptions.push({
            value: editing.edge_node_id,
            label: editing.edge_node_name || editing.edge_node_imei || editing.edge_node_id,
        });
    }
    const modelId = Form.useWatch('protocol_config_id', form);
    const linkId = Form.useWatch('link_id', form);
    const channel = linkOptions.find((value) => value.id === linkId);
    const protocol = source === 'edge' ? edgeProtocol : channel?.protocol;
    const linkMode = source === 'edge' ? edgeMode : channel?.endpoint.mode;
    const showHeartbeat =
        (source === 'collector' || edgeTransport === 'tcp') &&
        linkMode === 'TCP Server' &&
        protocol !== 'SL651';
    const showRegistration = source === 'collector' && showHeartbeat;
    const { data: models } = useProtocolConfigOptions(protocol ?? 'Modbus', {
        enabled: open && !!protocol,
    });
    const parameterDefinitions =
        models?.list.find((model) => model.id === modelId)?.parameters ??
        (modelId === editing?.protocol_config_id ? editing?.parameterDefinitions : undefined) ??
        [];
    const flatten = (
        nodes: DeviceGroup.TreeItem[]
    ): {
        value: string;
        label: string;
    }[] =>
        nodes.flatMap((node) => [
            { value: node.id, label: node.name },
            ...flatten(node.children ?? []),
        ]);
    const protocolsForConnection = (transport?: string, mode?: string): Link.Protocol[] =>
        transport === 'serial'
            ? ['Modbus', 'DLT645']
            : mode === 'TCP Server'
              ? ['Modbus', 'SL651', 'MC', 'FINS', 'DLT645']
              : ['Modbus', 'S7', 'MC', 'FINS', 'DLT645'];
    const changeConnection = (values: Partial<DeviceFormValues>) => {
        form.setFieldsValue(values);
        const transport = values.edge_transport ?? edgeTransport;
        const mode = values.edge_mode ?? edgeMode;
        if (edgeProtocol && !protocolsForConnection(transport, mode).includes(edgeProtocol)) {
            form.setFieldValue('edge_protocol', undefined);
            clearChannel();
        }
    };
    const protocolField = (
        <Form.Item name="edge_protocol" label="协议" rules={[{ required: true }]}>
            <Select
                placeholder="请选择协议"
                disabled={!edgeTransport || (edgeTransport === 'tcp' && !edgeMode)}
                options={protocolsForConnection(edgeTransport, edgeMode).map((value) => ({
                    value,
                    label: value,
                }))}
                onChange={clearChannel}
            />
        </Form.Item>
    );
    const packet = (name: 'heartbeat' | 'registration', label: string) => (
        <div className="col-span-full grid grid-cols-1 gap-x-4 sm:grid-cols-[140px_minmax(0,1fr)]">
            <Form.Item label={`${label}模式`} name={[name, 'mode']}>
                <Select
                    options={[
                        { value: 'OFF', label: '关闭' },
                        { value: 'HEX', label: 'HEX' },
                        { value: 'ASCII', label: 'ASCII' },
                    ]}
                />
            </Form.Item>
            <Form.Item noStyle dependencies={[[name, 'mode']]}>
                {({ getFieldValue }) => {
                    const mode = getFieldValue([name, 'mode']);
                    if (!mode || mode === 'OFF') return null;
                    return (
                        <Form.Item
                            label={`${label}内容`}
                            name={[name, 'content']}
                            rules={[
                                { required: true, message: `请输入${label}内容` },
                                {
                                    validator: async (_, value: string) => {
                                        if (
                                            mode === 'HEX' &&
                                            value &&
                                            !/^(?:[0-9a-f]{2})+$/i.test(value.replace(/\s/g, ''))
                                        )
                                            throw new Error(
                                                'HEX 内容必须由完整字节组成，例如 AA BB CC DD'
                                            );
                                    },
                                },
                            ]}
                            extra={
                                mode === 'HEX'
                                    ? '十六进制字符串，空格会自动去除'
                                    : 'ASCII 字符串，支持 \\r \\n 转义'
                            }
                        >
                            <Input placeholder={mode === 'HEX' ? 'AA BB CC DD' : 'HELLO\\r\\n'} />
                        </Form.Item>
                    );
                }}
            </Form.Item>
        </div>
    );
    return (
        <FormModal
            open={open}
            title={editing ? '编辑设备' : '新增设备'}
            onCancel={onCancel}
            onOk={() => form.submit()}
            confirmLoading={loading}
            destroyOnHidden
            afterOpenChange={(visible) => {
                if (visible) {
                    const currentLink = linkOptions.find((link) => link.id === editing?.link_id);
                    setSource(
                        currentLink?.execution === 'edge' || editing?.edge_node_id
                            ? 'edge'
                            : 'collector'
                    );
                    setNodeId(currentLink?.edge_node_id ?? editing?.edge_node_id);
                    form.resetFields();
                    form.setFieldsValue(
                        editing
                            ? {
                                  ...editing,
                                  edge_protocol: editing.protocol_type,
                                  heartbeat: {
                                      ...editing.heartbeat,
                                      mode: editing.heartbeat?.mode ?? 'OFF',
                                  },
                                  registration: {
                                      ...editing.registration,
                                      mode: editing.registration?.mode ?? 'OFF',
                                  },
                              }
                            : {
                                  status: 'enabled',
                                  online_timeout: 300,
                                  remote_control: true,
                                  timezone: '+08:00',
                                  slave_id: 1,
                                  edge_transport: 'tcp',
                                  edge_mode: 'TCP Client',
                                  serial_baud_rate: 9600,
                                  serial_data_bits: 8,
                                  serial_stop_bits: 1,
                                  serial_parity: 'none',
                                  heartbeat: { mode: 'OFF' },
                                  registration: { mode: 'OFF' },
                              }
                    );
                }
            }}
        >
            <ConfigProvider
                theme={{
                    components: { Form: { itemMarginBottom: 12, verticalLabelPadding: '0 0 4px' } },
                }}
            >
                <Form
                    className="grid grid-cols-1 gap-x-4 sm:grid-cols-2"
                    form={form}
                    layout="vertical"
                    onFinish={(values) => {
                        if (source === 'edge') {
                            if (!edgeProtocol || !nodeId) return;
                            values.link_id = undefined;
                            values.target_id = undefined;
                            values.registration = { mode: 'OFF' };
                            values.edge_connection = {
                                name: values.name,
                                edge_node_id: nodeId,
                                protocol: edgeProtocol,
                                status: 'enabled',
                                endpoint: {
                                    transport: edgeTransport,
                                    interface: values.edge_interface,
                                    mode: edgeMode ?? 'TCP Client',
                                    ip: values.edge_ip ?? '',
                                    port: values.edge_port ?? 0,
                                    targets: [],
                                    baud_rate: values.serial_baud_rate,
                                    data_bits: values.serial_data_bits,
                                    stop_bits: values.serial_stop_bits,
                                    parity: values.serial_parity,
                                },
                            };
                            if (protocol === 'Modbus')
                                values.modbus_mode = edgeTransport === 'serial' ? 'RTU' : 'TCP';
                        } else values.edge_connection = undefined;
                        if (!showHeartbeat) values.heartbeat = { mode: 'OFF' };
                        if (!showRegistration) values.registration = { mode: 'OFF' };
                        for (const packet of [values.heartbeat, values.registration]) {
                            if (packet?.mode === 'HEX' && packet.content)
                                packet.content = packet.content.replace(/\s/g, '');
                        }
                        if (protocol === 'SL651')
                            values.device_code = values.device_code.padStart(10, '0');
                        if (protocol === 'DLT645')
                            values.device_code = values.device_code.padStart(12, '0');
                        values.parameters = deviceParameterValues(
                            parameterDefinitions,
                            values.parameterInputs ?? {},
                            editing?.parameters
                        );
                        delete values.parameterInputs;
                        onFinish(values);
                    }}
                >
                    <Divider
                        className="col-span-full"
                        style={{ margin: '8px 0 12px' }}
                        titlePlacement="start"
                    >
                        基本信息
                    </Divider>
                    <Form.Item name="name" label="设备名称" rules={[{ required: true }]}>
                        <Input maxLength={100} />
                    </Form.Item>
                    <Form.Item
                        name="device_code"
                        label={protocol === 'DLT645' ? '电表地址（12 位数字）' : '设备编码'}
                        rules={[
                            { required: true },
                            ...(protocol === 'DLT645'
                                ? [
                                      {
                                          pattern: /^\d{1,12}$/,
                                          message: '表地址须为 1 至 12 位数字，保存时补零',
                                      },
                                  ]
                                : []),
                        ]}
                    >
                        <Input maxLength={100} />
                    </Form.Item>
                    <Divider
                        className="col-span-full"
                        style={{ margin: '8px 0 12px' }}
                        titlePlacement="start"
                    >
                        接入配置
                    </Divider>
                    <Form.Item label="接入方式">
                        <Radio.Group
                            aria-label="接入方式"
                            optionType="button"
                            value={source}
                            options={[
                                { value: 'collector', label: '本地链路' },
                                { value: 'edge', label: '边缘节点' },
                            ]}
                            onChange={(event) => {
                                setSource(event.target.value);
                                setNodeId(undefined);
                                form.setFieldValue('edge_node_id', undefined);
                                clearChannel();
                            }}
                        />
                    </Form.Item>
                    {source === 'edge' && (
                        <Form.Item
                            name="edge_node_id"
                            label="边缘节点"
                            rules={[{ required: true, message: '请选择边缘节点' }]}
                        >
                            <Select
                                aria-label="边缘节点"
                                value={nodeId}
                                showSearch
                                optionFilterProp="label"
                                placeholder="请选择边缘节点"
                                loading={loadingNodes}
                                options={nodeOptions}
                                onChange={(value) => {
                                    setNodeId(value);
                                    form.setFieldValue('edge_interface', undefined);
                                    clearChannel();
                                }}
                            />
                        </Form.Item>
                    )}
                    {source === 'edge' && (
                        <>
                            <Form.Item
                                name="edge_transport"
                                label="传输方式"
                                rules={[{ required: true }]}
                            >
                                <Select
                                    options={['tcp', 'serial'].map((value) => ({
                                        value,
                                        label: value === 'tcp' ? 'TCP' : '串口',
                                    }))}
                                    onChange={(value) =>
                                        changeConnection({
                                            edge_transport: value,
                                            edge_interface: undefined,
                                            edge_mode: edgeMode ?? 'TCP Client',
                                            serial_baud_rate:
                                                form.getFieldValue('serial_baud_rate') ?? 9600,
                                            serial_data_bits:
                                                form.getFieldValue('serial_data_bits') ?? 8,
                                            serial_stop_bits:
                                                form.getFieldValue('serial_stop_bits') ?? 1,
                                            serial_parity:
                                                form.getFieldValue('serial_parity') ?? 'none',
                                        })
                                    }
                                />
                            </Form.Item>
                            <Form.Item
                                name="edge_interface"
                                label={edgeTransport === 'serial' ? '串口' : '网口'}
                                rules={[{ required: true }]}
                            >
                                <Select
                                    showSearch
                                    options={
                                        edgeTransport === 'serial'
                                            ? (node?.serialPorts ?? []).map((port) => ({
                                                  value: port.path,
                                                  label: port.path,
                                              }))
                                            : (node?.interfaces ?? [])
                                                  .filter((port) => port.ipv4)
                                                  .map((port) => ({
                                                      value: port.name,
                                                      label: `${port.name} · ${port.ipv4}`,
                                                  }))
                                    }
                                    onChange={(value) => {
                                        if (edgeMode === 'TCP Server')
                                            form.setFieldValue(
                                                'edge_ip',
                                                node?.interfaces?.find(
                                                    (port) => port.name === value
                                                )?.ipv4
                                            );
                                    }}
                                />
                            </Form.Item>
                            {edgeTransport === 'tcp' ? (
                                <>
                                    <Form.Item
                                        name="edge_mode"
                                        label="TCP 模式"
                                        rules={[{ required: true }]}
                                    >
                                        <Select
                                            options={['TCP Client', 'TCP Server'].map((value) => ({
                                                value,
                                                label: value,
                                            }))}
                                            onChange={(value) =>
                                                changeConnection({ edge_mode: value })
                                            }
                                        />
                                    </Form.Item>
                                    {protocolField}
                                    <Form.Item
                                        name="edge_ip"
                                        label={edgeMode === 'TCP Server' ? '监听 IP' : '目标 IP'}
                                        rules={[{ required: true }]}
                                    >
                                        <Input />
                                    </Form.Item>
                                    <Form.Item
                                        name="edge_port"
                                        label={edgeMode === 'TCP Server' ? '监听端口' : '目标端口'}
                                        rules={[{ required: true }]}
                                    >
                                        <InputNumber className="w-full" min={1} max={65535} />
                                    </Form.Item>
                                </>
                            ) : (
                                <>
                                    {protocolField}
                                    <Form.Item
                                        name="serial_baud_rate"
                                        label="波特率"
                                        rules={[{ required: true }]}
                                    >
                                        <InputNumber className="w-full" min={300} max={4000000} />
                                    </Form.Item>
                                    <Form.Item name="serial_data_bits" label="数据位">
                                        <Select
                                            options={[5, 6, 7, 8].map((value) => ({ value }))}
                                        />
                                    </Form.Item>
                                    <Form.Item name="serial_stop_bits" label="停止位">
                                        <Select options={[1, 2].map((value) => ({ value }))} />
                                    </Form.Item>
                                    <Form.Item name="serial_parity" label="校验位">
                                        <Select
                                            options={[
                                                { value: 'none', label: '无' },
                                                { value: 'even', label: '偶校验' },
                                                { value: 'odd', label: '奇校验' },
                                            ]}
                                        />
                                    </Form.Item>
                                </>
                            )}
                        </>
                    )}
                    {source === 'collector' && (
                        <Form.Item
                            name="link_id"
                            label="本地链路"
                            rules={[
                                {
                                    required: true,
                                    message: '请选择链路；没有可用链路时请先在链路管理中创建',
                                },
                            ]}
                        >
                            <Select
                                showSearch
                                optionFilterProp="label"
                                placeholder="请选择链路"
                                options={availableLinks.map((c) => ({
                                    value: c.id,
                                    label: `${c.name} · ${c.protocol}`,
                                }))}
                                onChange={() =>
                                    form.setFieldsValue({
                                        protocol_config_id: undefined,
                                        target_id: undefined,
                                    })
                                }
                            />
                        </Form.Item>
                    )}
                    {source === 'collector' && channel?.endpoint.mode === 'TCP Client' && (
                        <Form.Item name="target_id" label="目标地址" rules={[{ required: true }]}>
                            <Select
                                options={channel.endpoint.targets.map((t) => ({
                                    value: t.id,
                                    label: `${t.name} · ${t.ip}:${t.port}`,
                                }))}
                            />
                        </Form.Item>
                    )}
                    <Divider
                        className="col-span-full"
                        style={{ margin: '8px 0 12px' }}
                        titlePlacement="start"
                    >
                        采集配置
                    </Divider>
                    <Form.Item
                        name="protocol_config_id"
                        label="设备类型"
                        rules={[{ required: true }]}
                    >
                        <Select
                            disabled={!protocol}
                            options={models?.list.map((m) => ({ value: m.id, label: m.name }))}
                        />
                    </Form.Item>
                    {protocol === 'Modbus' && (
                        <>
                            <Form.Item
                                name="slave_id"
                                label="从站地址"
                                rules={[{ required: true }]}
                            >
                                <InputNumber className="w-full" min={1} max={247} />
                            </Form.Item>
                            {source === 'collector' && (
                                <Form.Item
                                    name="modbus_mode"
                                    label="Modbus 模式"
                                    rules={[{ required: true }]}
                                >
                                    <Select
                                        options={['TCP', 'RTU'].map((value) => ({
                                            value,
                                            label: value,
                                        }))}
                                    />
                                </Form.Item>
                            )}
                        </>
                    )}
                    {(showRegistration || showHeartbeat) && (
                        <Divider
                            className="col-span-full"
                            style={{ margin: '8px 0 12px' }}
                            titlePlacement="start"
                        >
                            连接识别
                        </Divider>
                    )}
                    {showRegistration && packet('registration', '注册包')}
                    {showHeartbeat && packet('heartbeat', '心跳包')}
                    <Divider
                        className="col-span-full"
                        style={{ margin: '8px 0 12px' }}
                        titlePlacement="start"
                    >
                        运行设置
                    </Divider>
                    <Form.Item name="group_id" label="设备分组">
                        <Select allowClear options={flatten(groups)} />
                    </Form.Item>
                    <Form.Item name="status" label="状态">
                        <Select
                            options={[
                                { value: 'enabled', label: '启用' },
                                { value: 'disabled', label: '停用' },
                            ]}
                        />
                    </Form.Item>
                    <Form.Item name="online_timeout" label="离线超时（秒）">
                        <InputNumber className="w-full" min={1} max={86400} />
                    </Form.Item>
                    <Form.Item name="remote_control" label="允许远控" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                    <Form.Item name="timezone" label="设备时区">
                        <Select
                            showSearch
                            options={Array.from({ length: 105 }, (_, index) => {
                                const minutes = index * 15 - 720;
                                const value = `${minutes >= 0 ? '+' : '-'}${String(Math.floor(Math.abs(minutes) / 60)).padStart(2, '0')}:${String(Math.abs(minutes) % 60).padStart(2, '0')}`;
                                return { value, label: `UTC${value}` };
                            })}
                        />
                    </Form.Item>
                    <Divider className="col-span-full" titlePlacement="start">
                        设备参数
                    </Divider>
                    {!parameterDefinitions.length && (
                        <p className="col-span-full text-sm text-slate-500">
                            该设备类型尚未定义参数，可在协议配置的设备属性中添加。
                        </p>
                    )}
                    {parameterDefinitions.map((parameter) => {
                        const value = editing?.parameters?.[parameter.name];
                        const initialValue =
                            parameter.dataType === 'JSON'
                                ? value === undefined
                                    ? ''
                                    : JSON.stringify(value, null, 2)
                                : parameter.dataType === 'BOOL'
                                  ? value === true
                                      ? 'true'
                                      : value === false
                                        ? 'false'
                                        : undefined
                                  : (value ?? undefined);
                        return (
                            <Form.Item
                                key={`${modelId}:${parameter.name}:${parameter.dataType}`}
                                name={['parameterInputs', parameter.name]}
                                label={parameter.name}
                                className={
                                    parameter.dataType === 'JSON' ? 'col-span-full' : undefined
                                }
                                initialValue={initialValue}
                                preserve={false}
                                rules={[
                                    {
                                        validator: (_, value) => {
                                            const parsed = deviceParameterInputSchema(
                                                parameter.dataType
                                            ).safeParse(value);
                                            return parsed.success
                                                ? Promise.resolve()
                                                : Promise.reject(
                                                      new Error(parsed.error.issues[0]?.message)
                                                  );
                                        },
                                    },
                                ]}
                            >
                                {parameter.dataType === 'DOUBLE' ? (
                                    <InputNumber className="w-full" placeholder="请输入数值" />
                                ) : parameter.dataType === 'BOOL' ? (
                                    <Select
                                        allowClear
                                        placeholder="请选择"
                                        options={[
                                            { value: 'true', label: '是' },
                                            { value: 'false', label: '否' },
                                        ]}
                                    />
                                ) : parameter.dataType === 'JSON' ? (
                                    <JsonEditor label={`${parameter.name}参数值`} height="140px" />
                                ) : (
                                    <Input placeholder="请输入参数值" />
                                )}
                            </Form.Item>
                        );
                    })}
                    <Form.Item className="col-span-full" name="remark" label="备注">
                        <Input.TextArea />
                    </Form.Item>
                </Form>
            </ConfigProvider>
        </FormModal>
    );
}

const deviceGroupTitle = (group: DeviceGroup.TreeItem) =>
    `${group.name} (${group.deviceCount ?? 0})`;
const deviceGroupFormValues = (group: DeviceGroup.TreeItem): GroupFormValues => ({
    id: group.id,
    name: group.name,
    parentId: group.parent_id || undefined,
    sortOrder: group.sort_order,
    status: group.status,
    remark: group.remark,
});
interface DeviceGroupPanelProps {
    groups: DeviceGroup.TreeItem[];
    loading: boolean;
    selectedGroupId: string | null;
    onSelect: (groupId: string | null) => void;
    canManageGroup: boolean;
    ungroupedCount: number;
    onShare: (group: DeviceGroup.TreeItem) => void;
}
const DeviceGroupPanel = ({
    groups,
    loading,
    selectedGroupId,
    onSelect,
    canManageGroup,
    ungroupedCount,
    onShare,
}: DeviceGroupPanelProps) => {
    const saveMutation = useDeviceGroupSave();
    const deleteMutation = useDeviceGroupDelete();
    const shareActions = useCallback(
        (group: DeviceGroup.TreeItem): GroupTreeAction[] =>
            group.can_share
                ? [
                      {
                          key: 'share',
                          label: '分享分组',
                          icon: <ShareAltOutlined />,
                          onClick: () => onShare(group),
                      },
                  ]
                : [],
        [onShare]
    );
    return (
        <GroupTreePanel
            groups={groups}
            loading={loading}
            selectedGroupId={selectedGroupId}
            onSelect={onSelect}
            allLabel="全部设备"
            ungroupedCount={ungroupedCount}
            groupTitle={deviceGroupTitle}
            canManage={canManageGroup}
            formNoun="分组"
            deleteHint="删除后该分组下的子分组和设备不会被删除，但需要先移除或转移。"
            saving={saveMutation.isPending}
            toFormValues={deviceGroupFormValues}
            onSave={({ parentId, sortOrder, ...values }, onSaved) =>
                saveMutation.mutate(
                    // 后端以空字符串表示顶级分组；parent_id 不接受 null。
                    { ...values, parent_id: parentId ?? '', sort_order: sortOrder },
                    { onSuccess: onSaved }
                )
            }
            onDelete={(group) => deleteMutation.mutate(group.id)}
            extraActions={shareActions}
        />
    );
};

/**
 * 设备管理页面。
 */
const { Search } = Input;
const EMPTY_DEVICE_LIST: Device.Overview[] = [];
const EMPTY_GROUP_TREE: DeviceGroup.TreeItem[] = [];
const EMPTY_LINK_OPTIONS: Link.Item[] = [];
const EMPTY_COMMAND_OPERATIONS: Device.CommandOperation[] = [];
const DEVICE_CARD_GRID_CLASS =
    'grid grid-cols-1 items-stretch gap-3 lg:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4';
const DEVICE_CARD_ACTION_BUTTON_CLASS =
    '!flex !h-8 !w-8 items-center justify-center !rounded-md text-slate-500 hover:!bg-slate-100 hover:!text-slate-900';
const DEVICE_CARD_DANGER_BUTTON_CLASS =
    '!flex !h-8 !w-8 items-center justify-center !rounded-md hover:!bg-red-50';
const WIDE_DEVICE_CARD_ITEM_COUNT = 18;
const DEVICE_GRID_GAP = 12;
const DEVICE_CARD_MIN_WIDTH = 300;
interface DeviceProtocolStats {
    total: number;
    online: number;
    offline: number;
    enabled: number;
}
interface DeviceStats extends DeviceProtocolStats {
    byProtocol: Record<string, DeviceProtocolStats>;
}
const createDeviceStats = (): DeviceStats => ({
    total: 0,
    online: 0,
    offline: 0,
    enabled: 0,
    byProtocol: {},
});
interface DeviceGroupStats extends DeviceProtocolStats {}
const createGroupStats = (): DeviceGroupStats => ({ total: 0, online: 0, offline: 0, enabled: 0 });
const accumulateStats = <T extends DeviceProtocolStats>(stats: T, device: Device.Overview) => {
    stats.total++;
    if (isDeviceOnline(device)) stats.online++;
    else stats.offline++;
    if (device.status === 'enabled') stats.enabled++;
};
const buildDeviceStats = (devices: Device.Overview[]) => {
    const stats = createDeviceStats();
    for (const device of devices) {
        accumulateStats(stats, device);
        const protocol = device.protocol_type || device.protocol_name || '未知';
        stats.byProtocol[protocol] ??= { total: 0, online: 0, offline: 0, enabled: 0 };
        accumulateStats(stats.byProtocol[protocol], device);
    }
    return stats;
};
const buildGroupIndex = (groups: DeviceGroup.TreeItem[]) => {
    const index = new Map<string, DeviceGroup.TreeItem>();
    const walk = (nodes: DeviceGroup.TreeItem[]) => {
        for (const node of nodes) {
            index.set(node.id, node);
            if (node.children?.length) walk(node.children);
        }
    };
    walk(groups);
    return index;
};
const buildGroupScopeIds = (group?: DeviceGroup.TreeItem) => {
    const scope = new Set<string>();
    const walk = (node: DeviceGroup.TreeItem) => {
        scope.add(node.id);
        node.children?.forEach(walk);
    };
    if (group) walk(group);
    return scope;
};
const buildGroupStats = (
    groups: DeviceGroup.TreeItem[],
    deviceMap: Map<string, Device.Overview[]>
) => {
    const result = new Map<string, DeviceGroupStats>();
    const walk = (group: DeviceGroup.TreeItem): DeviceGroupStats => {
        const stats = createGroupStats();
        for (const device of deviceMap.get(group.id) ?? EMPTY_DEVICE_LIST) {
            accumulateStats(stats, device);
        }
        for (const child of group.children ?? []) {
            const childStats = walk(child);
            stats.total += childStats.total;
            stats.online += childStats.online;
            stats.offline += childStats.offline;
            stats.enabled += childStats.enabled;
        }
        result.set(group.id, stats);
        return stats;
    };
    groups.forEach(walk);
    return result;
};
const formatElementValue = (element: Device.Element) => {
    if (element.value === null || element.value === undefined || element.value === '') return '-';
    if (element.dictConfig?.mapType === 'VALUE') {
        return (
            element.dictConfig.items.find((item) => item.key === String(element.value))?.label ??
            String(element.value)
        );
    }
    const numeric = Number(element.value);
    const value =
        !Number.isNaN(numeric) && element.decimals !== undefined && element.decimals >= 0
            ? numeric.toFixed(element.decimals)
            : String(element.value);
    return element.unit ? `${value} ${element.unit}` : value;
};
const buildCardItems = (device: Device.Overview): DeviceCardItem[] => {
    if (device.elements?.length) {
        return getDeviceCardElements(device).map((element, index) => ({
            key: index,
            label: element.name,
            children: formatElementValue(element),
            group: element.group,
        }));
    }
    const count = device.element_count ?? 0;
    return count > 0 ? [{ key: 'elements', label: '采集要素', children: `${count} 个` }] : [];
};
const getDeviceDisplayElementCount = (device: Device.Overview) =>
    device.cardPointIds?.length ?? device.element_count ?? device.elements?.length ?? 0;
const edgeNodeLabel = (device: Device.Overview) =>
    device.edge_node_name || device.edge_node_imei || device.edge_node_id || '未绑定节点';
const edgeEndpointLabel = (device: Device.Overview) => {
    if (device.edge_transport === 'serial') {
        const parity =
            device.serial_parity === 'even' ? 'E' : device.serial_parity === 'odd' ? 'O' : 'N';
        const settings = [
            device.serial_baud_rate,
            device.serial_data_bits && device.serial_stop_bits
                ? `${device.serial_data_bits}${parity}${device.serial_stop_bits}`
                : '',
        ]
            .filter(Boolean)
            .join(' · ');
        return `串口 ${device.edge_interface || '-'}${settings ? ` · ${settings}` : ''}`;
    }
    if (device.edge_transport === 'tcp') {
        const address =
            device.edge_ip && device.edge_port ? `${device.edge_ip}:${device.edge_port}` : '-';
        return `网口/TCP ${device.edge_mode || '-'} · ${address}`;
    }
    return '边缘链路未配置';
};
type TcpRuntimeStatus = EdgeStatus | Link.Runtime;
const tcpRuntimeStatus = (
    device: Device.Overview,
    link?: Link.Item
): TcpRuntimeStatus | undefined => {
    if (device.edge_node_id) return device.edgeStatus;
    if (device.link_mode === 'TCP Server' && device.connected !== undefined) {
        return {
            ...link?.runtime,
            state: device.connected ? 'connected' : 'disconnected',
        };
    }
    if (device.target_id) {
        const target = link?.endpoint.targets.find((item) => item.id === device.target_id);
        if (target?.runtime) return target.runtime;
    }
    if (link?.runtime) return link.runtime;
    if (device.connected !== undefined)
        return { state: device.connected ? 'connected' : 'disconnected' };
    return undefined;
};
const tcpStateText = (status?: TcpRuntimeStatus) => {
    const state = status?.state?.trim();
    const clientCount = status?.clientCount ?? 0;
    if (!state) return 'TCP状态：未知';
    const labelMap: Record<string, string> = {
        online: '已连接',
        connected: '已连接',
        listening: '监听中',
        partial: '部分连接',
        connecting: '重连中',
        reconnecting: '重连中',
        offline: '已断开',
        disconnected: '已断开',
        stopped: '已断开',
        idle: '已断开',
        error: '已断开',
        failed: '已断开',
    };
    const label = labelMap[state.toLowerCase()] ?? state;
    const clients = clientCount > 0 ? ` · ${clientCount}连接` : '';
    return `TCP状态：${label}${clients}`;
};
const tcpStateColor = (status?: TcpRuntimeStatus) => {
    const state = status?.state?.toLowerCase();
    if (!state) return 'default';
    if (['online', 'connected'].includes(state)) return 'green';
    if (['listening', 'partial', 'connecting', 'reconnecting'].includes(state)) return 'processing';
    if (['error', 'failed'].includes(state)) return 'red';
    return 'default';
};
const DEVICE_ACCESS_LEVEL_OPTIONS: Array<{
    value: Device.ShareAccessLevel;
    label: string;
}> = [
    { value: 'view', label: '只读：查看设备与遥测' },
    { value: 'operate', label: '操作：只读 + 下发命令' },
];
interface DeviceShareFormValues {
    department_ids?: string[];
    user_ids?: string[];
    access_level: Device.ShareAccessLevel;
}
interface DeviceShareDrawerProps {
    resource: {
        kind: 'device' | 'group';
        id: string;
        name: string;
    } | null;
    onClose: () => void;
}
const DeviceShareDrawer = ({ resource, onClose }: DeviceShareDrawerProps) => {
    const { message } = App.useApp();
    const [form] = Form.useForm<DeviceShareFormValues>();
    const open = !!resource;
    const isGroup = resource?.kind === 'group';
    const deviceId = resource?.kind === 'device' ? resource.id : undefined;
    const groupId = resource?.kind === 'group' ? resource.id : undefined;
    const deviceShares = useDeviceShares(deviceId, {
        enabled: open && !isGroup,
    });
    const deviceTargets = useDeviceShareTargets(deviceId, {
        enabled: open && !isGroup,
    });
    const groupShares = useDeviceGroupShares(groupId, { enabled: open && isGroup });
    const groupTargets = useDeviceGroupShareTargets(groupId, { enabled: open && isGroup });
    const replaceDeviceShares = useReplaceDeviceShares();
    const replaceGroupShares = useReplaceDeviceGroupShares();
    const shares = (isGroup ? groupShares.data : deviceShares.data) ?? [];
    const targets = (isGroup ? groupTargets.data : deviceTargets.data) ?? [];
    const sharesLoading = isGroup ? groupShares.isLoading : deviceShares.isLoading;
    const targetsLoading = isGroup ? groupTargets.isLoading : deviceTargets.isLoading;
    const replacing = replaceDeviceShares.isPending || replaceGroupShares.isPending;
    const departmentOptions = useMemo(
        () =>
            targets
                .filter((target) => target.subject_type === 'department')
                .map((target) => ({ label: target.subject_name, value: target.subject_id })),
        [targets]
    );
    const userOptions = useMemo(
        () =>
            targets
                .filter((target) => target.subject_type === 'user')
                .map((target) => ({ label: target.subject_name, value: target.subject_id })),
        [targets]
    );
    const replace = (nextShares: Device.ReplaceSharesDto['shares'], onSuccess?: () => void) => {
        if (!resource) return;
        if (resource.kind === 'group') {
            replaceGroupShares.mutate(
                { groupId: resource.id, data: { shares: nextShares } },
                { onSuccess }
            );
            return;
        }
        replaceDeviceShares.mutate(
            { deviceId: resource.id, data: { shares: nextShares } },
            { onSuccess }
        );
    };
    const addOrUpdate = async () => {
        const values = await form.validateFields();
        const departmentIds = values.department_ids ?? [];
        const userIds = values.user_ids ?? [];
        if (departmentIds.length === 0 && userIds.length === 0) {
            message.warning('请至少选择一个部门或用户');
            return;
        }
        const nextShares = new Map<string, Device.ReplaceSharesDto['shares'][number]>();
        for (const share of shares) {
            if (share.inherited) continue;
            nextShares.set(`${share.subject_type}:${share.subject_id}`, {
                subject_type: share.subject_type,
                subject_id: share.subject_id,
                access_level: share.access_level,
            });
        }
        for (const subjectId of departmentIds) {
            nextShares.set(`department:${subjectId}`, {
                subject_type: 'department',
                subject_id: subjectId,
                access_level: values.access_level,
            });
        }
        for (const subjectId of userIds) {
            nextShares.set(`user:${subjectId}`, {
                subject_type: 'user',
                subject_id: subjectId,
                access_level: values.access_level,
            });
        }
        replace([...nextShares.values()], () => {
            form.setFieldsValue({ department_ids: [], user_ids: [] });
        });
    };
    const removeShare = (share: Device.ShareItem) => {
        replace(
            shares
                .filter((item) => !item.inherited && item.id !== share.id)
                .map((item) => ({
                    subject_type: item.subject_type,
                    subject_id: item.subject_id,
                    access_level: item.access_level,
                }))
        );
    };
    const columns: ColumnsType<Device.ShareItem> = [
        {
            title: '类型',
            dataIndex: 'subject_type',
            width: 82,
            render: (value: Device.ShareItem['subject_type']) =>
                value === 'department' ? (
                    <Tag color="purple">部门</Tag>
                ) : (
                    <Tag color="blue">用户</Tag>
                ),
        },
        { title: '分享对象', dataIndex: 'subject_name', ellipsis: true },
        {
            title: '访问级别',
            dataIndex: 'access_level',
            width: 104,
            render: (value: Device.ShareAccessLevel) => {
                const option = DEVICE_ACCESS_LEVEL_OPTIONS.find((item) => item.value === value);
                const color = value === 'operate' ? 'orange' : 'blue';
                return <Tag color={color}>{option?.label.split('：')[0] ?? value}</Tag>;
            },
        },
        {
            title: '更新时间',
            dataIndex: 'updated_at',
            width: 176,
            render: (value?: string) => formatDateTime(value),
        },
        {
            title: '来源',
            key: 'source',
            width: 150,
            render: (_, share) =>
                share.inherited ? (
                    <Tooltip title={`只能在设备分组「${share.source_group_name}」中修改`}>
                        <Tag color="cyan">继承 · {share.source_group_name}</Tag>
                    </Tooltip>
                ) : (
                    <Tag>直接授权</Tag>
                ),
        },
        {
            title: '操作',
            key: 'actions',
            width: 76,
            render: (_, share) =>
                share.inherited ? (
                    <Typography.Text type="secondary">分组维护</Typography.Text>
                ) : (
                    <Popconfirm
                        title={`取消「${share.subject_name}」的分享？`}
                        okText="确认"
                        cancelText="取消"
                        onConfirm={() => removeShare(share)}
                    >
                        <Button type="link" danger icon={<DeleteOutlined />}>
                            移除
                        </Button>
                    </Popconfirm>
                ),
        },
    ];
    return (
        <Drawer
            open={open}
            title={
                resource
                    ? `${resource.kind === 'group' ? '设备分组分享' : '设备分享'}：${resource.name}`
                    : '分享'
            }
            placement="right"
            width={760}
            onClose={onClose}
            destroyOnHidden
            afterOpenChange={(nextOpen) => {
                if (!nextOpen) form.resetFields();
            }}
        >
            <Space direction="vertical" size="large" className="w-full">
                <Typography.Text type="secondary">
                    {isGroup
                        ? '权限覆盖本组及全部子分组，新增或移入的设备自动继承，移出后自动失效。分享对象只能查看设备或下发命令。'
                        : '分组继承权限在这里仅展示，必须回到来源分组修改；设备直接授权可独立新增、更新或移除，最终权限与继承权限取并集。'}
                </Typography.Text>
                <Form<DeviceShareFormValues>
                    form={form}
                    layout="vertical"
                    initialValues={{ department_ids: [], user_ids: [], access_level: 'view' }}
                >
                    <Form.Item label="部门" name="department_ids">
                        <Select
                            mode="multiple"
                            allowClear
                            showSearch
                            optionFilterProp="label"
                            placeholder="选择部门（可多选）"
                            options={departmentOptions}
                            loading={targetsLoading}
                        />
                    </Form.Item>
                    <Form.Item label="用户" name="user_ids">
                        <Select
                            mode="multiple"
                            allowClear
                            showSearch
                            optionFilterProp="label"
                            placeholder="选择用户（可多选）"
                            options={userOptions}
                            loading={targetsLoading}
                        />
                    </Form.Item>
                    <Form.Item label="访问级别" name="access_level">
                        <Select options={DEVICE_ACCESS_LEVEL_OPTIONS} />
                    </Form.Item>
                    <Form.Item className="!mb-0">
                        <Button type="primary" block loading={replacing} onClick={addOrUpdate}>
                            添加 / 更新分享
                        </Button>
                    </Form.Item>
                </Form>
                <Table<Device.ShareItem>
                    rowKey={(share) => `${share.source_type ?? 'device'}:${share.id}`}
                    columns={columns}
                    dataSource={shares}
                    loading={sharesLoading}
                    pagination={false}
                    size="middle"
                    scroll={{ x: 'max-content', y: 'calc(100dvh - 520px)' }}
                    locale={{
                        emptyText: isGroup ? '当前分组暂无分享记录' : '当前设备暂无分享记录',
                    }}
                />
            </Space>
        </Drawer>
    );
};
const createDefaultHistoryRange = (): [Dayjs, Dayjs] => [dayjs().subtract(24, 'hour'), dayjs()];
const getHistoryTimePresets = () => [
    {
        label: '最近1小时',
        value: [dayjs().subtract(1, 'hour'), dayjs()] as [Dayjs, Dayjs],
    },
    {
        label: '最近6小时',
        value: [dayjs().subtract(6, 'hour'), dayjs()] as [Dayjs, Dayjs],
    },
    {
        label: '最近24小时',
        value: createDefaultHistoryRange(),
    },
    {
        label: '最近3天',
        value: [dayjs().subtract(3, 'day').startOf('day'), dayjs()] as [Dayjs, Dayjs],
    },
    {
        label: '最近7天',
        value: [dayjs().subtract(7, 'day').startOf('day'), dayjs()] as [Dayjs, Dayjs],
    },
];
const isPlainRecord = (value: unknown): value is Record<string, unknown> =>
    typeof value === 'object' && value !== null && !Array.isArray(value);
const normalizeHistoryPoint = (point: unknown, fallbackName?: string): Device.HistoryPointValue => {
    if (!isPlainRecord(point)) return { name: fallbackName, value: point };
    return {
        name: typeof point.name === 'string' && point.name.trim() ? point.name : fallbackName,
        value: Object.hasOwn(point, 'value') ? point.value : point,
        unit: typeof point.unit === 'string' ? point.unit : undefined,
    };
};
const parseHistoryBitLabels = (
    value: unknown,
    dictConfig: NonNullable<Device.Element['dictConfig']>
) => {
    const textValue = typeof value === 'string' ? value.trim() : '';
    const numeric =
        typeof value === 'number'
            ? value
            : textValue
              ? Number.parseInt(
                    textValue,
                    textValue.startsWith('0x') || /[A-Fa-f]/.test(textValue) ? 16 : 10
                )
              : Number.NaN;
    if (!Number.isFinite(numeric)) return [];
    return dictConfig.items
        .filter((item) => {
            const bitIndex = Number.parseInt(item.key, 10);
            if (!Number.isInteger(bitIndex) || bitIndex < 0 || bitIndex > 31) return false;
            const bitValue = (numeric >> bitIndex) & 1;
            const triggerValue = item.value || '1';
            return (
                (triggerValue === '1' && bitValue === 1) || (triggerValue === '0' && bitValue === 0)
            );
        })
        .map((item) => item.label)
        .filter(Boolean);
};
const formatHistoryValue = (
    point: Device.HistoryPointValue | undefined,
    meta?: Pick<Device.Element, 'decimals' | 'scale' | 'dictConfig' | 'unit'>
) => {
    if (!point) return '-';
    const value = point.value;
    if (value === null || value === undefined || value === '') return '-';
    if (meta?.dictConfig?.mapType === 'VALUE') {
        const mapped = meta.dictConfig.items.find((item) => item.key === String(value))?.label;
        if (mapped) return mapped;
    }
    if (meta?.dictConfig?.mapType === 'BIT') {
        const labels = parseHistoryBitLabels(value, meta.dictConfig);
        if (labels.length) return labels.join('、');
    }
    const numeric = typeof value === 'number' ? value : Number(value);
    let formatted =
        Number.isFinite(numeric) && meta?.decimals !== undefined && meta.decimals >= 0
            ? numeric.toFixed(meta.decimals)
            : typeof value === 'object'
              ? JSON.stringify(value)
              : String(value);
    if (
        typeof value === 'number' &&
        Number.isFinite(value) &&
        (meta?.decimals === undefined || meta.decimals < 0) &&
        meta?.scale !== undefined &&
        Number.isFinite(meta.scale) &&
        meta.scale !== 0
    ) {
        const [coefficient, exponent = '0'] = Math.abs(meta.scale).toString().split('e');
        const decimals = Math.max(0, (coefficient.split('.')[1]?.length ?? 0) - Number(exponent));
        if (decimals <= 15) {
            const rounded = Number(value.toFixed(decimals));
            const tolerance = Number.EPSILON * Math.abs(value);
            if (Math.abs(value - rounded) <= tolerance) formatted = String(rounded);
        }
    }
    const unit = point.unit ?? meta?.unit;
    return unit ? `${formatted} ${unit}` : formatted;
};
interface HistoryPointColumn {
    key: string;
    keys: string[];
    label: string;
    order: number;
    unit?: string;
    decimals?: number;
    scale?: number;
    dictConfig?: Device.Element['dictConfig'];
}
const buildHistoryPointColumns = (
    device: Device.Overview,
    records: Device.HistoryRecord[]
): HistoryPointColumn[] => {
    const configuredByKey = new Map<string, HistoryPointColumn>();
    const configuredByName = new Map<string, HistoryPointColumn>();
    device.elements?.forEach((element, index) => {
        if (element.visible === false) return;
        const label = element.name?.trim();
        const key = element.id?.trim() || label || `configured_${index}`;
        const meta: HistoryPointColumn = {
            key,
            keys: [key],
            label: label || key,
            order: index,
            unit: element.unit,
            decimals: element.decimals,
            scale: element.scale,
            dictConfig: element.dictConfig,
        };
        configuredByKey.set(key, meta);
        if (label) configuredByName.set(label, meta);
    });
    const columns = new Map<string, HistoryPointColumn>();
    let fallbackOrder = device.elements?.length ?? 0;
    for (const record of records) {
        for (const [pointKey, rawPoint] of Object.entries(record.values ?? {})) {
            if (
                device.elements?.some(
                    (element) => element.id === pointKey && element.visible === false
                )
            )
                continue;
            const point = normalizeHistoryPoint(rawPoint, pointKey);
            const label = point.name?.trim();
            const configured =
                configuredByKey.get(pointKey) || (label ? configuredByName.get(label) : undefined);
            const key = configured?.key ?? pointKey;
            const existing = columns.get(key);
            if (existing) {
                if (!existing.keys.includes(pointKey)) existing.keys.push(pointKey);
                continue;
            }
            columns.set(key, {
                ...(configured ?? {
                    key,
                    keys: [pointKey],
                    label: label || pointKey,
                    order: fallbackOrder++,
                }),
                keys: Array.from(new Set([...(configured?.keys ?? []), pointKey])),
                unit: point.unit ?? configured?.unit,
            });
        }
    }
    return [...columns.values()].sort(
        (a, b) => a.order - b.order || a.label.localeCompare(b.label)
    );
};
const getHistoryPoint = (record: Device.HistoryRecord, column: HistoryPointColumn) => {
    for (const key of column.keys) {
        if (Object.hasOwn(record.values ?? {}, key)) {
            return normalizeHistoryPoint(record.values[key], column.label);
        }
    }
    return Object.entries(record.values ?? {})
        .map(([key, point]) => normalizeHistoryPoint(point, key))
        .find((point) => point.name === column.label);
};
const DeviceHistoryModal = ({
    device,
    onClose,
}: {
    device: Device.Overview;
    onClose: () => void;
}) => {
    const [pagination, setPagination] = useState({ page: 1, pageSize: 20 });
    const [range, setRange] = useState<[Dayjs, Dayjs]>(createDefaultHistoryRange);
    const query = useMemo<Device.HistoryRecordQuery>(
        () => ({
            ...pagination,
            startTime: range[0].toISOString(),
            endTime: range[1].toISOString(),
        }),
        [pagination, range]
    );
    const { data, isLoading, isFetching } = useDeviceHistory(device.id, query);
    const records = data?.list ?? [];
    const pointColumns = useMemo(
        () => buildHistoryPointColumns(device, records),
        [device, records]
    );
    const columns = useMemo<ColumnsType<Device.HistoryRecord>>(() => {
        const tableColumns: ColumnsType<Device.HistoryRecord> = [
            {
                title: '上报时间',
                dataIndex: 'reportTime',
                key: 'reportTime',
                width: 180,
                fixed: 'left',
                render: (value) => formatDateTime(value),
            },
            ...pointColumns.map((column) => ({
                title: column.label,
                key: column.key,
                width: 150,
                ellipsis: true,
                render: (_: unknown, record: Device.HistoryRecord) => {
                    const point = getHistoryPoint(record, column);
                    const text = formatHistoryValue(point, column);
                    return (
                        <Tooltip title={text}>
                            <span className="block min-w-0 truncate tabular-nums">{text}</span>
                        </Tooltip>
                    );
                },
            })),
            {
                title: '原始报文',
                key: 'rawPayloadHex',
                width: 100,
                render: (_, record) => (
                    <HistoryRawPayloadButton payloads={record.rawPayloadHex ?? []} />
                ),
            },
        ];
        return tableColumns;
    }, [pointColumns]);
    const tableWidth = Math.max(760, 280 + pointColumns.length * 150);
    return (
        <Modal
            open
            onCancel={onClose}
            title={`历史数据 - ${device.name || device.device_code}`}
            width="min(1180px, 94vw)"
            footer={null}
            destroyOnHidden
            styles={{
                body: {
                    height: '75vh',
                    display: 'flex',
                    flexDirection: 'column',
                    overflow: 'hidden',
                },
            }}
        >
            <div className="flex h-full min-h-0 flex-col">
                <Flex justify="space-between" align="center" gap={12} wrap className="mb-3">
                    <DatePicker.RangePicker
                        showTime
                        allowClear={false}
                        value={range}
                        presets={getHistoryTimePresets()}
                        onChange={(value) => {
                            if (!value?.[0] || !value[1]) return;
                            setRange([value[0], value[1]]);
                            setPagination((current) => ({ ...current, page: 1 }));
                        }}
                    />
                    <Button
                        onClick={() => {
                            setRange(createDefaultHistoryRange());
                            setPagination((current) => ({ ...current, page: 1 }));
                        }}
                    >
                        重置
                    </Button>
                </Flex>
                <div className="min-h-0 flex-1">
                    <Table
                        rowKey="id"
                        size="small"
                        sticky
                        pagination={false}
                        columns={columns}
                        dataSource={records}
                        loading={isLoading || isFetching}
                        scroll={{ x: tableWidth, y: 'calc(75vh - 150px)' }}
                        locale={{ emptyText: <Empty description="当前时间范围暂无历史数据" /> }}
                    />
                </div>
                <Flex justify="flex-end" className="mt-3 shrink-0">
                    <Pagination
                        {...pagination}
                        total={data?.total ?? 0}
                        showSizeChanger
                        showTotal={(total) => `共 ${total} 条`}
                        onChange={(page, pageSize) => setPagination({ page, pageSize })}
                    />
                </Flex>
            </div>
        </Modal>
    );
};

function HistoryRawPayloadButton({ payloads }: { payloads: string[] }) {
    const { message } = App.useApp();
    if (!payloads.length) return <Typography.Text type="secondary">—</Typography.Text>;
    const text = JSON.stringify(payloads, null, 2);
    return (
        <Popover
            title={`原始报文（${payloads.length} 条，点击按钮复制全部）`}
            content={
                <pre className="m-0 max-h-[60dvh] max-w-[min(680px,80vw)] overflow-auto whitespace-pre-wrap break-all font-mono text-xs">
                    {text}
                </pre>
            }
        >
            <Button
                type="text"
                size="small"
                icon={<CopyOutlined />}
                aria-label="复制原始报文"
                onClick={async () => {
                    try {
                        await navigator.clipboard.writeText(text);
                        message.success('原始报文已复制');
                    } catch {
                        message.error('复制失败，请在悬浮内容中手动复制');
                    }
                }}
            />
        </Popover>
    );
}

function DeviceDebug({
    commandIds,
    item,
    open,
    onOpen,
    onClose,
}: {
    commandIds: string[];
    item: Device.Overview;
    open: boolean;
    onOpen: () => void;
    onClose: () => void;
}) {
    const closeOnUnmount = useRef({ open, onClose });
    closeOnUnmount.current = { open, onClose };
    useEffect(
        () => () => {
            if (closeOnUnmount.current.open) closeOnUnmount.current.onClose();
        },
        []
    );
    const { packets, toggle } = useDeviceDebug(
        item.id,
        open && item.debug_enabled === true,
        commandIds
    );
    return (
        <PacketDebugPanel
            scope="device"
            protocol={item.protocol_type ?? ''}
            buttonClassName={DEVICE_CARD_ACTION_BUTTON_CLASS}
            title={`设备调试 · ${item.name}`}
            enabled={item.debug_enabled === true}
            open={open}
            pending={toggle.isPending}
            loading={packets.isLoading}
            error={packets.error}
            acquisitions={packets.data}
            onToggle={() => toggle.mutate(!item.debug_enabled)}
            onOpen={onOpen}
            onClose={onClose}
        />
    );
}

interface DeviceGridItemProps {
    commandIds: string[];
    commandPending: boolean;
    onSubmitCommand: (deviceId: string, data: Device.Command) => void;
    debugDeviceId?: string;
    onDebug: Dispatch<SetStateAction<string | undefined>>;
    device: Device.Overview;
    online: boolean;
    /** 当前行内实际占用的列数，由虚拟网格按容器宽度计算。 */
    span: number;
    linkById: ReadonlyMap<string, Link.Item>;
    onHistory: (device: Device.Overview) => void;
    onShare: (device: Device.Overview) => void;
    onEdit: (device: Device.Overview) => void;
    onRemove: (device: Device.Overview) => void;
    commandPopoverOpen: boolean;
    commandDeviceId?: string;
    commandDevice: Device.Overview | null;
    commandFunc: Device.CommandOperation | null;
    commandLoadingId?: string;
    onOpenCommandPopover: (device: Device.Overview) => void;
    onSelectCommandOperation: (operation: Device.CommandOperation) => void;
    onCloseCommandPopover: () => void;
}
const DeviceGridItem = memo(
    ({
        device,
        commandIds,
        online,
        span,
        debugDeviceId,
        onDebug,
        linkById,
        onHistory,
        onShare,
        onEdit,
        onRemove,
        commandPopoverOpen,
        commandDeviceId,
        commandDevice,
        commandFunc,
        commandLoadingId,
        commandPending,
        onSubmitCommand,
        onOpenCommandPopover,
        onSelectCommandOperation,
        onCloseCommandPopover,
    }: DeviceGridItemProps) => {
        const items = useMemo(() => buildCardItems(device), [device]);
        const wide = getDeviceDisplayElementCount(device) >= WIDE_DEVICE_CARD_ITEM_COUNT;
        const spanStyle = useMemo<CSSProperties | undefined>(
            () => (span > 1 ? { gridColumn: `span ${span}` } : undefined),
            [span]
        );
        const activeCommandDevice =
            commandPopoverOpen && commandDeviceId === device.id ? commandDevice : null;
        const commandOperations =
            activeCommandDevice?.commandOperations ?? EMPTY_COMMAND_OPERATIONS;
        const canRemoteControl = device.remote_control !== false;
        const isCommandPopoverOpen = commandPopoverOpen && commandDeviceId === device.id;
        const commandLoading = commandLoadingId === device.id;
        const link = linkById.get(device.link_id);
        const isTcp = device.edge_node_id ? device.edge_transport === 'tcp' : !!device.link_id;
        const tcpStatus = isTcp ? tcpRuntimeStatus(device, link) : undefined;
        const registrationConfigured =
            !device.edge_node_id &&
            device.link_mode === 'TCP Server' &&
            device.registration?.mode !== undefined &&
            device.registration.mode !== 'OFF' &&
            !!device.registration.content?.trim();
        return (
            <div className="flex h-full min-w-0 flex-col" style={spanStyle}>
                <DeviceCard
                    title={
                        <Flex
                            justify="space-between"
                            align="start"
                            gap={10}
                            className="w-full min-w-0"
                        >
                            <div className="min-w-0 flex-1 pr-1 text-left">
                                <Tooltip title={device.name}>
                                    <div className="truncate whitespace-nowrap leading-5">
                                        {device.name}
                                    </div>
                                </Tooltip>
                                <div className="mt-0.5 min-w-0 text-xs font-normal leading-4 text-slate-400">
                                    <div className="truncate">编码：{device.device_code}</div>
                                    <Tooltip title={`设备 ID：${device.id}`}>
                                        <div className="truncate">设备 ID：{device.id}</div>
                                    </Tooltip>
                                </div>
                            </div>
                            <Tag
                                color={online ? 'success' : 'error'}
                                className="!mr-0 shrink-0 !rounded-md !px-2"
                            >
                                {online ? '在线' : '离线'}
                            </Tag>
                        </Flex>
                    }
                    subtitle={
                        <div className="flex w-full min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
                            {device.edge_node_id ? (
                                <>
                                    <Tag color="blue" className="!mr-0 !rounded-md">
                                        边缘：{edgeNodeLabel(device)}
                                    </Tag>
                                    <Tag color="cyan" className="!mr-0 !rounded-md">
                                        {edgeEndpointLabel(device)}
                                    </Tag>
                                    {isTcp && (
                                        <Tooltip title={tcpStatus?.reason || undefined}>
                                            <Tag
                                                color={tcpStateColor(tcpStatus)}
                                                className="!mr-0 !rounded-md"
                                            >
                                                {tcpStateText(tcpStatus)}
                                            </Tag>
                                        </Tooltip>
                                    )}
                                </>
                            ) : (
                                <Tag color="blue" className="!mr-0 !rounded-md">
                                    {device.link_name || '未绑定连接'}
                                </Tag>
                            )}
                            {!device.edge_node_id && isTcp && (
                                <Tooltip title={tcpStatus?.reason || undefined}>
                                    <Tag
                                        color={tcpStateColor(tcpStatus)}
                                        className="!mr-0 !rounded-md"
                                    >
                                        {tcpStateText(tcpStatus)}
                                    </Tag>
                                </Tooltip>
                            )}
                            {registrationConfigured && (
                                <Tag
                                    color={device.connected ? 'green' : 'default'}
                                    className="!mr-0 !rounded-md"
                                >
                                    注册状态：{device.connected ? '已注册' : '未注册'}
                                </Tag>
                            )}
                            <Tag color="purple" className="!mr-0 !rounded-md">
                                {device.protocol_name || device.protocol_type || '未配置协议'}
                            </Tag>
                            <span className="min-w-0 truncate text-xs text-slate-400">
                                上报：{formatDateTime(device.reportTime)}
                            </span>
                        </div>
                    }
                    items={items}
                    column={wide ? 8 : 4}
                    extra={
                        <Flex align="center" justify="center" gap={10} wrap className="w-full">
                            <Popover
                                open={isCommandPopoverOpen}
                                trigger="click"
                                placement="bottomRight"
                                content={
                                    isCommandPopoverOpen && commandFunc && activeCommandDevice ? (
                                        <CommandPopover
                                            key={commandFunc.mqttMessageId ?? commandFunc.name}
                                            pending={commandPending}
                                            onSubmit={onSubmitCommand}
                                            device={activeCommandDevice}
                                            online={online}
                                            func={commandFunc}
                                            onClose={onCloseCommandPopover}
                                        />
                                    ) : isCommandPopoverOpen && commandOperations.length ? (
                                        <Space direction="vertical" size={4}>
                                            {commandOperations.map((operation) => (
                                                <Button
                                                    key={operation.name}
                                                    type="text"
                                                    className="!h-8 !px-3"
                                                    onClick={() =>
                                                        onSelectCommandOperation(operation)
                                                    }
                                                >
                                                    {operation.name}
                                                </Button>
                                            ))}
                                        </Space>
                                    ) : null
                                }
                                onOpenChange={(open) => {
                                    if (!open) onCloseCommandPopover();
                                }}
                            >
                                <Tooltip
                                    title={
                                        !canRemoteControl
                                            ? '该设备未开启远程控制'
                                            : !device.can_command
                                              ? '当前账号没有设备下发权限'
                                              : online
                                                ? '下发指令'
                                                : '设备离线（点击后将提示）'
                                    }
                                >
                                    <Button
                                        type="text"
                                        size="small"
                                        className={DEVICE_CARD_ACTION_BUTTON_CLASS}
                                        icon={<SendOutlined />}
                                        disabled={!device.can_command || !canRemoteControl}
                                        loading={commandLoading}
                                        onClick={() => onOpenCommandPopover(device)}
                                    />
                                </Tooltip>
                            </Popover>
                            {device.can_edit && (
                                <DeviceDebug
                                    commandIds={commandIds}
                                    item={device}
                                    open={debugDeviceId === device.id}
                                    onOpen={() => onDebug(device.id)}
                                    onClose={() =>
                                        onDebug((current) =>
                                            current === device.id ? undefined : current
                                        )
                                    }
                                />
                            )}
                            <Tooltip title="历史数据">
                                <Button
                                    type="text"
                                    size="small"
                                    className={DEVICE_CARD_ACTION_BUTTON_CLASS}
                                    icon={<HistoryOutlined />}
                                    onClick={() => onHistory(device)}
                                />
                            </Tooltip>
                            {device.can_share && (
                                <Tooltip title="分享设备">
                                    <Button
                                        type="text"
                                        size="small"
                                        className={DEVICE_CARD_ACTION_BUTTON_CLASS}
                                        icon={<ShareAltOutlined />}
                                        onClick={() => onShare(device)}
                                    />
                                </Tooltip>
                            )}
                            {device.can_edit && (
                                <Tooltip title="编辑设备">
                                    <Button
                                        type="text"
                                        size="small"
                                        className={DEVICE_CARD_ACTION_BUTTON_CLASS}
                                        icon={<EditOutlined />}
                                        onClick={() => onEdit(device)}
                                    />
                                </Tooltip>
                            )}
                            {device.can_delete && (
                                <Tooltip title="删除设备">
                                    <Button
                                        type="text"
                                        danger
                                        size="small"
                                        className={DEVICE_CARD_DANGER_BUTTON_CLASS}
                                        icon={<DeleteOutlined />}
                                        onClick={() => onRemove(device)}
                                    />
                                </Tooltip>
                            )}
                        </Flex>
                    }
                />
            </div>
        );
    }
);
interface DeviceGridProps extends Omit<DeviceGridItemProps, 'device' | 'online' | 'span'> {
    devices: Device.Overview[];
}
const getDeviceKey = (device: Device.Overview) => device.id;
const getDeviceSpan = (device: Device.Overview) =>
    getDeviceDisplayElementCount(device) >= WIDE_DEVICE_CARD_ITEM_COUNT ? 2 : 1;
const DeviceGrid = memo(({ devices, ...itemProps }: DeviceGridProps) => {
    if (devices.length === 0) return null;
    return (
        <VirtualGrid
            className="mt-4 w-full"
            rowClassName="items-stretch"
            items={devices}
            getItemKey={getDeviceKey}
            getItemSpan={getDeviceSpan}
            minItemWidth={DEVICE_CARD_MIN_WIDTH}
            maxColumns={4}
            estimateSize={340}
            gap={DEVICE_GRID_GAP}
            overscan={3}
        >
            {(device, span) => (
                <DeviceGridItem
                    key={device.id}
                    device={device}
                    span={span}
                    online={isDeviceOnline(device)}
                    {...itemProps}
                />
            )}
        </VirtualGrid>
    );
});
const EMPTY_COMMAND_IDS: string[] = [];
const DevicePage = () => {
    const { modal, message } = App.useApp();
    const token = useAuthStore((state) => state.token);
    const { has } = usePermissions();
    const canQuery = has('iot:device:query');
    const canAdd = has('iot:device:add');
    const canManageGroup =
        has('iot:device-group:add') ||
        has('iot:device-group:edit') ||
        has('iot:device-group:delete');
    const [debugDeviceId, setDebugDeviceId] = useState<string>();
    const [searchText, setSearchText] = useState('');
    const [keyword, setKeyword] = useState('');
    const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
    const [formOpen, setFormOpen] = useState(false);
    const [editing, setEditing] = useState<Device.Overview | null>(null);
    const [sharing, setSharing] = useState<{
        kind: 'device' | 'group';
        id: string;
        name: string;
    } | null>(null);
    const [historyDevice, setHistoryDevice] = useState<Device.Overview | null>(null);
    const [commandPopoverOpen, setCommandPopoverOpen] = useState(false);
    const [commandDevice, setCommandDevice] = useState<Device.Overview | null>(null);
    const [commandFunc, setCommandFunc] = useState<Device.CommandOperation | null>(null);
    const [commandLoadingId, setCommandLoadingId] = useState<string>();
    const [submittedCommand, setSubmittedCommand] = useState<{
        ids: string[];
        complete: boolean;
        unknown?: boolean;
        unknownMessage?: string;
        tracking?: boolean;
    } | null>(null);
    const submitCommand = useDeviceCommand();
    const canTrackCommandResults =
        !!token &&
        canQuery &&
        !!submittedCommand &&
        (!submittedCommand.complete || submittedCommand.tracking === true);
    const commandIds =
        canTrackCommandResults && submittedCommand ? submittedCommand.ids : EMPTY_COMMAND_IDS;
    const commandResults = useDeviceCommandResults(
        commandIds,
        canTrackCommandResults,
        debugDeviceId
    );
    const commandPending = submitCommand.isPending;
    useEffect(() => {
        if (!submittedCommand || !token || !canQuery) return;
        if (commandResults.error) {
            if (submittedCommand.unknown) {
                if (submittedCommand.tracking) {
                    message.error(commandResults.error.message || '指令结果连接失败');
                    setSubmittedCommand({ ...submittedCommand, tracking: false });
                }
            } else {
                message.error(commandResults.error.message || '指令结果连接失败');
                setSubmittedCommand(null);
            }
            return;
        }
        if (!commandResults.data?.complete) return;
        const unknownStatuses = commandResults.data.statuses.filter(
            (status) => status.status === 'UNKNOWN'
        );
        if (unknownStatuses.length) {
            if (!submittedCommand.unknown) {
                const warning = summarizeDeviceCommandResult({
                    ...commandResults.data,
                    statuses: unknownStatuses,
                });
                setSubmittedCommand({
                    ...submittedCommand,
                    complete: true,
                    unknown: true,
                    unknownMessage: warning.message,
                    tracking: true,
                });
            }
            return;
        }
        const summary = summarizeDeviceCommandResult(commandResults.data);
        if (summary.failed) message.error(summary.message);
        else message.success(summary.message);
        setSubmittedCommand(null);
    }, [canQuery, commandResults.data, commandResults.error, message, submittedCommand, token]);
    useEffect(() => {
        if (!submittedCommand) return;
        if (!token) {
            setSubmittedCommand(null);
            return;
        }
        if (canQuery) return;
        if (submittedCommand.unknown) {
            if (submittedCommand.tracking) {
                setSubmittedCommand({ ...submittedCommand, tracking: false });
            }
            return;
        }
        setSubmittedCommand(null);
    }, [canQuery, submittedCommand, token]);
    const unknownCommandWarning = submittedCommand?.unknown ? (
        <Alert
            type="warning"
            showIcon
            closable
            message={submittedCommand.unknownMessage}
            description={
                canTrackCommandResults
                    ? '正在监听该指令的后续实时结果；请核对设备状态，勿直接重发。关闭此提示会停止跟踪。'
                    : '实时跟踪已停止；请核对设备状态，勿直接重发。'
            }
            onClose={() => setSubmittedCommand(null)}
            className="mb-3"
        />
    ) : null;
    const sendCommand = useCallback(
        (deviceId: string, command: Device.Command) => {
            if (commandPending) return;
            if (submittedCommand?.unknown) {
                message.warning('请先核对设备状态并关闭结果未知警示，再下发新指令；勿直接重发');
                return;
            }
            submitCommand.mutate(
                { deviceId, data: command },
                {
                    onSuccess: (accepted) => {
                        setSubmittedCommand({ ids: accepted.command_ids, complete: false });
                        setCommandPopoverOpen(false);
                    },
                }
            );
        },
        [commandPending, message, submittedCommand, submitCommand]
    );
    const scrollContainerRef = useRef<HTMLDivElement>(null);
    const {
        data,
        isLoading,
        error: listError,
        isFetching: isListFetching,
        refetch,
    } = useDeviceList({
        debugDeviceId,
        commandIds,
        enabled: canQuery,
        // Device metadata is stable between edits. Realtime snapshots below keep the page fresh
        // without repeatedly rebuilding and transferring the complete device list.
    });
    const {
        data: realtimeSnapshotData,
        error: realtimeError,
        isFetching: isRealtimeSnapshotFetching,
        refetch: refetchRealtimeSnapshot,
    } = useDeviceRealtimeSnapshot({
        debugDeviceId,
        commandIds,
        enabled: canQuery && !!data,
    });
    const { data: groupTree = EMPTY_GROUP_TREE, isLoading: isGroupTreeLoading } =
        useDeviceGroupTreeWithCount({ enabled: canQuery }, debugDeviceId, commandIds);
    const { data: linkOptions = EMPTY_LINK_OPTIONS } = useLinkOptions({
        enabled: canQuery,
        refetchOnWindowFocus: false,
    });
    const saveMutation = useDeviceSave();
    const { mutateAsync: deleteDevice } = useDeviceDelete();
    const loadDeviceDetail = useDeviceDetailLoader();
    const committedDevices = useRef<Device.Overview[]>([]);
    const deviceList = useMemo(() => {
        const realtimeSnapshotById = new Map(
            (realtimeSnapshotData?.list ?? []).map((device) => [device.id, device] as const)
        );
        const merged = (data?.list ?? EMPTY_DEVICE_LIST).map((device) => {
            const snapshot = realtimeSnapshotById.get(device.id);
            if (!snapshot) return device;
            return {
                ...device,
                connected: snapshot.connected,
                connectionState: snapshot.connectionState,
                runtimeState: snapshot.runtimeState,
                runtimeReason: snapshot.runtimeReason,
                reportTime: snapshot.reportTime,
                elements: snapshot.elements ?? device.elements,
                edgeStatus: snapshot.edgeStatus ?? device.edgeStatus,
            };
        });
        // Keep unchanged card props stable when another device reports a value.
        return replaceEqualDeep(committedDevices.current, merged);
    }, [data, realtimeSnapshotData]);
    useLayoutEffect(() => {
        committedDevices.current = deviceList;
    }, [deviceList]);
    const isFetching = isListFetching || isRealtimeSnapshotFetching;
    const linkById = useMemo(
        () => new Map(linkOptions.map((link) => [link.id, link])),
        [linkOptions]
    );
    const groupIndex = useMemo(() => buildGroupIndex(groupTree), [groupTree]);
    const selectedGroup = useMemo(
        () =>
            selectedGroupId && selectedGroupId !== 'ungrouped'
                ? groupIndex.get(selectedGroupId)
                : undefined,
        [groupIndex, selectedGroupId]
    );
    const selectedScope = useMemo(() => buildGroupScopeIds(selectedGroup), [selectedGroup]);
    const ungroupedCount = useMemo(
        () => deviceList.filter((device) => !device.group_id).length,
        [deviceList]
    );
    const scopedDevices = useMemo(() => {
        if (selectedGroupId === null) return deviceList;
        if (selectedGroupId === 'ungrouped') return deviceList.filter((device) => !device.group_id);
        return deviceList.filter(
            (device) => !!device.group_id && selectedScope.has(device.group_id)
        );
    }, [deviceList, selectedGroupId, selectedScope]);
    const normalizedKeyword = keyword.trim().toLowerCase();
    const scrollScopeKey = `${selectedGroupId ?? 'all'}:${normalizedKeyword}`;
    const previousScrollScopeKeyRef = useRef(scrollScopeKey);
    useLayoutEffect(() => {
        if (previousScrollScopeKeyRef.current !== scrollScopeKey && scrollContainerRef.current) {
            scrollContainerRef.current.scrollTop = 0;
        }
        previousScrollScopeKeyRef.current = scrollScopeKey;
    }, [scrollScopeKey]);
    const filteredDevices = useMemo(() => {
        if (!normalizedKeyword) return scopedDevices;
        return scopedDevices.filter((device) =>
            [device.name, device.device_code, device.protocol_name].some((value) =>
                value?.toLowerCase().includes(normalizedKeyword)
            )
        );
    }, [normalizedKeyword, scopedDevices]);
    const stats = useMemo(() => buildDeviceStats(scopedDevices), [scopedDevices]);
    const protocolStatsEntries = useMemo(
        () => Object.entries(stats.byProtocol),
        [stats.byProtocol]
    );
    const visibleDeviceMap = useMemo(() => {
        const map = new Map<string, Device.Overview[]>();
        for (const device of filteredDevices) {
            if (!device.group_id) continue;
            const devices = map.get(device.group_id) ?? [];
            devices.push(device);
            map.set(device.group_id, devices);
        }
        return map;
    }, [filteredDevices]);
    const groupStats = useMemo(
        () => buildGroupStats(groupTree, visibleDeviceMap),
        [groupTree, visibleDeviceMap]
    );
    const groupRoots = useMemo(() => {
        if (selectedGroupId === 'ungrouped') return [];
        if (selectedGroupId !== null) return selectedGroup ? [selectedGroup] : [];
        return groupTree;
    }, [groupTree, selectedGroup, selectedGroupId]);
    const ungroupedDevices = useMemo(
        () => filteredDevices.filter((device) => !device.group_id),
        [filteredDevices]
    );
    const applySearch = useCallback((value: string) => {
        setSearchText(value);
        setKeyword(value.trim());
    }, []);
    const openCreate = () => {
        setEditing(null);
        setFormOpen(true);
    };
    const openEdit = useCallback((device: Device.Overview) => {
        setEditing(device);
        setFormOpen(true);
    }, []);
    const openShare = useCallback((device: Device.Overview) => {
        setSharing({ kind: 'device', id: device.id, name: device.name });
    }, []);
    const openHistory = useCallback((device: Device.Overview) => {
        setHistoryDevice(device);
    }, []);
    const openGroupShare = useCallback((group: DeviceGroup.TreeItem) => {
        setSharing({ kind: 'group', id: group.id, name: group.name });
    }, []);
    const closeForm = () => {
        setFormOpen(false);
        setEditing(null);
    };
    const save = (values: DeviceFormValues) => {
        const parsed = saveDeviceSchema.safeParse(values);
        if (!parsed.success) {
            message.error(parsed.error.issues[0]?.message ?? '设备参数无效');
            return;
        }
        saveMutation.mutate(
            { ...values, parameters: parsed.data.parameters, id: editing?.id },
            { onSuccess: closeForm }
        );
    };
    const remove = useCallback(
        (device: Device.Overview) => {
            modal.confirm({
                title: `确认删除设备「${device.name}」吗？`,
                content: '删除后设备将停止数据采集，历史数据仍会保留。此操作不可撤销。',
                okText: '确定删除',
                okButtonProps: { danger: true },
                onOk: () => deleteDevice(device.id),
            });
        },
        [deleteDevice, modal]
    );
    const unavailable = useCallback(() => message.info('拓扑视图暂未开放'), [message]);
    const openCommandPopover = useCallback(
        async (device: Device.Overview) => {
            if (!device.can_command || device.remote_control === false) return;
            setCommandLoadingId(device.id);
            try {
                const detail = await loadDeviceDetail(device.id);
                const operations = detail.commandOperations ?? EMPTY_COMMAND_OPERATIONS;
                if (!operations.length) {
                    message.info('当前协议没有可下发要素');
                    return;
                }
                setCommandDevice({ ...device, ...detail });
                setCommandFunc(operations.length === 1 ? operations[0] : null);
                setCommandPopoverOpen(true);
            } catch {
                message.error('加载下发要素失败');
            } finally {
                setCommandLoadingId(undefined);
            }
        },
        [loadDeviceDetail, message]
    );
    const selectCommandOperation = useCallback((operation: Device.CommandOperation) => {
        setCommandFunc(operation);
    }, []);
    const closeCommandPopover = useCallback(() => {
        setCommandPopoverOpen(false);
        setCommandDevice(null);
        setCommandFunc(null);
    }, []);
    const renderDeviceCards = (devices: Device.Overview[]) => (
        <DeviceGrid
            commandIds={commandIds}
            debugDeviceId={debugDeviceId}
            onDebug={setDebugDeviceId}
            devices={devices}
            linkById={linkById}
            onHistory={openHistory}
            onShare={openShare}
            onEdit={openEdit}
            onRemove={remove}
            commandPopoverOpen={commandPopoverOpen}
            commandDeviceId={commandDevice?.id}
            commandDevice={commandDevice}
            commandFunc={commandFunc}
            commandLoadingId={commandLoadingId}
            commandPending={commandPending}
            onSubmitCommand={sendCommand}
            onOpenCommandPopover={openCommandPopover}
            onSelectCommandOperation={selectCommandOperation}
            onCloseCommandPopover={closeCommandPopover}
        />
    );
    const renderSectionStats = (sectionStats: DeviceGroupStats) => (
        <Space size={6} wrap>
            <Tag color="blue">{sectionStats.total} 个</Tag>
            {sectionStats.online > 0 && <Tag color="green">{sectionStats.online} 在线</Tag>}
            {sectionStats.offline > 0 && <Tag color="red">{sectionStats.offline} 离线</Tag>}
            {sectionStats.enabled > 0 && <Tag color="purple">{sectionStats.enabled} 已启用</Tag>}
        </Space>
    );
    const renderGroupSection = (group: DeviceGroup.TreeItem, depth = 0): ReactNode => {
        const sectionStats = groupStats.get(group.id);
        if (!sectionStats?.total) return null;
        const directDevices = visibleDeviceMap.get(group.id) ?? [];
        const visibleChildren = (group.children ?? []).filter(
            (child) => (groupStats.get(child.id)?.total ?? 0) > 0
        );
        return (
            <section
                key={group.id}
                className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4 [margin-left:var(--group-depth-offset)]"
                style={
                    {
                        '--group-depth-offset': depth > 0 ? `${depth * 16}px` : '0px',
                    } as CSSProperties
                }
            >
                <Flex justify="space-between" align="center" gap={12} wrap>
                    <div className="text-sm font-semibold text-slate-800">{group.name}</div>
                    <Space size={6} wrap>
                        {renderSectionStats(sectionStats)}
                        {group.can_share && (
                            <Tooltip title="分享整个设备分组">
                                <Button
                                    type="text"
                                    size="small"
                                    icon={<ShareAltOutlined />}
                                    onClick={() => openGroupShare(group)}
                                />
                            </Tooltip>
                        )}
                    </Space>
                </Flex>
                {directDevices.length > 0 && renderDeviceCards(directDevices)}
                {visibleChildren.length > 0 && (
                    <Space direction="vertical" className="mt-4 w-full" size="middle">
                        {visibleChildren.map((child) => renderGroupSection(child, depth + 1))}
                    </Space>
                )}
            </section>
        );
    };
    const renderUngroupedSection = (devices: Device.Overview[]) => {
        if (!devices.length) return null;
        return (
            <section className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                <Flex justify="space-between" align="center" gap={12} wrap>
                    <div className="min-w-0">
                        <div className="text-sm font-semibold text-slate-800">未分组</div>
                        <div className="mt-1 text-xs text-slate-500">
                            没有绑定设备分组的卡片会统一在这里展示
                        </div>
                    </div>
                    {renderSectionStats(buildDeviceStats(devices))}
                </Flex>
                {renderDeviceCards(devices)}
            </section>
        );
    };
    if (!canQuery) {
        return (
            <PageContainer>
                {unknownCommandWarning}
                <Result
                    status="403"
                    title="无权限"
                    subTitle="您没有查询设备列表的权限，请联系管理员"
                />
            </PageContainer>
        );
    }
    if (listError || realtimeError) {
        return (
            <PageContainer>
                {unknownCommandWarning}
                <Result
                    status="error"
                    title="设备数据加载失败"
                    subTitle={(listError ?? realtimeError)?.message}
                    extra={
                        <Button
                            onClick={() => void Promise.all([refetch(), refetchRealtimeSnapshot()])}
                        >
                            重新加载
                        </Button>
                    }
                />
            </PageContainer>
        );
    }
    return (
        <PageContainer
            header={
                <div className="flex flex-wrap items-center justify-between gap-2">
                    <h3 className="m-0 text-base font-medium">设备管理</h3>
                    <Space wrap>
                        <DeviceGroupPanel
                            groups={groupTree}
                            loading={isGroupTreeLoading}
                            selectedGroupId={selectedGroupId}
                            onSelect={setSelectedGroupId}
                            canManageGroup={canManageGroup}
                            ungroupedCount={ungroupedCount}
                            onShare={openGroupShare}
                        />
                        <Search
                            allowClear
                            enterButton
                            value={searchText}
                            placeholder="设备名称 / 编码 / 类型"
                            onChange={(event) => {
                                const value = event.target.value;
                                setSearchText(value);
                                if (!value) setKeyword('');
                            }}
                            onSearch={applySearch}
                            className="w-60"
                        />
                        <Tooltip title="拓扑视图">
                            <Button icon={<ApartmentOutlined />} onClick={unavailable} />
                        </Tooltip>
                        <Tooltip title="刷新">
                            <Button
                                icon={<ReloadOutlined />}
                                onClick={() => {
                                    void Promise.all([refetch(), refetchRealtimeSnapshot()]);
                                }}
                                loading={isFetching}
                            />
                        </Tooltip>
                        {canAdd && (
                            <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
                                新建设备
                            </Button>
                        )}
                    </Space>
                </div>
            }
        >
            {unknownCommandWarning}
            <div ref={scrollContainerRef} className="h-full overflow-y-auto overflow-x-hidden">
                <Flex gap={12} className="mb-3" wrap>
                    {[
                        {
                            label: '设备总数',
                            value: stats.total,
                            className: 'text-blue-600',
                            tag: 'blue',
                            field: 'total',
                        },
                        {
                            label: '在线设备',
                            value: stats.online,
                            className: 'text-green-600',
                            tag: 'green',
                            field: 'online',
                        },
                        {
                            label: '离线设备',
                            value: stats.offline,
                            className: 'text-red-500',
                            tag: 'red',
                            field: 'offline',
                        },
                        {
                            label: '已启用',
                            value: stats.enabled,
                            className: 'text-purple-700',
                            tag: 'purple',
                            field: 'enabled',
                        },
                    ].map((item) => (
                        <Card
                            key={item.label}
                            size="small"
                            className="min-w-[140px] flex-1"
                            classNames={{ body: 'px-4 py-3' }}
                        >
                            <Flex justify="space-between" align="center" className="mb-2.5">
                                <span className="text-[13px] text-gray-500">{item.label}</span>
                                <span className={`text-lg font-semibold ${item.className}`}>
                                    {item.value}
                                    {item.field !== 'total' && (
                                        <span className="text-[13px] font-normal text-gray-400">
                                            / {stats.total}
                                        </span>
                                    )}
                                </span>
                            </Flex>
                            <Flex gap={6} wrap>
                                {protocolStatsEntries.map(([protocol, protocolStats]) => (
                                    <Tag
                                        key={protocol}
                                        color={item.tag}
                                        className="!m-0 !px-3 !py-1 !text-sm !leading-5"
                                    >
                                        {protocol}:{' '}
                                        {protocolStats[item.field as keyof DeviceProtocolStats]}
                                        {item.field !== 'total' && `/${protocolStats.total}`}
                                    </Tag>
                                ))}
                            </Flex>
                        </Card>
                    ))}
                </Flex>

                {isLoading && filteredDevices.length === 0 ? (
                    <div className={DEVICE_CARD_GRID_CLASS}>
                        {['first', 'second', 'third', 'fourth'].map((key) => (
                            <div key={key} className="rounded-lg bg-white px-3.5 py-3">
                                <Skeleton active title paragraph={{ rows: 4 }} />
                            </div>
                        ))}
                    </div>
                ) : filteredDevices.length === 0 ? (
                    <div className="py-12">
                        <Empty
                            description={keyword ? '搜索无结果，请尝试调整关键词' : '暂无设备数据'}
                        />
                    </div>
                ) : (
                    <Space direction="vertical" className="w-full" size="large">
                        {groupRoots.length > 0 ? (
                            <>
                                {groupRoots.map((group) => renderGroupSection(group))}
                                {selectedGroupId === null &&
                                    renderUngroupedSection(ungroupedDevices)}
                            </>
                        ) : selectedGroupId === 'ungrouped' ? (
                            (renderUngroupedSection(ungroupedDevices) ?? (
                                <div className="py-12">
                                    <Empty description="暂无未分组设备" />
                                </div>
                            ))
                        ) : (
                            <section className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                                <Flex justify="space-between" align="center" gap={12} wrap>
                                    <div className="text-sm font-semibold text-slate-800">
                                        {selectedGroup?.name ?? '全部设备'}
                                    </div>
                                    {renderSectionStats(buildDeviceStats(filteredDevices))}
                                </Flex>
                                {renderDeviceCards(filteredDevices)}
                            </section>
                        )}
                    </Space>
                )}
            </div>

            <DeviceFormModal
                open={formOpen}
                editing={editing}
                loading={saveMutation.isPending}
                linkOptions={linkOptions}
                groups={groupTree}
                onCancel={closeForm}
                onFinish={save}
            />
            <DeviceShareDrawer resource={sharing} onClose={() => setSharing(null)} />
            {historyDevice && (
                <DeviceHistoryModal
                    key={historyDevice.id}
                    device={historyDevice}
                    onClose={() => setHistoryDevice(null)}
                />
            )}
        </PageContainer>
    );
};

export default DevicePage;
