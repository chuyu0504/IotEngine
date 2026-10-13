import { DownloadOutlined, HolderOutlined, UploadOutlined } from '@ant-design/icons';
import type { DragEndEvent } from '@dnd-kit/core';
import { closestCenter, DndContext, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import {
    arrayMove,
    rectSortingStrategy,
    SortableContext,
    useSortable,
    verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { useVirtualizer } from '@tanstack/react-virtual';
import {
    Alert,
    App,
    AutoComplete,
    Button,
    Card,
    Checkbox,
    Col,
    Collapse,
    Divider,
    Empty,
    Flex,
    Form,
    Input,
    InputNumber,
    Modal,
    Popconfirm,
    Result,
    Row,
    Select,
    Skeleton,
    Space,
    Switch,
    Table,
    Tabs,
    Tag,
    Tooltip,
    Tree,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { CSSProperties, ReactNode } from 'react';
import {
    forwardRef,
    memo,
    useCallback,
    useEffect,
    useImperativeHandle,
    useLayoutEffect,
    useMemo,
    useRef,
    useState,
} from 'react';
import { FormModal } from '@/components/form_modal';
import { PageContainer } from '@/components/page_container';
import { usePermissions } from '@/hooks/use_permission';
import { createUuid } from '@/utils/uuid';
import {
    checkAddressConflict,
    compileMqttTemplate,
    derivedPointSchema,
    formatMqttTemplate,
    formatTsapValue,
    industrialConfigSchema,
    mqttCommandPoints,
    mqttTemplateDraft,
    parseProtocolImport,
    validatePointExpression,
    validateTsapValue,
} from './protocol.schema';
import {
    bitOnlyAreaTypes,
    buildConnectionConfig,
    buildGroupSections,
    buildRegisterGroupSections,
    buildRemoteTsapFromRackSlot,
    defaultConfig,
    formatScaleValue,
    getConnectionFormValues,
    getDataTypeSize,
    getGroupKey,
    getModbusDeviceTypeFormValues,
    getPlcPreset,
    getS7DeviceTypeFormValues,
    getSl651DeviceTypeFormValues,
    inferConnectionMode,
    loadProtocolExportData,
    normalizeAreaTypeForPlcModel,
    normalizeGroupName,
    normalizeModbusRegisters,
    normalizePacketConfig,
    normalizeRegisterType,
    normalizeS7DataType,
    numberOrDefault,
    plcModelOptions,
    previewMqttPayload,
    protocolDerivedPoints,
    protocolPointOptions,
    reorderItemsByGroupOrder,
    reorderItemsWithinGroupOrder,
    sortSectionsByOrder,
    supportsBitAddress,
    supportsS7Decimals,
    testProtocolExpression,
    useDlt645PointCreate,
    useProtocolConfigDelete,
    useProtocolConfigImport,
    useProtocolConfigList,
    useProtocolConfigSave,
    writableAreaTypes,
} from './protocol.service';
import type {
    DerivedPoint,
    DeviceTypeFormValues,
    DeviceTypeTimingConfig,
    FormCondition,
    FormMapItem,
    IndustrialConfig,
    IndustrialPoint,
    IndustrialProtocol,
    Modbus,
    DeviceTypeModalRef as ModbusDeviceTypeModalRef,
    ModbusDictConfig,
    MqttCommandMessage,
    MqttConfig,
    MqttPoint,
    MqttPreviewResult,
    MqttTemplateDraft,
    Protocol,
    RegisterModalRef,
    S7,
    SL651,
    StoragePolicy,
} from './protocol.types';

const STORAGE_POLICY_OPTIONS: {
    value: StoragePolicy;
    label: string;
}[] = [
    { value: 'report', label: '上报时' },
    { value: 'change', label: '数据改变时' },
];

const REGISTER_TYPE_ORDER: Modbus.RegisterType[] = [
    'COIL',
    'DISCRETE_INPUT',
    'INPUT_REGISTER',
    'HOLDING_REGISTER',
];
const REGISTER_TYPE_META: Record<
    Modbus.RegisterType,
    {
        label: string;
        color: string;
        prefix: string;
        short: string;
    }
> = {
    COIL: { label: '0X - 线圈 (Coil)', color: 'green', prefix: '0X', short: '0X' },
    DISCRETE_INPUT: {
        label: '1X - 离散输入 (Discrete Input)',
        color: 'blue',
        prefix: '1X',
        short: '1X',
    },
    INPUT_REGISTER: {
        label: '3X - 输入寄存器 (Input Register)',
        color: 'cyan',
        prefix: '3X',
        short: '3X',
    },
    HOLDING_REGISTER: {
        label: '4X - 保持寄存器 (Holding Register)',
        color: 'orange',
        prefix: '4X',
        short: '4X',
    },
};
const UNKNOWN_REGISTER_TYPE_META = {
    label: '未知寄存器类型',
    color: 'default',
    prefix: '',
    short: '?',
};
const getRegisterTypeMeta = (value: unknown) => {
    const registerType = normalizeRegisterType(value);
    if (registerType) return REGISTER_TYPE_META[registerType];
    const suffix = typeof value === 'string' && value.trim() ? `（${value.trim()}）` : '';
    return { ...UNKNOWN_REGISTER_TYPE_META, label: `${UNKNOWN_REGISTER_TYPE_META.label}${suffix}` };
};
/** 编码类型列表 */
const EncodeList: SL651.EncodeType[] = ['BCD', 'TIME_YYMMDDHHMMSS', 'JPEG', 'DICT', 'HEX'];

function useProtocolImportExport(protocol: Protocol.Type) {
    const { message } = App.useApp();
    const importConfigs = useProtocolConfigImport(protocol);
    const fileInputRef = useRef<HTMLInputElement | null>(null);
    const [exporting, setExporting] = useState(false);
    const [importing, setImporting] = useState(false);
    /** 导出当前协议的所有配置 */
    const exportConfigs = useCallback(async () => {
        setExporting(true);
        try {
            const exportData = await loadProtocolExportData(protocol);
            if (!exportData.length) {
                message.warning('没有可导出的配置');
                return;
            }
            const json = JSON.stringify(exportData, null, 2);
            const blob = new Blob([json], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const date = new Date().toISOString().slice(0, 10).replace(/-/g, '');
            const a = document.createElement('a');
            a.href = url;
            a.download = `${protocol}_configs_${date}.json`;
            a.style.display = 'none';
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.setTimeout(() => URL.revokeObjectURL(url), 1000);
            message.success(`已导出 ${exportData.length} 条配置`);
        } catch (error) {
            const reason = error instanceof Error ? error.message : '未知错误';
            message.error(`导出失败：${reason}`);
        } finally {
            setExporting(false);
        }
    }, [protocol, message]);
    /** 处理导入文件 */
    const processImport = useCallback(
        async (file: File) => {
            setImporting(true);
            try {
                const text = await file.text();
                let items: Protocol.CreateDto[];
                try {
                    items = parseProtocolImport(text, protocol);
                } catch (error) {
                    message.error(error instanceof Error ? error.message : '未知错误');
                    return;
                }
                const result = await importConfigs(items);
                // 显示结果
                if (result.success === result.total) {
                    const renameInfo =
                        result.renamed.length > 0 ? `\n重命名：${result.renamed.join('、')}` : '';
                    message.success(`成功导入 ${result.success} 条配置${renameInfo}`);
                } else {
                    const failInfo = result.failed.map((f) => `${f.name}(${f.reason})`).join('、');
                    message.warning(
                        `导入完成：${result.success}/${result.total} 成功${failInfo ? `，失败：${failInfo}` : ''}`
                    );
                }
            } catch (error) {
                const reason = error instanceof Error ? error.message : '未知错误';
                message.error(`导入失败：${reason}`);
            } finally {
                setImporting(false);
                // 重置文件输入，允许再次选择同一文件
                if (fileInputRef.current) fileInputRef.current.value = '';
            }
        },
        [protocol, message, importConfigs]
    );
    /** 触发文件选择 */
    const triggerImport = useCallback(() => {
        if (!fileInputRef.current) {
            const input = document.createElement('input');
            input.type = 'file';
            input.accept = '.json';
            input.style.display = 'none';
            document.body.appendChild(input);
            fileInputRef.current = input;
        }
        fileInputRef.current.onchange = (event) => {
            const file = (event.target as HTMLInputElement).files?.[0];
            if (file) processImport(file);
        };
        fileInputRef.current.click();
    }, [processImport]);
    // 组件卸载时清理动态创建的 input 元素
    useEffect(() => {
        return () => {
            if (fileInputRef.current) {
                fileInputRef.current.remove();
                fileInputRef.current = null;
            }
        };
    }, []);
    return { exportConfigs, triggerImport, exporting, importing };
}

/**
 * Modbus 协议配置
 */
/** 寄存器类型选项 */
const RegisterTypeOptions: {
    value: Modbus.RegisterType;
    label: string;
}[] = [
    { value: 'COIL', label: '0X - 线圈 (Coil)' },
    { value: 'DISCRETE_INPUT', label: '1X - 离散输入 (Discrete Input)' },
    { value: 'INPUT_REGISTER', label: '3X - 输入寄存器 (Input Register)' },
    { value: 'HOLDING_REGISTER', label: '4X - 保持寄存器 (Holding Register)' },
];
/** 数据类型选项 */
const DataTypeOptions: {
    value: Modbus.DataType;
    label: string;
    quantity: number;
}[] = [
    { value: 'BOOL', label: 'BOOL (1 bit)', quantity: 1 },
    { value: 'INT16', label: 'INT16 (16位有符号)', quantity: 1 },
    { value: 'UINT16', label: 'UINT16 (16位无符号)', quantity: 1 },
    { value: 'INT32', label: 'INT32 (32位有符号)', quantity: 2 },
    { value: 'UINT32', label: 'UINT32 (32位无符号)', quantity: 2 },
    { value: 'FLOAT32', label: 'FLOAT32 (32位浮点)', quantity: 2 },
    { value: 'INT64', label: 'INT64 (64位有符号)', quantity: 4 },
    { value: 'UINT64', label: 'UINT64 (64位无符号)', quantity: 4 },
    { value: 'DOUBLE', label: 'DOUBLE (64位浮点)', quantity: 4 },
];
/** 字节序选项 */
const ByteOrderOptions: {
    value: Modbus.ByteOrder;
    label: string;
}[] = [
    { value: 'BIG_ENDIAN', label: 'Big-endian' },
    { value: 'LITTLE_ENDIAN', label: 'Little-endian' },
    { value: 'BIG_ENDIAN_BYTE_SWAP', label: 'Big-endian byte swap' },
    { value: 'LITTLE_ENDIAN_BYTE_SWAP', label: 'Little-endian byte swap' },
];
/** 根据数据类型获取寄存器数量 */
const getQuantityByDataType = (dataType: Modbus.DataType): number => {
    const opt = DataTypeOptions.find((o) => o.value === dataType);
    return opt?.quantity ?? 1;
};
const connectionTypeOptions: {
    value: S7.ConnectionType;
    label: string;
}[] = [
    { value: 'PG', label: 'PG' },
    { value: 'OP', label: 'OP' },
    { value: 'S7_BASIC', label: 'S7BASIC' },
];
const connectionModeOptions: {
    value: S7.ConnectionMode;
    label: string;
}[] = [
    { value: 'RACK_SLOT', label: 'Rack/Slot' },
    { value: 'TSAP', label: 'TSAP' },
];
const getConnectionModeOptions = (
    plcModel?: S7.PlcModel
): {
    value: S7.ConnectionMode;
    label: string;
}[] => (plcModel === 'S7-200' ? [{ value: 'TSAP', label: 'TSAP' }] : connectionModeOptions);
const connectionTypeTips: Record<S7.ConnectionType, string> = {
    PG: 'PG（编程）模式，常用于本地接线连接与离线调试',
    OP: 'OP（操作）模式，适合上位机常态采集场景',
    S7_BASIC: 'S7 Basic，兼容部分第三方网关的轻量模式',
};
const connectionModeTips: Record<S7.ConnectionMode, string> = {
    RACK_SLOT: '常规西门子 PLC 连接方式，适合 S7-300/400/1200/1500 等场景',
    TSAP: '直接指定本地/远端 TSAP，适合 S7-200、CP243 或需要手工指定 TSAP 的场景',
};
const getConnectionModeTip = (plcModel?: S7.PlcModel, connectionMode?: S7.ConnectionMode) => {
    if (plcModel === 'S7-200') {
        return 'S7-200 仅支持 TSAP 连接模式';
    }
    return connectionModeTips[connectionMode || 'RACK_SLOT'];
};
const getConnectionTypeLabel = (connectionType?: S7.ConnectionType) =>
    connectionTypeOptions.find((item) => item.value === connectionType)?.label || 'PG';
const getConnectionModeLabel = (connectionMode?: S7.ConnectionMode) =>
    connectionModeOptions.find((item) => item.value === connectionMode)?.label || 'Rack/Slot';
const getAreaTypeOptions = (
    plcModel?: S7.PlcModel
): {
    value: S7.AreaType;
    label: string;
}[] => {
    if (plcModel === 'S7-200') {
        return [
            { value: 'V', label: 'V（变量存储器）' },
            { value: 'MK', label: 'M（MK，标记位）' },
            { value: 'PE', label: 'I（PE，系统输入）' },
            { value: 'PA', label: 'Q（PA，系统输出）' },
            { value: 'CT', label: 'C（CT，计数器）' },
            { value: 'TM', label: 'T（TM，定时器）' },
        ];
    }
    return [
        { value: 'DB', label: 'DB（数据块）' },
        { value: 'MK', label: 'M（MK，标记位）' },
        { value: 'PE', label: 'I（PE，系统输入）' },
        { value: 'PA', label: 'Q（PA，系统输出）' },
        { value: 'CT', label: 'C（CT，计数器）' },
        { value: 'TM', label: 'T（TM，定时器）' },
    ];
};
const areaDataTypeOptions: {
    value: S7.AreaDataType;
    label: string;
}[] = [
    { value: 'BOOL', label: '布尔（BOOL）' },
    { value: 'INT8', label: '8位整型（INT8）' },
    { value: 'UINT8', label: '8位无符号整型（UINT8）' },
    { value: 'INT16', label: '16位有符号整型（INT16）' },
    { value: 'UINT16', label: '16位无符号整型（UINT16）' },
    { value: 'INT32', label: '32位有符号整型（INT32）' },
    { value: 'UINT32', label: '32位无符号整型（UINT32）' },
    { value: 'FLOAT', label: '浮点（FLOAT）' },
    { value: 'LREAL', label: '双精度（LREAL）' },
    { value: 'STRING', label: '字符串（STRING）' },
];
const getAreaDataTypeOptions = (
    areaType?: S7.AreaType
): {
    value: S7.AreaDataType;
    label: string;
}[] => {
    if (areaType === 'CT' || areaType === 'TM') {
        return areaDataTypeOptions.filter((item) => item.value === 'UINT16');
    }
    if (bitOnlyAreaTypes.includes(areaType || 'DB')) {
        return areaDataTypeOptions.filter((item) => item.value === 'BOOL');
    }
    return areaDataTypeOptions;
};
const areaAddressPrefixMap: Record<
    S7.AreaType,
    {
        bool: string;
        number: string;
    }
> = {
    DB: { bool: 'X', number: 'DB' },
    V: { bool: 'V', number: 'V' },
    MK: { bool: 'M', number: 'M' },
    PE: { bool: 'I', number: 'I' },
    PA: { bool: 'Q', number: 'Q' },
    CT: { bool: 'C', number: 'C' },
    TM: { bool: 'T', number: 'T' },
};
const areaAddressHintMap: Record<S7.AreaType, string> = {
    DB: 'DB1.DBX0.0、DB1.DBW10、DB1.DBD0、DB1.DBD1（LREAL，DBD 为4字节）',
    V: 'V0.0、VB10、VW20、VD30',
    MK: 'M0.0、MB10、MW20、MD30',
    PE: 'I0.0、IB10、IW20、ID30',
    PA: 'Q0.0、QB10、QW20、QD30',
    CT: 'C0、C1',
    TM: 'T0、T1',
};
const areaAddressTypeMap: Record<S7.AreaDataType, string> = {
    BOOL: 'X',
    INT8: 'B',
    UINT8: 'B',
    INT16: 'W',
    UINT16: 'W',
    INT32: 'D',
    UINT32: 'D',
    FLOAT: 'D',
    LREAL: 'D',
    STRING: 'B',
};
const getAreaAddressSample = (
    areaType?: S7.AreaType,
    dataType?: S7.AreaDataType,
    dbNumber?: number,
    start?: number,
    bit?: number
) => {
    if (!areaType) return '';
    const safeStart = typeof start === 'number' ? start : 0;
    const safeBit = typeof bit === 'number' ? bit : 0;
    const area = areaAddressPrefixMap[areaType];
    const suffix = areaAddressTypeMap[dataType || 'INT16'];
    if (areaType === 'CT' || areaType === 'TM') {
        return `${area.number}${safeStart}`;
    }
    if (areaType === 'DB') {
        if (!dbNumber) return '';
        if (suffix === 'X') return `DB${dbNumber}.DBX${safeStart}.${safeBit}`;
        return `DB${dbNumber}.DB${suffix}${safeStart}`;
    }
    if (areaType === 'V') {
        if (suffix === 'X') return `V${safeStart}.${safeBit}`;
        return `V${suffix}${safeStart}`;
    }
    if (suffix === 'X') return `${area.bool}${safeStart}.${safeBit}`;
    return `${area.number}${suffix}${safeStart}`;
};
const getAddressSuffixExample = (areaType?: S7.AreaType, dataType?: S7.AreaDataType) => {
    if (!areaType) return '';
    const area = areaAddressPrefixMap[areaType];
    if (areaType === 'CT' || areaType === 'TM') return '';
    const suffix = areaAddressTypeMap[dataType || 'INT16'];
    if (areaType === 'V') {
        if (suffix === 'X') return 'V0.0';
        return `V${suffix}0`;
    }
    if (suffix === 'X') return `${area.bool}X.0`;
    return `${area.number}${suffix}0`;
};
const getAddressRuleText = (areaType: S7.AreaType | undefined, isBool: boolean) => {
    if (areaType === 'DB') {
        return isBool ? '请输入 DB 位号（如 DB1.DBX0.0）' : '请输入 DB 字节偏移（如 DB1.DBW10）';
    }
    if (areaType === 'V') {
        return isBool ? '请输入 V 位地址（如 V0.0）' : '请输入 V 偏移（如 VB10 / VW20）';
    }
    if (areaType === 'CT') {
        return '请输入计数器地址（如 C0）';
    }
    if (areaType === 'TM') {
        return '请输入定时器地址（如 T0）';
    }
    return isBool ? '请输入位地址（如 M0.0/I0.1/Q1.2）' : '请输入字节偏移';
};
const getAreaAddressRangeText = (area: S7.Area) => {
    const dataType = normalizeS7DataType(area.dataType);
    const start = typeof area.start === 'number' && area.start >= 0 ? area.start : 0;
    const size =
        typeof area.size === 'number' && area.size > 0 ? area.size : getDataTypeSize(dataType) || 1;
    const end =
        dataType === 'STRING'
            ? start + Math.max(size, 1) - 1
            : dataType === 'LREAL'
              ? start + 1
              : start;
    const startBit = typeof area.startBit === 'number' ? area.startBit : 0;
    const startAddress = getAreaAddressSample(area.area, dataType, area.dbNumber, start, startBit);
    const endAddress = getAreaAddressSample(area.area, dataType, area.dbNumber, end, startBit);
    return {
        start: startAddress || '地址异常',
        end: endAddress || '地址异常',
    };
};

const useFilterableGroupOptions = (groups: string[]) => {
    const [searchText, setSearchText] = useState('');
    const [showAllOnOpen, setShowAllOnOpen] = useState(false);
    const options = useMemo(
        () =>
            groups
                .map((value) => value.trim())
                .filter(Boolean)
                .sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'))
                .map((value) => ({ value })),
        [groups]
    );
    const filteredOptions = useMemo(() => {
        if (showAllOnOpen || !searchText.trim()) {
            return options;
        }
        const keyword = searchText.trim().toLowerCase();
        return options.filter((option) => option.value.toLowerCase().includes(keyword));
    }, [options, searchText, showAllOnOpen]);
    return {
        options: filteredOptions,
        onDropdownVisibleChange: (open: boolean) => {
            setShowAllOnOpen(open);
            if (!open) {
                setSearchText('');
            }
        },
        onSearch: (value: string) => {
            setSearchText(value);
            setShowAllOnOpen(false);
        },
    };
};

const pairedFormItemClassName = 'min-w-0 flex-1';

const numericInputClassName = 'min-w-0 flex-1';

const numericUnitClassName = 'pointer-events-none !w-20 text-center';

const REGISTER_CARD_GRID_STYLE = {
    gridTemplateColumns: 'repeat(auto-fill, minmax(min(240px, 100%), 1fr))',
};

const AREA_CARD_GRID_STYLE: CSSProperties = {
    gridTemplateColumns: 'repeat(auto-fill, minmax(min(320px, 100%), 1fr))',
};

/**
 * Modbus 设备类型编辑弹窗（从 ModbusConfig 抽离）
 */
export interface DeviceTypeModalProps {
    onSuccess?: () => void;
    saveMutation: ReturnType<typeof useProtocolConfigSave>;
}
export const DeviceTypeModal = forwardRef<ModbusDeviceTypeModalRef, DeviceTypeModalProps>(
    ({ onSuccess, saveMutation }, ref) => {
        const [open, setOpen] = useState(false);
        const [mode, setMode] = useState<'create' | 'edit'>('create');
        const [current, setCurrent] = useState<Protocol.Item>();
        const [form] = Form.useForm();
        useImperativeHandle(ref, () => ({
            open(m, data) {
                setMode(m);
                setCurrent(data);
                setOpen(true);
            },
        }));
        useEffect(() => {
            if (!open) return;
            form.resetFields();
            form.setFieldsValue(getModbusDeviceTypeFormValues(current));
        }, [current, form, open]);
        const handleOk = async () => {
            const values = await form.validateFields();
            const existingConfig = (current?.config as Modbus.Config) || { registers: [] };
            const packet = normalizePacketConfig({
                mergeGap: values.packetMergeGap,
                maxQuantity: values.packetMaxQuantity,
            });
            await saveMutation.mutateAsync({
                id: current?.id,
                protocol: 'Modbus',
                name: values.name,
                enabled: values.enabled,
                config: {
                    byteOrder: values.byteOrder,
                    readInterval: values.readInterval,
                    storagePolicy: values.storagePolicy,
                    commandFastReadDuration: values.commandFastReadDuration,
                    commandFastReadInterval: values.commandFastReadInterval,
                    packet,
                    registers: normalizeModbusRegisters(existingConfig.registers),
                },
                remark: values.remark,
            });
            onSuccess?.();
            setOpen(false);
        };
        return (
            <FormModal
                title={mode === 'create' ? '新增设备类型' : '编辑设备类型'}
                open={open}
                onOk={handleOk}
                onCancel={() => setOpen(false)}
                confirmLoading={saveMutation.isPending}
                forceRender
            >
                <Form form={form} layout="vertical" initialValues={getModbusDeviceTypeFormValues()}>
                    <Divider titlePlacement="start" plain className="!my-4">
                        基础信息
                    </Divider>
                    <Flex gap={16} align="start">
                        <Form.Item
                            label="名称"
                            name="name"
                            rules={[{ required: true, message: '请输入名称' }]}
                            className={pairedFormItemClassName}
                        >
                            <Input placeholder="如：温湿度传感器、电表" maxLength={64} />
                        </Form.Item>
                        <Form.Item
                            label="字节序"
                            name="byteOrder"
                            rules={[{ required: true, message: '请选择字节序' }]}
                            extra="不同字节序将影响寄存器值解析"
                            className={pairedFormItemClassName}
                        >
                            <Select options={ByteOrderOptions} />
                        </Form.Item>
                    </Flex>
                    <Divider titlePlacement="start" plain className="!my-4">
                        采集与存储
                    </Divider>
                    <Flex gap={16} align="start">
                        <Form.Item
                            label="读取间隔（秒）"
                            extra="数值越小采集越频繁，建议按设备负载设置间隔"
                            className={pairedFormItemClassName}
                        >
                            <Space.Compact block>
                                <Form.Item name="readInterval" noStyle>
                                    <InputNumber
                                        min={1}
                                        max={3600}
                                        className={numericInputClassName}
                                    />
                                </Form.Item>
                                <Input
                                    value="秒"
                                    readOnly
                                    tabIndex={-1}
                                    className={numericUnitClassName}
                                    aria-label="单位：秒"
                                />
                            </Space.Compact>
                        </Form.Item>
                        <Form.Item
                            label="存储策略"
                            name="storagePolicy"
                            rules={[{ required: true, message: '请选择存储策略' }]}
                            extra="上报时存储每条历史数据；数据改变时仅在点位值变化时存储"
                            className={pairedFormItemClassName}
                        >
                            <Select options={STORAGE_POLICY_OPTIONS} />
                        </Form.Item>
                    </Flex>
                    <Divider titlePlacement="start" plain className="!my-4">
                        下发快读
                    </Divider>
                    <Flex gap={16} align="start">
                        <Form.Item
                            label="下发快读窗口"
                            className={pairedFormItemClassName}
                            extra="下发成功后保持快读的时长，0 表示关闭"
                        >
                            <Space.Compact block>
                                <Form.Item name="commandFastReadDuration" noStyle>
                                    <InputNumber
                                        min={0}
                                        max={3600}
                                        className={numericInputClassName}
                                    />
                                </Form.Item>
                                <Input
                                    value="秒"
                                    readOnly
                                    tabIndex={-1}
                                    className={numericUnitClassName}
                                    aria-label="单位：秒"
                                />
                            </Space.Compact>
                        </Form.Item>
                        <Form.Item
                            label="快读间隔"
                            className={pairedFormItemClassName}
                            extra="快读窗口内的读取间隔"
                        >
                            <Space.Compact block>
                                <Form.Item name="commandFastReadInterval" noStyle>
                                    <InputNumber
                                        min={1}
                                        max={60}
                                        className={numericInputClassName}
                                    />
                                </Form.Item>
                                <Input
                                    value="秒"
                                    readOnly
                                    tabIndex={-1}
                                    className={numericUnitClassName}
                                    aria-label="单位：秒"
                                />
                            </Space.Compact>
                        </Form.Item>
                    </Flex>
                    <Divider titlePlacement="start" plain className="!my-4">
                        组包参数
                    </Divider>
                    <Flex gap={16} align="start">
                        <Form.Item
                            label="组包地址间隙"
                            className={pairedFormItemClassName}
                            extra="地址间隙 <= 该值时会合并成同一读包，0 表示只合并连续地址"
                        >
                            <Space.Compact block>
                                <Form.Item name="packetMergeGap" noStyle>
                                    <InputNumber
                                        min={0}
                                        max={2000}
                                        className={numericInputClassName}
                                    />
                                </Form.Item>
                                <Input
                                    value="寄存器"
                                    readOnly
                                    tabIndex={-1}
                                    className={numericUnitClassName}
                                    aria-label="单位：寄存器"
                                />
                            </Space.Compact>
                        </Form.Item>
                        <Form.Item
                            label="单包最大寄存器数"
                            className={pairedFormItemClassName}
                            extra="每个读包最多读取的字寄存器数量"
                        >
                            <Space.Compact block>
                                <Form.Item name="packetMaxQuantity" noStyle>
                                    <InputNumber
                                        min={1}
                                        max={125}
                                        className={numericInputClassName}
                                    />
                                </Form.Item>
                                <Input
                                    value="个"
                                    readOnly
                                    tabIndex={-1}
                                    className={numericUnitClassName}
                                    aria-label="单位：个"
                                />
                            </Space.Compact>
                        </Form.Item>
                    </Flex>
                    <Divider titlePlacement="start" plain className="!my-4">
                        其他
                    </Divider>
                    <Form.Item label="备注" name="remark">
                        <Input.TextArea rows={3} placeholder="备注说明" />
                    </Form.Item>
                    <Form.Item label="启用" name="enabled" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                </Form>
            </FormModal>
        );
    }
);

/**
 * Modbus 寄存器编辑弹窗（从 ModbusConfig 抽离）
 */
export interface RegisterModalProps {
    types: Protocol.Item[];
    onSuccess?: () => void;
    saveMutation: ReturnType<typeof useProtocolConfigSave>;
}
export const RegisterModal = forwardRef<RegisterModalRef, RegisterModalProps>(
    ({ types, onSuccess, saveMutation }, ref) => {
        const [open, setOpen] = useState(false);
        const [mode, setMode] = useState<'create' | 'edit'>('create');
        const [typeId, setTypeId] = useState<string>();
        const [current, setCurrent] = useState<Modbus.Register>();
        const [form] = Form.useForm();
        // 监听寄存器类型和数据类型变化
        const registerType = Form.useWatch('registerType', form);
        const dataType = Form.useWatch('dataType', form);
        const groupNames = useMemo(() => {
            const currentType = types.find((t) => t.id === typeId);
            const config = currentType?.config as Modbus.Config | undefined;
            const groups = new Set<string>();
            for (const register of config?.registers || []) {
                const group = normalizeGroupName(register.group);
                if (group) groups.add(group);
            }
            const currentGroup = normalizeGroupName(current?.group);
            if (currentGroup) groups.add(currentGroup);
            return Array.from(groups);
        }, [current?.group, typeId, types]);
        const groupOptions = useFilterableGroupOptions(groupNames);
        useImperativeHandle(ref, () => ({
            open(m, t, register) {
                setMode(m);
                setTypeId(t);
                setCurrent(register);
                form.resetFields();
                if (register) {
                    form.setFieldsValue({
                        ...register,
                        group: normalizeGroupName(register.group) || undefined,
                        scale: typeof register.scale === 'number' ? register.scale : 1,
                        boolLabel0: register.dictConfig?.items?.find((i) => i.key === '0')?.label,
                        boolLabel1: register.dictConfig?.items?.find((i) => i.key === '1')?.label,
                    });
                } else {
                    form.setFieldsValue({
                        registerType: 'HOLDING_REGISTER',
                        dataType: 'INT16',
                        writable: false,
                        scale: 1,
                    });
                }
                form.setFieldValue(
                    'visible',
                    !register ||
                        (types.find((item) => item.id === t)?.config as DeviceTypeTimingConfig)
                            ?.pointVisibility?.[register.id] !== false
                );
                setOpen(true);
            },
        }));
        const handleOk = async () => {
            if (!typeId) return;
            const values = await form.validateFields();
            const type = types.find((t) => t.id === typeId);
            if (!type) return;
            const config = type.config as Modbus.Config;
            const registers = normalizeModbusRegisters(config.registers);
            const actualQuantity = getQuantityByDataType(values.dataType);
            // 检查地址 + 数量是否溢出 uint16 范围
            if (values.address + actualQuantity - 1 > 65535) {
                Modal.error({
                    title: '地址溢出',
                    content: `地址 ${values.address} + 数量 ${actualQuantity} 超出范围（末地址不能超过 65535）`,
                });
                return;
            }
            // 检查地址冲突
            const conflictCheck = checkAddressConflict(
                registers,
                {
                    registerType: values.registerType,
                    address: values.address,
                    quantity: actualQuantity,
                },
                mode === 'edit' ? current?.id : undefined
            );
            if (conflictCheck.conflict) {
                const reg = conflictCheck.conflictWith;
                if (!reg) return;
                Modal.error({
                    title: '地址冲突',
                    content: `与寄存器「${reg.name}」地址范围冲突 (地址 ${reg.address}-${reg.address + reg.quantity - 1})`,
                });
                return;
            }
            // 构建 dictConfig（Bool 值映射；非 BOOL 类型保留已有配置）
            let dictConfig: ModbusDictConfig | undefined;
            if (values.dataType === 'BOOL') {
                dictConfig =
                    values.boolLabel0 || values.boolLabel1
                        ? {
                              items: [
                                  ...(values.boolLabel0
                                      ? [{ key: '0', label: values.boolLabel0 }]
                                      : []),
                                  ...(values.boolLabel1
                                      ? [{ key: '1', label: values.boolLabel1 }]
                                      : []),
                              ],
                          }
                        : undefined;
            } else {
                // 非 BOOL 类型：编辑时保留已有 dictConfig，创建时无
                dictConfig = mode === 'edit' ? current?.dictConfig : undefined;
            }
            // 仅 COIL/HOLDING_REGISTER 可配置 writable，其他类型始终 false
            const isWritableType =
                values.registerType === 'COIL' || values.registerType === 'HOLDING_REGISTER';
            const writable = isWritableType ? !!values.writable : false;
            const inputScale = Number(values.scale);
            const scale =
                values.registerType === 'COIL' || values.registerType === 'DISCRETE_INPUT'
                    ? 1
                    : Number.isFinite(inputScale) && inputScale > 0
                      ? inputScale
                      : 1;
            const group = normalizeGroupName(values.group);
            const registerFields = {
                name: values.name,
                group: group || undefined,
                registerType: values.registerType,
                address: values.address,
                dataType: values.dataType,
                byteOrder:
                    values.registerType === 'HOLDING_REGISTER' ||
                    values.registerType === 'INPUT_REGISTER'
                        ? values.byteOrder || undefined
                        : undefined,
                quantity: actualQuantity,
                writable,
                unit: values.unit,
                scale,
                decimals: values.decimals,
                dictConfig,
                remark: values.remark,
            };
            const pointId = mode === 'create' ? createUuid() : current?.id;
            if (!pointId) return;
            let newRegisters: Modbus.Register[];
            if (mode === 'create') {
                newRegisters = [...registers, { id: pointId, ...registerFields }];
            } else {
                newRegisters = registers.map((r) =>
                    r.id === current?.id ? { ...r, ...registerFields } : r
                );
            }
            await saveMutation.mutateAsync({
                id: typeId,
                protocol: 'Modbus',
                config: {
                    byteOrder: config.byteOrder,
                    readInterval: config.readInterval,
                    packet: normalizePacketConfig(config.packet),
                    registers: newRegisters,
                    pointVisibility: { ...config.pointVisibility, [pointId]: values.visible },
                },
            });
            onSuccess?.();
            setOpen(false);
        };
        // 线圈/离散输入只支持 BOOL；保持寄存器/输入寄存器不支持 BOOL
        const isBitRegister = registerType === 'COIL' || registerType === 'DISCRETE_INPUT';
        const isWordRegister =
            registerType === 'HOLDING_REGISTER' || registerType === 'INPUT_REGISTER';
        return (
            <FormModal
                title={mode === 'create' ? '新增寄存器' : '编辑寄存器'}
                open={open}
                onOk={handleOk}
                onCancel={() => setOpen(false)}
                confirmLoading={saveMutation.isPending}
                forceRender
            >
                <Form form={form} layout="vertical">
                    <PointDisplayField />
                    <Form.Item
                        label="名称"
                        name="name"
                        rules={[{ required: true, message: '请输入名称' }]}
                    >
                        <Input placeholder="如：温度、湿度、电压" />
                    </Form.Item>

                    <Form.Item
                        label="分组"
                        name="group"
                        extra="同一分组的寄存器会在配置页聚合为同一组卡片，留空则显示在未分组中"
                    >
                        <AutoComplete
                            allowClear
                            options={groupOptions.options}
                            placeholder="例如：基础信息、告警、控制"
                            filterOption={false}
                            onDropdownVisibleChange={groupOptions.onDropdownVisibleChange}
                            onSearch={groupOptions.onSearch}
                        />
                    </Form.Item>

                    <Flex gap={16}>
                        <Form.Item
                            label="寄存器类型"
                            name="registerType"
                            rules={[{ required: true, message: '请选择寄存器类型' }]}
                            className="flex-1"
                        >
                            <Select
                                options={RegisterTypeOptions}
                                onChange={(val) => {
                                    if (val === 'COIL' || val === 'DISCRETE_INPUT') {
                                        form.setFieldsValue({
                                            dataType: 'BOOL',
                                            decimals: undefined,
                                            writable: false,
                                            scale: 1,
                                            byteOrder: undefined,
                                        });
                                    } else if (form.getFieldValue('dataType') === 'BOOL') {
                                        form.setFieldsValue({
                                            dataType: 'INT16',
                                            decimals: undefined,
                                            boolLabel0: undefined,
                                            boolLabel1: undefined,
                                        });
                                    }
                                    // 切换到只读类型时清除 writable
                                    if (val === 'DISCRETE_INPUT' || val === 'INPUT_REGISTER') {
                                        form.setFieldsValue({ writable: false });
                                    }
                                }}
                            />
                        </Form.Item>
                        <Form.Item
                            label="地址"
                            name="address"
                            rules={[{ required: true, message: '请输入地址' }]}
                            className="flex-1"
                        >
                            <InputNumber
                                min={0}
                                max={65535}
                                className="!w-full"
                                placeholder="0-65535"
                            />
                        </Form.Item>
                    </Flex>

                    <Form.Item
                        label="数据类型"
                        name="dataType"
                        rules={[{ required: true, message: '请选择数据类型' }]}
                    >
                        <Select
                            options={
                                isBitRegister
                                    ? [
                                          {
                                              value: 'BOOL' as const,
                                              label: 'BOOL (1 bit)',
                                              quantity: 1,
                                          },
                                      ]
                                    : isWordRegister
                                      ? DataTypeOptions.filter((o) => o.value !== 'BOOL')
                                      : DataTypeOptions
                            }
                            disabled={isBitRegister}
                            onChange={() => {
                                form.setFieldsValue({
                                    decimals: undefined,
                                    boolLabel0: undefined,
                                    boolLabel1: undefined,
                                });
                            }}
                        />
                    </Form.Item>

                    {isWordRegister && (
                        <Form.Item
                            label="字节序"
                            name="byteOrder"
                            extra="留空时继承设备配置；选择后仅覆盖当前寄存器的读写字节序"
                        >
                            <Select
                                allowClear
                                options={ByteOrderOptions}
                                placeholder="继承设备配置"
                            />
                        </Form.Item>
                    )}

                    {dataType === 'BOOL' && (
                        <Flex gap={16}>
                            <Form.Item label="0 值显示" name="boolLabel0" className="flex-1">
                                <Input placeholder="如：关闭、OFF" />
                            </Form.Item>
                            <Form.Item label="1 值显示" name="boolLabel1" className="flex-1">
                                <Input placeholder="如：开启、ON" />
                            </Form.Item>
                        </Flex>
                    )}

                    <Flex gap={16}>
                        <Form.Item label="单位" name="unit" className="flex-1">
                            <Input placeholder="如：V、A、℃、%" />
                        </Form.Item>
                        <Form.Item
                            label="缩放系数"
                            name="scale"
                            className="flex-1"
                            extra="入库值 = 原始值 × 缩放系数（默认 1）"
                            rules={[
                                {
                                    validator: async (_, value) => {
                                        const numericValue = Number(value);
                                        if (!Number.isFinite(numericValue) || numericValue <= 0) {
                                            throw new Error('请输入大于 0 的缩放系数');
                                        }
                                    },
                                },
                            ]}
                        >
                            <InputNumber
                                min={0.000001 as number}
                                max={1000000 as number}
                                step={0.1}
                                precision={6}
                                formatter={(value) => formatScaleValue(value)}
                                parser={(value) => Number(value || 1)}
                                className="!w-full"
                                disabled={isBitRegister}
                            />
                        </Form.Item>
                        {(dataType === 'FLOAT32' || dataType === 'DOUBLE') && (
                            <Form.Item label="小数位数" name="decimals" className="flex-1">
                                <InputNumber
                                    min={0}
                                    max={8}
                                    placeholder="不限制"
                                    className="!w-full"
                                />
                            </Form.Item>
                        )}
                    </Flex>

                    {(registerType === 'COIL' || registerType === 'HOLDING_REGISTER') && (
                        <Form.Item
                            label="可写"
                            name="writable"
                            valuePropName="checked"
                            extra="开启后该寄存器可用于写操作下发"
                        >
                            <Switch />
                        </Form.Item>
                    )}

                    <Form.Item label="备注" name="remark">
                        <Input placeholder="备注信息" />
                    </Form.Item>
                </Form>
            </FormModal>
        );
    }
);

interface SortableGroupSectionFrameProps {
    id: string;
    title: ReactNode;
    description?: ReactNode;
    meta?: ReactNode;
    actions?: ReactNode;
    children: ReactNode;
    disabled?: boolean;
    className?: string;
    bodyClassName?: string;
    style?: CSSProperties;
}
export const SortableGroupSectionFrame = memo(
    ({
        id,
        title,
        description,
        meta,
        actions,
        children,
        disabled = false,
        className,
        bodyClassName,
        style,
    }: SortableGroupSectionFrameProps) => {
        const { attributes, listeners, setNodeRef, transform, transition, isDragging } =
            useSortable({
                id,
                disabled,
            });
        return (
            <section
                ref={setNodeRef}
                className={className}
                style={{
                    transform: CSS.Transform.toString(transform),
                    transition: transition || undefined,
                    opacity: isDragging ? 0.75 : 1,
                    zIndex: isDragging ? 1 : undefined,
                    ...style,
                }}
            >
                <Flex justify="space-between" align="center" gap={12} wrap className="mb-3">
                    <div className="min-w-0">
                        <div className="text-sm font-semibold text-slate-800">{title}</div>
                        {description ? (
                            <div className="mt-1 text-xs text-slate-500">{description}</div>
                        ) : null}
                    </div>

                    <Space size={6} wrap>
                        {meta}
                        {actions}
                        <Button
                            type="text"
                            size="small"
                            icon={<HolderOutlined />}
                            disabled={disabled}
                            aria-label="拖拽排序"
                            title="拖拽排序"
                            className="!cursor-grab active:!cursor-grabbing"
                            style={{ touchAction: 'none' }}
                            {...attributes}
                            {...listeners}
                        />
                    </Space>
                </Flex>

                <div className={bodyClassName}>{children}</div>
            </section>
        );
    }
);
interface SortableGroupItemFrameProps {
    id: string;
    children: (dragHandle: ReactNode) => ReactNode;
    disabled?: boolean;
    className?: string;
    style?: CSSProperties;
}
export const SortableGroupItemFrame = memo(
    ({ id, children, disabled = false, className, style }: SortableGroupItemFrameProps) => {
        const { attributes, listeners, setNodeRef, transform, transition, isDragging } =
            useSortable({
                id,
                disabled,
            });
        const dragHandle = (
            <Button
                type="text"
                size="small"
                icon={<HolderOutlined />}
                disabled={disabled}
                aria-label="拖拽排序"
                title="拖拽排序"
                className="!cursor-grab active:!cursor-grabbing"
                style={{ touchAction: 'none' }}
                {...attributes}
                {...listeners}
            />
        );
        return (
            <div
                ref={setNodeRef}
                className={className}
                style={{
                    transform: CSS.Transform.toString(transform),
                    transition: transition || undefined,
                    opacity: isDragging ? 0.75 : 1,
                    zIndex: isDragging ? 1 : undefined,
                    ...style,
                }}
            >
                {children(dragHandle)}
            </div>
        );
    }
);
interface SortableGroupSectionListProps<
    T extends {
        key: string;
    },
> {
    sections: T[];
    children: (section: T) => ReactNode;
    empty?: ReactNode;
    className?: string;
    disabled?: boolean;
    onOrderChange?: (nextOrder: string[]) => void;
}
const areOrdersEqual = (left: string[], right: string[]) =>
    left.length === right.length && left.every((value, index) => value === right[index]);
const reconcileOrder = (previousOrder: string[], currentKeys: string[]) => {
    if (currentKeys.length === 0) return [];
    if (previousOrder.length === 0) return currentKeys;
    const currentKeySet = new Set(currentKeys);
    const filtered = previousOrder.filter((key) => currentKeySet.has(key));
    const missing = currentKeys.filter((key) => !filtered.includes(key));
    if (filtered.length === 0) return currentKeys;
    const nextOrder = [...filtered, ...missing];
    return areOrdersEqual(previousOrder, nextOrder) ? previousOrder : nextOrder;
};
const findVerticalScrollParent = (element: HTMLElement) => {
    let parent = element.parentElement;
    while (parent) {
        const overflowY = window.getComputedStyle(parent).overflowY;
        if (overflowY === 'auto' || overflowY === 'scroll') return parent;
        parent = parent.parentElement;
    }
    return null;
};
interface VirtualStackProps<T> {
    items: T[];
    getItemKey: (item: T) => string;
    children: (item: T, index: number) => ReactNode;
    className?: string;
    estimateSize?: number;
    gap?: number;
    overscan?: number;
}
export const VirtualStack = <T,>({
    items,
    getItemKey,
    children,
    className,
    estimateSize = 320,
    gap = 16,
    overscan = 4,
}: VirtualStackProps<T>) => {
    const rootRef = useRef<HTMLDivElement>(null);
    const [scrollElement, setScrollElement] = useState<HTMLElement | null>(null);
    const [scrollMargin, setScrollMargin] = useState(0);
    useLayoutEffect(() => {
        if (!rootRef.current) return;
        setScrollElement(findVerticalScrollParent(rootRef.current));
    }, []);
    const updateScrollMargin = useCallback(() => {
        const root = rootRef.current;
        if (!root || !scrollElement) return;
        const rootRect = root.getBoundingClientRect();
        const scrollRect = scrollElement.getBoundingClientRect();
        const nextMargin = rootRect.top - scrollRect.top + scrollElement.scrollTop;
        setScrollMargin((currentMargin) =>
            Math.abs(currentMargin - nextMargin) < 0.5 ? currentMargin : nextMargin
        );
    }, [scrollElement]);
    useLayoutEffect(() => {
        const root = rootRef.current;
        if (!root || !scrollElement) return;
        updateScrollMargin();
        const observer = new ResizeObserver(updateScrollMargin);
        observer.observe(scrollElement);
        let ancestor = root.parentElement;
        while (ancestor && ancestor !== scrollElement) {
            observer.observe(ancestor);
            ancestor = ancestor.parentElement;
        }
        window.addEventListener('resize', updateScrollMargin);
        return () => {
            observer.disconnect();
            window.removeEventListener('resize', updateScrollMargin);
        };
    }, [scrollElement, updateScrollMargin]);
    const virtualizer = useVirtualizer<HTMLElement, HTMLDivElement>({
        count: items.length,
        getScrollElement: () => scrollElement,
        estimateSize: () => estimateSize,
        getItemKey: (index) => getItemKey(items[index]),
        overscan,
        scrollMargin,
    });
    return (
        <div className={className}>
            <div
                ref={rootRef}
                className="relative w-full"
                style={{ height: virtualizer.getTotalSize() }}
            >
                {virtualizer.getVirtualItems().map((virtualItem) => (
                    <div
                        key={virtualItem.key}
                        ref={virtualizer.measureElement}
                        data-index={virtualItem.index}
                        className="absolute left-0 top-0 w-full"
                        style={{
                            transform: `translateY(${virtualItem.start - scrollMargin}px)`,
                            paddingBottom: gap,
                        }}
                    >
                        {children(items[virtualItem.index], virtualItem.index)}
                    </div>
                ))}
            </div>
        </div>
    );
};
interface VirtualGridProps<T> extends VirtualStackProps<T> {
    gridClassName?: string;
    gridStyle?: CSSProperties;
    minItemWidth?: number;
}
const VirtualGrid = <T,>({
    items,
    getItemKey,
    children,
    className,
    gridClassName,
    gridStyle,
    minItemWidth = 280,
    estimateSize = 220,
    gap = 12,
    overscan,
}: VirtualGridProps<T>) => {
    const widthRef = useRef<HTMLDivElement>(null);
    const [columnCount, setColumnCount] = useState(1);
    useLayoutEffect(() => {
        const root = widthRef.current;
        if (!root) return;
        const updateColumnCount = () => {
            const width = root.getBoundingClientRect().width;
            const nextCount = Math.max(1, Math.floor((width + gap) / (minItemWidth + gap)));
            setColumnCount((currentCount) =>
                currentCount === nextCount ? currentCount : nextCount
            );
        };
        updateColumnCount();
        const observer = new ResizeObserver(updateColumnCount);
        observer.observe(root);
        return () => observer.disconnect();
    }, [gap, minItemWidth]);
    const rows = useMemo(() => {
        const nextRows: T[][] = [];
        for (let index = 0; index < items.length; index += columnCount) {
            nextRows.push(items.slice(index, index + columnCount));
        }
        return nextRows;
    }, [columnCount, items]);
    return (
        <div ref={widthRef} className={className}>
            <VirtualStack
                items={rows}
                getItemKey={(row) => row.map(getItemKey).join(':')}
                estimateSize={estimateSize}
                gap={gap}
                overscan={overscan}
            >
                {(row) => (
                    <div className={gridClassName} style={gridStyle}>
                        {row.map((item) => children(item, items.indexOf(item)))}
                    </div>
                )}
            </VirtualStack>
        </div>
    );
};
export const SortableGroupSectionList = <
    T extends {
        key: string;
    },
>({
    sections,
    children,
    empty,
    className,
    disabled = false,
    onOrderChange,
}: SortableGroupSectionListProps<T>) => {
    const currentKeys = useMemo(() => sections.map((section) => section.key), [sections]);
    const [order, setOrder] = useState<string[]>(currentKeys);
    useEffect(() => {
        setOrder((previousOrder) => reconcileOrder(previousOrder, currentKeys));
    }, [currentKeys]);
    const orderedSections = useMemo(() => sortSectionsByOrder(sections, order), [sections, order]);
    const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 8 } }));
    const handleDragEnd = useCallback(
        ({ active, over }: DragEndEvent) => {
            if (!over || active.id === over.id) return;
            const activeKey = String(active.id);
            const overKey = String(over.id);
            const activeIndex = order.indexOf(activeKey);
            const overIndex = order.indexOf(overKey);
            if (activeIndex < 0 || overIndex < 0) return;
            const nextOrder = arrayMove(order, activeIndex, overIndex);
            setOrder(nextOrder);
            onOrderChange?.(nextOrder);
        },
        [onOrderChange, order]
    );
    if (orderedSections.length === 0) {
        return empty ?? null;
    }
    return (
        <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={disabled ? undefined : handleDragEnd}
        >
            <SortableContext
                items={orderedSections.map((section) => section.key)}
                strategy={verticalListSortingStrategy}
            >
                <VirtualStack
                    items={orderedSections}
                    getItemKey={(section) => section.key}
                    className={className ?? 'w-full'}
                    estimateSize={420}
                    gap={24}
                >
                    {children}
                </VirtualStack>
            </SortableContext>
        </DndContext>
    );
};
interface SortableGroupItemListProps<
    T extends {
        id: string;
    },
> {
    items: T[];
    children: (item: T, dragHandle: ReactNode) => ReactNode;
    empty?: ReactNode;
    className?: string;
    style?: CSSProperties;
    minItemWidth?: number;
    disabled?: boolean;
    onOrderChange?: (nextOrder: string[]) => void;
}
const reconcileItemOrder = (previousOrder: string[], currentIds: string[]) => {
    if (currentIds.length === 0) return [];
    if (previousOrder.length === 0) return currentIds;
    const currentIdSet = new Set(currentIds);
    const filtered = previousOrder.filter((id) => currentIdSet.has(id));
    const missing = currentIds.filter((id) => !filtered.includes(id));
    if (filtered.length === 0) return currentIds;
    const nextOrder = [...filtered, ...missing];
    return areOrdersEqual(previousOrder, nextOrder) ? previousOrder : nextOrder;
};
export const SortableGroupItemList = <
    T extends {
        id: string;
    },
>({
    items,
    children,
    empty,
    className,
    style,
    minItemWidth = 280,
    disabled = false,
    onOrderChange,
}: SortableGroupItemListProps<T>) => {
    const currentIds = useMemo(() => items.map((item) => item.id), [items]);
    const [order, setOrder] = useState<string[]>(currentIds);
    useEffect(() => {
        setOrder((previousOrder) => reconcileItemOrder(previousOrder, currentIds));
    }, [currentIds]);
    const orderedItems = useMemo(() => {
        const orderIndex = new Map<string, number>(order.map((id, index) => [id, index]));
        const originalIndex = new Map<string, number>(items.map((item, index) => [item.id, index]));
        return [...items].sort((left, right) => {
            const leftOrder = orderIndex.get(left.id);
            const rightOrder = orderIndex.get(right.id);
            if (leftOrder !== undefined && rightOrder !== undefined) {
                return leftOrder - rightOrder;
            }
            if (leftOrder !== undefined) return -1;
            if (rightOrder !== undefined) return 1;
            return (originalIndex.get(left.id) ?? 0) - (originalIndex.get(right.id) ?? 0);
        });
    }, [items, order]);
    const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 8 } }));
    const handleDragEnd = useCallback(
        ({ active, over }: DragEndEvent) => {
            if (!over || active.id === over.id) return;
            const activeId = String(active.id);
            const overId = String(over.id);
            const activeIndex = order.indexOf(activeId);
            const overIndex = order.indexOf(overId);
            if (activeIndex < 0 || overIndex < 0) return;
            const nextOrder = arrayMove(order, activeIndex, overIndex);
            setOrder(nextOrder);
            onOrderChange?.(nextOrder);
        },
        [onOrderChange, order]
    );
    if (orderedItems.length === 0) {
        return empty ?? null;
    }
    return (
        <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={disabled ? undefined : handleDragEnd}
        >
            <SortableContext
                items={orderedItems.map((item) => item.id)}
                strategy={rectSortingStrategy}
            >
                <VirtualGrid
                    items={orderedItems}
                    getItemKey={(item) => item.id}
                    className="w-full"
                    gridClassName={className}
                    gridStyle={style}
                    minItemWidth={minItemWidth}
                    estimateSize={220}
                    gap={12}
                >
                    {(item) => (
                        <SortableGroupItemFrame
                            key={item.id}
                            id={item.id}
                            disabled={disabled}
                            className="h-full"
                        >
                            {(dragHandle) => children(item, dragHandle)}
                        </SortableGroupItemFrame>
                    )}
                </VirtualGrid>
            </SortableContext>
        </DndContext>
    );
};

/**
 * Modbus 协议配置页面
 * 布局：左侧设备类型列表 + 右侧寄存器配置
 */
const ModbusConfigPage = () => {
    const derivedEditor = useRef<DerivedPointsEditorRef>(null);
    // 权限检查
    const { has } = usePermissions();
    const canQuery = has('iot:protocol:query');
    const canAdd = has('iot:protocol:add');
    const canEdit = has('iot:protocol:edit');
    const canDelete = has('iot:protocol:delete');
    const canImport = has('iot:protocol:import') && canAdd;
    const canExport = has('iot:protocol:export');
    // 设备类型列表查询
    const {
        data: configList,
        isLoading: loadingTypes,
        refetch: refetchTypes,
    } = useProtocolConfigList({ protocol: 'Modbus' }, { enabled: canQuery });
    // 保存和删除 mutations
    const saveMutation = useProtocolConfigSave();
    const deleteMutation = useProtocolConfigDelete();
    // 导入导出
    const { exportConfigs, triggerImport, exporting, importing } =
        useProtocolImportExport('Modbus');
    // 当前选中的设备类型 ID（用户手动选择）
    const [selectedTypeId, setSelectedTypeId] = useState<string>();
    // Modal refs
    const deviceTypeModalRef = useRef<ModbusDeviceTypeModalRef>(null);
    const registerModalRef = useRef<RegisterModalRef>(null);
    // 设备类型列表（使用 useMemo 保持引用稳定）
    const types = useMemo(() => configList ?? [], [configList]);
    const emptyTypeDesc = types.length ? '未选择设备类型' : '暂无设备类型';
    // 计算当前激活的类型 ID：优先用户选择，否则默认第一个
    const activeTypeId = useMemo(() => {
        if (selectedTypeId && types.some((t) => t.id === selectedTypeId)) {
            return selectedTypeId;
        }
        return types.length > 0 ? types[0].id : undefined;
    }, [selectedTypeId, types]);
    // 当前激活的设备类型
    const activeType = useMemo(() => {
        return types.find((t) => t.id === activeTypeId);
    }, [activeTypeId, types]);
    // 寄存器列表（派生状态，根据 activeTypeId 计算）
    const registers = useMemo<Modbus.Register[]>(() => {
        if (!activeType) return [];
        const config = activeType.config as Modbus.Config;
        return normalizeModbusRegisters(config?.registers);
    }, [activeType]);
    const registerGroups = useMemo(() => buildRegisterGroupSections(registers), [registers]);
    const writableRegisterCount = useMemo(
        () => registers.filter((register) => register.writable).length,
        [registers]
    );
    const saveRegisterConfig = useCallback(
        async (nextRegisters: Modbus.Register[]) => {
            if (!activeTypeId || !activeType) return;
            const config = activeType.config as Modbus.Config;
            if (nextRegisters.length === registers.length) {
                const isSameOrder =
                    nextRegisters.every(
                        (register, index) => register.id === registers[index]?.id
                    ) &&
                    nextRegisters.every(
                        (register, index) =>
                            getGroupKey(register.group) === getGroupKey(registers[index]?.group)
                    );
                if (isSameOrder) return;
            }
            await saveMutation.mutateAsync({
                id: activeTypeId,
                protocol: 'Modbus',
                config: {
                    byteOrder: config.byteOrder,
                    readInterval: numberOrDefault(config.readInterval, 300),
                    packet: normalizePacketConfig(config.packet),
                    registers: nextRegisters,
                },
            });
            await refetchTypes();
        },
        [activeType, activeTypeId, refetchTypes, registers, saveMutation]
    );
    const handleRegisterGroupOrderChange = useCallback(
        async (nextOrder: string[]) => {
            await saveRegisterConfig(reorderItemsByGroupOrder(registers, nextOrder));
        },
        [registers, saveRegisterConfig]
    );
    const handleRegisterItemOrderChange = useCallback(
        async (groupKey: string, nextOrder: string[]) => {
            await saveRegisterConfig(reorderItemsWithinGroupOrder(registers, groupKey, nextOrder));
        },
        [registers, saveRegisterConfig]
    );
    // 加载状态（与数据加载状态同步）
    const loadingRegisters = loadingTypes;
    // ========== 设备类型操作 ==========
    const handleDeleteDeviceType = async () => {
        if (!activeTypeId) return;
        // 删除前记录下一个可选中的类型
        const idx = types.findIndex((t) => t.id === activeTypeId);
        const nextType = types[idx + 1] ?? types[idx - 1];
        await deleteMutation.mutateAsync(activeTypeId);
        setSelectedTypeId(nextType?.id);
    };
    // ========== 寄存器操作 ==========
    const handleDeleteRegister = async (registerId: string) => {
        if (!activeTypeId || !activeType) return;
        const config = activeType.config as Modbus.Config;
        const newConfig: Modbus.Config = {
            byteOrder: config.byteOrder,
            readInterval: numberOrDefault(config.readInterval, 300),
            packet: normalizePacketConfig(config.packet),
            registers: registers.filter((register) => register.id !== registerId),
        };
        await saveMutation.mutateAsync({
            id: activeTypeId,
            protocol: 'Modbus',
            config: newConfig,
        });
    };
    const renderRegisterCard = (register: Modbus.Register, dragHandle?: ReactNode) => {
        const meta = getRegisterTypeMeta(register.registerType);
        const typeLabel = RegisterTypeOptions.find(
            (opt) => opt.value === register.registerType
        )?.label;
        const dataTypeLabel = DataTypeOptions.find((opt) => opt.value === register.dataType)?.label;
        const byteOrderLabel = ByteOrderOptions.find(
            (opt) => opt.value === register.byteOrder
        )?.label;
        const isWritableType =
            register.registerType === 'COIL' || register.registerType === 'HOLDING_REGISTER';
        const addressLabel = `${meta.prefix}${register.address}`;
        return (
            <Card
                key={register.id}
                size="small"
                hoverable
                className="h-full border-slate-200 shadow-[0_1px_4px_rgba(15,23,42,0.06)]"
                styles={{ body: { padding: 12 } }}
            >
                <Flex justify="space-between" gap={12} align="start" className="mb-2">
                    <div className="min-w-0 flex-1">
                        <div className="truncate text-sm font-semibold text-slate-800">
                            {register.name}
                        </div>
                        <div className="mt-0.5 text-[12px] text-slate-400">地址 {addressLabel}</div>
                    </div>
                    <Space size={4} className="shrink-0">
                        {dragHandle}
                        {canEdit && (
                            <Button
                                size="small"
                                type="link"
                                onClick={() => {
                                    if (activeTypeId)
                                        registerModalRef.current?.open(
                                            'edit',
                                            activeTypeId,
                                            register
                                        );
                                }}
                            >
                                编辑
                            </Button>
                        )}
                        {canDelete && (
                            <Popconfirm
                                title="确认删除？"
                                onConfirm={() => handleDeleteRegister(register.id)}
                            >
                                <Button size="small" danger type="link">
                                    删除
                                </Button>
                            </Popconfirm>
                        )}
                    </Space>
                </Flex>

                <Space size={6} wrap className="mb-2">
                    <Tag color={meta.color}>{typeLabel || meta.label}</Tag>
                    <Tag>{addressLabel}</Tag>
                    <Tag color="geekblue">{dataTypeLabel || register.dataType}</Tag>
                    {register.registerType === 'HOLDING_REGISTER' ||
                    register.registerType === 'INPUT_REGISTER' ? (
                        byteOrderLabel ? (
                            <Tag color="purple">字节序 {byteOrderLabel}</Tag>
                        ) : (
                            <Tag>字节序 继承设备</Tag>
                        )
                    ) : null}
                    {isWritableType ? (
                        register.writable ? (
                            <Tag color="orange">读写</Tag>
                        ) : (
                            <Tag>只读</Tag>
                        )
                    ) : (
                        <Tag>只读</Tag>
                    )}
                    {register.unit ? <Tag>{register.unit}</Tag> : null}
                    {typeof register.scale === 'number' && register.scale !== 1 ? (
                        <Tag color="geekblue">x{formatScaleValue(register.scale)}</Tag>
                    ) : null}
                    {typeof register.decimals === 'number' ? (
                        <Tag>小数 {register.decimals}</Tag>
                    ) : null}
                </Space>

                {register.remark ? (
                    <div className="text-xs leading-5 text-slate-500">{register.remark}</div>
                ) : (
                    <div className="text-xs leading-5 text-slate-400">暂无备注</div>
                )}
            </Card>
        );
    };
    // 权限检查
    if (!canQuery) {
        return (
            <PageContainer title="Modbus配置">
                <Result status="403" title="无权访问" subTitle="您没有权限访问此页面" />
            </PageContainer>
        );
    }
    return (
        <PageContainer title="Modbus配置">
            <div className="flex h-full min-h-0 flex-col gap-3 overflow-hidden lg:flex-row lg:gap-0">
                {/* 左侧：设备类型列表 */}
                <div className="h-48 min-h-0 shrink-0 lg:h-full lg:w-[360px] lg:pr-3">
                    <Card
                        title="设备类型"
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 16 },
                        }}
                        extra={
                            <Space size={4}>
                                {canAdd && (
                                    <Button
                                        size="small"
                                        type="primary"
                                        onClick={() => deviceTypeModalRef.current?.open('create')}
                                    >
                                        新增
                                    </Button>
                                )}
                                {canEdit && (
                                    <Button
                                        size="small"
                                        disabled={!activeTypeId}
                                        onClick={() =>
                                            deviceTypeModalRef.current?.open('edit', activeType)
                                        }
                                    >
                                        编辑
                                    </Button>
                                )}
                                {canDelete && (
                                    <Popconfirm
                                        title="确认删除该设备类型？"
                                        onConfirm={handleDeleteDeviceType}
                                        disabled={!activeTypeId}
                                    >
                                        <Button size="small" danger disabled={!activeTypeId}>
                                            删除
                                        </Button>
                                    </Popconfirm>
                                )}
                                {canExport && (
                                    <Tooltip title="导出">
                                        <Button
                                            size="small"
                                            icon={<DownloadOutlined />}
                                            disabled={loadingTypes || importing}
                                            loading={exporting}
                                            onClick={exportConfigs}
                                        />
                                    </Tooltip>
                                )}
                                {canImport && (
                                    <Tooltip title="导入">
                                        <Button
                                            size="small"
                                            icon={<UploadOutlined />}
                                            disabled={exporting}
                                            loading={importing}
                                            onClick={triggerImport}
                                        />
                                    </Tooltip>
                                )}
                            </Space>
                        }
                    >
                        {loadingTypes ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : types.length === 0 ? (
                            <Empty description="暂无设备类型" />
                        ) : (
                            <Tree
                                blockNode
                                className="[&_.ant-tree-switcher]:hidden"
                                selectedKeys={activeTypeId ? [String(activeTypeId)] : []}
                                onSelect={(keys) => {
                                    if (keys.length > 0) {
                                        setSelectedTypeId(String(keys[0]));
                                    }
                                }}
                                treeData={types.map((t) => {
                                    const config = t.config as Modbus.Config;
                                    const regCount = config?.registers?.length || 0;
                                    return {
                                        key: String(t.id),
                                        title: (
                                            <Tooltip
                                                title={t.remark || '暂无备注'}
                                                placement="right"
                                            >
                                                <Flex
                                                    justify="space-between"
                                                    align="center"
                                                    className="h-8 p-1"
                                                >
                                                    <Space size={4}>
                                                        <span>{t.name}</span>
                                                        <Tag color="blue">{regCount}个寄存器</Tag>
                                                    </Space>
                                                    {t.enabled ? (
                                                        <Tag color="green">启用</Tag>
                                                    ) : (
                                                        <Tag color="red">禁用</Tag>
                                                    )}
                                                </Flex>
                                            </Tooltip>
                                        ),
                                    };
                                })}
                            />
                        )}
                    </Card>
                </div>

                {/* 右侧：寄存器配置 */}
                <div className="min-h-0 min-w-0 flex-1">
                    <Card
                        title={
                            <Flex justify="space-between" align="center" gap={8} wrap>
                                <div className="min-w-0 whitespace-normal">
                                    {activeType ? (
                                        <Space wrap>
                                            <span>寄存器配置</span>
                                            <Tag>
                                                {ByteOrderOptions.find(
                                                    (o) =>
                                                        o.value ===
                                                        (activeType.config as Modbus.Config)
                                                            ?.byteOrder
                                                )?.label || 'Big-endian'}
                                            </Tag>
                                            <Tag>
                                                间隔{' '}
                                                {(activeType.config as Modbus.Config)
                                                    ?.readInterval ?? 300}
                                                s
                                            </Tag>
                                            <Tag>
                                                组包 gap≤
                                                {
                                                    normalizePacketConfig(
                                                        (activeType.config as Modbus.Config)?.packet
                                                    ).mergeGap
                                                }
                                            </Tag>
                                            <Tag>
                                                单包≤
                                                {
                                                    normalizePacketConfig(
                                                        (activeType.config as Modbus.Config)?.packet
                                                    ).maxQuantity
                                                }
                                            </Tag>
                                            <Tag color="blue">{registers.length} 个寄存器</Tag>
                                            <Tag color="geekblue">
                                                {registerGroups.length} 个分组
                                            </Tag>
                                            {writableRegisterCount > 0 && (
                                                <Tag color="orange">
                                                    {writableRegisterCount} 个可写
                                                </Tag>
                                            )}
                                        </Space>
                                    ) : types.length > 0 ? (
                                        '请选择设备类型'
                                    ) : (
                                        '暂无设备类型'
                                    )}
                                </div>
                                <div className="shrink-0">
                                    {activeTypeId && canAdd && (
                                        <Space wrap>
                                            <Button
                                                type="primary"
                                                onClick={() =>
                                                    registerModalRef.current?.open(
                                                        'create',
                                                        activeTypeId
                                                    )
                                                }
                                            >
                                                新增寄存器
                                            </Button>
                                            <Button onClick={() => derivedEditor.current?.create()}>
                                                新增派生值
                                            </Button>
                                        </Space>
                                    )}
                                </div>
                            </Flex>
                        }
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 16 },
                        }}
                    >
                        {!activeTypeId ? (
                            <Empty description={emptyTypeDesc} />
                        ) : loadingRegisters ? (
                            <Skeleton active paragraph={{ rows: 5 }} />
                        ) : registers.length === 0 ? (
                            <Empty description="暂无寄存器，点击右上角新增寄存器" />
                        ) : (
                            <SortableGroupSectionList
                                sections={registerGroups}
                                className="w-full"
                                disabled={saveMutation.isPending}
                                onOrderChange={handleRegisterGroupOrderChange}
                                empty={<Empty description="暂无寄存器，点击右上角新增寄存器" />}
                            >
                                {(group) => (
                                    <SortableGroupSectionFrame
                                        id={group.key}
                                        key={group.key}
                                        className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4"
                                        bodyClassName="mt-4"
                                        disabled={saveMutation.isPending}
                                        title={group.label}
                                        meta={
                                            <Space size={6} wrap>
                                                <Tag color="blue">{group.count} 个</Tag>
                                                {REGISTER_TYPE_ORDER.map((type) =>
                                                    group.typeCounts[type] > 0 ? (
                                                        <Tag
                                                            key={type}
                                                            color={REGISTER_TYPE_META[type].color}
                                                        >
                                                            {REGISTER_TYPE_META[type].short}{' '}
                                                            {group.typeCounts[type]}
                                                        </Tag>
                                                    ) : null
                                                )}
                                            </Space>
                                        }
                                    >
                                        <SortableGroupItemList
                                            items={group.registers}
                                            className="grid gap-3"
                                            style={REGISTER_CARD_GRID_STYLE}
                                            minItemWidth={240}
                                            disabled={saveMutation.isPending}
                                            onOrderChange={(nextOrder) =>
                                                handleRegisterItemOrderChange(group.key, nextOrder)
                                            }
                                        >
                                            {(register, dragHandle) =>
                                                renderRegisterCard(register, dragHandle)
                                            }
                                        </SortableGroupItemList>
                                    </SortableGroupSectionFrame>
                                )}
                            </SortableGroupSectionList>
                        )}
                        <DerivedPointsEditor
                            key={activeType?.id}
                            ref={derivedEditor}
                            item={activeType}
                            editable={canEdit}
                        />
                    </Card>
                </div>
            </div>

            {/* 设备类型 Modal */}
            <DeviceTypeModal
                ref={deviceTypeModalRef}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />

            {/* 寄存器 Modal */}
            <RegisterModal
                ref={registerModalRef}
                types={types}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />
        </PageContainer>
    );
};

/**
 * S7 寄存器（Area）编辑弹窗（从 S7Config 抽离）
 */
export interface AreaModalProps {
    open: boolean;
    mode: 'create' | 'edit';
    initialValue?: S7.Area;
    plcModel?: S7.PlcModel;
    groupOptions?: string[];
    onCancel: () => void;
    visible?: boolean;
    onSubmit: (value: S7.Area, visible: boolean) => void;
}
export function AreaModal({
    open,
    mode,
    initialValue,
    visible = true,
    plcModel,
    groupOptions = [],
    onCancel,
    onSubmit,
}: AreaModalProps) {
    const [form] = Form.useForm<S7.Area & { visible: boolean }>();
    const areaType = Form.useWatch('area', form);
    const dataType = normalizeS7DataType(Form.useWatch('dataType', form) as string | undefined);
    const dbNumber = Form.useWatch('dbNumber', form);
    const startAddress = Form.useWatch('start', form);
    const startBit = Form.useWatch('startBit', form);
    const stringLength = Form.useWatch('size', form);
    const parsedAreaType = areaType as S7.AreaType | undefined;
    const areaTypeOptions = getAreaTypeOptions(plcModel);
    const defaultAreaType = areaTypeOptions[0]?.value ?? 'DB';
    const normalizedInitialArea =
        normalizeAreaTypeForPlcModel(plcModel, initialValue?.area) ?? defaultAreaType;
    const initialDataType = normalizeS7DataType(initialValue?.dataType as string | undefined);
    const initialDbNumber =
        normalizedInitialArea === 'V'
            ? undefined
            : (initialValue?.dbNumber ?? (normalizedInitialArea === 'DB' ? 1 : undefined));
    const initialFormValues = useMemo<Partial<S7.Area>>(
        () =>
            initialValue
                ? {
                      ...initialValue,
                      area: normalizedInitialArea,
                      dbNumber: initialDbNumber,
                      dataType: initialDataType,
                      decimals: supportsS7Decimals(initialDataType)
                          ? initialValue.decimals
                          : undefined,
                      group: initialValue.group,
                  }
                : {
                      name: '',
                      group: undefined,
                      area: defaultAreaType,
                      dbNumber: initialDbNumber,
                      dataType: 'INT16' as S7.AreaDataType,
                      start: 0,
                      size: 1,
                      unit: undefined,
                      decimals: undefined,
                      writable: false,
                      remark: '',
                      startBit: undefined,
                  },
        [defaultAreaType, initialDataType, initialDbNumber, initialValue, normalizedInitialArea]
    );
    const groupOptionList = useFilterableGroupOptions(groupOptions);
    const isStringType = dataType === 'STRING';
    const isBoolDataType = dataType === 'BOOL';
    const canUseDecimals = supportsS7Decimals(dataType);
    const isWritableArea = writableAreaTypes.includes(parsedAreaType as S7.AreaType);
    const showBitInput = supportsBitAddress(parsedAreaType, dataType);
    const addressExample = getAddressSuffixExample(parsedAreaType, dataType);
    const calcPreviewSize =
        dataType === 'STRING'
            ? typeof stringLength === 'number' && stringLength > 0
                ? stringLength
                : 1
            : getDataTypeSize(dataType);
    const startOffset = typeof startAddress === 'number' && startAddress >= 0 ? startAddress : 0;
    const endOffset =
        dataType === 'STRING'
            ? startOffset + Math.max(calcPreviewSize, 1) - 1
            : dataType === 'LREAL'
              ? startOffset + 1
              : startOffset;
    const addressSample = getAreaAddressSample(
        parsedAreaType,
        dataType,
        dbNumber,
        startOffset,
        startBit
    );
    useEffect(() => {
        if (!open) {
            form.resetFields();
            return;
        }
        form.resetFields();
        form.setFieldsValue({ ...initialFormValues, visible });
    }, [form, initialFormValues, open, visible]);
    const endAddressSample = getAreaAddressSample(
        parsedAreaType,
        dataType,
        dbNumber,
        endOffset,
        startBit
    );
    const canUseBoolAddress =
        parsedAreaType === 'DB' ||
        parsedAreaType === 'V' ||
        parsedAreaType === 'MK' ||
        parsedAreaType === 'PE' ||
        parsedAreaType === 'PA';
    const handleOk = async () => {
        const { visible: pointVisible, ...values } = await form.validateFields();
        const resolvedDataType =
            parsedAreaType === 'CT' || parsedAreaType === 'TM'
                ? values.dataType || 'UINT16'
                : parsedAreaType === 'PE' || parsedAreaType === 'PA'
                  ? 'BOOL'
                  : values.dataType;
        const size =
            resolvedDataType === 'STRING' ? values.size : getDataTypeSize(resolvedDataType);
        const nextDbNumber = parsedAreaType === 'DB' ? values.dbNumber : undefined;
        const nextStartBit = isBoolDataType && canUseBoolAddress ? values.startBit : undefined;
        const nextDecimals =
            supportsS7Decimals(resolvedDataType) && typeof values.decimals === 'number'
                ? values.decimals
                : undefined;
        const nextValues = {
            ...values,
            group: normalizeGroupName(values.group) || undefined,
            dbNumber: nextDbNumber,
            startBit: nextStartBit,
            decimals: nextDecimals,
            dataType: resolvedDataType,
            id: mode === 'create' ? createUuid() : initialValue?.id || values.id,
            size,
            writable: isWritableArea ? values.writable : false,
        };
        onSubmit(nextValues, pointVisible);
    };
    return (
        <FormModal
            title={mode === 'create' ? '新增寄存器' : '编辑寄存器'}
            open={open}
            onCancel={onCancel}
            onOk={handleOk}
            destroyOnHidden
        >
            <Form form={form} layout="vertical" initialValues={initialFormValues}>
                <PointDisplayField />
                <Form.Item
                    name="name"
                    label="寄存器名称"
                    rules={[{ required: true, message: '请输入寄存器名称' }]}
                >
                    <Input placeholder="例如: 温度寄存器" />
                </Form.Item>
                <Form.Item
                    name="group"
                    label="分组"
                    extra="同一分组的寄存器会在配置页聚合为同一组卡片，留空则显示在未分组中"
                >
                    <AutoComplete
                        allowClear
                        options={groupOptionList.options}
                        placeholder="例如: 基础信息、告警、控制"
                        filterOption={false}
                        onDropdownVisibleChange={groupOptionList.onDropdownVisibleChange}
                        onSearch={groupOptionList.onSearch}
                    />
                </Form.Item>
                <Row gutter={12}>
                    <Col xs={24} sm={12}>
                        <Form.Item
                            name="area"
                            label="寄存器类型"
                            rules={[{ required: true }]}
                            extra={
                                plcModel === 'S7-200'
                                    ? 'S7-200 使用 V/M/I/Q/C/T 区域'
                                    : '不同区域类型映射不同读取方式'
                            }
                        >
                            <Select
                                options={areaTypeOptions}
                                onChange={(value: S7.AreaType) => {
                                    const currentDataType = normalizeS7DataType(
                                        form.getFieldValue('dataType') as string | undefined
                                    );
                                    const updates: Partial<S7.Area> = {};
                                    const nextDataType =
                                        value === 'CT' || value === 'TM'
                                            ? 'UINT16'
                                            : bitOnlyAreaTypes.includes(value)
                                              ? 'BOOL'
                                              : currentDataType;
                                    if (value === 'CT' || value === 'TM') {
                                        updates.dataType = nextDataType;
                                        updates.size = getDataTypeSize(nextDataType);
                                    } else if (bitOnlyAreaTypes.includes(value)) {
                                        updates.dataType = nextDataType;
                                        updates.size = getDataTypeSize('BOOL');
                                    } else if (nextDataType !== 'STRING') {
                                        updates.size = getDataTypeSize(nextDataType);
                                    }
                                    if (value !== 'DB') {
                                        updates.dbNumber = undefined;
                                    }
                                    if (value === 'DB' && form.getFieldValue('dbNumber') == null) {
                                        updates.dbNumber = 1;
                                    }
                                    updates.startBit = undefined;
                                    if (!supportsS7Decimals(nextDataType)) {
                                        updates.decimals = undefined;
                                    }
                                    if (Object.keys(updates).length > 0) {
                                        form.setFieldsValue(updates);
                                    }
                                }}
                            />
                        </Form.Item>
                    </Col>
                    <Col xs={24} sm={12}>
                        <Form.Item
                            name="dataType"
                            label="数据类型"
                            rules={[{ required: true }]}
                            extra="不同数据类型对应不同解析方式"
                        >
                            <Select
                                options={getAreaDataTypeOptions(
                                    areaType as S7.AreaType | undefined
                                )}
                                onChange={(value: S7.AreaDataType) => {
                                    const updates: Partial<S7.Area> = {};
                                    if (value !== 'BOOL') {
                                        updates.startBit = undefined;
                                    }
                                    if (value === 'STRING') {
                                        const currentSize = form.getFieldValue('size') as
                                            | number
                                            | undefined;
                                        if (
                                            typeof currentSize !== 'number' ||
                                            Number.isNaN(currentSize)
                                        ) {
                                            updates.size = 1;
                                        }
                                    } else {
                                        updates.size = getDataTypeSize(value);
                                    }
                                    if (!supportsS7Decimals(value)) {
                                        updates.decimals = undefined;
                                    }
                                    if (Object.keys(updates).length > 0) {
                                        form.setFieldsValue(updates);
                                    }
                                }}
                            />
                        </Form.Item>
                    </Col>
                </Row>
                {areaType === 'DB' && (
                    <Row gutter={12}>
                        <Col xs={24} sm={12}>
                            <Form.Item
                                name="dbNumber"
                                label="DB 编号"
                                rules={[{ required: true, message: '请输入 DB 编号' }]}
                                extra="示例：S7-300 上可读 DB1~DB999；S7-1200 常见为 DB1、DB100 等"
                            >
                                <InputNumber min={1} className="w-full" />
                            </Form.Item>
                        </Col>
                    </Row>
                )}
                <Flex gap={16} className="w-full" align="flex-start" wrap>
                    <Form.Item
                        name="start"
                        label="起始偏移"
                        rules={[
                            {
                                required: true,
                                message: getAddressRuleText(parsedAreaType, isBoolDataType),
                            },
                            { type: 'number', min: 0, message: '偏移不能小于 0' },
                        ]}
                        className="flex-1"
                    >
                        <InputNumber min={0} className="!w-full" />
                    </Form.Item>
                    {showBitInput && (
                        <Form.Item
                            name="startBit"
                            label="位号"
                            rules={
                                isBoolDataType
                                    ? [
                                          { required: true, message: '请输入位号' },
                                          {
                                              type: 'number',
                                              min: 0,
                                              max: 7,
                                              message: '位号只能是 0~7',
                                          },
                                      ]
                                    : undefined
                            }
                            className="flex-1"
                        >
                            <InputNumber min={0} max={7} className="!w-full" />
                        </Form.Item>
                    )}
                    {isStringType && (
                        <Form.Item
                            name="size"
                            label="长度（字节）"
                            rules={[{ required: true, message: '请输入字符串长度' }]}
                            className="flex-1"
                            extra="字符串长度单位为字节"
                        >
                            <InputNumber min={1} className="!w-full" />
                        </Form.Item>
                    )}
                </Flex>
                <Form.Item label="地址示例">
                    <div className="text-xs text-gray-500">
                        参考示例：
                        {addressExample ||
                            (parsedAreaType ? '按区域规则自动拼接' : '请先选择寄存器类型')}
                    </div>
                    <div className="text-xs text-gray-500">
                        {areaType ? `示例：${areaAddressHintMap[areaType]}` : '请先选择寄存器类型'}
                    </div>
                    <div className="text-xs text-gray-500">
                        当前起始地址：{addressSample || '暂无'}
                    </div>
                    <div className="text-xs text-gray-500">
                        当前结束地址：{endAddressSample || '暂无'}
                    </div>
                </Form.Item>
                <Row gutter={12}>
                    <Col xs={24} sm={canUseDecimals ? 12 : 24}>
                        <Form.Item name="unit" label="单位">
                            <Input placeholder="如：V、A、℃、%" />
                        </Form.Item>
                    </Col>
                    {canUseDecimals && (
                        <Col xs={24} sm={12}>
                            <Form.Item name="decimals" label="小数位数">
                                <InputNumber
                                    min={0}
                                    max={8}
                                    placeholder="例如 2"
                                    className="!w-full"
                                />
                            </Form.Item>
                        </Col>
                    )}
                </Row>
                {isWritableArea && (
                    <Form.Item name="writable" label="可写" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                )}
                <Form.Item name="remark" label="备注">
                    <Input.TextArea rows={3} placeholder="备注说明" />
                </Form.Item>
            </Form>
        </FormModal>
    );
}

/**
 * S7 协议配置页面
 * 布局：左侧设备类型列表 + 右侧寄存器配置
 */
const S7ConfigPage = () => {
    const derivedEditor = useRef<DerivedPointsEditorRef>(null);
    const { has } = usePermissions();
    const canQuery = has('iot:protocol:query');
    const canAdd = has('iot:protocol:add');
    const canEdit = has('iot:protocol:edit');
    const canDelete = has('iot:protocol:delete');
    const canImport = has('iot:protocol:import') && canAdd;
    const canExport = has('iot:protocol:export');
    const {
        data: configList,
        isLoading: loadingTypes,
        refetch,
    } = useProtocolConfigList({ protocol: 'S7' }, { enabled: canQuery });
    const saveMutation = useProtocolConfigSave();
    const deleteMutation = useProtocolConfigDelete();
    const { exportConfigs, triggerImport, exporting, importing } = useProtocolImportExport('S7');
    const [selectedTypeId, setSelectedTypeId] = useState<string>();
    const [deviceTypeModalOpen, setDeviceTypeModalOpen] = useState(false);
    const [editingDeviceType, setEditingDeviceType] = useState<boolean>(false);
    const [editingDeviceTypeItem, setEditingDeviceTypeItem] = useState<Protocol.Item>();
    const [createForm] = Form.useForm<DeviceTypeFormValues>();
    const [areaModalOpen, setAreaModalOpen] = useState(false);
    const [editingAreaId, setEditingAreaId] = useState<string | null>(null);
    const types = useMemo(() => configList ?? [], [configList]);
    const emptyTypeDesc = types.length ? '未选择设备类型' : '暂无设备类型';
    const selectedPlcModel = Form.useWatch('plcModel', createForm) as S7.PlcModel | undefined;
    const selectedConnectionType = Form.useWatch('connectionType', createForm) as
        | S7.ConnectionType
        | undefined;
    const selectedConnectionMode = Form.useWatch('connectionMode', createForm) as
        | S7.ConnectionMode
        | undefined;
    const connectionTypeTip = connectionTypeTips[selectedConnectionType || 'PG'];
    const connectionModeTip = getConnectionModeTip(selectedPlcModel, selectedConnectionMode);
    const connectionModeSelectOptions = getConnectionModeOptions(selectedPlcModel);
    const activeTypeId = useMemo(() => {
        if (selectedTypeId && types.some((t) => t.id === selectedTypeId)) {
            return selectedTypeId;
        }
        return types.length > 0 ? types[0].id : undefined;
    }, [selectedTypeId, types]);
    const activeType = useMemo(
        () => types.find((t) => t.id === activeTypeId),
        [activeTypeId, types]
    );
    useEffect(() => {
        if (!deviceTypeModalOpen) return;
        createForm.resetFields();
        createForm.setFieldsValue(getS7DeviceTypeFormValues(editingDeviceTypeItem));
    }, [createForm, deviceTypeModalOpen, editingDeviceTypeItem]);
    const activeConfig = (activeType?.config as S7.Config) ?? null;
    const activeAreas = activeConfig?.areas ?? [];
    const areaGroups = useMemo(() => buildGroupSections(activeAreas), [activeAreas]);
    const areaGroupNames = useMemo(
        () =>
            Array.from(
                new Set(activeAreas.map((area) => normalizeGroupName(area.group)).filter(Boolean))
            ),
        [activeAreas]
    );
    const activeConnectionMode = activeConfig
        ? inferConnectionMode(activeConfig.plcModel, activeConfig.connection)
        : undefined;
    const activeConnectionPreset = activeConfig
        ? getConnectionFormValues(activeConfig.plcModel, activeConfig.connection)
        : null;
    const persistAreas = useCallback(
        async (nextAreas: S7.Area[]) => {
            if (!activeTypeId || !activeConfig) return;
            if (nextAreas.length === activeAreas.length) {
                const isSameOrder =
                    nextAreas.every((area, index) => area.id === activeAreas[index]?.id) &&
                    nextAreas.every(
                        (area, index) =>
                            getGroupKey(area.group) === getGroupKey(activeAreas[index]?.group)
                    );
                if (isSameOrder) return;
            }
            await saveMutation.mutateAsync({
                id: activeTypeId,
                protocol: 'S7',
                config: {
                    ...activeConfig,
                    areas: nextAreas,
                },
            });
            await refetch();
        },
        [activeAreas, activeConfig, activeTypeId, refetch, saveMutation]
    );
    const handleAreaGroupOrderChange = useCallback(
        async (nextOrder: string[]) => {
            await persistAreas(reorderItemsByGroupOrder(activeAreas, nextOrder));
        },
        [activeAreas, persistAreas]
    );
    const handleAreaItemOrderChange = useCallback(
        async (groupKey: string, nextOrder: string[]) => {
            await persistAreas(reorderItemsWithinGroupOrder(activeAreas, groupKey, nextOrder));
        },
        [activeAreas, persistAreas]
    );
    const renderAreaCard = (area: S7.Area, dragHandle?: ReactNode) => {
        const displayArea =
            normalizeAreaTypeForPlcModel(activeConfig?.plcModel, area.area) ?? area.area;
        const displayDataType = normalizeS7DataType(area.dataType);
        const addressRange = getAreaAddressRangeText({ ...area, area: displayArea });
        const canShowDecimals = supportsS7Decimals(displayDataType);
        const areaLocationLabel =
            displayArea === 'DB'
                ? `DB ${area.dbNumber ?? '-'}`
                : displayArea === 'V'
                  ? 'V 区'
                  : `${displayArea} 区`;
        return (
            <Card
                key={area.id}
                size="small"
                hoverable
                className="h-full border-slate-200 shadow-[0_1px_4px_rgba(15,23,42,0.06)]"
                styles={{ body: { padding: 12 } }}
            >
                <Flex justify="space-between" gap={12} align="start" className="mb-2">
                    <div className="min-w-0 flex-1">
                        <div className="truncate text-sm font-semibold text-slate-800">
                            {area.name}
                        </div>
                        <div className="mt-0.5 text-[12px] text-slate-400">
                            起始 {addressRange.start} · 结束 {addressRange.end}
                        </div>
                    </div>
                    <Space size={4} className="shrink-0">
                        {dragHandle}
                        {canEdit && (
                            <Button
                                size="small"
                                type="link"
                                onClick={() => {
                                    setEditingAreaId(area.id);
                                    setAreaModalOpen(true);
                                }}
                            >
                                编辑
                            </Button>
                        )}
                        {canDelete && (
                            <Popconfirm
                                title="确认删除该寄存器？"
                                onConfirm={async () => {
                                    if (!activeTypeId || !activeConfig) return;
                                    const nextAreas = activeConfig.areas.filter(
                                        (item) => item.id !== area.id
                                    );
                                    await saveMutation.mutateAsync({
                                        id: activeTypeId,
                                        protocol: 'S7',
                                        config: {
                                            ...activeConfig,
                                            areas: nextAreas,
                                        },
                                    });
                                    await refetch();
                                }}
                            >
                                <Button size="small" danger type="link">
                                    删除
                                </Button>
                            </Popconfirm>
                        )}
                    </Space>
                </Flex>

                <Space size={6} wrap className="mb-2">
                    <Tag color="blue">{displayArea}</Tag>
                    <Tag color="geekblue">{displayDataType}</Tag>
                    <Tag>{areaLocationLabel}</Tag>
                    <Tag>{area.size} 字节</Tag>
                    {area.writable ? <Tag color="orange">可写</Tag> : <Tag>只读</Tag>}
                    {area.unit ? <Tag>{area.unit}</Tag> : null}
                    {canShowDecimals && typeof area.decimals === 'number' ? (
                        <Tag>小数位数 {area.decimals}</Tag>
                    ) : null}
                    {displayDataType === 'BOOL' && typeof area.startBit === 'number' ? (
                        <Tag>位 {area.startBit}</Tag>
                    ) : null}
                </Space>

                {area.remark ? (
                    <div className="text-xs leading-5 text-slate-500">{area.remark}</div>
                ) : (
                    <div className="text-xs leading-5 text-slate-400">暂无备注</div>
                )}
            </Card>
        );
    };
    const handleDeleteType = async () => {
        if (!activeTypeId) return;
        const idx = types.findIndex((t) => t.id === activeTypeId);
        const nextType = types[idx + 1] ?? types[idx - 1];
        await deleteMutation.mutateAsync(activeTypeId);
        setSelectedTypeId(nextType?.id);
        await refetch();
    };
    const handleOpenCreateType = () => {
        setEditingDeviceType(false);
        setEditingDeviceTypeItem(undefined);
        setDeviceTypeModalOpen(true);
    };
    const handleOpenEditType = () => {
        if (!activeType) return;
        setEditingDeviceType(true);
        setEditingDeviceTypeItem(activeType);
        setDeviceTypeModalOpen(true);
    };
    const handlePlcModelChange = (plcModel: S7.PlcModel) => {
        const preset = getPlcPreset(plcModel);
        createForm.setFieldsValue({
            connectionMode: preset.mode,
            rack: preset.rack,
            slot: preset.slot,
            localTSAP: preset.localTSAP,
            remoteTSAP: preset.remoteTSAP,
        });
    };
    const handleConnectionModeChange = (connectionMode: S7.ConnectionMode) => {
        const plcModel =
            (createForm.getFieldValue('plcModel') as S7.PlcModel | undefined) ?? 'S7-1200';
        const preset = getPlcPreset(plcModel);
        if (connectionMode === 'RACK_SLOT') {
            createForm.setFieldsValue({
                rack: (createForm.getFieldValue('rack') as number | undefined) ?? preset.rack,
                slot: (createForm.getFieldValue('slot') as number | undefined) ?? preset.slot,
            });
            return;
        }
        const connectionType =
            (createForm.getFieldValue('connectionType') as S7.ConnectionType | undefined) ?? 'PG';
        const rack = (createForm.getFieldValue('rack') as number | undefined) ?? preset.rack;
        const slot = (createForm.getFieldValue('slot') as number | undefined) ?? preset.slot;
        const currentLocalTSAP = formatTsapValue(
            createForm.getFieldValue('localTSAP') as string | undefined
        );
        const currentRemoteTSAP = formatTsapValue(
            createForm.getFieldValue('remoteTSAP') as string | undefined
        );
        createForm.setFieldsValue({
            localTSAP: currentLocalTSAP ?? (preset.mode === 'TSAP' ? preset.localTSAP : '0100'),
            remoteTSAP:
                currentRemoteTSAP ??
                (preset.mode === 'TSAP'
                    ? preset.remoteTSAP
                    : buildRemoteTsapFromRackSlot(rack, slot, connectionType)),
        });
    };
    const handleSaveDeviceType = async () => {
        const values = await createForm.validateFields();
        if (editingDeviceType) {
            if (!activeTypeId || !activeType) return;
            const currentConfig = (activeType.config as S7.Config) ?? defaultConfig();
            const nextConfig: S7.Config = {
                ...currentConfig,
                plcModel: values.plcModel,
                connection: {
                    ...currentConfig.connection,
                    ...buildConnectionConfig(values),
                },
                readInterval: values.readInterval,
                storagePolicy: values.storagePolicy,
                commandFastReadDuration: values.commandFastReadDuration,
                commandFastReadInterval: values.commandFastReadInterval,
            };
            await saveMutation.mutateAsync({
                id: activeTypeId,
                protocol: 'S7',
                name: values.deviceType,
                enabled: values.enabled,
                config: nextConfig,
                remark: values.remark,
            });
        } else {
            const config = defaultConfig();
            const nextConfig: S7.Config = {
                ...config,
                deviceType: values.deviceType,
                plcModel: values.plcModel,
                readInterval: values.readInterval,
                storagePolicy: values.storagePolicy,
                commandFastReadDuration: values.commandFastReadDuration,
                commandFastReadInterval: values.commandFastReadInterval,
                connection: buildConnectionConfig(values),
            };
            await saveMutation.mutateAsync({
                protocol: 'S7',
                name: values.deviceType,
                enabled: values.enabled,
                config: nextConfig,
                remark: values.remark,
            });
        }
        setDeviceTypeModalOpen(false);
        setEditingDeviceType(false);
        setEditingDeviceTypeItem(undefined);
        createForm.resetFields();
        await refetch();
    };
    const loadingAreas = loadingTypes;
    const editingArea = activeConfig?.areas.find((area) => area.id === editingAreaId);
    if (!canQuery) {
        return (
            <PageContainer title="S7配置">
                <Result status="403" title="无权访问" subTitle="您没有权限访问此页面" />
            </PageContainer>
        );
    }
    return (
        <PageContainer title="S7配置">
            <div className="flex h-full min-h-0 flex-col gap-3 overflow-hidden lg:flex-row lg:gap-0">
                <div className="h-48 min-h-0 shrink-0 lg:h-full lg:w-[360px] lg:pr-3">
                    <Card
                        title="设备类型"
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 16 },
                        }}
                        extra={
                            <Space size={4}>
                                {canAdd && (
                                    <Button
                                        size="small"
                                        type="primary"
                                        onClick={handleOpenCreateType}
                                    >
                                        新增
                                    </Button>
                                )}
                                {canEdit && (
                                    <Button
                                        size="small"
                                        disabled={!activeTypeId}
                                        onClick={handleOpenEditType}
                                    >
                                        编辑
                                    </Button>
                                )}
                                {canDelete && (
                                    <Popconfirm
                                        title="确认删除该设备类型？"
                                        onConfirm={handleDeleteType}
                                        disabled={!activeTypeId}
                                    >
                                        <Button size="small" danger disabled={!activeTypeId}>
                                            删除
                                        </Button>
                                    </Popconfirm>
                                )}
                                {canExport && (
                                    <Tooltip title="导出">
                                        <Button
                                            size="small"
                                            icon={<DownloadOutlined />}
                                            disabled={loadingTypes || importing}
                                            loading={exporting}
                                            onClick={exportConfigs}
                                        />
                                    </Tooltip>
                                )}
                                {canImport && (
                                    <Tooltip title="导入">
                                        <Button
                                            size="small"
                                            icon={<UploadOutlined />}
                                            disabled={exporting}
                                            loading={importing}
                                            onClick={triggerImport}
                                        />
                                    </Tooltip>
                                )}
                            </Space>
                        }
                    >
                        {loadingTypes ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : types.length === 0 ? (
                            <Empty description="暂无设备类型" />
                        ) : (
                            <Tree
                                blockNode
                                className="[&_.ant-tree-switcher]:hidden"
                                selectedKeys={activeTypeId ? [String(activeTypeId)] : []}
                                onSelect={(keys) => {
                                    if (keys.length > 0) {
                                        setSelectedTypeId(String(keys[0]));
                                    }
                                }}
                                treeData={types.map((t) => {
                                    const config = t.config as S7.Config;
                                    const regCount = config?.areas?.length || 0;
                                    return {
                                        key: String(t.id),
                                        title: (
                                            <Tooltip
                                                title={t.remark || '暂无备注'}
                                                placement="right"
                                            >
                                                <Flex
                                                    justify="space-between"
                                                    align="center"
                                                    className="h-8 p-1"
                                                >
                                                    <Space size={4}>
                                                        <span>{t.name}</span>
                                                        <Tag color="blue">{regCount}个寄存器</Tag>
                                                    </Space>
                                                    {t.enabled ? (
                                                        <Tag color="green">启用</Tag>
                                                    ) : (
                                                        <Tag color="red">禁用</Tag>
                                                    )}
                                                </Flex>
                                            </Tooltip>
                                        ),
                                    };
                                })}
                            />
                        )}
                    </Card>
                </div>

                <div className="min-h-0 min-w-0 flex-1">
                    <Card
                        title={
                            <Flex justify="space-between" align="center" gap={8} wrap>
                                <div className="min-w-0 whitespace-normal">
                                    {activeType ? (
                                        <Space>
                                            <span>寄存器配置</span>
                                            {activeType?.enabled ? (
                                                <Tag color="green">启用</Tag>
                                            ) : (
                                                <Tag color="red">禁用</Tag>
                                            )}
                                            <Tag
                                                color={
                                                    activeConnectionMode === 'TSAP'
                                                        ? 'cyan'
                                                        : 'blue'
                                                }
                                            >
                                                {getConnectionModeLabel(activeConnectionMode)}
                                            </Tag>
                                            <Tag color="geekblue">
                                                {activeAreas.length} 个寄存器
                                            </Tag>
                                            <Tag color="purple">{areaGroups.length} 个分组</Tag>
                                            {activeConnectionMode === 'TSAP' ? (
                                                <Tag>
                                                    TSAP{' '}
                                                    {activeConnectionPreset?.localTSAP ?? '0100'} /{' '}
                                                    {activeConnectionPreset?.remoteTSAP ?? '0100'}
                                                </Tag>
                                            ) : (
                                                <>
                                                    <Tag color="blue">
                                                        {getConnectionTypeLabel(
                                                            (activeType.config as S7.Config)
                                                                ?.connection?.connectionType
                                                        )}
                                                    </Tag>
                                                    <Tag>
                                                        Rack {activeConnectionPreset?.rack ?? 0} /
                                                        Slot {activeConnectionPreset?.slot ?? 1}
                                                    </Tag>
                                                </>
                                            )}
                                            <Tag>
                                                读取间隔{' '}
                                                {(activeType.config as S7.Config)?.readInterval ??
                                                    300}
                                                s
                                            </Tag>
                                        </Space>
                                    ) : types.length > 0 ? (
                                        '请选择设备类型'
                                    ) : (
                                        '暂无设备类型'
                                    )}
                                </div>
                                <div className="shrink-0">
                                    {canAdd && activeTypeId && (
                                        <Space wrap>
                                            <Button
                                                size="small"
                                                type="primary"
                                                onClick={() => {
                                                    setEditingAreaId(null);
                                                    setAreaModalOpen(true);
                                                }}
                                            >
                                                新增寄存器
                                            </Button>
                                            <Button onClick={() => derivedEditor.current?.create()}>
                                                新增派生值
                                            </Button>
                                        </Space>
                                    )}
                                </div>
                            </Flex>
                        }
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 0 },
                        }}
                    >
                        {!activeType ? (
                            <Empty description={emptyTypeDesc} />
                        ) : loadingAreas ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : activeAreas.length === 0 ? (
                            <Empty description="暂无寄存器，点击右上角新增寄存器" />
                        ) : (
                            <SortableGroupSectionList
                                sections={areaGroups}
                                className="w-full p-4"
                                disabled={saveMutation.isPending || !canEdit}
                                onOrderChange={handleAreaGroupOrderChange}
                                empty={<Empty description="暂无寄存器，点击右上角新增寄存器" />}
                            >
                                {(group) => {
                                    const writableCount = group.items.filter(
                                        (area) => area.writable
                                    ).length;
                                    return (
                                        <SortableGroupSectionFrame
                                            id={group.key}
                                            key={group.key}
                                            className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4"
                                            bodyClassName="mt-4"
                                            disabled={saveMutation.isPending || !canEdit}
                                            title={group.label}
                                            meta={
                                                <Space size={6} wrap>
                                                    <Tag color="blue">{group.count} 个</Tag>
                                                    {writableCount > 0 && (
                                                        <Tag color="orange">
                                                            {writableCount} 个可写
                                                        </Tag>
                                                    )}
                                                </Space>
                                            }
                                        >
                                            <SortableGroupItemList
                                                items={group.items}
                                                className="grid gap-3"
                                                style={AREA_CARD_GRID_STYLE}
                                                minItemWidth={320}
                                                disabled={saveMutation.isPending || !canEdit}
                                                empty={<Empty description="暂无寄存器" />}
                                                onOrderChange={(nextOrder) =>
                                                    handleAreaItemOrderChange(group.key, nextOrder)
                                                }
                                            >
                                                {(area, dragHandle) =>
                                                    renderAreaCard(area, dragHandle)
                                                }
                                            </SortableGroupItemList>
                                        </SortableGroupSectionFrame>
                                    );
                                }}
                            </SortableGroupSectionList>
                        )}
                        <DerivedPointsEditor
                            key={activeType?.id}
                            ref={derivedEditor}
                            item={activeType}
                            editable={canEdit}
                        />
                    </Card>
                </div>
            </div>

            <FormModal
                title={editingDeviceType ? '编辑设备类型' : '新建设备类型'}
                open={deviceTypeModalOpen}
                confirmLoading={saveMutation.isPending}
                onCancel={() => {
                    setDeviceTypeModalOpen(false);
                    setEditingDeviceType(false);
                    setEditingDeviceTypeItem(undefined);
                    createForm.resetFields();
                }}
                onOk={handleSaveDeviceType}
                forceRender
            >
                <Form
                    form={createForm}
                    layout="vertical"
                    initialValues={getS7DeviceTypeFormValues()}
                >
                    <Divider titlePlacement="start" plain className="!my-4">
                        基础信息
                    </Divider>
                    <Form.Item
                        name="deviceType"
                        label="名称"
                        rules={[{ required: true, message: '请输入名称' }]}
                        extra="用于区分同一协议下不同设备类别"
                    >
                        <Input placeholder="例如: 温湿度传感器" maxLength={64} />
                    </Form.Item>
                    <Divider titlePlacement="start" plain className="!my-4">
                        连接参数
                    </Divider>
                    <Form.Item
                        name="plcModel"
                        label="PLC型号"
                        rules={[{ required: true, message: '请选择PLC型号' }]}
                        extra="切换型号会自动带入对应的默认连接模式和参数"
                    >
                        <Select
                            options={plcModelOptions.map(({ value, label }) => ({ value, label }))}
                            onChange={(value) => handlePlcModelChange(value as S7.PlcModel)}
                        />
                    </Form.Item>
                    <Form.Item
                        name="connectionMode"
                        label="连接模式"
                        rules={[{ required: true, message: '请选择连接模式' }]}
                        extra={connectionModeTip}
                    >
                        <Select
                            options={connectionModeSelectOptions}
                            onChange={(value) =>
                                handleConnectionModeChange(value as S7.ConnectionMode)
                            }
                        />
                    </Form.Item>
                    {selectedConnectionMode === 'TSAP' ? (
                        <Row gutter={12}>
                            <Col xs={24} sm={12}>
                                <Form.Item
                                    name="localTSAP"
                                    label="本地 TSAP"
                                    rules={[
                                        { required: true, message: '请输入本地 TSAP' },
                                        { validator: validateTsapValue },
                                    ]}
                                    extra="支持 4D57、0200、0x4D57、4D.57"
                                >
                                    <Input placeholder="例如 4D57" />
                                </Form.Item>
                            </Col>
                            <Col xs={24} sm={12}>
                                <Form.Item
                                    name="remoteTSAP"
                                    label="远端 TSAP"
                                    rules={[
                                        { required: true, message: '请输入远端 TSAP' },
                                        { validator: validateTsapValue },
                                    ]}
                                    extra="S7-200 常见值为 4D57 或 0200"
                                >
                                    <Input placeholder="例如 4D57" />
                                </Form.Item>
                            </Col>
                        </Row>
                    ) : (
                        <>
                            <Form.Item
                                name="connectionType"
                                label="连接类型"
                                rules={[{ required: true, message: '请选择连接类型' }]}
                                extra={connectionTypeTip}
                            >
                                <Select options={connectionTypeOptions} />
                            </Form.Item>
                            <Row gutter={12}>
                                <Col xs={24} sm={12}>
                                    <Form.Item
                                        name="rack"
                                        label="Rack"
                                        rules={[{ required: true, message: '请输入 Rack' }]}
                                        extra="西门子机架号，通常从 0 开始"
                                    >
                                        <InputNumber min={0} className="w-full" />
                                    </Form.Item>
                                </Col>
                                <Col xs={24} sm={12}>
                                    <Form.Item
                                        name="slot"
                                        label="Slot"
                                        rules={[{ required: true, message: '请输入 Slot' }]}
                                        extra="CPU 或通信模块所在槽位"
                                    >
                                        <InputNumber min={0} className="w-full" />
                                    </Form.Item>
                                </Col>
                            </Row>
                        </>
                    )}
                    <Form.Item
                        name="probeMode"
                        label="连接探测模式"
                        rules={[{ required: true, message: '请选择探测模式' }]}
                        extra="标准模式完成 ISO-CC 和通信协商后才读数据；兼容模式直接读；自动模式仅在标准握手超时后降级"
                    >
                        <Select
                            options={[
                                { value: 'STANDARD', label: '标准模式（推荐）' },
                                { value: 'COMPATIBLE', label: '兼容模式（直接读）' },
                                { value: 'AUTO', label: '自动降级' },
                            ]}
                        />
                    </Form.Item>
                    <Row gutter={12}>
                        <Col xs={24} sm={12}>
                            <Form.Item
                                name="handshakeTimeout"
                                label="握手超时"
                                rules={[{ required: true, message: '请输入握手超时' }]}
                            >
                                <InputNumber
                                    min={1000}
                                    max={30000}
                                    step={1000}
                                    className="w-full"
                                    addonAfter="ms"
                                />
                            </Form.Item>
                        </Col>
                        <Col xs={24} sm={12}>
                            <Form.Item
                                name="directProbeTimeout"
                                label="兼容探测超时"
                                rules={[{ required: true, message: '请输入兼容探测超时' }]}
                            >
                                <InputNumber
                                    min={1000}
                                    max={30000}
                                    step={1000}
                                    className="w-full"
                                    addonAfter="ms"
                                />
                            </Form.Item>
                        </Col>
                    </Row>
                    <Divider titlePlacement="start" plain className="!my-4">
                        采集与存储
                    </Divider>
                    <Form.Item
                        name="readInterval"
                        label="读取/上报间隔（秒）"
                        rules={[{ required: true, message: '请输入读取/上报间隔' }]}
                        extra="平台采集时控制读取频率；边缘采集时控制上报频率，底层读取固定为 1 秒"
                    >
                        <InputNumber min={1} max={3600} className="w-full" addonAfter="秒" />
                    </Form.Item>
                    <Form.Item
                        name="storagePolicy"
                        label="存储策略"
                        rules={[{ required: true, message: '请选择存储策略' }]}
                        extra="上报时存储每条历史数据；数据改变时仅在点位值变化时存储"
                    >
                        <Select options={STORAGE_POLICY_OPTIONS} />
                    </Form.Item>
                    <Divider titlePlacement="start" plain className="!my-4">
                        下发快读
                    </Divider>
                    <Row gutter={12}>
                        <Col xs={24} sm={12}>
                            <Form.Item
                                name="commandFastReadDuration"
                                label="下发快读窗口"
                                rules={[{ required: true, message: '请输入快读窗口' }]}
                                extra="下发成功后保持快读的时长，0 表示关闭"
                            >
                                <InputNumber
                                    min={0}
                                    max={3600}
                                    className="w-full"
                                    addonAfter="秒"
                                />
                            </Form.Item>
                        </Col>
                        <Col xs={24} sm={12}>
                            <Form.Item
                                name="commandFastReadInterval"
                                label="快读间隔"
                                rules={[{ required: true, message: '请输入快读间隔' }]}
                                extra="快读窗口内的读取间隔"
                            >
                                <InputNumber min={1} max={60} className="w-full" addonAfter="秒" />
                            </Form.Item>
                        </Col>
                    </Row>
                    <Divider titlePlacement="start" plain className="!my-4">
                        其他
                    </Divider>
                    <Form.Item name="remark" label="备注">
                        <Input.TextArea rows={3} placeholder="备注说明" />
                    </Form.Item>
                    <Form.Item name="enabled" label="启用" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                </Form>
            </FormModal>

            <AreaModal
                open={areaModalOpen}
                mode={editingArea ? 'edit' : 'create'}
                initialValue={
                    editingArea
                        ? {
                              ...editingArea,
                              area:
                                  normalizeAreaTypeForPlcModel(
                                      activeConfig?.plcModel,
                                      editingArea.area
                                  ) ?? editingArea.area,
                              dataType: bitOnlyAreaTypes.includes(editingArea.area as S7.AreaType)
                                  ? 'BOOL'
                                  : editingArea.area === 'CT' || editingArea.area === 'TM'
                                    ? 'UINT16'
                                    : normalizeS7DataType(editingArea.dataType),
                              size: bitOnlyAreaTypes.includes(editingArea.area as S7.AreaType)
                                  ? 1
                                  : editingArea.area === 'CT' || editingArea.area === 'TM'
                                    ? getDataTypeSize('UINT16')
                                    : editingArea.size,
                              dbNumber:
                                  normalizeAreaTypeForPlcModel(
                                      activeConfig?.plcModel,
                                      editingArea.area
                                  ) === 'V'
                                      ? undefined
                                      : editingArea.dbNumber,
                              startBit: editingArea.startBit,
                          }
                        : undefined
                }
                plcModel={activeConfig?.plcModel}
                groupOptions={areaGroupNames}
                onCancel={() => setAreaModalOpen(false)}
                visible={!editingArea || activeConfig?.pointVisibility?.[editingArea.id] !== false}
                onSubmit={async (value, visible) => {
                    if (!activeTypeId || !activeConfig) return;
                    const nextAreas = activeConfig.areas.some((area) => area.id === value.id)
                        ? activeConfig.areas.map((area) => (area.id === value.id ? value : area))
                        : [...activeConfig.areas, value];
                    await saveMutation.mutateAsync({
                        id: activeTypeId,
                        protocol: 'S7',
                        config: {
                            ...activeConfig,
                            areas: nextAreas,
                            pointVisibility: {
                                ...activeConfig.pointVisibility,
                                [value.id]: visible,
                            },
                        },
                    });
                    await refetch();
                    setAreaModalOpen(false);
                    setEditingAreaId(null);
                }}
            />
        </PageContainer>
    );
};

/**
 * SL651 设备类型 Modal
 */
export interface DeviceTypeModalRef {
    open: (mode: 'create' | 'edit', data?: Protocol.Item) => void;
}
interface Sl651DeviceTypeModalDeviceTypeModalProps {
    onSuccess?: () => void;
    saveMutation: SaveMutation;
}
const Sl651DeviceTypeModalDeviceTypeModal = forwardRef<
    DeviceTypeModalRef,
    Sl651DeviceTypeModalDeviceTypeModalProps
>(({ onSuccess, saveMutation }, ref) => {
    const [open, setOpen] = useState(false);
    const [mode, setMode] = useState<'create' | 'edit'>('create');
    const [current, setCurrent] = useState<Protocol.Item>();
    const [form] = Form.useForm();
    useImperativeHandle(ref, () => ({
        open(m, data) {
            setMode(m);
            setCurrent(data);
            setOpen(true);
        },
    }));
    useEffect(() => {
        if (!open) return;
        form.resetFields();
        form.setFieldsValue(getSl651DeviceTypeFormValues(current));
    }, [current, form, open]);
    const handleOk = async () => {
        const values = await form.validateFields();
        const existingConfig = (current?.config as SL651.Config) || { funcs: [] };
        await saveMutation.mutateAsync({
            id: current?.id,
            protocol: 'SL651',
            name: values.name,
            enabled: values.enabled,
            config: {
                ...existingConfig,
                responseMode: values.responseMode,
                storagePolicy: values.storagePolicy,
            },
            remark: values.remark,
        });
        onSuccess?.();
        setOpen(false);
    };
    return (
        <FormModal
            title={mode === 'create' ? '新增设备类型' : '编辑设备类型'}
            open={open}
            onOk={handleOk}
            onCancel={() => setOpen(false)}
            confirmLoading={saveMutation.isPending}
            forceRender
        >
            <Form form={form} layout="vertical" initialValues={getSl651DeviceTypeFormValues()}>
                <Divider titlePlacement="start" plain className="!my-4">
                    基础信息
                </Divider>
                <Form.Item
                    label="名称"
                    name="name"
                    rules={[{ required: true, message: '请输入名称' }]}
                >
                    <Input maxLength={64} />
                </Form.Item>
                <Divider titlePlacement="start" plain className="!my-4">
                    协议参数
                </Divider>
                <Form.Item
                    label="应答模式"
                    name="responseMode"
                    rules={[{ required: true, message: '请选择应答模式' }]}
                >
                    <Select
                        options={[
                            { value: 'M1', label: 'M1 - 发送/无回答' },
                            { value: 'M2', label: 'M2 - 发送/确认' },
                            { value: 'M3', label: 'M3 - 多包发送/确认' },
                            { value: 'M4', label: 'M4 - 查询/响应' },
                        ]}
                    />
                </Form.Item>
                <Divider titlePlacement="start" plain className="!my-4">
                    存储策略
                </Divider>
                <Form.Item
                    label="存储策略"
                    name="storagePolicy"
                    rules={[{ required: true, message: '请选择存储策略' }]}
                    extra="上报时存储每条历史数据；数据改变时仅在点位值变化时存储"
                >
                    <Select options={STORAGE_POLICY_OPTIONS} />
                </Form.Item>
                <Divider titlePlacement="start" plain className="!my-4">
                    其他
                </Divider>
                <Form.Item label="启用" name="enabled" valuePropName="checked">
                    <Switch />
                </Form.Item>
                <Form.Item label="备注" name="remark">
                    <Input.TextArea rows={3} />
                </Form.Item>
            </Form>
        </FormModal>
    );
});

/**
 * SL651 字典配置 Modal
 */
export interface DictConfigModalRef {
    open: (typeId: string, funcId: string, element: SL651.Element) => void;
}
interface DictConfigModalProps {
    types: Protocol.Item[];
    onSuccess?: () => void;
    saveMutation: SaveMutation;
}
const DictConfigModal = forwardRef<DictConfigModalRef, DictConfigModalProps>(
    ({ types, onSuccess, saveMutation }, ref) => {
        const [open, setOpen] = useState(false);
        const [typeId, setTypeId] = useState<string>();
        const [funcId, setFuncId] = useState<string>();
        const [element, setElement] = useState<SL651.Element>();
        const [pendingElement, setPendingElement] = useState<SL651.Element>();
        const [form] = Form.useForm();
        /** Modal 打开动画完成后设置表单值，避免闪烁 */
        const initForm = (ele: SL651.Element) => {
            form.resetFields();
            const mapType = ele.dictConfig?.mapType || 'VALUE';
            const items = (ele.dictConfig?.items || [])
                .filter((item) => item && typeof item === 'object')
                .map((item) => {
                    if (mapType === 'VALUE') {
                        return {
                            key: item.key || '',
                            label: item.label || '',
                        };
                    }
                    const validConditions = (item.dependsOn?.conditions || [])
                        .filter(
                            (c: FormCondition) =>
                                c &&
                                typeof c === 'object' &&
                                c.bitIndex !== undefined &&
                                c.bitValue !== undefined
                        )
                        .map((c: FormCondition) => ({
                            bitIndex: String(c.bitIndex),
                            bitValue: String(c.bitValue),
                        }));
                    return {
                        key: item.key || '',
                        label: item.label || '',
                        value: item.value || '1',
                        dependsOn: {
                            operator: item.dependsOn?.operator || 'AND',
                            conditions: validConditions,
                        },
                    };
                });
            form.setFieldsValue({ mapType, items });
        };
        useImperativeHandle(ref, () => ({
            open(t, fId, ele) {
                setTypeId(t);
                setFuncId(fId);
                setElement(ele);
                setPendingElement(ele);
                setOpen(true);
            },
        }));
        const handleOk = async () => {
            if (!typeId || !funcId || !element) return;
            const values = await form.validateFields();
            const type = types.find((t) => t.id === typeId);
            if (!type) return;
            // 根据映射类型清理数据，过滤掉空项
            const cleanedItems = (values.items || [])
                .filter((item: FormMapItem) => {
                    const key = String(item.key || '').trim();
                    const label = String(item.label || '').trim();
                    return key !== '' && label !== '';
                })
                .map((item: FormMapItem) => {
                    if (values.mapType === 'VALUE') {
                        return {
                            key: String(item.key || '').trim(),
                            label: String(item.label || '').trim(),
                        };
                    } else {
                        const cleanedItem: SL651.DictMapItem = {
                            key: String(item.key || '').trim(),
                            label: String(item.label || '').trim(),
                            value: item.value || '1',
                        };
                        const validConditions = (item.dependsOn?.conditions || [])
                            .filter(
                                (c: FormCondition) =>
                                    c &&
                                    typeof c === 'object' &&
                                    c.bitIndex !== undefined &&
                                    c.bitValue !== undefined &&
                                    String(c.bitIndex).trim() !== ''
                            )
                            .map((c: FormCondition) => ({
                                bitIndex: String(c.bitIndex).trim(),
                                bitValue: String(c.bitValue),
                            }));
                        if (validConditions.length > 0) {
                            cleanedItem.dependsOn = {
                                operator: item.dependsOn?.operator || 'AND',
                                conditions: validConditions,
                            };
                        }
                        return cleanedItem;
                    }
                });
            const config = type.config as SL651.Config;
            const newFuncs = config.funcs.map((f) => {
                if (f.id !== funcId) return f;
                const newElements = f.elements.map((e) => {
                    if (e.id !== element.id) return e;
                    if (cleanedItems.length === 0) {
                        const { dictConfig: _dictConfig, ...rest } = e;
                        return rest;
                    }
                    return {
                        ...e,
                        dictConfig: {
                            mapType: values.mapType,
                            items: cleanedItems,
                        },
                    };
                });
                return { ...f, elements: newElements };
            });
            await saveMutation.mutateAsync({
                id: typeId,
                protocol: 'SL651',
                config: { funcs: newFuncs },
            });
            onSuccess?.();
            setOpen(false);
        };
        const mapType = Form.useWatch('mapType', form);
        return (
            <FormModal
                open={open}
                title={`字典配置 - ${element?.name}`}
                onCancel={() => setOpen(false)}
                onOk={handleOk}
                confirmLoading={saveMutation.isPending}
                afterOpenChange={(visible) => {
                    if (visible && pendingElement) {
                        initForm(pendingElement);
                        setPendingElement(undefined);
                    }
                }}
                forceRender
                width={800}
            >
                <Form form={form} layout="vertical">
                    <Form.Item
                        label="映射类型"
                        name="mapType"
                        rules={[{ required: true, message: '请选择映射类型' }]}
                    >
                        <Select
                            options={[
                                { value: 'VALUE', label: '值映射 - 根据数值映射文本' },
                                { value: 'BIT', label: '位映射 - 根据二进制位映射文本' },
                            ]}
                            onChange={() => {
                                form.setFieldsValue({ items: [] });
                            }}
                        />
                    </Form.Item>

                    <Form.Item
                        label="映射项"
                        extra={
                            mapType === 'BIT'
                                ? '配置二进制位对应的文本（位号范围 0-31，可选择位值为0或1时触发）'
                                : '配置数值对应的文本'
                        }
                    >
                        <Form.List name="items">
                            {(fields, { add, remove }) => (
                                <>
                                    {fields.map(({ key, name: itemName, ...restField }) => (
                                        <div
                                            key={key}
                                            className="mb-3 border border-gray-100 p-3 rounded"
                                        >
                                            {mapType === 'BIT' ? (
                                                <>
                                                    <Flex gap={8} align="center" wrap="nowrap">
                                                        <Form.Item
                                                            {...restField}
                                                            name={[itemName, 'key']}
                                                            rules={[
                                                                {
                                                                    required: true,
                                                                    message: '请输入位号',
                                                                },
                                                                {
                                                                    pattern:
                                                                        /^([0-9]|[1-2][0-9]|3[0-1])$/,
                                                                    message: '位号范围 0-31',
                                                                },
                                                            ]}
                                                            className="flex-1 !mb-0"
                                                        >
                                                            <Input placeholder="位号(0-31)" />
                                                        </Form.Item>
                                                        <Form.Item
                                                            {...restField}
                                                            name={[itemName, 'value']}
                                                            initialValue="1"
                                                            rules={[
                                                                {
                                                                    required: true,
                                                                    message: '请选择触发值',
                                                                },
                                                            ]}
                                                            className="w-20 !mb-0"
                                                        >
                                                            <Select
                                                                options={[
                                                                    { value: '1', label: '位=1' },
                                                                    { value: '0', label: '位=0' },
                                                                ]}
                                                            />
                                                        </Form.Item>
                                                        <Form.Item
                                                            {...restField}
                                                            name={[itemName, 'label']}
                                                            rules={[
                                                                { required: true, message: '' },
                                                            ]}
                                                            className="flex-[2] !mb-0"
                                                        >
                                                            <Input placeholder="映射文本" />
                                                        </Form.Item>
                                                        <Tooltip title="添加依赖条件">
                                                            <Button
                                                                size="small"
                                                                type="text"
                                                                icon={
                                                                    <span className="text-base font-bold">
                                                                        +
                                                                    </span>
                                                                }
                                                                onClick={(e) => {
                                                                    e.stopPropagation();
                                                                    const conditions =
                                                                        form.getFieldValue([
                                                                            'items',
                                                                            itemName,
                                                                            'dependsOn',
                                                                            'conditions',
                                                                        ]) || [];
                                                                    form.setFieldValue(
                                                                        [
                                                                            'items',
                                                                            itemName,
                                                                            'dependsOn',
                                                                            'conditions',
                                                                        ],
                                                                        [
                                                                            ...conditions,
                                                                            {
                                                                                bitIndex: '',
                                                                                bitValue: '1',
                                                                            },
                                                                        ]
                                                                    );
                                                                }}
                                                                className="shrink-0"
                                                            />
                                                        </Tooltip>
                                                        <Button
                                                            type="text"
                                                            danger
                                                            onClick={(e) => {
                                                                e.stopPropagation();
                                                                remove(itemName);
                                                            }}
                                                            className="shrink-0"
                                                        >
                                                            删除
                                                        </Button>
                                                    </Flex>

                                                    {/* operator 字段 */}
                                                    <Form.Item
                                                        noStyle
                                                        shouldUpdate={(prev, curr) => {
                                                            const prevConds =
                                                                prev.items?.[itemName]?.dependsOn
                                                                    ?.conditions;
                                                            const currConds =
                                                                curr.items?.[itemName]?.dependsOn
                                                                    ?.conditions;
                                                            return (
                                                                prevConds?.length !==
                                                                currConds?.length
                                                            );
                                                        }}
                                                    >
                                                        {() => {
                                                            const conditions =
                                                                form.getFieldValue([
                                                                    'items',
                                                                    itemName,
                                                                    'dependsOn',
                                                                    'conditions',
                                                                ]) || [];
                                                            return conditions.length > 0 ? (
                                                                <div className="pl-3 border-l-2 border-gray-200 mt-2">
                                                                    <Flex
                                                                        justify="space-between"
                                                                        align="center"
                                                                        className="mb-2"
                                                                    >
                                                                        <span className="text-xs text-gray-500">
                                                                            依赖条件
                                                                        </span>
                                                                        <Form.Item
                                                                            name={[
                                                                                itemName,
                                                                                'dependsOn',
                                                                                'operator',
                                                                            ]}
                                                                            initialValue="AND"
                                                                            className="!mb-0"
                                                                        >
                                                                            <Select
                                                                                size="small"
                                                                                className="!w-[140px]"
                                                                                options={[
                                                                                    {
                                                                                        value: 'AND',
                                                                                        label: 'AND（全满足）',
                                                                                    },
                                                                                    {
                                                                                        value: 'OR',
                                                                                        label: 'OR（任一满足）',
                                                                                    },
                                                                                ]}
                                                                            />
                                                                        </Form.Item>
                                                                    </Flex>
                                                                </div>
                                                            ) : null;
                                                        }}
                                                    </Form.Item>

                                                    {/* 依赖条件列表 */}
                                                    <Form.List
                                                        name={[itemName, 'dependsOn', 'conditions']}
                                                    >
                                                        {(condFields, { remove: removeCond }) => (
                                                            <>
                                                                {condFields.length > 0 && (
                                                                    <div className="pl-3 border-l-2 border-gray-200 -mt-2">
                                                                        {condFields.map(
                                                                            ({
                                                                                key: condKey,
                                                                                name: condName,
                                                                                ...condRestField
                                                                            }) => (
                                                                                <Flex
                                                                                    key={condKey}
                                                                                    gap={8}
                                                                                    align="center"
                                                                                    className="mb-1"
                                                                                >
                                                                                    <span className="text-xs text-gray-400 w-[60px]">
                                                                                        依赖位号
                                                                                    </span>
                                                                                    <Form.Item
                                                                                        {...condRestField}
                                                                                        name={[
                                                                                            condName,
                                                                                            'bitIndex',
                                                                                        ]}
                                                                                        rules={[
                                                                                            {
                                                                                                required: true,
                                                                                                message:
                                                                                                    '请输入位号',
                                                                                            },
                                                                                            {
                                                                                                pattern:
                                                                                                    /^([0-9]|[1-2][0-9]|3[0-1])$/,
                                                                                                message:
                                                                                                    '位号 0-31',
                                                                                            },
                                                                                        ]}
                                                                                        className="flex-1 !mb-0"
                                                                                    >
                                                                                        <Input
                                                                                            size="small"
                                                                                            placeholder="位号(0-31)"
                                                                                        />
                                                                                    </Form.Item>
                                                                                    <span className="text-xs text-gray-400">
                                                                                        期望值
                                                                                    </span>
                                                                                    <Form.Item
                                                                                        {...condRestField}
                                                                                        name={[
                                                                                            condName,
                                                                                            'bitValue',
                                                                                        ]}
                                                                                        initialValue="1"
                                                                                        rules={[
                                                                                            {
                                                                                                required: true,
                                                                                                message:
                                                                                                    '请选择',
                                                                                            },
                                                                                        ]}
                                                                                        className="w-20 !mb-0"
                                                                                    >
                                                                                        <Select
                                                                                            size="small"
                                                                                            options={[
                                                                                                {
                                                                                                    value: '1',
                                                                                                    label: '=1',
                                                                                                },
                                                                                                {
                                                                                                    value: '0',
                                                                                                    label: '=0',
                                                                                                },
                                                                                            ]}
                                                                                        />
                                                                                    </Form.Item>
                                                                                    <Button
                                                                                        size="small"
                                                                                        type="text"
                                                                                        danger
                                                                                        onClick={(
                                                                                            e
                                                                                        ) => {
                                                                                            e.stopPropagation();
                                                                                            removeCond(
                                                                                                condName
                                                                                            );
                                                                                        }}
                                                                                    >
                                                                                        删除
                                                                                    </Button>
                                                                                </Flex>
                                                                            )
                                                                        )}
                                                                    </div>
                                                                )}
                                                            </>
                                                        )}
                                                    </Form.List>
                                                </>
                                            ) : (
                                                <Flex gap={8} align="center">
                                                    <Form.Item
                                                        {...restField}
                                                        name={[itemName, 'key']}
                                                        rules={[
                                                            { required: true, message: '请输入值' },
                                                        ]}
                                                        className="flex-1 !mb-0"
                                                    >
                                                        <Input placeholder="数值" />
                                                    </Form.Item>
                                                    <Form.Item
                                                        {...restField}
                                                        name={[itemName, 'label']}
                                                        rules={[{ required: true, message: '' }]}
                                                        className="flex-1 !mb-0"
                                                    >
                                                        <Input placeholder="映射文本" />
                                                    </Form.Item>
                                                    <Button
                                                        type="text"
                                                        danger
                                                        onClick={() => remove(itemName)}
                                                    >
                                                        删除
                                                    </Button>
                                                </Flex>
                                            )}
                                        </div>
                                    ))}
                                    <Button
                                        type="dashed"
                                        onClick={() => {
                                            if (mapType === 'BIT') {
                                                add({
                                                    key: '',
                                                    label: '',
                                                    value: '1',
                                                    dependsOn: { operator: 'AND', conditions: [] },
                                                });
                                            } else {
                                                add({ key: '', label: '' });
                                            }
                                        }}
                                        block
                                    >
                                        + 添加映射项
                                    </Button>
                                </>
                            )}
                        </Form.List>
                    </Form.Item>
                </Form>
            </FormModal>
        );
    }
);

/**
 * SL651 要素 Modal
 */
export interface ElementModalRef {
    open: (
        mode: 'create' | 'edit',
        typeId: string,
        funcId: string,
        element?: SL651.Element
    ) => void;
}
interface ElementModalProps {
    types: Protocol.Item[];
    onSuccess?: () => void;
    saveMutation: SaveMutation;
}
const ElementModal = forwardRef<ElementModalRef, ElementModalProps>(
    ({ types, onSuccess, saveMutation }, ref) => {
        const [open, setOpen] = useState(false);
        const [mode, setMode] = useState<'create' | 'edit'>('create');
        const [typeId, setTypeId] = useState<string>();
        const [funcId, setFuncId] = useState<string>();
        const [current, setCurrent] = useState<SL651.Element>();
        const [form] = Form.useForm();
        const groupNames = useMemo(() => {
            const type = types.find((t) => t.id === typeId);
            const config = type?.config as SL651.Config | undefined;
            const groups = new Set<string>();
            for (const func of config?.funcs || []) {
                for (const element of func.elements || []) {
                    const group = normalizeGroupName(element.group);
                    if (group) groups.add(group);
                }
                for (const element of func.responseElements || []) {
                    const group = normalizeGroupName(element.group);
                    if (group) groups.add(group);
                }
            }
            const currentGroup = normalizeGroupName(current?.group);
            if (currentGroup) groups.add(currentGroup);
            return Array.from(groups);
        }, [current?.group, typeId, types]);
        const groupOptions = useFilterableGroupOptions(groupNames);
        useImperativeHandle(ref, () => ({
            open(m, t, fId, element) {
                setMode(m);
                setTypeId(t);
                setFuncId(fId);
                setCurrent(element);
                form.resetFields();
                form.setFieldsValue(element ?? { encode: 'BCD', length: 1, digits: 0 });
                form.setFieldValue(
                    'visible',
                    !element ||
                        (types.find((item) => item.id === t)?.config as DeviceTypeTimingConfig)
                            ?.pointVisibility?.[element.id] !== false
                );
                setOpen(true);
            },
        }));
        const handleOk = async () => {
            if (!typeId || !funcId) return;
            const values = await form.validateFields();
            const type = types.find((t) => t.id === typeId);
            if (!type) return;
            const config = type.config as SL651.Config;
            const elementFields = {
                name: values.name,
                group: normalizeGroupName(values.group) || undefined,
                guideHex: (values.guideHex ?? '').replace(/\s/g, ''),
                positionMode: values.positionMode ?? 'GUIDE',
                byteOffset: values.byteOffset ?? 0,
                encode: values.encode,
                length: values.length,
                digits: values.digits,
                unit: values.unit,
                remark: values.remark,
            };
            const pointId = mode === 'create' ? createUuid() : current?.id;
            if (!pointId) return;
            const newFuncs = config.funcs.map((f) => {
                if (f.id !== funcId) return f;
                let newElements: SL651.Element[];
                if (mode === 'create') {
                    const newElement: SL651.Element = {
                        id: pointId,
                        ...elementFields,
                    };
                    newElements = [...(f.elements || []), newElement];
                } else {
                    newElements = f.elements.map((e) =>
                        e.id === current?.id ? { ...e, ...elementFields } : e
                    );
                }
                return { ...f, elements: newElements };
            });
            await saveMutation.mutateAsync({
                id: typeId,
                protocol: 'SL651',
                config: {
                    funcs: newFuncs,
                    pointVisibility: { ...config.pointVisibility, [pointId]: values.visible },
                },
            });
            onSuccess?.();
            setOpen(false);
        };
        return (
            <FormModal
                open={open}
                title={mode === 'create' ? '新增要素' : '编辑要素'}
                onCancel={() => setOpen(false)}
                onOk={handleOk}
                confirmLoading={saveMutation.isPending}
                forceRender
                width={520}
            >
                <Form form={form} layout="vertical">
                    <PointDisplayField />
                    <Form.Item
                        label="要素名称"
                        name="name"
                        rules={[{ required: true, message: '请输入名称' }]}
                    >
                        <Input />
                    </Form.Item>
                    <Form.Item
                        label="分组"
                        name="group"
                        extra="同一分组的要素会在配置页聚合为同一组卡片，留空则显示在未分组中"
                    >
                        <AutoComplete
                            allowClear
                            options={groupOptions.options}
                            placeholder="例如：基础信息、告警、控制"
                            filterOption={false}
                            onDropdownVisibleChange={groupOptions.onDropdownVisibleChange}
                            onSearch={groupOptions.onSearch}
                        />
                    </Form.Item>
                    <Form.Item label="定位方式" name="positionMode" initialValue="GUIDE">
                        <Select
                            options={[
                                { value: 'GUIDE', label: '按引导符' },
                                { value: 'OFFSET', label: '固定位置' },
                            ]}
                        />
                    </Form.Item>
                    <Form.Item
                        label="正文偏移（字节）"
                        name="byteOffset"
                        initialValue={0}
                        extra="固定位置使用：从流水号首字节计 0，分包先重组；偏移 8 跳过流水号和发报时间。"
                    >
                        <InputNumber min={0} max={8388607} precision={0} className="!w-full" />
                    </Form.Item>
                    <Form.Item
                        label="引导符（HEX，仅按引导符时使用）"
                        name="guideHex"
                        normalize={(value: string) => value.replace(/\s/g, '')}
                        rules={[
                            ({ getFieldValue }) => ({
                                validator: (_, value) =>
                                    getFieldValue('positionMode') === 'OFFSET' || value
                                        ? Promise.resolve()
                                        : Promise.reject(new Error('请输入引导符')),
                            }),
                        ]}
                    >
                        <Input placeholder="例如：3912、FFB028 或 F3F3" />
                    </Form.Item>
                    <Form.Item
                        label="编码"
                        name="encode"
                        rules={[{ required: true, message: '请选择编码' }]}
                    >
                        <Select options={EncodeList.map((e) => ({ value: e, label: e }))} />
                    </Form.Item>
                    <Form.Item
                        label="长度（变长要素填 0）"
                        name="length"
                        rules={[{ required: true, message: '请输入长度' }]}
                    >
                        <InputNumber min={0} className="!w-full" />
                    </Form.Item>
                    <Form.Item label="单位" name="unit">
                        <Input placeholder="例如 V、℃、m³/s" />
                    </Form.Item>
                    <Form.Item
                        label="小数位数"
                        name="digits"
                        rules={[{ required: true, message: '请输入小数位数' }]}
                    >
                        <InputNumber min={0} max={7} className="!w-full" />
                    </Form.Item>
                    <Form.Item label="备注" name="remark">
                        <Input.TextArea rows={2} />
                    </Form.Item>
                </Form>
            </FormModal>
        );
    }
);

/**
 * SL651 功能码 Modal
 */
export interface FuncModalRef {
    open: (mode: 'create' | 'edit', typeId: string, func?: SL651.Func) => void;
}
interface FuncModalProps {
    types: Protocol.Item[];
    onSuccess?: () => void;
    saveMutation: SaveMutation;
}
const FuncModal = forwardRef<FuncModalRef, FuncModalProps>(
    ({ types, onSuccess, saveMutation }, ref) => {
        const { message } = App.useApp();
        const [open, setOpen] = useState(false);
        const [mode, setMode] = useState<'create' | 'edit'>('create');
        const [typeId, setTypeId] = useState<string>();
        const [current, setCurrent] = useState<SL651.Func>();
        const [form] = Form.useForm();
        useImperativeHandle(ref, () => ({
            open(m, t, func) {
                setMode(m);
                setTypeId(t);
                setCurrent(func);
                form.resetFields();
                form.setFieldsValue(func ?? { dir: 'UP' });
                setOpen(true);
            },
        }));
        const handleOk = async () => {
            if (!typeId) return;
            const values = await form.validateFields();
            const type = types.find((t) => t.id === typeId);
            if (!type) return;
            const config = type.config as SL651.Config;
            // 检查功能码唯一性（排除自身）
            const duplicate = (config.funcs || []).find(
                (f) => f.funcCode === values.funcCode && (mode === 'create' || f.id !== current?.id)
            );
            if (duplicate) {
                message.error(`功能码 ${values.funcCode} 已存在（${duplicate.name}）`);
                return;
            }
            let newFuncs: SL651.Func[];
            if (mode === 'create') {
                const newFunc: SL651.Func = {
                    id: createUuid(),
                    funcCode: values.funcCode,
                    dir: values.dir,
                    name: values.name,
                    remark: values.remark,
                    elements: [],
                };
                newFuncs = [...(config.funcs || []), newFunc];
            } else {
                newFuncs = config.funcs.map((f) =>
                    f.id === current?.id
                        ? {
                              ...f,
                              funcCode: values.funcCode,
                              dir: values.dir,
                              name: values.name,
                              remark: values.remark,
                          }
                        : f
                );
            }
            await saveMutation.mutateAsync({
                id: typeId,
                protocol: 'SL651',
                config: { funcs: newFuncs },
            });
            onSuccess?.();
            setOpen(false);
        };
        return (
            <FormModal
                title={mode === 'create' ? '新增功能码' : '编辑功能码'}
                open={open}
                onOk={handleOk}
                onCancel={() => setOpen(false)}
                confirmLoading={saveMutation.isPending}
                forceRender
            >
                <Form form={form} layout="vertical">
                    <Form.Item
                        label="名称"
                        name="name"
                        rules={[{ required: true, message: '请输入名称' }]}
                    >
                        <Input />
                    </Form.Item>
                    <Form.Item
                        label="功能码"
                        name="funcCode"
                        rules={[{ required: true, message: '请输入功能码' }]}
                    >
                        <Input placeholder="例如 2F" />
                    </Form.Item>
                    <Form.Item
                        label="方向"
                        name="dir"
                        rules={[{ required: true, message: '请选择方向' }]}
                    >
                        <Select
                            options={[
                                { value: 'UP', label: '上行' },
                                { value: 'DOWN', label: '下行' },
                            ]}
                        />
                    </Form.Item>
                    <Form.Item label="备注" name="remark">
                        <Input.TextArea rows={3} />
                    </Form.Item>
                </Form>
            </FormModal>
        );
    }
);

/**
 * SL651 预设值 Modal
 */
export interface PresetValueModalRef {
    open: (typeId: string, funcId: string, element: SL651.Element) => void;
}
interface PresetValueModalProps {
    types: Protocol.Item[];
    onSuccess?: () => void;
    saveMutation: SaveMutation;
}
const PresetValueModal = forwardRef<PresetValueModalRef, PresetValueModalProps>(
    ({ types, onSuccess, saveMutation }, ref) => {
        const [open, setOpen] = useState(false);
        const [typeId, setTypeId] = useState<string>();
        const [funcId, setFuncId] = useState<string>();
        const [element, setElement] = useState<SL651.Element>();
        const [form] = Form.useForm();
        useImperativeHandle(ref, () => ({
            open(t, fId, ele) {
                setTypeId(t);
                setFuncId(fId);
                setElement(ele);
                form.resetFields();
                form.setFieldsValue({ options: ele.options || [] });
                setOpen(true);
            },
        }));
        const handleOk = async () => {
            if (!typeId || !funcId || !element) return;
            const values = await form.validateFields();
            const type = types.find((t) => t.id === typeId);
            if (!type) return;
            const config = type.config as SL651.Config;
            const newFuncs = config.funcs.map((f) => {
                if (f.id !== funcId) return f;
                const newElements = f.elements.map((e) =>
                    e.id === element.id ? { ...e, options: values.options } : e
                );
                return { ...f, elements: newElements };
            });
            await saveMutation.mutateAsync({
                id: typeId,
                protocol: 'SL651',
                config: { funcs: newFuncs },
            });
            onSuccess?.();
            setOpen(false);
        };
        return (
            <FormModal
                open={open}
                title={`配置预设值 - ${element?.name}`}
                onCancel={() => setOpen(false)}
                onOk={handleOk}
                confirmLoading={saveMutation.isPending}
                forceRender
                width={600}
            >
                <Form form={form} layout="vertical">
                    <Form.Item label="预设值选项" extra="配置后指令下发时可从下拉列表选择">
                        <Form.List name="options">
                            {(fields, { add, remove }) => (
                                <>
                                    {fields.map(({ key, name, ...restField }) => (
                                        <Flex key={key} gap={8} align="center" className="mb-2">
                                            <Form.Item
                                                {...restField}
                                                name={[name, 'label']}
                                                rules={[{ required: true, message: '请输入名称' }]}
                                                className="flex-1 !mb-0"
                                            >
                                                <Input placeholder="显示名称" />
                                            </Form.Item>
                                            <Form.Item
                                                {...restField}
                                                name={[name, 'value']}
                                                rules={[{ required: true, message: '请输入值' }]}
                                                className="flex-1 !mb-0"
                                            >
                                                <Input placeholder="实际值" />
                                            </Form.Item>
                                            <Button type="text" danger onClick={() => remove(name)}>
                                                删除
                                            </Button>
                                        </Flex>
                                    ))}
                                    <Button type="dashed" onClick={() => add()} block>
                                        + 添加预设值
                                    </Button>
                                </>
                            )}
                        </Form.List>
                    </Form.Item>
                </Form>
            </FormModal>
        );
    }
);

/**
 * SL651 应答要素 Modal
 */
export interface ResponseElementsModalRef {
    open: (typeId: string, func: SL651.Func) => void;
}
interface ResponseElementsModalProps {
    types: Protocol.Item[];
    onSuccess?: () => void;
    saveMutation: SaveMutation;
}
const RESPONSE_ELEMENT_CARD_GRID_STYLE: CSSProperties = {
    gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
};
const ResponseElementsModal = forwardRef<ResponseElementsModalRef, ResponseElementsModalProps>(
    ({ types, onSuccess, saveMutation }, ref) => {
        const [open, setOpen] = useState(false);
        const [typeId, setTypeId] = useState<string>();
        const [func, setFunc] = useState<SL651.Func>();
        const [form] = Form.useForm();
        const groupNames = useMemo(() => {
            const groups = new Set<string>();
            for (const element of [...(func?.elements || []), ...(func?.responseElements || [])]) {
                const group = normalizeGroupName(element.group);
                if (group) groups.add(group);
            }
            return Array.from(groups);
        }, [func]);
        const groupOptions = useFilterableGroupOptions(groupNames);
        useImperativeHandle(ref, () => ({
            open(t, f) {
                setTypeId(t);
                setFunc(f);
                form.resetFields();
                form.setFieldsValue({
                    responseElements: f.responseElements || [],
                });
                setOpen(true);
            },
        }));
        const handleOk = async () => {
            if (!typeId || !func) return;
            const values = await form.validateFields();
            const type = types.find((t) => t.id === typeId);
            if (!type) return;
            // 过滤掉不完整的要素
            const cleanedElements = (values.responseElements || [])
                .filter(
                    (ele: Partial<SL651.Element>) =>
                        ele.name?.trim() &&
                        (ele.positionMode === 'OFFSET' || ele.guideHex?.replace(/\s/g, '')) &&
                        ele.encode &&
                        ele.length !== undefined
                )
                .map((ele: Partial<SL651.Element>) => ({
                    id: ele.id || createUuid(),
                    name: ele.name?.trim(),
                    group: normalizeGroupName(ele.group) || undefined,
                    guideHex: ele.guideHex?.replace(/\s/g, '') ?? '',
                    positionMode: ele.positionMode ?? 'GUIDE',
                    byteOffset: ele.byteOffset ?? 0,
                    encode: ele.encode,
                    length: ele.length,
                    digits: Number.isFinite(ele.digits) ? ele.digits : 0,
                    unit: ele.unit?.trim() || undefined,
                    remark: ele.remark?.trim() || undefined,
                }));
            const config = type.config as SL651.Config;
            const newFuncs = config.funcs.map((f) => {
                if (f.id !== func.id) return f;
                return {
                    ...f,
                    responseElements: cleanedElements.length > 0 ? cleanedElements : undefined,
                };
            });
            await saveMutation.mutateAsync({
                id: typeId,
                protocol: 'SL651',
                config: { funcs: newFuncs },
            });
            onSuccess?.();
            setOpen(false);
        };
        return (
            <FormModal
                open={open}
                title={`应答要素配置 - ${func?.name} (${func?.funcCode})`}
                onCancel={() => setOpen(false)}
                onOk={handleOk}
                confirmLoading={saveMutation.isPending}
                forceRender
                width={900}
            >
                <div className="mb-4">
                    <Tag color="blue">下行功能码</Tag>
                    <span className="text-gray-500 ml-2">
                        配置设备应答报文的解析要素。设备收到下行指令后会返回应答报文，这里定义如何解析应答报文中的数据。
                    </span>
                </div>

                <Form form={form} layout="vertical">
                    <Form.List name="responseElements">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.length > 0 ? (
                                    <div
                                        className="grid gap-3"
                                        style={RESPONSE_ELEMENT_CARD_GRID_STYLE}
                                    >
                                        {fields.map((field, index) => (
                                            <Card
                                                key={field.key}
                                                size="small"
                                                hoverable
                                                className="h-full border-slate-200 shadow-[0_1px_4px_rgba(15,23,42,0.06)]"
                                                styles={{ body: { padding: 12 } }}
                                            >
                                                <Flex
                                                    justify="space-between"
                                                    gap={12}
                                                    align="start"
                                                    className="mb-3"
                                                >
                                                    <div className="min-w-0 flex-1">
                                                        <div className="text-sm font-semibold text-slate-800">
                                                            应答要素 {index + 1}
                                                        </div>
                                                        <div className="mt-0.5 text-[12px] text-slate-400">
                                                            配置名称、定位方式、编码、长度、单位和小数位数
                                                        </div>
                                                    </div>
                                                    <Button
                                                        type="text"
                                                        danger
                                                        size="small"
                                                        onClick={() => remove(field.name)}
                                                    >
                                                        删除
                                                    </Button>
                                                </Flex>

                                                <div className="grid gap-3 sm:grid-cols-2">
                                                    <Form.Item
                                                        name={[field.name, 'name']}
                                                        rules={[
                                                            { required: true, message: '必填' },
                                                        ]}
                                                        className="!mb-0"
                                                    >
                                                        <Input placeholder="要素名称" />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'group']}
                                                        className="!mb-0"
                                                    >
                                                        <AutoComplete
                                                            allowClear
                                                            options={groupOptions.options}
                                                            placeholder="分组"
                                                            filterOption={false}
                                                            onDropdownVisibleChange={
                                                                groupOptions.onDropdownVisibleChange
                                                            }
                                                            onSearch={groupOptions.onSearch}
                                                        />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'guideHex']}
                                                        normalize={(value: string) =>
                                                            value.replace(/\s/g, '')
                                                        }
                                                        className="!mb-0"
                                                    >
                                                        <Input placeholder="引导符，例如 3912；固定位置可留空" />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'positionMode']}
                                                        label="定位方式"
                                                        initialValue="GUIDE"
                                                        className="!mb-0"
                                                    >
                                                        <Select
                                                            options={[
                                                                {
                                                                    value: 'GUIDE',
                                                                    label: '按引导符',
                                                                },
                                                                {
                                                                    value: 'OFFSET',
                                                                    label: '固定位置',
                                                                },
                                                            ]}
                                                        />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'byteOffset']}
                                                        label="正文偏移（从流水号计 0）"
                                                        initialValue={0}
                                                        className="!mb-0"
                                                    >
                                                        <InputNumber
                                                            min={0}
                                                            max={8388607}
                                                            precision={0}
                                                            className="!w-full"
                                                        />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'encode']}
                                                        rules={[
                                                            { required: true, message: '必填' },
                                                        ]}
                                                        className="!mb-0"
                                                    >
                                                        <Select
                                                            placeholder="选择"
                                                            options={EncodeList.map((e) => ({
                                                                value: e,
                                                                label: e,
                                                            }))}
                                                        />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'length']}
                                                        rules={[
                                                            { required: true, message: '必填' },
                                                        ]}
                                                        className="!mb-0"
                                                    >
                                                        <InputNumber min={1} className="!w-full" />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'unit']}
                                                        className="!mb-0"
                                                    >
                                                        <Input placeholder="单位" />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'digits']}
                                                        className="!mb-0"
                                                    >
                                                        <InputNumber
                                                            min={0}
                                                            max={7}
                                                            className="!w-full"
                                                        />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={[field.name, 'remark']}
                                                        className="!mb-0 sm:col-span-2"
                                                    >
                                                        <Input placeholder="备注" />
                                                    </Form.Item>
                                                </div>
                                            </Card>
                                        ))}
                                    </div>
                                ) : (
                                    <Empty description="暂无应答要素，点击下方按钮添加" />
                                )}
                                <Button
                                    type="dashed"
                                    onClick={() =>
                                        add({
                                            id: createUuid(),
                                            name: '',
                                            group: undefined,
                                            guideHex: '',
                                            encode: 'BCD',
                                            length: 1,
                                            digits: 0,
                                        })
                                    }
                                    block
                                    className="mt-3"
                                >
                                    + 添加应答要素
                                </Button>
                            </>
                        )}
                    </Form.List>
                </Form>
            </FormModal>
        );
    }
);

/**
 * SL651 协议配置 - Modal 组件导出
 */

/**
 * SL651 协议配置页面
 * 布局：左侧设备类型列表 + 右侧功能码/要素配置
 */
/** 带要素的功能码 */
interface FuncWithElements extends SL651.Func {
    elements: SL651.Element[];
    responseElements?: SL651.Element[];
}
const ELEMENT_CARD_GRID_STYLE: CSSProperties = {
    gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
};
const SL651ConfigPage = () => {
    const derivedEditor = useRef<DerivedPointsEditorRef>(null);
    // 权限检查
    const { has } = usePermissions();
    const canQuery = has('iot:protocol:query');
    const canAdd = has('iot:protocol:add');
    const canEdit = has('iot:protocol:edit');
    const canDelete = has('iot:protocol:delete');
    const canImport = has('iot:protocol:import') && canAdd;
    const canExport = has('iot:protocol:export');
    // 设备类型列表查询
    const {
        data: configList,
        isLoading: loadingTypes,
        refetch: refetchTypes,
    } = useProtocolConfigList({ protocol: 'SL651' }, { enabled: canQuery });
    // 保存和删除 mutations
    const saveMutation = useProtocolConfigSave();
    const deleteMutation = useProtocolConfigDelete();
    // 导入导出
    const { exportConfigs, triggerImport, exporting, importing } = useProtocolImportExport('SL651');
    // 当前选中的设备类型 ID（用户手动选择）
    const [selectedTypeId, setSelectedTypeId] = useState<string>();
    // Modal refs
    const deviceTypeModalRef = useRef<DeviceTypeModalRef>(null);
    const funcModalRef = useRef<FuncModalRef>(null);
    const elementModalRef = useRef<ElementModalRef>(null);
    const presetValueModalRef = useRef<PresetValueModalRef>(null);
    const dictConfigModalRef = useRef<DictConfigModalRef>(null);
    const responseElementsModalRef = useRef<ResponseElementsModalRef>(null);
    // 设备类型列表（使用 useMemo 保持引用稳定）
    const types = useMemo(() => configList ?? [], [configList]);
    const emptyTypeDesc = types.length ? '未选择设备类型' : '暂无设备类型';
    // 计算当前激活的类型 ID：优先用户选择，否则默认第一个
    const activeTypeId = useMemo(() => {
        if (selectedTypeId && types.some((t) => t.id === selectedTypeId)) {
            return selectedTypeId;
        }
        return types.length > 0 ? types[0].id : undefined;
    }, [selectedTypeId, types]);
    // 功能码列表（派生状态，根据 activeTypeId 计算）
    const funcs = useMemo<FuncWithElements[]>(() => {
        if (!activeTypeId) return [];
        const type = types.find((t) => t.id === activeTypeId);
        if (!type) return [];
        const config = type.config as SL651.Config;
        return config?.funcs || [];
    }, [activeTypeId, types]);
    // 加载状态（与数据加载状态同步）
    const loadingFuncs = loadingTypes;
    // ========== 设备类型操作 ==========
    const handleDeleteDeviceType = async () => {
        if (!activeTypeId) return;
        // 删除前记录下一个可选中的类型
        const idx = types.findIndex((t) => t.id === activeTypeId);
        const nextType = types[idx + 1] ?? types[idx - 1];
        await deleteMutation.mutateAsync(activeTypeId);
        setSelectedTypeId(nextType?.id);
    };
    // ========== 功能码操作 ==========
    const handleDeleteFunc = async (funcId: string) => {
        if (!activeTypeId) return;
        const type = types.find((t) => t.id === activeTypeId);
        if (!type) return;
        const config = type.config as SL651.Config;
        const newConfig: SL651.Config = {
            funcs: config.funcs.filter((f) => f.id !== funcId),
        };
        await saveMutation.mutateAsync({
            id: activeTypeId,
            protocol: 'SL651',
            config: newConfig,
        });
    };
    // ========== 要素操作 ==========
    const handleDeleteElement = async (funcId: string, eleId: string) => {
        if (!activeTypeId) return;
        const type = types.find((t) => t.id === activeTypeId);
        if (!type) return;
        const config = type.config as SL651.Config;
        const newConfig: SL651.Config = {
            funcs: config.funcs.map((f) =>
                f.id === funcId ? { ...f, elements: f.elements.filter((e) => e.id !== eleId) } : f
            ),
        };
        await saveMutation.mutateAsync({
            id: activeTypeId,
            protocol: 'SL651',
            config: newConfig,
        });
    };
    const persistElements = useCallback(
        async (
            funcId: string,
            listKind: 'elements' | 'responseElements',
            nextItems: SL651.Element[],
            currentItems: SL651.Element[]
        ) => {
            if (!activeTypeId) return;
            const type = types.find((t) => t.id === activeTypeId);
            if (!type) return;
            if (nextItems.length === currentItems.length) {
                const isSameOrder =
                    nextItems.every((item, index) => item.id === currentItems[index]?.id) &&
                    nextItems.every(
                        (item, index) =>
                            getGroupKey(item.group) === getGroupKey(currentItems[index]?.group)
                    );
                if (isSameOrder) return;
            }
            const config = type.config as SL651.Config;
            const newFuncs = config.funcs.map((func) => {
                if (func.id !== funcId) return func;
                if (listKind === 'responseElements') {
                    return {
                        ...func,
                        responseElements: nextItems.length > 0 ? nextItems : undefined,
                    };
                }
                return {
                    ...func,
                    elements: nextItems,
                };
            });
            await saveMutation.mutateAsync({
                id: activeTypeId,
                protocol: 'SL651',
                config: { funcs: newFuncs },
            });
            await refetchTypes();
        },
        [activeTypeId, refetchTypes, saveMutation, types]
    );
    const handleReorderElements = useCallback(
        async (
            funcId: string,
            listKind: 'elements' | 'responseElements',
            nextOrder: string[],
            currentItems: SL651.Element[]
        ) => {
            await persistElements(
                funcId,
                listKind,
                reorderItemsByGroupOrder(currentItems, nextOrder),
                currentItems
            );
        },
        [persistElements]
    );
    const handleReorderElementItems = useCallback(
        async (
            funcId: string,
            listKind: 'elements' | 'responseElements',
            groupKey: string,
            nextOrder: string[],
            currentItems: SL651.Element[]
        ) => {
            await persistElements(
                funcId,
                listKind,
                reorderItemsWithinGroupOrder(currentItems, groupKey, nextOrder),
                currentItems
            );
        },
        [persistElements]
    );
    const renderElementCard = (
        element: SL651.Element,
        funcId: string,
        funcDir: SL651.Direction,
        options?: {
            advancedActions?: boolean;
        },
        dragHandle?: ReactNode
    ) => {
        const advancedActions = options?.advancedActions !== false;
        const presetValueCount = element.options?.length ?? 0;
        const dictItemCount = element.dictConfig?.items?.length ?? 0;
        const showPresetValueTag = funcDir === 'DOWN' && advancedActions && presetValueCount > 0;
        const showDictTag = advancedActions && element.encode === 'DICT' && dictItemCount > 0;
        return (
            <Card
                key={element.id}
                size="small"
                hoverable
                className="h-full border-slate-200 shadow-[0_1px_4px_rgba(15,23,42,0.06)]"
                styles={{ body: { padding: 12 } }}
            >
                <Flex justify="space-between" gap={12} align="start" className="mb-2">
                    <div className="min-w-0 flex-1">
                        <div className="truncate text-sm font-semibold text-slate-800">
                            {element.name}
                        </div>
                        <div className="mt-0.5 text-[12px] text-slate-400">
                            {element.positionMode === 'OFFSET'
                                ? `正文偏移 ${element.byteOffset ?? 0} 字节`
                                : `引导符 ${element.guideHex}`}
                        </div>
                    </div>
                    <Space size={4} className="shrink-0">
                        {dragHandle}
                        {canEdit && (
                            <Button
                                size="small"
                                type="link"
                                onClick={() => {
                                    if (activeTypeId)
                                        elementModalRef.current?.open(
                                            'edit',
                                            activeTypeId,
                                            funcId,
                                            element
                                        );
                                }}
                            >
                                编辑
                            </Button>
                        )}
                        {canDelete && (
                            <Popconfirm
                                title="确认删除？"
                                onConfirm={() => handleDeleteElement(funcId, element.id)}
                            >
                                <Button size="small" danger type="link">
                                    删除
                                </Button>
                            </Popconfirm>
                        )}
                    </Space>
                </Flex>

                <Space size={6} wrap className="mb-2">
                    <Tag color="blue">{element.encode}</Tag>
                    <Tag>{element.length} 字节</Tag>
                    {typeof element.digits === 'number' ? (
                        <Tag>小数位数 {element.digits}</Tag>
                    ) : null}
                    {element.unit ? <Tag>{element.unit}</Tag> : null}
                    {showPresetValueTag ? (
                        <Tag color="cyan">{presetValueCount} 个预设值</Tag>
                    ) : null}
                    {showDictTag ? <Tag color="purple">{dictItemCount} 个字典项</Tag> : null}
                </Space>

                {element.remark ? (
                    <div className="text-xs leading-5 text-slate-500">{element.remark}</div>
                ) : (
                    <div className="text-xs leading-5 text-slate-400">暂无备注</div>
                )}

                {(funcDir === 'DOWN' && advancedActions && canEdit) ||
                (advancedActions && element.encode === 'DICT' && canEdit) ? (
                    <Space size={4} wrap className="mt-3">
                        {funcDir === 'DOWN' && advancedActions && canEdit && (
                            <Button
                                size="small"
                                onClick={() => {
                                    if (activeTypeId)
                                        presetValueModalRef.current?.open(
                                            activeTypeId,
                                            funcId,
                                            element
                                        );
                                }}
                            >
                                预设值
                            </Button>
                        )}
                        {advancedActions && element.encode === 'DICT' && canEdit && (
                            <Button
                                size="small"
                                onClick={() => {
                                    if (activeTypeId)
                                        dictConfigModalRef.current?.open(
                                            activeTypeId,
                                            funcId,
                                            element
                                        );
                                }}
                            >
                                字典
                            </Button>
                        )}
                    </Space>
                ) : null}
            </Card>
        );
    };
    const renderGroupedElements = (
        elements: SL651.Element[] | undefined,
        funcId: string,
        funcDir: SL651.Direction,
        emptyDescription: string,
        options?: {
            advancedActions?: boolean;
            listKind?: 'elements' | 'responseElements';
        }
    ) => {
        const advancedActions = options?.advancedActions !== false;
        const listKind = options?.listKind ?? 'elements';
        const groups = buildGroupSections(elements || []);
        if (!groups.length) {
            return <Empty description={emptyDescription} />;
        }
        return (
            <SortableGroupSectionList
                sections={groups}
                className="w-full"
                disabled={saveMutation.isPending || !canEdit}
                onOrderChange={(nextOrder) =>
                    handleReorderElements(funcId, listKind, nextOrder, elements || [])
                }
                empty={<Empty description={emptyDescription} />}
            >
                {(group) => (
                    <SortableGroupSectionFrame
                        id={group.key}
                        key={group.key}
                        className="rounded-xl border border-slate-200 bg-white p-3"
                        bodyClassName="mt-4"
                        disabled={saveMutation.isPending || !canEdit}
                        title={group.label}
                        meta={<Tag color="blue">{group.count} 个</Tag>}
                    >
                        <SortableGroupItemList
                            items={group.items}
                            className="grid gap-3"
                            style={ELEMENT_CARD_GRID_STYLE}
                            minItemWidth={280}
                            disabled={saveMutation.isPending || !canEdit}
                            empty={<Empty description={emptyDescription} />}
                            onOrderChange={(nextOrder) =>
                                handleReorderElementItems(
                                    funcId,
                                    listKind,
                                    group.key,
                                    nextOrder,
                                    elements || []
                                )
                            }
                        >
                            {(element, dragHandle) =>
                                renderElementCard(
                                    element,
                                    funcId,
                                    funcDir,
                                    { advancedActions },
                                    dragHandle
                                )
                            }
                        </SortableGroupItemList>
                    </SortableGroupSectionFrame>
                )}
            </SortableGroupSectionList>
        );
    };
    const renderFuncCard = (record: FuncWithElements) => {
        const elementCount = record.elements?.length ?? 0;
        const responseCount = record.responseElements?.length ?? 0;
        return (
            <Card
                key={record.id}
                size="small"
                hoverable
                className="w-full border-slate-200 shadow-[0_1px_4px_rgba(15,23,42,0.06)]"
                styles={{ body: { padding: 16 } }}
            >
                <Flex justify="space-between" gap={12} align="start" className="mb-2">
                    <div className="min-w-0 flex-1">
                        <div className="truncate text-sm font-semibold text-slate-800">
                            {record.name}
                        </div>
                        <div className="mt-0.5 text-[12px] text-slate-400">
                            功能码 {record.funcCode}
                        </div>
                    </div>
                    <Space size={4} className="shrink-0">
                        {canEdit && (
                            <Button
                                size="small"
                                type="link"
                                onClick={() => {
                                    if (activeTypeId)
                                        funcModalRef.current?.open('edit', activeTypeId, record);
                                }}
                            >
                                编辑
                            </Button>
                        )}
                        {canDelete && (
                            <Popconfirm
                                title="确认删除？"
                                onConfirm={() => handleDeleteFunc(record.id)}
                            >
                                <Button size="small" danger type="link">
                                    删除
                                </Button>
                            </Popconfirm>
                        )}
                    </Space>
                </Flex>

                <Space size={6} wrap className="mb-2">
                    <Tag color={record.dir === 'UP' ? 'green' : 'orange'}>
                        {record.dir === 'UP' ? '上行' : '下行'}
                    </Tag>
                    <Tag color="blue">{elementCount} 个要素</Tag>
                    {record.dir === 'DOWN' ? (
                        <Tag color="cyan">{responseCount} 个应答要素</Tag>
                    ) : null}
                </Space>

                {record.remark ? (
                    <div className="text-xs leading-5 text-slate-500">{record.remark}</div>
                ) : (
                    <div className="text-xs leading-5 text-slate-400">暂无备注</div>
                )}

                <Space size={4} wrap className="mt-3">
                    {record.dir === 'DOWN' && canEdit && (
                        <Button
                            size="small"
                            onClick={() => {
                                if (activeTypeId)
                                    responseElementsModalRef.current?.open(activeTypeId, record);
                            }}
                        >
                            应答要素
                        </Button>
                    )}
                </Space>

                <div className="mt-4 space-y-4">
                    <div>
                        <Flex justify="space-between" align="center" gap={12} className="mb-2">
                            <div className="text-sm font-semibold text-slate-700">要素配置</div>
                            {canAdd && (
                                <Button
                                    size="small"
                                    type="primary"
                                    onClick={() => {
                                        if (activeTypeId)
                                            elementModalRef.current?.open(
                                                'create',
                                                activeTypeId,
                                                record.id
                                            );
                                    }}
                                >
                                    新增要素
                                </Button>
                            )}
                        </Flex>
                        {renderGroupedElements(record.elements, record.id, record.dir, '暂无要素', {
                            listKind: 'elements',
                        })}
                    </div>
                    {record.dir === 'DOWN' && (
                        <div>
                            <div className="mb-2 text-sm font-semibold text-slate-700">
                                应答要素
                            </div>
                            {renderGroupedElements(
                                record.responseElements,
                                record.id,
                                record.dir,
                                '暂无应答要素',
                                {
                                    advancedActions: false,
                                    listKind: 'responseElements',
                                }
                            )}
                        </div>
                    )}
                </div>
            </Card>
        );
    };
    const emptyFuncDesc = activeTypeId ? '暂无功能码，点击右上角新增功能码' : emptyTypeDesc;
    // 权限检查
    if (!canQuery) {
        return (
            <PageContainer title="SL651配置">
                <Result status="403" title="无权访问" subTitle="您没有权限访问此页面" />
            </PageContainer>
        );
    }
    return (
        <PageContainer title="SL651配置">
            <div className="flex h-full min-h-0 flex-col gap-3 overflow-hidden lg:flex-row lg:gap-0">
                {/* 左侧：设备类型列表 */}
                <div className="h-48 min-h-0 shrink-0 lg:h-full lg:w-[360px] lg:pr-3">
                    <Card
                        title="设备类型"
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 16 },
                        }}
                        extra={
                            <Space size={4}>
                                {canAdd && (
                                    <Button
                                        size="small"
                                        type="primary"
                                        onClick={() => deviceTypeModalRef.current?.open('create')}
                                    >
                                        新增
                                    </Button>
                                )}
                                {canEdit && (
                                    <Button
                                        size="small"
                                        disabled={!activeTypeId}
                                        onClick={() =>
                                            deviceTypeModalRef.current?.open(
                                                'edit',
                                                types.find((t) => t.id === activeTypeId)
                                            )
                                        }
                                    >
                                        编辑
                                    </Button>
                                )}
                                {canDelete && (
                                    <Popconfirm
                                        title="确认删除该设备类型？"
                                        onConfirm={handleDeleteDeviceType}
                                        disabled={!activeTypeId}
                                    >
                                        <Button size="small" danger disabled={!activeTypeId}>
                                            删除
                                        </Button>
                                    </Popconfirm>
                                )}
                                {canExport && (
                                    <Tooltip title="导出">
                                        <Button
                                            size="small"
                                            icon={<DownloadOutlined />}
                                            disabled={loadingTypes || importing}
                                            loading={exporting}
                                            onClick={exportConfigs}
                                        />
                                    </Tooltip>
                                )}
                                {canImport && (
                                    <Tooltip title="导入">
                                        <Button
                                            size="small"
                                            icon={<UploadOutlined />}
                                            disabled={exporting}
                                            loading={importing}
                                            onClick={triggerImport}
                                        />
                                    </Tooltip>
                                )}
                            </Space>
                        }
                    >
                        {loadingTypes ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : types.length === 0 ? (
                            <Empty description="暂无设备类型" />
                        ) : (
                            <Tree
                                blockNode
                                className="[&_.ant-tree-switcher]:hidden"
                                selectedKeys={activeTypeId ? [String(activeTypeId)] : []}
                                onSelect={(keys) => {
                                    if (keys.length > 0) {
                                        setSelectedTypeId(String(keys[0]));
                                    }
                                }}
                                treeData={types.map((t) => {
                                    const config = t.config as SL651.Config;
                                    const mode = config?.responseMode || 'M1';
                                    return {
                                        key: String(t.id),
                                        title: (
                                            <Tooltip
                                                title={t.remark || '暂无备注'}
                                                placement="right"
                                            >
                                                <Flex
                                                    justify="space-between"
                                                    align="center"
                                                    className="h-8 p-1"
                                                >
                                                    <Space size={4}>
                                                        <span>{t.name}</span>
                                                        <Tag color="blue">{mode}</Tag>
                                                    </Space>
                                                    {t.enabled ? (
                                                        <Tag color="green">启用</Tag>
                                                    ) : (
                                                        <Tag color="red">禁用</Tag>
                                                    )}
                                                </Flex>
                                            </Tooltip>
                                        ),
                                    };
                                })}
                            />
                        )}
                    </Card>
                </div>

                {/* 右侧：功能码配置 */}
                <div className="min-h-0 min-w-0 flex-1">
                    <Card
                        title={
                            <Flex justify="space-between" align="center" gap={8} wrap>
                                <div className="min-w-0 whitespace-normal">
                                    {activeTypeId
                                        ? '功能码配置'
                                        : types.length > 0
                                          ? '请选择设备类型'
                                          : '暂无设备类型'}
                                </div>
                                <div className="shrink-0">
                                    {activeTypeId && canAdd && (
                                        <Space wrap>
                                            <Button
                                                type="primary"
                                                onClick={() =>
                                                    funcModalRef.current?.open(
                                                        'create',
                                                        activeTypeId
                                                    )
                                                }
                                            >
                                                新增功能码
                                            </Button>
                                            <Button onClick={() => derivedEditor.current?.create()}>
                                                新增派生值
                                            </Button>
                                        </Space>
                                    )}
                                </div>
                            </Flex>
                        }
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 0 },
                        }}
                    >
                        {!activeTypeId ? (
                            <Empty description={emptyTypeDesc} />
                        ) : loadingFuncs ? (
                            <Skeleton active paragraph={{ rows: 6 }} className="p-4" />
                        ) : funcs.length === 0 ? (
                            <Empty description={emptyFuncDesc} />
                        ) : (
                            <VirtualStack
                                items={funcs}
                                getItemKey={(record) => record.id}
                                className="p-4"
                                estimateSize={520}
                                gap={16}
                            >
                                {(record) => renderFuncCard(record)}
                            </VirtualStack>
                        )}
                        <DerivedPointsEditor
                            key={types.find((item) => item.id === activeTypeId)?.id}
                            ref={derivedEditor}
                            item={types.find((item) => item.id === activeTypeId)}
                            editable={canEdit}
                        />
                    </Card>
                </div>
            </div>

            {/* 设备类型 Modal */}
            <Sl651DeviceTypeModalDeviceTypeModal
                ref={deviceTypeModalRef}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />

            {/* 功能码 Modal */}
            <FuncModal
                ref={funcModalRef}
                types={types}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />

            {/* 要素 Modal */}
            <ElementModal
                ref={elementModalRef}
                types={types}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />

            {/* 预设值 Modal */}
            <PresetValueModal
                ref={presetValueModalRef}
                types={types}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />

            {/* 字典配置 Modal */}
            <DictConfigModal
                ref={dictConfigModalRef}
                types={types}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />

            {/* 应答要素 Modal */}
            <ResponseElementsModal
                ref={responseElementsModalRef}
                types={types}
                onSuccess={refetchTypes}
                saveMutation={saveMutation}
            />
        </PageContainer>
    );
};

export { ModbusConfigPage, S7ConfigPage, SL651ConfigPage };

export type SaveMutation = ReturnType<typeof useProtocolConfigSave>;

function IndustrialConfigPage({ protocol }: { protocol: IndustrialProtocol }) {
    const derivedEditor = useRef<DerivedPointsEditorRef>(null);
    const { has } = usePermissions();
    const { message } = App.useApp();
    const canQuery = has('iot:protocol:query');
    const canEdit = has('iot:protocol:edit');
    const { data, isLoading, error } = useProtocolConfigList({ protocol }, { enabled: canQuery });
    const types = useMemo(() => data ?? [], [data]);
    const save = useProtocolConfigSave();
    const remove = useProtocolConfigDelete();
    const transfer = useProtocolImportExport(protocol);
    const [selected, setSelected] = useState<string>();
    const active = types.find((item) => item.id === selected) ?? types[0];
    const config = active?.config as IndustrialConfig | undefined;
    const [typeOpen, setTypeOpen] = useState(false);
    const [editingType, setEditingType] = useState<Protocol.Item>();
    const [pointOpen, setPointOpen] = useState(false);
    const [editingPoint, setEditingPoint] = useState<string>();
    const [pointTypeId, setPointTypeId] = useState<string>();
    const tableViewport = useRef<HTMLDivElement>(null);
    const [tableBodyHeight, setTableBodyHeight] = useState(240);
    useEffect(() => {
        if (!active?.id) return;
        const viewport = tableViewport.current;
        if (!viewport) return;
        const observer = new ResizeObserver(([entry]) => {
            setTableBodyHeight(Math.max(80, Math.floor(entry.contentRect.height) - 96));
        });
        observer.observe(viewport);
        return () => observer.disconnect();
    }, [active?.id]);
    const [typeForm] = Form.useForm<{
        name: string;
        enabled: boolean;
        remark?: string;
        config: IndustrialConfig;
    }>();
    const [pointForm] = Form.useForm<IndustrialPoint & { visible: boolean }>();
    const create645Point = useDlt645PointCreate();
    const pointType = Form.useWatch('dataType', pointForm);
    const label =
        protocol === 'MC' ? '三菱 MC / SLMP' : protocol === 'FINS' ? '欧姆龙 FINS' : 'DL/T645';
    const connectionFields: {
        name: keyof IndustrialConfig['connection'];
        label: string;
        max: number;
        min?: number;
    }[] =
        protocol === 'MC'
            ? [
                  { name: 'network', label: '网络号', max: 255 },
                  { name: 'station', label: '目标站号（本机 255）', max: 255 },
                  { name: 'moduleIo', label: '目标模块 I/O 号（十进制）', max: 65535 },
                  { name: 'multidrop', label: '多点站号', max: 255 },
                  {
                      name: 'monitoringTimer',
                      label: '监视定时器（每单位 250 ms）',
                      min: 1,
                      max: 65535,
                  },
              ]
            : protocol === 'FINS'
              ? [
                    { name: 'sourceNetwork', label: '源网络号', max: 127 },
                    { name: 'sourceNode', label: '源节点号（0 为自动分配）', max: 254 },
                    { name: 'sourceUnit', label: '源单元号', max: 255 },
                    { name: 'destinationNetwork', label: '目标网络号', max: 127 },
                    { name: 'destinationNode', label: '目标节点号（0 使用握手结果）', max: 254 },
                    { name: 'destinationUnit', label: '目标单元号', max: 255 },
                ]
              : [{ name: 'wakeupBytes', label: '唤醒字节数', max: 4 }];
    const openType = (item?: Protocol.Item) => {
        setEditingType(item);
        typeForm.resetFields();
        typeForm.setFieldsValue(
            item
                ? { ...item, config: item.config as IndustrialConfig }
                : {
                      name: '',
                      enabled: true,
                      config: {
                          storagePolicy: 'report',
                          readInterval: 300,
                          commandFastReadDuration: 60,
                          commandFastReadInterval: 1,
                          points: [],
                          connection:
                              protocol === 'MC'
                                  ? {
                                        frame: '3E',
                                        network: 0,
                                        station: 255,
                                        moduleIo: 1023,
                                        multidrop: 0,
                                        monitoringTimer: 16,
                                    }
                                  : protocol === 'FINS'
                                    ? {
                                          sourceNetwork: 0,
                                          sourceNode: 0,
                                          sourceUnit: 0,
                                          destinationNetwork: 0,
                                          destinationNode: 0,
                                          destinationUnit: 0,
                                      }
                                    : {
                                          version: '2007',
                                          wakeupBytes: 4,
                                          writePassword: '',
                                          operatorCode: '',
                                      },
                      },
                  }
        );
        setTypeOpen(true);
    };
    const saveConfig = async (next: IndustrialConfig, item = active) => {
        if (!item) return;
        const validation = industrialConfigSchema(protocol).safeParse(next);
        if (!validation.success) {
            message.error(validation.error.issues[0].message);
            return false;
        }
        await save.mutateAsync({ id: item.id, protocol, config: next });
        return true;
    };
    const openPoint = (point?: IndustrialPoint) => {
        setPointTypeId(active?.id);
        setEditingPoint(point?.id);
        pointForm.resetFields();
        pointForm.setFieldsValue(
            point ?? {
                ...(protocol === 'DLT645' ? {} : { id: createUuid() }),
                name: '',
                dataType: protocol === 'DLT645' ? 'BCD' : 'UINT16',
                writable: false,
                ...(protocol === 'DLT645'
                    ? {
                          identifier: config?.connection.version === '1997' ? '9010' : '00000000',
                          length: 4,
                          digits: 2,
                      }
                    : {
                          area: 'D',
                          address: 0,
                          bit: 0,
                          byteOrder: protocol === 'MC' ? 'LITTLE_ENDIAN' : 'BIG_ENDIAN',
                          scale: 1,
                          decimals: -1,
                      }),
            }
        );
        pointForm.setFieldValue('visible', !point || config?.pointVisibility?.[point.id] !== false);
        setPointOpen(true);
    };
    if (!canQuery || error) {
        return (
            <PageContainer title={`${label}配置`}>
                <Result
                    status={error ? 'error' : '403'}
                    title={error ? '设备类型加载失败' : '无权访问'}
                    subTitle={error?.message ?? '您没有权限访问此页面'}
                />
            </PageContainer>
        );
    }
    return (
        <PageContainer title={`${label}配置`}>
            <div className="flex h-full min-h-0 flex-col gap-3 overflow-hidden lg:flex-row lg:gap-0">
                <div className="h-48 min-h-0 shrink-0 lg:h-full lg:w-[360px] lg:pr-3">
                    <Card
                        title="设备类型"
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{ body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 16 } }}
                        extra={
                            <Space size={4}>
                                {has('iot:protocol:add') && (
                                    <Button size="small" type="primary" onClick={() => openType()}>
                                        新增
                                    </Button>
                                )}
                                {canEdit && (
                                    <Button
                                        size="small"
                                        disabled={!active}
                                        onClick={() => openType(active)}
                                    >
                                        编辑
                                    </Button>
                                )}
                                {has('iot:protocol:delete') && (
                                    <Popconfirm
                                        title="确认删除该设备类型？"
                                        disabled={!active}
                                        onConfirm={() => active && remove.mutateAsync(active.id)}
                                    >
                                        <Button
                                            size="small"
                                            danger
                                            disabled={!active}
                                            loading={remove.isPending}
                                        >
                                            删除
                                        </Button>
                                    </Popconfirm>
                                )}
                                {has('iot:protocol:export') && (
                                    <Tooltip title="导出">
                                        <Button
                                            size="small"
                                            aria-label="导出"
                                            icon={<DownloadOutlined />}
                                            disabled={isLoading || transfer.importing}
                                            loading={transfer.exporting}
                                            onClick={transfer.exportConfigs}
                                        />
                                    </Tooltip>
                                )}
                                {has('iot:protocol:import') && has('iot:protocol:add') && (
                                    <Tooltip title="导入">
                                        <Button
                                            size="small"
                                            aria-label="导入"
                                            icon={<UploadOutlined />}
                                            disabled={transfer.exporting}
                                            loading={transfer.importing}
                                            onClick={transfer.triggerImport}
                                        />
                                    </Tooltip>
                                )}
                            </Space>
                        }
                    >
                        {isLoading ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : types.length === 0 ? (
                            <Empty description="暂无设备类型" />
                        ) : (
                            <Tree
                                blockNode
                                className="[&_.ant-tree-switcher]:hidden"
                                selectedKeys={active ? [active.id] : []}
                                onSelect={(keys) => {
                                    if (keys.length) setSelected(String(keys[0]));
                                }}
                                treeData={types.map((item) => ({
                                    key: item.id,
                                    title: (
                                        <Tooltip
                                            title={item.remark || '暂无备注'}
                                            placement="right"
                                        >
                                            <Flex
                                                justify="space-between"
                                                align="center"
                                                gap={4}
                                                className="h-8 min-w-0 p-1"
                                            >
                                                <span className="min-w-0 truncate">
                                                    {item.name}
                                                </span>
                                                <Space size={4} className="shrink-0">
                                                    <Tag color="blue">
                                                        {(item.config as IndustrialConfig).points
                                                            ?.length ?? 0}
                                                        个点位
                                                    </Tag>
                                                    <Tag color={item.enabled ? 'green' : 'red'}>
                                                        {item.enabled ? '启用' : '禁用'}
                                                    </Tag>
                                                </Space>
                                            </Flex>
                                        </Tooltip>
                                    ),
                                }))}
                            />
                        )}
                    </Card>
                </div>
                <div className="min-h-0 min-w-0 flex-1">
                    <Card
                        title={
                            <Flex justify="space-between" align="center" gap={8} wrap>
                                <div className="min-w-0 whitespace-normal">
                                    {active ? (
                                        <Space size={4} wrap>
                                            <span>点位配置</span>
                                            <Tag>
                                                {protocol === 'MC'
                                                    ? `${config?.connection.frame ?? '3E'} 帧`
                                                    : protocol === 'DLT645'
                                                      ? `DL/T645-${config?.connection.version ?? '2007'}`
                                                      : 'FINS/TCP'}
                                            </Tag>
                                            <Tag>间隔 {config?.readInterval ?? 300}s</Tag>
                                            <Tag color="blue">
                                                {config?.points.length ?? 0} 个点位
                                            </Tag>
                                            {!!config?.points.some((point) => point.writable) && (
                                                <Tag color="orange">
                                                    {
                                                        config.points.filter(
                                                            (point) => point.writable
                                                        ).length
                                                    }{' '}
                                                    个可写
                                                </Tag>
                                            )}
                                        </Space>
                                    ) : (
                                        '暂无设备类型'
                                    )}
                                </div>
                                <div className="shrink-0">
                                    {active && canEdit && (
                                        <Space wrap>
                                            <Button type="primary" onClick={() => openPoint()}>
                                                新增点位
                                            </Button>
                                            <Button onClick={() => derivedEditor.current?.create()}>
                                                新增派生值
                                            </Button>
                                        </Space>
                                    )}
                                </div>
                            </Flex>
                        }
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            header: { flexShrink: 0 },
                            body: {
                                display: 'flex',
                                flexDirection: 'column',
                                flex: 1,
                                minHeight: 0,
                                overflow: 'auto',
                                padding: 16,
                            },
                        }}
                    >
                        {isLoading ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : active ? (
                            <div
                                ref={tableViewport}
                                className="min-h-[320px] shrink-0 overflow-hidden"
                            >
                                <Table<IndustrialPoint>
                                    key={active.id}
                                    rowKey="id"
                                    size="small"
                                    loading={isLoading}
                                    dataSource={config?.points ?? []}
                                    pagination={{
                                        pageSize: 20,
                                        showSizeChanger: false,
                                        showTotal: (total) => `共 ${total} 个点位`,
                                    }}
                                    locale={{
                                        emptyText: (
                                            <Empty description="暂无点位，点击右上角新增点位" />
                                        ),
                                    }}
                                    scroll={{ x: 920, y: tableBodyHeight }}
                                    columns={[
                                        { title: '名称', dataIndex: 'name', width: 160 },
                                        ...(protocol === 'DLT645'
                                            ? []
                                            : [
                                                  {
                                                      title: '点位标识',
                                                      dataIndex: 'id',
                                                      width: 170,
                                                      ellipsis: true,
                                                  },
                                              ]),
                                        {
                                            title: protocol === 'DLT645' ? '数据标识 DI' : '地址',
                                            width: 140,
                                            render: (_, point) =>
                                                protocol === 'DLT645'
                                                    ? point.identifier
                                                    : `${point.area}${point.address}${protocol === 'FINS' && point.dataType === 'BOOL' ? `.${point.bit ?? 0}` : ''}`,
                                        },
                                        { title: '数据类型', dataIndex: 'dataType', width: 110 },
                                        { title: '单位', dataIndex: 'unit', width: 80 },
                                        {
                                            title: '读写',
                                            width: 80,
                                            render: (_, point) =>
                                                point.writable ? '可写' : '只读',
                                        },
                                        {
                                            title: '操作',
                                            width: 140,
                                            fixed: 'right',
                                            render: (_, point) =>
                                                canEdit && (
                                                    <Space>
                                                        <Button
                                                            type="link"
                                                            size="small"
                                                            onClick={() => openPoint(point)}
                                                        >
                                                            编辑
                                                        </Button>
                                                        <Popconfirm
                                                            title="删除此点位？"
                                                            onConfirm={() =>
                                                                config &&
                                                                saveConfig({
                                                                    ...config,
                                                                    points: config.points.filter(
                                                                        (item) =>
                                                                            item.id !== point.id
                                                                    ),
                                                                })
                                                            }
                                                        >
                                                            <Button type="link" size="small" danger>
                                                                删除
                                                            </Button>
                                                        </Popconfirm>
                                                    </Space>
                                                ),
                                        },
                                    ]}
                                />
                            </div>
                        ) : (
                            <Empty description="暂无设备类型，请先新增设备类型" />
                        )}
                        <DerivedPointsEditor
                            key={active?.id}
                            ref={derivedEditor}
                            item={active}
                            editable={canEdit}
                        />
                    </Card>
                </div>
            </div>
            <FormModal
                open={typeOpen}
                title={`${editingType ? '编辑' : '新增'}${label}设备类型`}
                onCancel={() => setTypeOpen(false)}
                confirmLoading={save.isPending}
                onOk={async () => {
                    const values = await typeForm.validateFields();
                    const next = {
                        ...values.config,
                        points: (editingType?.config as IndustrialConfig | undefined)?.points ?? [],
                    };
                    const validation = industrialConfigSchema(protocol).safeParse(next);
                    if (!validation.success) {
                        message.error(validation.error.issues[0].message);
                        return;
                    }
                    await save.mutateAsync({
                        ...values,
                        protocol,
                        id: editingType?.id,
                        config: next,
                    });
                    setTypeOpen(false);
                }}
            >
                <Form name={`industrial-type-${protocol}`} form={typeForm} layout="vertical">
                    <Divider titlePlacement="start" plain className="!my-4">
                        基础信息
                    </Divider>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="name" label="名称" rules={[{ required: true, max: 64 }]}>
                            <Input />
                        </Form.Item>
                        <Form.Item name="enabled" label="启用" valuePropName="checked">
                            <Switch />
                        </Form.Item>
                    </div>
                    <Divider titlePlacement="start" plain className="!my-4">
                        连接参数
                    </Divider>
                    {protocol === 'MC' && (
                        <Form.Item name={['config', 'connection', 'frame']} label="二进制帧格式">
                            <Select
                                options={[
                                    { value: '3E', label: '3E' },
                                    { value: '4E', label: '4E（带事务号）' },
                                ]}
                            />
                        </Form.Item>
                    )}
                    {protocol === 'DLT645' && (
                        <Form.Item name={['config', 'connection', 'version']} label="协议版本">
                            <Select
                                options={[
                                    { value: '2007', label: 'DL/T645-2007' },
                                    { value: '1997', label: 'DL/T645-1997' },
                                ]}
                            />
                        </Form.Item>
                    )}
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        {connectionFields.map((field) => (
                            <Form.Item
                                key={field.name}
                                name={['config', 'connection', field.name]}
                                label={field.label}
                                rules={[{ required: true }]}
                            >
                                <InputNumber
                                    className="w-full"
                                    min={field.min ?? 0}
                                    max={field.max}
                                    precision={0}
                                />
                            </Form.Item>
                        ))}
                    </div>
                    {protocol === 'DLT645' && (
                        <>
                            <Form.Item
                                name={['config', 'connection', 'writePassword']}
                                label="写入认证（权限字节＋密码，按报文字节顺序）"
                                extra="只读采集可留空；配置可写点位时需填写。"
                            >
                                <Input.Password maxLength={8} autoComplete="new-password" />
                            </Form.Item>
                            <Form.Item
                                name={['config', 'connection', 'operatorCode']}
                                label="操作者代码（2007 版写入需要，8 位 HEX）"
                            >
                                <Input maxLength={8} />
                            </Form.Item>
                        </>
                    )}
                    <Divider titlePlacement="start" plain className="!my-4">
                        采集与存储
                    </Divider>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name={['config', 'storagePolicy']} label="历史存储策略">
                            <Select options={STORAGE_POLICY_OPTIONS} />
                        </Form.Item>
                        <Form.Item name={['config', 'readInterval']} label="采集间隔（秒）">
                            <InputNumber className="w-full" min={1} max={3600} precision={0} />
                        </Form.Item>
                    </div>
                    <Divider titlePlacement="start" plain className="!my-4">
                        下发快读
                    </Divider>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item
                            name={['config', 'commandFastReadDuration']}
                            label="写入后快读时长（秒，0 关闭）"
                        >
                            <InputNumber className="w-full" min={0} max={3600} precision={0} />
                        </Form.Item>
                        <Form.Item
                            name={['config', 'commandFastReadInterval']}
                            label="快读间隔（秒）"
                        >
                            <InputNumber className="w-full" min={1} max={3600} precision={0} />
                        </Form.Item>
                    </div>
                    <Divider titlePlacement="start" plain className="!my-4">
                        备注
                    </Divider>
                    <Form.Item name="remark" label="备注">
                        <Input.TextArea rows={3} placeholder="选填，说明设备类型的用途" />
                    </Form.Item>
                </Form>
            </FormModal>
            <FormModal
                open={pointOpen}
                title={editingPoint ? '编辑点位' : '新增点位'}
                onCancel={() => setPointOpen(false)}
                confirmLoading={save.isPending || create645Point.isPending}
                onOk={async () => {
                    const target = types.find((item) => item.id === pointTypeId);
                    if (!target) return;
                    const targetConfig = target.config as IndustrialConfig;
                    const { visible, ...values } = await pointForm.validateFields();
                    if (protocol === 'DLT645' && !editingPoint) {
                        const { id: _id, ...point } = values;
                        await create645Point.mutateAsync({
                            id: target.id,
                            point: { ...point, visible },
                        });
                        setPointOpen(false);
                        return;
                    }
                    const point = {
                        ...values,
                        id: editingPoint ?? values.id,
                    };
                    const points = editingPoint
                        ? targetConfig.points.map((item) =>
                              item.id === editingPoint ? point : item
                          )
                        : [...targetConfig.points, point];
                    if (
                        await saveConfig(
                            {
                                ...targetConfig,
                                points,
                                pointVisibility: {
                                    ...targetConfig.pointVisibility,
                                    [point.id]: visible,
                                },
                            },
                            target
                        )
                    )
                        setPointOpen(false);
                }}
            >
                <Form name={`industrial-point-${protocol}`} form={pointForm} layout="vertical">
                    <PointDisplayField />
                    <Divider titlePlacement="start" plain className="!my-4">
                        基础信息
                    </Divider>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        {protocol !== 'DLT645' && (
                            <Form.Item
                                name="id"
                                label="点位标识"
                                rules={[{ required: true, max: 64 }]}
                            >
                                <Input disabled={Boolean(editingPoint)} />
                            </Form.Item>
                        )}
                        <Form.Item
                            name="name"
                            label="名称"
                            rules={[{ required: true, max: protocol === 'DLT645' ? 100 : 128 }]}
                        >
                            <Input />
                        </Form.Item>
                    </div>
                    <Divider titlePlacement="start" plain className="!my-4">
                        地址与数据解析
                    </Divider>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="dataType" label="数据类型" rules={[{ required: true }]}>
                            <Select
                                onChange={() => pointForm.setFieldValue('bit', 0)}
                                options={(protocol === 'DLT645'
                                    ? ['BCD', 'BCD_SIGNED', 'HEX']
                                    : [
                                          'BOOL',
                                          'INT16',
                                          'UINT16',
                                          'INT32',
                                          'UINT32',
                                          'FLOAT32',
                                          'INT64',
                                          'UINT64',
                                          'DOUBLE',
                                      ]
                                ).map((value) => ({ value, label: value }))}
                            />
                        </Form.Item>
                        {protocol === 'DLT645' ? (
                            <>
                                <Form.Item
                                    name="identifier"
                                    label="数据标识 DI（高字节在前）"
                                    rules={[{ required: true }]}
                                >
                                    <Input
                                        maxLength={config?.connection.version === '1997' ? 4 : 8}
                                    />
                                </Form.Item>
                                <Form.Item
                                    name="length"
                                    label="数据长度（字节，不包含 DI）"
                                    rules={[{ required: true }]}
                                >
                                    <InputNumber
                                        className="w-full"
                                        min={1}
                                        max={pointType === 'HEX' ? 200 : 8}
                                        precision={0}
                                    />
                                </Form.Item>
                                <Form.Item name="digits" label="小数位数">
                                    <InputNumber className="w-full" min={0} max={8} precision={0} />
                                </Form.Item>
                            </>
                        ) : (
                            <>
                                <Form.Item
                                    name="area"
                                    label="存储区域"
                                    rules={[{ required: true }]}
                                >
                                    <Select
                                        options={(protocol === 'MC'
                                            ? [
                                                  'D',
                                                  'W',
                                                  'R',
                                                  'ZR',
                                                  'M',
                                                  'X',
                                                  'Y',
                                                  'B',
                                                  'L',
                                                  'F',
                                                  'V',
                                                  'S',
                                                  'TN',
                                                  'CN',
                                                  'TS',
                                                  'CS',
                                              ]
                                            : ['D', 'CIO', 'W', 'H', 'A']
                                        ).map((value) => ({ value, label: value }))}
                                    />
                                </Form.Item>
                                <Form.Item
                                    name="address"
                                    label="地址（十进制）"
                                    extra={
                                        protocol === 'MC'
                                            ? 'X、Y、B、W 等十六进制地址先换算为十进制，例如 X10 填 16。'
                                            : undefined
                                    }
                                    rules={[{ required: true }]}
                                >
                                    <InputNumber
                                        className="w-full"
                                        min={0}
                                        max={protocol === 'MC' ? 16777215 : 65535}
                                        precision={0}
                                    />
                                </Form.Item>
                                {protocol === 'FINS' && pointType === 'BOOL' && (
                                    <Form.Item name="bit" label="位号">
                                        <InputNumber
                                            className="w-full"
                                            min={0}
                                            max={15}
                                            precision={0}
                                        />
                                    </Form.Item>
                                )}
                                <Form.Item name="byteOrder" label="字节序">
                                    <Select options={ByteOrderOptions} />
                                </Form.Item>
                                <Form.Item name="scale" label="采集值缩放系数">
                                    <InputNumber className="w-full" />
                                </Form.Item>
                                <Form.Item name="decimals" label="采集值小数位（-1 保留原精度）">
                                    <InputNumber
                                        className="w-full"
                                        min={-1}
                                        max={8}
                                        precision={0}
                                    />
                                </Form.Item>
                            </>
                        )}
                    </div>
                    <Divider titlePlacement="start" plain className="!my-4">
                        显示与控制
                    </Divider>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="unit" label="单位">
                            <Input />
                        </Form.Item>
                        <Form.Item name="writable" label="允许写入" valuePropName="checked">
                            <Switch />
                        </Form.Item>
                    </div>
                </Form>
            </FormModal>
        </PageContainer>
    );
}

export const McConfigPage = () => <IndustrialConfigPage protocol="MC" />;
export const FinsConfigPage = () => <IndustrialConfigPage protocol="FINS" />;
export const Dlt645ConfigPage = () => <IndustrialConfigPage protocol="DLT645" />;

const MQTT_REPORT_EXAMPLE = JSON.stringify(
    {
        deviceId: '$deviceCode',
        timestamp: '$time',
        data: { temperature: '$point:温度', humidity: '$point:湿度', online: '$point:在线' },
    },
    null,
    2
);
const MQTT_POINT_TYPES = [
    { value: 'DOUBLE', label: '数值' },
    { value: 'BOOL', label: '布尔' },
    { value: 'STRING', label: '文本' },
];
const EMPTY_MQTT_POINTS: MqttPoint[] = [];
const EMPTY_MQTT_COMMANDS: MqttCommandMessage[] = [];

function MqttMessageInput({
    value = '',
    onChange,
    points,
    label,
    topic = false,
    report = false,
}: {
    value?: string;
    onChange?: (value: string) => void;
    points: readonly MqttPoint[];
    label: string;
    topic?: boolean;
    report?: boolean;
}) {
    const selection = useRef<[number, number] | null>(null);
    const { message } = App.useApp();
    const options = [
        { label: '设备编码', value: topic ? '{deviceCode}' : '"$deviceCode"' },
        ...(!topic && report ? [{ label: '设备时间', value: '"$time"' }] : []),
        ...(!topic && !report ? [{ label: '自动指令编号', value: '"$commandId"' }] : []),
        ...(topic && report
            ? []
            : points
                  .filter((point) => point.name.trim())
                  .map((point) => ({
                      label: point.name,
                      value: topic
                          ? `{point:${point.name}}`
                          : JSON.stringify(`$point:${point.name}`),
                  }))),
    ];
    const input = {
        value,
        'aria-label': label,
        spellCheck: false,
        onChange: (event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
            onChange?.(event.target.value),
        onSelect: (event: React.SyntheticEvent<HTMLInputElement | HTMLTextAreaElement>) => {
            const target = event.currentTarget;
            selection.current = [
                target.selectionStart ?? value.length,
                target.selectionEnd ?? value.length,
            ];
        },
    };
    return (
        <div>
            {!topic && (
                <Button
                    size="small"
                    className="mb-2"
                    aria-label={`${label}格式化`}
                    onClick={() => {
                        try {
                            onChange?.(formatMqttTemplate(value));
                            selection.current = null;
                        } catch (error) {
                            message.error(error instanceof Error ? error.message : 'JSON 格式无效');
                        }
                    }}
                >
                    格式化 JSON
                </Button>
            )}
            {topic ? (
                <Input {...input} maxLength={1024} />
            ) : (
                <Input.TextArea {...input} rows={7} className="font-mono" />
            )}
            <Select<string>
                className="mt-2 w-full"
                aria-label={`${label}插入属性`}
                placeholder="在光标处插入属性或设备变量"
                value={undefined}
                options={options}
                onSelect={(token: string) => {
                    const [start, end] = selection.current ?? [value.length, value.length];
                    onChange?.(value.slice(0, start) + token + value.slice(end));
                    selection.current = [start + token.length, start + token.length];
                }}
            />
        </div>
    );
}

export function MqttConfigPage() {
    const { has } = usePermissions();
    const { message } = App.useApp();
    const canQuery = has('iot:protocol:query');
    const canEdit = has('iot:protocol:edit');
    const { data, isLoading, error, refetch } = useProtocolConfigList(
        { protocol: 'MQTT' },
        { enabled: canQuery }
    );
    const save = useProtocolConfigSave();
    const remove = useProtocolConfigDelete();
    const [selected, setSelected] = useState<string>();
    const [editing, setEditing] = useState<Protocol.Item>();
    const [open, setOpen] = useState(false);
    const [tab, setTab] = useState('points');
    const [expandedPoints, setExpandedPoints] = useState<string[]>([]);
    const [form] = Form.useForm<MqttTemplateDraft & { name: string; enabled: boolean }>();
    const points: MqttPoint[] =
        Form.useWatch('points', { form, preserve: true }) ?? EMPTY_MQTT_POINTS;
    const commands: MqttCommandMessage[] =
        Form.useWatch('commands', { form, preserve: true }) ?? EMPTY_MQTT_COMMANDS;
    const [formError, setFormError] = useState('');
    const [sampleTopic, setSampleTopic] = useState('devices/D001/telemetry');
    const [sampleCode, setSampleCode] = useState('D001');
    const [sampleTimezone, setSampleTimezone] = useState('+08:00');
    const [samplePayload, setSamplePayload] = useState(
        '{"deviceId":"D001","timestamp":1790740800123,"data":{"temperature":25.6,"humidity":60,"online":true}}'
    );
    const [preview, setPreview] = useState<{
        records: (MqttPreviewResult['records'][number] & { key: string })[];
    }>();
    const [previewError, setPreviewError] = useState('');
    const [previewBusy, setPreviewBusy] = useState(false);
    const previewSequence = useRef(0);
    const active = data?.find((item) => item.id === selected) ?? data?.[0];
    const config = active?.config as MqttConfig | undefined;
    const viewport = useRef<HTMLDivElement>(null);
    const editor = useRef<HTMLDivElement>(null);
    useEffect(() => {
        if (formError) editor.current?.parentElement?.scrollTo({ top: 0 });
    }, [formError]);
    const [bodyHeight, setBodyHeight] = useState(240);
    useEffect(() => {
        if (!active?.id) return;
        const element = viewport.current;
        if (!element) return;
        const observer = new ResizeObserver(([entry]) =>
            setBodyHeight(Math.max(80, Math.floor(entry.contentRect.height) - 100))
        );
        observer.observe(element);
        return () => observer.disconnect();
    }, [active?.id]);
    const invalidatePreview = () => {
        ++previewSequence.current;
        setPreview(undefined);
        setPreviewError('');
        setPreviewBusy(false);
    };
    const edit = (item?: Protocol.Item, initialTab = 'points', pointId?: string) => {
        let draft: MqttTemplateDraft;
        try {
            draft = item
                ? mqttTemplateDraft(item.config as MqttConfig)
                : {
                      topic: 'devices/{deviceCode}/telemetry',
                      reportTemplate: MQTT_REPORT_EXAMPLE,
                      commands: [],
                      storagePolicy: 'report',
                      timeFormat: 'unix_ms',
                      qos: 1,
                      points: [
                          {
                              id: crypto.randomUUID(),
                              name: '温度',
                              field: '',
                              dataType: 'DOUBLE',
                              unit: '℃',
                          },
                          {
                              id: crypto.randomUUID(),
                              name: '湿度',
                              field: '',
                              dataType: 'DOUBLE',
                              unit: '%',
                          },
                          { id: crypto.randomUUID(), name: '在线', field: '', dataType: 'BOOL' },
                      ],
                  };
            draft.points = compileMqttTemplate(draft).points;
            if (!draft.commands) {
                draft.commands =
                    draft.commandTopic && draft.commandTemplate
                        ? [
                              {
                                  id: crypto.randomUUID(),
                                  name: '控制消息',
                                  topic: draft.commandTopic,
                                  template: draft.commandTemplate,
                                  requiredPointIds: [],
                              },
                          ]
                        : [];
                // 旧模板数组占位符本来就必须提供；迁移时保留这一约束。
                for (const command of draft.commands) {
                    const findArrayPoints = (value: unknown): void => {
                        if (Array.isArray(value))
                            value.forEach((child) => {
                                if (typeof child === 'string' && child.startsWith('$point:')) {
                                    const point = draft.points.find(
                                        (point) => point.name === child.slice(7)
                                    );
                                    if (point && !command.requiredPointIds.includes(point.id))
                                        command.requiredPointIds.push(point.id);
                                }
                                findArrayPoints(child);
                            });
                        else if (value && typeof value === 'object')
                            Object.values(value).forEach(findArrayPoints);
                    };
                    findArrayPoints(JSON.parse(command.template));
                }
                draft.commandTopic = '';
                draft.commandTemplate = '';
            }
        } catch (error) {
            message.error(error instanceof Error ? error.message : '无法读取消息模板');
            return;
        }
        invalidatePreview();
        setFormError('');
        setEditing(item);
        setTab(initialTab);
        setExpandedPoints(pointId ? [pointId] : []);
        form.resetFields();
        form.setFieldsValue({ ...draft, name: item?.name ?? '', enabled: item?.enabled ?? true });
        setOpen(true);
    };
    const compile = () => {
        const compiled = compileMqttTemplate(form.getFieldsValue(true));
        form.setFieldValue('points', compiled.points);
        setFormError('');
        return compiled;
    };
    const validateMessages = () => {
        try {
            compile();
            message.success('属性引用与消息配置校验通过');
        } catch (error) {
            setFormError(error instanceof Error ? error.message : '模板无效');
        }
    };
    const close = () => {
        invalidatePreview();
        setOpen(false);
    };
    const submit = async () => {
        try {
            await form.validateFields();
        } catch {
            setFormError('请检查配置名称和点位属性。');
            return;
        }
        let compiled: MqttConfig;
        try {
            compiled = compile();
        } catch (error) {
            setFormError(error instanceof Error ? error.message : '模板无效');
            return;
        }
        const values = form.getFieldsValue(true);
        try {
            await save.mutateAsync({
                id: editing?.id,
                protocol: 'MQTT',
                name: values.name,
                enabled: values.enabled,
                config: compiled,
            });
            close();
            const refreshed = await refetch();
            setSelected(
                editing?.id ?? refreshed.data?.find((item) => item.name === values.name)?.id
            );
        } catch {
            // 保存错误由统一 mutation 提示，保留当前编辑内容。
        }
    };
    const parseSample = async () => {
        const sequence = ++previewSequence.current;
        setPreviewBusy(true);
        setPreview(undefined);
        setPreviewError('');
        try {
            const result = await previewMqttPayload({
                config: compile(),
                topic: sampleTopic,
                payload: samplePayload,
                deviceCode: sampleCode,
                timezone: sampleTimezone,
            });
            if (sequence === previewSequence.current)
                setPreview({
                    records: result.records.map((record) => ({ ...record, key: createUuid() })),
                });
        } catch (error) {
            if (sequence === previewSequence.current)
                setPreviewError(error instanceof Error ? error.message : '解析失败');
        } finally {
            if (sequence === previewSequence.current) setPreviewBusy(false);
        }
    };
    const columns: ColumnsType<MqttPoint> = [
        { title: '名称', dataIndex: 'name', width: 160 },
        { title: '点位标识', dataIndex: 'id', width: 170, ellipsis: true },
        { title: '消息路径', dataIndex: 'field', width: 200, ellipsis: true },
        {
            title: '数据类型',
            dataIndex: 'dataType',
            width: 100,
            render: (value) =>
                MQTT_POINT_TYPES.find((item) => item.value === value)?.label ?? value,
        },
        { title: '单位', dataIndex: 'unit', width: 80 },
        {
            title: '读写',
            dataIndex: 'writable',
            width: 80,
            render: (value) => (value ? '可写' : '只读'),
        },
        {
            title: '操作',
            width: 100,
            fixed: 'right',
            render: (_, point) =>
                canEdit && (
                    <Button
                        type="link"
                        size="small"
                        onClick={() => edit(active, 'points', point.id)}
                    >
                        编辑
                    </Button>
                ),
        },
    ];
    if (!canQuery || error)
        return (
            <PageContainer title="MQTT 配置">
                <Result
                    status={canQuery ? 'error' : '403'}
                    title={canQuery ? '配置加载失败' : '无权查看 MQTT 配置'}
                />
            </PageContainer>
        );
    return (
        <PageContainer title="MQTT 配置">
            <div className="flex h-full min-h-0 flex-col gap-3 overflow-hidden lg:flex-row lg:gap-0">
                <div className="h-48 min-h-0 shrink-0 lg:h-full lg:w-[360px] lg:pr-3">
                    <Card
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{ body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 16 } }}
                        title={
                            <Flex
                                justify="space-between"
                                align="center"
                                gap={8}
                                wrap
                                className="py-2"
                            >
                                <span>设备类型</span>
                                <Space size={4}>
                                    {has('iot:protocol:add') && (
                                        <Button size="small" type="primary" onClick={() => edit()}>
                                            新增
                                        </Button>
                                    )}
                                    {canEdit && (
                                        <Button
                                            size="small"
                                            disabled={!active}
                                            onClick={() => edit(active)}
                                        >
                                            编辑
                                        </Button>
                                    )}
                                    {has('iot:protocol:delete') && (
                                        <Popconfirm
                                            title="确认删除该设备类型？"
                                            disabled={!active}
                                            onConfirm={() =>
                                                active && remove.mutateAsync(active.id)
                                            }
                                        >
                                            <Button
                                                size="small"
                                                danger
                                                disabled={!active}
                                                loading={remove.isPending}
                                            >
                                                删除
                                            </Button>
                                        </Popconfirm>
                                    )}
                                </Space>
                            </Flex>
                        }
                    >
                        {isLoading ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : !data?.length ? (
                            <Empty description="暂无设备类型" />
                        ) : (
                            <Tree
                                blockNode
                                selectedKeys={active ? [active.id] : []}
                                onSelect={(keys) => {
                                    if (keys.length) setSelected(String(keys[0]));
                                }}
                                treeData={data.map((item) => ({
                                    key: item.id,
                                    title: (
                                        <Flex
                                            justify="space-between"
                                            align="center"
                                            gap={4}
                                            wrap
                                            className="min-w-0 p-1"
                                        >
                                            <span
                                                title={item.name}
                                                className="min-w-0 max-w-full truncate"
                                            >
                                                {item.name}
                                            </span>
                                            <Space size={4} className="shrink-0">
                                                <Tag color="blue">
                                                    {(item.config as MqttConfig).points?.length ??
                                                        0}{' '}
                                                    个点位
                                                </Tag>
                                                <Tag color={item.enabled ? 'green' : 'red'}>
                                                    {item.enabled ? '启用' : '禁用'}
                                                </Tag>
                                            </Space>
                                        </Flex>
                                    ),
                                }))}
                            />
                        )}
                    </Card>
                </div>
                <div className="min-h-0 min-w-0 flex-1">
                    <Card
                        title={
                            <Flex justify="space-between" align="center" gap={8} wrap>
                                <Space size={4} wrap>
                                    <span>属性配置</span>
                                    {active && (
                                        <>
                                            <Tag>MQTT 3.1.1</Tag>
                                            <Tag color="blue">
                                                {config?.points.length ?? 0} 个点位
                                            </Tag>
                                            <Tag color="orange">
                                                {config?.points.filter((point) => point.writable)
                                                    .length ?? 0}{' '}
                                                个可写
                                            </Tag>
                                        </>
                                    )}
                                </Space>
                                {active && canEdit && (
                                    <Button type="primary" onClick={() => edit(active)}>
                                        编辑属性与消息
                                    </Button>
                                )}
                            </Flex>
                        }
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{
                            header: { flexShrink: 0 },
                            body: {
                                display: 'flex',
                                flexDirection: 'column',
                                flex: 1,
                                minHeight: 0,
                                overflow: 'hidden',
                                padding: 16,
                            },
                        }}
                    >
                        {active ? (
                            <>
                                <div className="mb-3 shrink-0 space-y-1 break-all text-sm">
                                    <div>上报 Topic：{config?.topic}</div>
                                    {config?.commandTopic && (
                                        <div>控制 Topic：{config.commandTopic}</div>
                                    )}
                                    {config?.commands?.map((command) => (
                                        <div key={command.id}>
                                            {command.name}：{command.topic}
                                        </div>
                                    ))}
                                </div>
                                <div ref={viewport} className="min-h-0 flex-1">
                                    <Table
                                        key={active.id}
                                        rowKey="id"
                                        size="small"
                                        loading={isLoading}
                                        columns={columns}
                                        dataSource={config?.points ?? []}
                                        scroll={{ x: 890, y: bodyHeight }}
                                        pagination={{
                                            pageSize: 20,
                                            showSizeChanger: false,
                                            showTotal: (total) => `共 ${total} 个点位`,
                                        }}
                                    />
                                </div>
                            </>
                        ) : (
                            <Empty description={isLoading ? '正在加载' : '请选择或新增设备类型'} />
                        )}
                    </Card>
                </div>
            </div>
            <FormModal
                open={open}
                title={editing ? '编辑 MQTT 设备类型' : '新增 MQTT 设备类型'}
                onCancel={close}
                onOk={submit}
                confirmLoading={save.isPending}
                afterOpenChange={(visible) => {
                    if (visible) editor.current?.parentElement?.scrollTo({ top: 0 });
                }}
            >
                <div ref={editor}>
                    <Form
                        form={form}
                        layout="vertical"
                        onValuesChange={() => {
                            invalidatePreview();
                            setFormError('');
                        }}
                    >
                        {formError && (
                            <Alert type="error" showIcon title={formError} className="mb-3" />
                        )}
                        <div className="grid grid-cols-[minmax(0,1fr)_auto] gap-x-4">
                            <Form.Item
                                name="name"
                                label="配置名称"
                                rules={[
                                    {
                                        required: true,
                                        whitespace: true,
                                        max: 64,
                                        message: '请输入配置名称，最多 64 个字符',
                                    },
                                ]}
                            >
                                <Input placeholder="例如：温湿度传感器" maxLength={64} />
                            </Form.Item>
                            <Form.Item name="enabled" label="启用" valuePropName="checked">
                                <Switch />
                            </Form.Item>
                        </div>
                        <Tabs
                            activeKey={tab}
                            onChange={(key) => {
                                setTab(key);
                                editor.current?.parentElement?.scrollTo({ top: 0 });
                            }}
                            items={[
                                {
                                    key: 'points',
                                    label: `属性（${points.length}）`,
                                    forceRender: true,
                                    children: (
                                        <>
                                            <p className="mb-3 text-sm text-slate-500">
                                                先定义属性，再在上报或控制消息中引用。属性可以仅用于控制；修改模板不会删除属性。
                                            </p>
                                            <Table<MqttPoint>
                                                rowKey="id"
                                                size="small"
                                                pagination={false}
                                                dataSource={points}
                                                scroll={{ x: 580 }}
                                                columns={[
                                                    {
                                                        title: '属性名称',
                                                        width: 160,
                                                        render: (_, _point, index) => (
                                                            <Form.Item
                                                                name={['points', index, 'name']}
                                                                className="mb-0"
                                                                rules={[
                                                                    {
                                                                        required: true,
                                                                        whitespace: true,
                                                                        max: 100,
                                                                        message: '请输入属性名称',
                                                                    },
                                                                ]}
                                                            >
                                                                <Input
                                                                    aria-label={`属性 ${index + 1} 名称`}
                                                                    maxLength={100}
                                                                />
                                                            </Form.Item>
                                                        ),
                                                    },
                                                    {
                                                        title: '类型',
                                                        width: 120,
                                                        render: (_, point, index) => (
                                                            <Form.Item
                                                                name={['points', index, 'dataType']}
                                                                className="mb-0"
                                                            >
                                                                <Select
                                                                    aria-label={`${point.name}类型`}
                                                                    options={MQTT_POINT_TYPES}
                                                                />
                                                            </Form.Item>
                                                        ),
                                                    },
                                                    {
                                                        title: '单位',
                                                        width: 100,
                                                        render: (_, point, index) => (
                                                            <Form.Item
                                                                name={['points', index, 'unit']}
                                                                className="mb-0"
                                                            >
                                                                <Input
                                                                    aria-label={`${point.name}单位`}
                                                                    maxLength={32}
                                                                />
                                                            </Form.Item>
                                                        ),
                                                    },
                                                    {
                                                        title: '操作',
                                                        width: 75,
                                                        fixed: 'right',
                                                        render: (_, point) => (
                                                            <Popconfirm
                                                                title="删除此属性？相关消息引用需要同步修改。"
                                                                onConfirm={() => {
                                                                    form.setFieldValue(
                                                                        'points',
                                                                        form
                                                                            .getFieldValue('points')
                                                                            .filter(
                                                                                (
                                                                                    current: MqttPoint
                                                                                ) =>
                                                                                    current.id !==
                                                                                    point.id
                                                                            )
                                                                    );
                                                                    invalidatePreview();
                                                                }}
                                                            >
                                                                <Button
                                                                    size="small"
                                                                    type="text"
                                                                    danger
                                                                >
                                                                    删除
                                                                </Button>
                                                            </Popconfirm>
                                                        ),
                                                    },
                                                ]}
                                                expandable={{
                                                    expandedRowKeys: expandedPoints,
                                                    onExpandedRowsChange: (keys) =>
                                                        setExpandedPoints(keys.map(String)),
                                                    expandedRowRender: (point, index) => (
                                                        <>
                                                            <div className="mb-3 break-all text-xs text-slate-500">
                                                                属性 ID：{point.id}
                                                            </div>
                                                            <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                                                                <Form.Item
                                                                    name={[
                                                                        'points',
                                                                        index,
                                                                        'scale',
                                                                    ]}
                                                                    label="倍率"
                                                                    getValueFromEvent={(value) =>
                                                                        value ?? undefined
                                                                    }
                                                                >
                                                                    <InputNumber
                                                                        className="w-full"
                                                                        placeholder="1"
                                                                    />
                                                                </Form.Item>
                                                                <Form.Item
                                                                    name={[
                                                                        'points',
                                                                        index,
                                                                        'offset',
                                                                    ]}
                                                                    label="偏移"
                                                                    getValueFromEvent={(value) =>
                                                                        value ?? undefined
                                                                    }
                                                                >
                                                                    <InputNumber
                                                                        className="w-full"
                                                                        placeholder="0"
                                                                    />
                                                                </Form.Item>
                                                            </div>
                                                            <Form.List
                                                                name={[
                                                                    'points',
                                                                    index,
                                                                    'enumValues',
                                                                ]}
                                                            >
                                                                {(
                                                                    fields,
                                                                    { add, remove: removeValue }
                                                                ) => (
                                                                    <>
                                                                        {fields.map((field) => (
                                                                            <div
                                                                                key={field.key}
                                                                                className="grid grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] gap-x-2"
                                                                            >
                                                                                <Form.Item
                                                                                    name={[
                                                                                        field.name,
                                                                                        'input',
                                                                                    ]}
                                                                                    label="枚举原值"
                                                                                >
                                                                                    <Input
                                                                                        maxLength={
                                                                                            256
                                                                                        }
                                                                                    />
                                                                                </Form.Item>
                                                                                <Form.Item
                                                                                    name={[
                                                                                        field.name,
                                                                                        'output',
                                                                                    ]}
                                                                                    label="转换值"
                                                                                >
                                                                                    <Input
                                                                                        maxLength={
                                                                                            256
                                                                                        }
                                                                                    />
                                                                                </Form.Item>
                                                                                <Button
                                                                                    className="mt-7"
                                                                                    onClick={() =>
                                                                                        removeValue(
                                                                                            field.name
                                                                                        )
                                                                                    }
                                                                                >
                                                                                    删除
                                                                                </Button>
                                                                            </div>
                                                                        ))}
                                                                        <Button
                                                                            size="small"
                                                                            disabled={
                                                                                fields.length >= 64
                                                                            }
                                                                            onClick={() =>
                                                                                add({
                                                                                    input: '',
                                                                                    output: '',
                                                                                })
                                                                            }
                                                                        >
                                                                            添加枚举映射
                                                                        </Button>
                                                                    </>
                                                                )}
                                                            </Form.List>
                                                        </>
                                                    ),
                                                }}
                                            />
                                            <Button
                                                className="mt-3"
                                                disabled={points.length >= 256}
                                                onClick={() => {
                                                    form.setFieldValue('points', [
                                                        ...form.getFieldValue('points'),
                                                        {
                                                            id: crypto.randomUUID(),
                                                            name: '',
                                                            field: '',
                                                            dataType: 'DOUBLE',
                                                        },
                                                    ]);
                                                    invalidatePreview();
                                                }}
                                            >
                                                添加属性
                                            </Button>
                                        </>
                                    ),
                                },
                                {
                                    key: 'topics',
                                    label: 'Topic',
                                    forceRender: true,
                                    children: (
                                        <>
                                            <Form.Item
                                                name="topic"
                                                label="上报 Topic"
                                                extra="用 {deviceCode} 标记设备编码。订阅 Topic 不能引用尚未收到的属性值；共享 Topic 时，在消息中使用 $deviceCode。"
                                            >
                                                <MqttMessageInput
                                                    label="上报 Topic"
                                                    points={points}
                                                    topic
                                                    report
                                                />
                                            </Form.Item>
                                            <Form.Item
                                                name="deviceParameters"
                                                label="设备参数（JSON）"
                                                extra={
                                                    '按 deviceCode 绑定参数，控制模板用 "$param:参数名" 引用，保留参数的 JSON 类型。'
                                                }
                                                getValueProps={(value: unknown) => ({
                                                    value:
                                                        typeof value === 'string'
                                                            ? value
                                                            : JSON.stringify(value ?? [], null, 2),
                                                })}
                                                getValueFromEvent={(event) => event.target.value}
                                            >
                                                <Input.TextArea
                                                    rows={5}
                                                    placeholder={
                                                        '[{"deviceCode":"D001","name":"address","value":"A001"}]'
                                                    }
                                                />
                                            </Form.Item>
                                            <Divider titlePlacement="start">控制消息 Topic</Divider>
                                            {commands.map((command, index) => (
                                                <Card
                                                    key={command.id}
                                                    size="small"
                                                    className="mb-3"
                                                    title={`控制消息 ${index + 1}`}
                                                >
                                                    <Form.Item
                                                        name={['commands', index, 'name']}
                                                        label="消息名称"
                                                        rules={[
                                                            {
                                                                required: true,
                                                                whitespace: true,
                                                                max: 100,
                                                                message: '请输入消息名称',
                                                            },
                                                        ]}
                                                    >
                                                        <Input
                                                            aria-label={`控制消息 ${index + 1} 名称`}
                                                            maxLength={100}
                                                            placeholder="例如：设置目标温度"
                                                        />
                                                    </Form.Item>
                                                    <Form.Item
                                                        name={['commands', index, 'topic']}
                                                        label="控制 Topic"
                                                        extra="可插入 {deviceCode} 或 {point:属性名称}，每个占位符独占一层；属性值不能包含 /、+、#。"
                                                    >
                                                        <MqttMessageInput
                                                            label={`控制消息 ${index + 1} Topic`}
                                                            points={points}
                                                            topic
                                                        />
                                                    </Form.Item>
                                                </Card>
                                            ))}
                                            <Button
                                                className="mb-4"
                                                disabled={commands.length >= 32}
                                                onClick={() =>
                                                    form.setFieldValue('commands', [
                                                        ...commands,
                                                        {
                                                            id: crypto.randomUUID(),
                                                            name: '',
                                                            topic: 'devices/{deviceCode}/command',
                                                            template: '{}',
                                                            requiredPointIds: [],
                                                        },
                                                    ])
                                                }
                                            >
                                                添加控制 Topic
                                            </Button>
                                            <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                                                <Form.Item name="qos" label="订阅 QoS">
                                                    <Select
                                                        options={[0, 1, 2].map((value) => ({
                                                            value,
                                                            label: `QoS ${value}`,
                                                        }))}
                                                    />
                                                </Form.Item>
                                                <Form.Item name="storagePolicy" label="存储策略">
                                                    <Select options={STORAGE_POLICY_OPTIONS} />
                                                </Form.Item>
                                                <Form.Item
                                                    name="timeFormat"
                                                    label="设备时间格式"
                                                    extra="模板未配置 $time 时，使用接收时间。"
                                                >
                                                    <Select
                                                        options={[
                                                            {
                                                                value: 'unix_ms',
                                                                label: 'Unix 毫秒',
                                                            },
                                                            { value: 'unix_s', label: 'Unix 秒' },
                                                            { value: 'iso8601', label: 'ISO 8601' },
                                                        ]}
                                                    />
                                                </Form.Item>
                                            </div>
                                        </>
                                    ),
                                },
                                {
                                    key: 'report',
                                    label: '上报消息',
                                    forceRender: true,
                                    children: (
                                        <>
                                            <Form.Item
                                                name="reportTemplate"
                                                label="上报消息模板"
                                                extra="用 $point:属性名称 绑定已定义的属性；$time 为设备时间。可在光标位置插入占位符；常量与不采集的字段可保留。"
                                            >
                                                <MqttMessageInput
                                                    label="上报消息模板"
                                                    points={points}
                                                    report
                                                />
                                            </Form.Item>
                                            <Button onClick={validateMessages}>校验消息配置</Button>
                                            <Collapse
                                                className="mt-4"
                                                items={[
                                                    {
                                                        key: 'preview',
                                                        label: '上报解析测试',
                                                        children: (
                                                            <>
                                                                <Alert
                                                                    type="info"
                                                                    title="使用服务端解析器验证上报消息，不保存配置、不连接 Broker，也不下发控制消息。"
                                                                    className="mb-3"
                                                                />
                                                                <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                                                                    <Form.Item label="示例 Topic">
                                                                        <Input
                                                                            aria-label="示例 Topic"
                                                                            value={sampleTopic}
                                                                            onChange={(event) => {
                                                                                invalidatePreview();
                                                                                setSampleTopic(
                                                                                    event.target
                                                                                        .value
                                                                                );
                                                                            }}
                                                                        />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        label="测试设备编码"
                                                                        extra="共享 Topic 时可留空，查看所有设备记录。"
                                                                    >
                                                                        <Input
                                                                            aria-label="测试设备编码"
                                                                            value={sampleCode}
                                                                            onChange={(event) => {
                                                                                invalidatePreview();
                                                                                setSampleCode(
                                                                                    event.target
                                                                                        .value
                                                                                );
                                                                            }}
                                                                        />
                                                                    </Form.Item>
                                                                </div>
                                                                <Form.Item label="实际 JSON 消息">
                                                                    <Input.TextArea
                                                                        aria-label="实际 JSON 消息"
                                                                        rows={8}
                                                                        className="font-mono"
                                                                        spellCheck={false}
                                                                        value={samplePayload}
                                                                        onChange={(event) => {
                                                                            invalidatePreview();
                                                                            setSamplePayload(
                                                                                event.target.value
                                                                            );
                                                                        }}
                                                                    />
                                                                </Form.Item>
                                                                <Form.Item
                                                                    label="测试设备时区"
                                                                    extra="仅用于不含时区的 ISO 时间，格式为 ±HH:MM。"
                                                                >
                                                                    <Input
                                                                        value={sampleTimezone}
                                                                        maxLength={6}
                                                                        onChange={(event) => {
                                                                            invalidatePreview();
                                                                            setSampleTimezone(
                                                                                event.target.value
                                                                            );
                                                                        }}
                                                                    />
                                                                </Form.Item>
                                                                <Button
                                                                    onClick={parseSample}
                                                                    loading={previewBusy}
                                                                >
                                                                    解析消息
                                                                </Button>
                                                                {previewError && (
                                                                    <Alert
                                                                        className="mt-3"
                                                                        type="error"
                                                                        title={previewError}
                                                                    />
                                                                )}
                                                                {preview?.records.map((record) => (
                                                                    <Card
                                                                        key={record.key}
                                                                        size="small"
                                                                        className="mt-3"
                                                                        title={`设备 ${record.deviceCode || '未识别'} · ${record.time}`}
                                                                    >
                                                                        {record.errors.length >
                                                                            0 && (
                                                                            <Alert
                                                                                type="warning"
                                                                                className="mb-2"
                                                                                title={record.errors.join(
                                                                                    '；'
                                                                                )}
                                                                            />
                                                                        )}
                                                                        <Table
                                                                            rowKey="id"
                                                                            size="small"
                                                                            pagination={false}
                                                                            scroll={{ x: 420 }}
                                                                            dataSource={
                                                                                record.points
                                                                            }
                                                                            columns={[
                                                                                {
                                                                                    title: '点位',
                                                                                    dataIndex:
                                                                                        'name',
                                                                                },
                                                                                {
                                                                                    title: '值',
                                                                                    dataIndex:
                                                                                        'value',
                                                                                },
                                                                                {
                                                                                    title: '单位',
                                                                                    dataIndex:
                                                                                        'unit',
                                                                                },
                                                                            ]}
                                                                        />
                                                                    </Card>
                                                                ))}
                                                                {preview &&
                                                                    preview.records.length ===
                                                                        0 && (
                                                                        <Empty description="未找到设备记录" />
                                                                    )}
                                                            </>
                                                        ),
                                                    },
                                                ]}
                                            />
                                        </>
                                    ),
                                },
                                {
                                    key: 'commands',
                                    label: `控制消息（${commands.length}）`,
                                    forceRender: true,
                                    children: (
                                        <>
                                            <p className="mb-3 text-sm text-slate-500">
                                                可配置多种控制消息，下发时选择一种。每条消息独立选择必填控制点，选填属性未填写时省略对应的
                                                JSON 字段。
                                            </p>
                                            <Form.List name="commands">
                                                {(fields, { add, remove: removeCommand }) => (
                                                    <>
                                                        {fields.map((field) => {
                                                            const command = commands[field.name];
                                                            let referenced: MqttPoint[] = [];
                                                            try {
                                                                referenced = mqttCommandPoints(
                                                                    command?.template || '{}',
                                                                    points,
                                                                    command?.topic || ''
                                                                );
                                                            } catch {
                                                                /* 编辑中的模板由保存校验报告错误。 */
                                                            }
                                                            return (
                                                                <Card
                                                                    key={field.key}
                                                                    size="small"
                                                                    className="mb-3"
                                                                    title={
                                                                        command?.name ||
                                                                        `控制消息 ${field.name + 1}`
                                                                    }
                                                                    extra={
                                                                        <Popconfirm
                                                                            title="删除此控制消息？"
                                                                            onConfirm={() =>
                                                                                removeCommand(
                                                                                    field.name
                                                                                )
                                                                            }
                                                                        >
                                                                            <Button
                                                                                size="small"
                                                                                type="text"
                                                                                danger
                                                                            >
                                                                                删除消息
                                                                            </Button>
                                                                        </Popconfirm>
                                                                    }
                                                                >
                                                                    <div className="mb-3 break-all text-sm text-slate-500">
                                                                        Topic：
                                                                        {command?.topic || '未配置'}{' '}
                                                                        <Button
                                                                            size="small"
                                                                            type="link"
                                                                            onClick={() =>
                                                                                setTab('topics')
                                                                            }
                                                                        >
                                                                            修改名称与 Topic
                                                                        </Button>
                                                                    </div>
                                                                    <Form.Item
                                                                        name={[
                                                                            field.name,
                                                                            'template',
                                                                        ]}
                                                                        label="控制消息模板"
                                                                        extra={
                                                                            '属性值按类型生成 JSON；"$commandId" 自动生成整数编号，"$param:参数名" 引用当前设备参数。'
                                                                        }
                                                                    >
                                                                        <MqttMessageInput
                                                                            label={`控制消息 ${field.name + 1} 模板`}
                                                                            points={points}
                                                                        />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[
                                                                            field.name,
                                                                            'response',
                                                                        ]}
                                                                        label="设备回执规则（JSON，可选）"
                                                                        extra="配置后等待设备回执；matchFields 与原指令同路径字段比较，必须包含指令编号。超时不会自动重发。留空时仅确认 Broker 接收。"
                                                                        getValueProps={(
                                                                            value: unknown
                                                                        ) => ({
                                                                            value:
                                                                                typeof value ===
                                                                                'string'
                                                                                    ? value
                                                                                    : value
                                                                                      ? JSON.stringify(
                                                                                            value,
                                                                                            null,
                                                                                            2
                                                                                        )
                                                                                      : '',
                                                                        })}
                                                                        getValueFromEvent={(
                                                                            event
                                                                        ) => event.target.value}
                                                                    >
                                                                        <Input.TextArea
                                                                            rows={6}
                                                                            placeholder={
                                                                                '{"topic":"devices/{deviceCode}/result","matchFields":["/id"],"codeField":"/code","successCode":0,"timeoutMs":10000}'
                                                                            }
                                                                        />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[
                                                                            field.name,
                                                                            'requiredPointIds',
                                                                        ]}
                                                                        label="必填控制点"
                                                                        extra="仅列出当前 Topic 或消息引用的属性。Topic 和数组直接引用的属性必须设为必填。"
                                                                    >
                                                                        <Checkbox.Group
                                                                            options={referenced.map(
                                                                                (point) => ({
                                                                                    label: point.name,
                                                                                    value: point.id,
                                                                                })
                                                                            )}
                                                                        />
                                                                    </Form.Item>
                                                                </Card>
                                                            );
                                                        })}
                                                        <Button
                                                            disabled={fields.length >= 32}
                                                            onClick={() => {
                                                                add({
                                                                    id: crypto.randomUUID(),
                                                                    name: '',
                                                                    topic: 'devices/{deviceCode}/command',
                                                                    template: '{}',
                                                                    requiredPointIds: [],
                                                                });
                                                                setTab('topics');
                                                            }}
                                                        >
                                                            添加控制消息
                                                        </Button>
                                                        <Button
                                                            className="ml-2"
                                                            onClick={validateMessages}
                                                        >
                                                            校验消息配置
                                                        </Button>
                                                    </>
                                                )}
                                            </Form.List>
                                        </>
                                    ),
                                },
                            ]}
                        />
                    </Form>
                </div>
            </FormModal>
        </PageContainer>
    );
}

function PointDisplayField() {
    return (
        <Form.Item
            name="visible"
            label="显示"
            valuePropName="checked"
            extra="关闭后仅隐藏展示，仍继续采集、计算、存储和告警。"
        >
            <Switch />
        </Form.Item>
    );
}

interface DerivedPointsEditorRef {
    create: () => void;
}
const DerivedPointsEditor = forwardRef<
    DerivedPointsEditorRef,
    { item?: Protocol.Item; editable: boolean }
>(function DerivedPointsEditor({ item, editable }, ref) {
    const { message } = App.useApp();
    const save = useProtocolConfigSave();
    const [editing, setEditing] = useState(false);
    const [testValues, setTestValues] = useState<Record<string, number | null>>({});
    const [testResult, setTestResult] = useState<{ ok: boolean; text: string }>();
    const [testing, setTesting] = useState(false);
    const testRevision = useRef(0);
    const invalidateTest = () => {
        testRevision.current += 1;
        setTesting(false);
        setTestResult(undefined);
    };
    const checkExpression = async () => {
        const revision = ++testRevision.current;
        setTestResult(undefined);
        setTesting(true);
        try {
            const values = form.getFieldsValue(true);
            const samples = (values.inputs ?? []).map((input: DerivedPoint['inputs'][number]) => {
                const value = testValues[input.alias];
                if (value == null || !Number.isFinite(value))
                    throw new Error(`请填写变量 ${input.alias || '（未命名）'} 的测试值`);
                return { alias: input.alias, value };
            });
            const result = await testProtocolExpression({
                expression: values.kind === 'expression' ? (values.expression ?? '') : '0',
                inputs: samples,
                unit: values.unit ?? '',
                unitRules: values.unitMode === 'conditional' ? (values.unitRules ?? []) : [],
            });
            if (revision !== testRevision.current) return;
            const resultValue =
                values.valueType === 'boolean' ? String(result.value !== 0) : String(result.value);
            const unitResult = `${result.unit || '无单位'}${values.unitMode === 'conditional' ? (result.matchedRule ? `（命中条件 ${result.matchedRule}）` : '（默认单位）') : ''}`;
            setTestResult({
                ok: true,
                text:
                    values.kind === 'expression'
                        ? `校验通过，计算结果：${resultValue}；单位：${unitResult}`
                        : `单位条件校验通过：${unitResult}。窗口统计结果由实际采样计算。`,
            });
        } catch (error) {
            if (revision === testRevision.current)
                setTestResult({
                    ok: false,
                    text: error instanceof Error ? error.message : '校验失败',
                });
        } finally {
            if (revision === testRevision.current) setTesting(false);
        }
    };
    const points = useMemo(() => (item ? protocolDerivedPoints(item) : []), [item]);
    const [form] = Form.useForm<DerivedPoint>();
    const kind = Form.useWatch('kind', form);
    const unitMode = Form.useWatch('unitMode', form);
    const inputs = Form.useWatch('inputs', form) as DerivedPoint['inputs'] | undefined;
    const currentId = Form.useWatch('id', form);
    const expressionRules = [
        {
            validator: async (_: unknown, value: string) => {
                validatePointExpression(
                    value ?? '',
                    (form.getFieldValue('inputs') ?? []).map(
                        (input: DerivedPoint['inputs'][number]) => input.alias
                    )
                );
            },
        },
    ];
    const physical = useMemo(() => (item ? protocolPointOptions(item) : []), [item]);
    const options = [
        ...physical,
        ...points
            .filter((point) => point.id !== currentId)
            .map((point) => ({ value: point.id, label: `${point.name}（派生）` })),
    ];
    const labels = {
        expression: '表达式 / 条件',
        average: '窗口平均值',
        minimum: '窗口最小值',
        maximum: '窗口最大值',
    };
    const edit = (point?: DerivedPoint) => {
        invalidateTest();
        setTestValues({});
        form.resetFields();
        form.setFieldsValue(
            point ?? {
                id: crypto.randomUUID(),
                name: '',
                kind: 'expression',
                expression: 'x',
                unitMode: 'fixed',
                unit: '',
                valueType: 'number',
                inputs: [{ alias: 'x', pointId: '' }],
                maxAgeSeconds: 300,
                visible: true,
            }
        );
        setEditing(true);
    };
    useImperativeHandle(ref, () => ({ create: () => edit() }));
    const savePoints = async (next: DerivedPoint[]) => {
        if (item) await save.mutateAsync({ id: item.id, config: { derivedPoints: next } });
    };
    if (!item) return null;
    return (
        <>
            <section className="mt-4 rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                <Flex justify="space-between" align="center" gap={8} wrap className="mb-4">
                    <Space>
                        <span className="font-semibold">派生值</span>
                        <Tag color="blue">{points.length} 个</Tag>
                    </Space>
                </Flex>
                {points.length === 0 ? (
                    <Empty
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        description="暂无派生值，点击上方新增派生值"
                    />
                ) : (
                    <div className="grid gap-3" style={REGISTER_CARD_GRID_STYLE}>
                        {points.map((point) => (
                            <Card
                                key={point.id}
                                size="small"
                                hoverable
                                className="h-full border-slate-200 shadow-[0_1px_4px_rgba(15,23,42,0.06)]"
                                styles={{ body: { padding: 12 } }}
                            >
                                <Flex
                                    justify="space-between"
                                    gap={12}
                                    align="start"
                                    className="mb-2"
                                >
                                    <div className="min-w-0 flex-1">
                                        <div className="truncate text-sm font-semibold text-slate-800">
                                            {point.name}
                                        </div>
                                        <div className="mt-0.5 text-[12px] text-slate-400">
                                            {labels[point.kind]}
                                        </div>
                                    </div>
                                    {editable && (
                                        <Space size={4} className="shrink-0">
                                            <Button
                                                size="small"
                                                type="link"
                                                onClick={() => edit(point)}
                                            >
                                                编辑
                                            </Button>
                                            <Popconfirm
                                                title="删除此派生值？引用它的公式也需要修改。"
                                                onConfirm={() =>
                                                    savePoints(
                                                        points.filter(
                                                            (value) => value.id !== point.id
                                                        )
                                                    )
                                                }
                                            >
                                                <Button
                                                    size="small"
                                                    type="link"
                                                    danger
                                                    loading={save.isPending}
                                                >
                                                    删除
                                                </Button>
                                            </Popconfirm>
                                        </Space>
                                    )}
                                </Flex>
                                <Space size={4} wrap className="mb-2">
                                    <Tag color="purple">派生值</Tag>
                                    <Tag>只读</Tag>
                                    {point.unitMode === 'conditional' ? (
                                        <Tag color="cyan">条件单位</Tag>
                                    ) : (
                                        point.unit && <Tag>{point.unit}</Tag>
                                    )}
                                    {point.kind !== 'expression' && (
                                        <Tag>{point.windowSeconds} 秒窗口</Tag>
                                    )}
                                    <Tag>{point.valueType === 'boolean' ? '布尔值' : '数值'}</Tag>
                                </Space>
                                <div className="mb-3 break-all text-xs leading-5 text-slate-500">
                                    {point.kind === 'expression'
                                        ? point.expression
                                        : `统计输入：${point.sourceAlias}`}
                                </div>
                            </Card>
                        ))}
                    </div>
                )}
            </section>

            <FormModal
                title={points.some((point) => point.id === currentId) ? '编辑派生值' : '新增派生值'}
                confirmLoading={save.isPending}
                open={editing}
                onCancel={() => {
                    invalidateTest();
                    setEditing(false);
                }}
                onOk={async () => {
                    const values = await form.validateFields();
                    if (values.kind !== 'expression') {
                        values.valueType = 'number';
                        values.expression = undefined;
                    } else values.windowSeconds = undefined;
                    if (values.unitMode === 'fixed') values.unitRules = [];
                    const result = derivedPointSchema.safeParse(values);
                    if (!result.success) {
                        form.setFields(
                            result.error.issues.map((issue) => ({
                                name: issue.path as Parameters<
                                    typeof form.setFields
                                >[0][number]['name'],
                                errors: [issue.message],
                            }))
                        );
                        message.error(result.error.issues[0]?.message ?? '配置无效');
                        return;
                    }
                    await savePoints(
                        points.some((point) => point.id === values.id)
                            ? points.map((point) => (point.id === values.id ? result.data : point))
                            : [...points, result.data]
                    );
                    setEditing(false);
                }}
            >
                <Form form={form} layout="vertical" onValuesChange={invalidateTest}>
                    <Form.Item name="id" hidden>
                        <Input />
                    </Form.Item>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="name" label="名称" rules={[{ required: true }]}>
                            <Input maxLength={100} />
                        </Form.Item>
                        <Form.Item name="unitMode" label="单位方式">
                            <Select
                                options={[
                                    { value: 'fixed', label: '固定单位' },
                                    { value: 'conditional', label: '条件单位' },
                                ]}
                            />
                        </Form.Item>
                        <Form.Item
                            name="unit"
                            label={unitMode === 'conditional' ? '默认单位（均不匹配时）' : '单位'}
                        >
                            <Input maxLength={32} />
                        </Form.Item>
                        <Form.Item name="kind" label="计算方式">
                            <Select
                                options={Object.entries(labels).map(([value, label]) => ({
                                    value,
                                    label,
                                }))}
                            />
                        </Form.Item>
                        <Form.Item name="valueType" label="结果类型">
                            <Select
                                disabled={kind !== 'expression'}
                                options={[
                                    { value: 'number', label: '数值' },
                                    { value: 'boolean', label: '布尔值' },
                                ]}
                            />
                        </Form.Item>
                    </div>
                    <Form.List name="inputs">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map((field) => (
                                    <div
                                        key={field.key}
                                        className="grid grid-cols-[100px_minmax(0,1fr)_auto] gap-2"
                                    >
                                        <Form.Item
                                            name={[field.name, 'alias']}
                                            label="变量名"
                                            rules={[{ required: true }]}
                                        >
                                            <Input />
                                        </Form.Item>
                                        <Form.Item
                                            name={[field.name, 'pointId']}
                                            label="输入点位"
                                            rules={[{ required: true }]}
                                        >
                                            <Select
                                                showSearch
                                                optionFilterProp="label"
                                                options={options}
                                            />
                                        </Form.Item>
                                        <Button
                                            className="mt-7"
                                            disabled={fields.length <= 1}
                                            onClick={() => remove(field.name)}
                                        >
                                            移除
                                        </Button>
                                    </div>
                                ))}
                                <Button
                                    className="mb-4"
                                    disabled={fields.length >= 16}
                                    onClick={() =>
                                        add({ alias: `x${fields.length + 1}`, pointId: '' })
                                    }
                                >
                                    添加输入
                                </Button>
                            </>
                        )}
                    </Form.List>
                    {kind !== 'expression' && (
                        <Form.Item
                            name="sourceAlias"
                            label="窗口计算输入"
                            rules={[{ required: true }]}
                        >
                            <Select
                                options={(inputs ?? []).map((input) => ({
                                    value: input.alias,
                                    label: input.alias,
                                }))}
                            />
                        </Form.Item>
                    )}
                    {kind === 'expression' ? (
                        <Form.Item
                            name="expression"
                            label="计算公式"
                            dependencies={['inputs']}
                            validateTrigger="onBlur"
                            extra="支持 + - * / %、比较、&& || ! 和 if(条件, 真值, 假值)、min、max、abs、sqrt、round。例如 if(x > 10, x * 2, 0)。"
                            rules={expressionRules}
                        >
                            <Input.TextArea rows={3} maxLength={512} />
                        </Form.Item>
                    ) : (
                        <Form.Item
                            name="windowSeconds"
                            label="滑动窗口（秒）"
                            rules={[{ required: true }]}
                            extra="平均值采用窗口内有效采样的算术平均。"
                        >
                            <InputNumber min={1} max={86400} className="w-full" />
                        </Form.Item>
                    )}
                    <Form.Item
                        name="maxAgeSeconds"
                        label="输入有效期（秒）"
                        rules={[{ required: true }]}
                        extra="输入缺失或超时，结果显示无效，不按 0 计算。"
                    >
                        <InputNumber min={1} max={86400} className="w-full" />
                    </Form.Item>
                    {unitMode === 'conditional' && (
                        <>
                            <Divider titlePlacement="start">单位条件</Divider>
                            <p>
                                按顺序使用第一条成立的条件；单位切换不自动换算数值，需要在数值公式中处理。
                            </p>
                            <Form.List name="unitRules">
                                {(fields, { add, remove, move }) => (
                                    <>
                                        {fields.map((field, index) => (
                                            <Card
                                                key={field.key}
                                                size="small"
                                                className="mb-3"
                                                title={`条件 ${index + 1}`}
                                                extra={
                                                    <Space>
                                                        <Button
                                                            size="small"
                                                            disabled={index === 0}
                                                            onClick={() => move(index, index - 1)}
                                                        >
                                                            上移
                                                        </Button>
                                                        <Button
                                                            size="small"
                                                            onClick={() => remove(field.name)}
                                                        >
                                                            移除
                                                        </Button>
                                                    </Space>
                                                }
                                            >
                                                <Form.Item
                                                    name={[field.name, 'condition']}
                                                    label="条件表达式"
                                                    dependencies={['inputs']}
                                                    validateTrigger="onBlur"
                                                    rules={expressionRules}
                                                >
                                                    <Input
                                                        placeholder="例如 mode == 1"
                                                        maxLength={512}
                                                    />
                                                </Form.Item>
                                                <Form.Item
                                                    name={[field.name, 'unit']}
                                                    label="匹配时单位"
                                                >
                                                    <Input maxLength={32} />
                                                </Form.Item>
                                            </Card>
                                        ))}
                                        <Button
                                            disabled={fields.length >= 8}
                                            onClick={() => add({ condition: '', unit: '' })}
                                        >
                                            添加单位条件
                                        </Button>
                                    </>
                                )}
                            </Form.List>
                        </>
                    )}
                    {(kind === 'expression' || unitMode === 'conditional') && (
                        <Card size="small" title="手动校验" className="my-4">
                            <p className="mb-3 text-xs text-slate-500">
                                填写各变量的测试值，使用与后台计算相同的规则校验；不会保存配置或写入设备。布尔输入用
                                0 / 1 表示。
                            </p>
                            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                                {Array.from(
                                    new Set((inputs ?? []).map((input) => input.alias))
                                ).map((alias) => (
                                    <label
                                        key={alias}
                                        htmlFor={`expression-test-${currentId}-${alias}`}
                                        className="flex flex-col gap-1"
                                    >
                                        <span>{alias || '未命名变量'} 的测试值</span>
                                        <InputNumber
                                            id={`expression-test-${currentId}-${alias}`}
                                            aria-label={`${alias || '未命名变量'} 的测试值`}
                                            value={testValues[alias] ?? null}
                                            className="w-full"
                                            onChange={(value) => {
                                                invalidateTest();
                                                setTestValues((current) => ({
                                                    ...current,
                                                    [alias]: value,
                                                }));
                                            }}
                                        />
                                    </label>
                                ))}
                            </div>
                            <Button className="mt-3" loading={testing} onClick={checkExpression}>
                                试算公式
                            </Button>
                            {testResult && (
                                <Alert
                                    className="mt-3"
                                    showIcon
                                    type={testResult.ok ? 'success' : 'error'}
                                    title={testResult.text}
                                />
                            )}
                        </Card>
                    )}
                    <Form.Item name="visible" label="显示" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                </Form>
            </FormModal>
        </>
    );
});
