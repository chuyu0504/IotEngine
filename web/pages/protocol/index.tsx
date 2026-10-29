import { DownloadOutlined, UploadOutlined } from '@ant-design/icons';
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
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { CSSProperties, ReactNode } from 'react';
import {
    forwardRef,
    useCallback,
    useEffect,
    useImperativeHandle,
    useMemo,
    useRef,
    useState,
} from 'react';
import {
    DerivedPointsEditor,
    type DerivedPointsEditorHandle,
} from '@/components/derived_points_editor';
import { DeviceTypeTree } from '@/components/device_type_tree';
import { FormModal } from '@/components/form_modal';
import { JsonEditor } from '@/components/json_editor';
import { PageContainer } from '@/components/page_container';
import { PointAccessFields } from '@/components/point_access_fields';
import {
    SortableGroupItemList,
    SortableGroupSectionFrame,
    SortableGroupSectionList,
} from '@/components/sortable_group_list';
import { VirtualStack } from '@/components/virtual_grid';
import { usePermissions } from '@/hooks/use_permission';
import { derivedPointSchema } from '@/utils/derived_point';
import { normalizeGroupName } from '@/utils/group_key';
import { createUuid } from '@/utils/uuid';
import {
    checkAddressConflict,
    compileMqttTemplate,
    formatMqttTemplate,
    formatTsapValue,
    industrialConfigSchema,
    mqttCommandPoints,
    mqttTemplateDraft,
    parseProtocolImport,
    validateTsapValue,
} from './protocol.schema';
import {
    bitOnlyAreaTypes,
    buildConnectionConfig,
    buildGroupSections,
    buildRegisterGroupSections,
    buildRemoteTsapFromRackSlot,
    defaultConfig,
    getConnectionFormValues,
    getDataTypeSize,
    getModbusDeviceTypeFormValues,
    getPlcPreset,
    getS7DeviceTypeFormValues,
    getSl651DeviceTypeFormValues,
    hasSameGroupedOrder,
    inferConnectionMode,
    loadProtocolExportData,
    normalizeAreaTypeForPlcModel,
    normalizeModbusRegisters,
    normalizePacketConfig,
    normalizeRegisterType,
    normalizeS7DataType,
    numberOrDefault,
    plcModelOptions,
    protocolDerivedPoints,
    protocolPointOptions,
    reorderItemsByGroupOrder,
    reorderItemsWithinGroupOrder,
    supportsBitAddress,
    supportsS7Decimals,
    useDlt645PointCreate,
    useExternalIoModels,
    useMqttPayloadPreview,
    useProtocolConfigDelete,
    useProtocolConfigImport,
    useProtocolConfigList,
    useProtocolConfigSave,
    useProtocolExpressionTest,
    validateProtocolExpression,
    writableAreaTypes,
} from './protocol.service';
import type {
    DerivedPoint,
    DeviceTypeFormValues,
    DeviceTypeTimingConfig,
    ExternalIoConfig,
    ExternalIoPoint,
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

const EMPTY_PROTOCOL_ITEMS: Protocol.Item[] = [];
const EMPTY_DERIVED_POINTS: DerivedPoint[] = [];
const EMPTY_POINT_OPTIONS: { value: string; label: string }[] = [];

/** 协议配置页共享状态：权限、设备类型列表与选中项、保存删除和导入导出。 */
function useProtocolConfigShell(protocol: Protocol.Type) {
    const { has } = usePermissions();
    const canQuery = has('iot:protocol:query');
    const canAdd = has('iot:protocol:add');
    const { data, isLoading, error, refetch } = useProtocolConfigList(
        { protocol },
        { enabled: canQuery }
    );
    const types = data ?? EMPTY_PROTOCOL_ITEMS;
    const saveMutation = useProtocolConfigSave();
    const deleteMutation = useProtocolConfigDelete();
    const transfer = useProtocolImportExport(protocol);
    // 用户选中的设备类型；未选中或已删除时默认第一个
    const [selectedTypeId, setSelectedTypeId] = useState<string>();
    const activeType = types.find((item) => item.id === selectedTypeId) ?? types[0];
    const deleteActiveType = async () => {
        if (!activeType) return;
        const index = types.indexOf(activeType);
        const nextType = types[index + 1] ?? types[index - 1];
        await deleteMutation.mutateAsync(activeType.id);
        setSelectedTypeId(nextType?.id);
    };
    return {
        canQuery,
        canAdd,
        canEdit: has('iot:protocol:edit'),
        canDelete: has('iot:protocol:delete'),
        canImport: has('iot:protocol:import') && canAdd,
        canExport: has('iot:protocol:export'),
        types,
        loading: isLoading,
        error,
        refetch,
        saveMutation,
        deleteMutation,
        transfer,
        activeType,
        selectType: setSelectedTypeId,
        deleteActiveType,
    };
}

type ProtocolConfigShell = ReturnType<typeof useProtocolConfigShell>;

interface ProtocolConfigLayoutProps {
    shell: ProtocolConfigShell;
    title: string;
    /** 设备类型列表中名称右侧的概要，如点位数量。 */
    typeSummary: (item: Protocol.Item) => ReactNode;
    onCreateType: () => void;
    onEditType: (item: Protocol.Item) => void;
    /** 选中设备类型后右侧卡片的标题内容。 */
    heading: ReactNode;
    /** 右侧卡片标题区的协议操作，排在“新增派生值”之前。 */
    actions?: ReactNode;
    /** 选中设备类型后的配置内容；`derivedPoints` 为派生值区域，由协议页决定位置。 */
    body: (derivedPoints: ReactNode) => ReactNode;
    /** 协议私有弹窗。 */
    children?: ReactNode;
}

/** 协议配置页外壳：左侧设备类型列表与操作，右侧配置卡片与派生值。 */
function ProtocolConfigLayout({
    shell,
    title,
    typeSummary,
    onCreateType,
    onEditType,
    heading,
    actions,
    body,
    children,
}: ProtocolConfigLayoutProps) {
    const { activeType, canEdit, saveMutation, transfer } = shell;
    const derivedEditor = useRef<DerivedPointsEditorHandle>(null);
    const expressionTest = useProtocolExpressionTest();
    const derivedPoints = useMemo(
        () => (activeType ? protocolDerivedPoints(activeType) : EMPTY_DERIVED_POINTS),
        [activeType]
    );
    const sourceOptions = useMemo(
        () => (activeType ? protocolPointOptions(activeType) : EMPTY_POINT_OPTIONS),
        [activeType]
    );
    if (!shell.canQuery || shell.error) {
        return (
            <PageContainer title={title}>
                <Result
                    status={shell.error ? 'error' : '403'}
                    title={shell.error ? '设备类型加载失败' : '无权访问'}
                    subTitle={shell.error?.message ?? '您没有权限访问此页面'}
                />
            </PageContainer>
        );
    }
    return (
        <PageContainer title={title}>
            <div className="flex h-full min-h-0 flex-col gap-3 overflow-hidden lg:flex-row lg:gap-0">
                <div className="h-48 min-h-0 shrink-0 lg:h-full lg:w-[360px] lg:pr-3">
                    <Card
                        title="设备类型"
                        className="flex h-full min-h-0 flex-col overflow-hidden"
                        styles={{ body: { flex: 1, minHeight: 0, overflow: 'auto', padding: 16 } }}
                        extra={
                            <Space size={4}>
                                {shell.canAdd && (
                                    <Button size="small" type="primary" onClick={onCreateType}>
                                        新增
                                    </Button>
                                )}
                                {canEdit && (
                                    <Button
                                        size="small"
                                        disabled={!activeType}
                                        onClick={() => activeType && onEditType(activeType)}
                                    >
                                        编辑
                                    </Button>
                                )}
                                {shell.canDelete && (
                                    <Popconfirm
                                        title="确认删除该设备类型？"
                                        disabled={!activeType}
                                        onConfirm={shell.deleteActiveType}
                                    >
                                        <Button
                                            size="small"
                                            danger
                                            disabled={!activeType}
                                            loading={shell.deleteMutation.isPending}
                                        >
                                            删除
                                        </Button>
                                    </Popconfirm>
                                )}
                                {shell.canExport && (
                                    <Tooltip title="导出">
                                        <Button
                                            size="small"
                                            aria-label="导出"
                                            icon={<DownloadOutlined />}
                                            disabled={shell.loading || transfer.importing}
                                            loading={transfer.exporting}
                                            onClick={transfer.exportConfigs}
                                        />
                                    </Tooltip>
                                )}
                                {shell.canImport && (
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
                        <DeviceTypeTree
                            items={shell.types}
                            loading={shell.loading}
                            selectedId={activeType?.id}
                            onSelect={shell.selectType}
                            summary={typeSummary}
                        />
                    </Card>
                </div>
                <div className="min-h-0 min-w-0 flex-1">
                    <Card
                        title={
                            <Flex justify="space-between" align="center" gap={8} wrap>
                                <div className="min-w-0 whitespace-normal">
                                    {activeType ? heading : '暂无设备类型'}
                                </div>
                                {activeType && (
                                    <Space wrap className="shrink-0">
                                        {actions}
                                        {canEdit && (
                                            <Button onClick={() => derivedEditor.current?.create()}>
                                                新增派生值
                                            </Button>
                                        )}
                                    </Space>
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
                                overflow: 'auto',
                                padding: 16,
                            },
                        }}
                    >
                        {shell.loading ? (
                            <Skeleton active paragraph={{ rows: 6 }} />
                        ) : activeType ? (
                            body(
                                <DerivedPointsEditor
                                    key={activeType.id}
                                    ref={derivedEditor}
                                    points={derivedPoints}
                                    sourceOptions={sourceOptions}
                                    editable={canEdit}
                                    saving={saveMutation.isPending}
                                    onSave={(points) =>
                                        saveMutation.mutateAsync({
                                            id: activeType.id,
                                            config: { derivedPoints: points },
                                        })
                                    }
                                    validatePoint={derivedPointSchema.safeParse}
                                    validateExpression={validateProtocolExpression}
                                    testExpression={expressionTest.mutateAsync}
                                />
                            )
                        ) : (
                            <Empty description="暂无设备类型，请先新增设备类型" />
                        )}
                    </Card>
                </div>
            </div>
            {children}
        </PageContainer>
    );
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
                        boolLabel0: register.dictConfig?.items?.find((i) => i.key === '0')?.label,
                        boolLabel1: register.dictConfig?.items?.find((i) => i.key === '1')?.label,
                    });
                } else {
                    form.setFieldsValue({
                        registerType: 'HOLDING_REGISTER',
                        dataType: 'INT16',
                        writable: false,
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

                    <PointAccessFields
                        showWritable={
                            registerType === 'COIL' || registerType === 'HOLDING_REGISTER'
                        }
                    />

                    <Form.Item label="备注" name="remark">
                        <Input placeholder="备注信息" />
                    </Form.Item>
                </Form>
            </FormModal>
        );
    }
);

/**
 * Modbus 协议配置页面
 * 布局：左侧设备类型列表 + 右侧寄存器配置
 */
const ModbusConfigPage = () => {
    const shell = useProtocolConfigShell('Modbus');
    const { activeType, canAdd, canEdit, canDelete, saveMutation, types, refetch } = shell;
    const deviceTypeModalRef = useRef<ModbusDeviceTypeModalRef>(null);
    const registerModalRef = useRef<RegisterModalRef>(null);
    const activeConfig = activeType?.config as Modbus.Config | undefined;
    const registers = useMemo(
        () => normalizeModbusRegisters(activeConfig?.registers),
        [activeConfig]
    );
    const registerGroups = useMemo(() => buildRegisterGroupSections(registers), [registers]);
    const writableRegisterCount = useMemo(
        () => registers.filter((register) => register.writable).length,
        [registers]
    );
    const packet = normalizePacketConfig(activeConfig?.packet);
    const saveRegisterConfig = useCallback(
        async (nextRegisters: Modbus.Register[]) => {
            if (!activeType || !activeConfig || hasSameGroupedOrder(nextRegisters, registers))
                return;
            await saveMutation.mutateAsync({
                id: activeType.id,
                protocol: 'Modbus',
                config: {
                    byteOrder: activeConfig.byteOrder,
                    readInterval: numberOrDefault(activeConfig.readInterval, 300),
                    packet: normalizePacketConfig(activeConfig.packet),
                    registers: nextRegisters,
                },
            });
            await refetch();
        },
        [activeConfig, activeType, refetch, registers, saveMutation]
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
    const handleDeleteRegister = async (registerId: string) => {
        if (!activeType || !activeConfig) return;
        await saveMutation.mutateAsync({
            id: activeType.id,
            protocol: 'Modbus',
            config: {
                byteOrder: activeConfig.byteOrder,
                readInterval: numberOrDefault(activeConfig.readInterval, 300),
                packet: normalizePacketConfig(activeConfig.packet),
                registers: registers.filter((register) => register.id !== registerId),
            },
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
                                    if (activeType)
                                        registerModalRef.current?.open(
                                            'edit',
                                            activeType.id,
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
                    {isWritableType && register.writable ? (
                        <Tag color="orange">读写</Tag>
                    ) : (
                        <Tag>只读</Tag>
                    )}
                    {register.unit ? <Tag>{register.unit}</Tag> : null}
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
    return (
        <ProtocolConfigLayout
            shell={shell}
            title="Modbus配置"
            typeSummary={(item) =>
                `${(item.config as Modbus.Config)?.registers?.length || 0}个寄存器`
            }
            onCreateType={() => deviceTypeModalRef.current?.open('create')}
            onEditType={(item) => deviceTypeModalRef.current?.open('edit', item)}
            heading={
                <Space wrap>
                    <span>寄存器配置</span>
                    <Tag>
                        {ByteOrderOptions.find((o) => o.value === activeConfig?.byteOrder)?.label ||
                            'Big-endian'}
                    </Tag>
                    <Tag>间隔 {activeConfig?.readInterval ?? 300}s</Tag>
                    <Tag>组包 gap≤{packet.mergeGap}</Tag>
                    <Tag>单包≤{packet.maxQuantity}</Tag>
                    <Tag color="blue">{registers.length} 个寄存器</Tag>
                    <Tag color="geekblue">{registerGroups.length} 个分组</Tag>
                    {writableRegisterCount > 0 && (
                        <Tag color="orange">{writableRegisterCount} 个可写</Tag>
                    )}
                </Space>
            }
            actions={
                canAdd && (
                    <Button
                        type="primary"
                        onClick={() =>
                            activeType && registerModalRef.current?.open('create', activeType.id)
                        }
                    >
                        新增寄存器
                    </Button>
                )
            }
            body={(derivedPoints) => (
                <>
                    {registers.length === 0 ? (
                        <Empty description="暂无寄存器，点击右上角新增寄存器" />
                    ) : (
                        <SortableGroupSectionList
                            sections={registerGroups}
                            className="w-full"
                            disabled={saveMutation.isPending}
                            onOrderChange={handleRegisterGroupOrderChange}
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
                    {derivedPoints}
                </>
            )}
        >
            <DeviceTypeModal
                ref={deviceTypeModalRef}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />
            <RegisterModal
                ref={registerModalRef}
                types={types}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />
        </ProtocolConfigLayout>
    );
};

/**
 * S7 数据区（Area）编辑弹窗（从 S7Config 抽离）
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
            title={mode === 'create' ? '新增数据区' : '编辑数据区'}
            open={open}
            onCancel={onCancel}
            onOk={handleOk}
            destroyOnHidden
        >
            <Form form={form} layout="vertical" initialValues={initialFormValues}>
                <Form.Item
                    name="name"
                    label="数据区名称"
                    rules={[{ required: true, message: '请输入数据区名称' }]}
                >
                    <Input placeholder="例如: 温度" />
                </Form.Item>
                <Form.Item
                    name="group"
                    label="分组"
                    extra="同一分组的数据区会在配置页聚合为同一组卡片，留空则显示在未分组中"
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
                            label="数据区类型"
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
                            (parsedAreaType ? '按区域规则自动拼接' : '请先选择数据区类型')}
                    </div>
                    <div className="text-xs text-gray-500">
                        {areaType ? `示例：${areaAddressHintMap[areaType]}` : '请先选择数据区类型'}
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
                <PointAccessFields showWritable={isWritableArea} />
                <Form.Item name="remark" label="备注">
                    <Input.TextArea rows={3} placeholder="备注说明" />
                </Form.Item>
            </Form>
        </FormModal>
    );
}

/**
 * S7 协议配置页面
 * 布局：左侧设备类型列表 + 右侧数据区配置
 */
const EMPTY_S7_AREAS: S7.Area[] = [];
const S7ConfigPage = () => {
    const shell = useProtocolConfigShell('S7');
    const { activeType, canAdd, canEdit, canDelete, saveMutation, refetch } = shell;
    const activeTypeId = activeType?.id;
    const [deviceTypeModalOpen, setDeviceTypeModalOpen] = useState(false);
    const [editingDeviceType, setEditingDeviceType] = useState<boolean>(false);
    const [editingDeviceTypeItem, setEditingDeviceTypeItem] = useState<Protocol.Item>();
    const [createForm] = Form.useForm<DeviceTypeFormValues>();
    const [areaModalOpen, setAreaModalOpen] = useState(false);
    const [editingAreaId, setEditingAreaId] = useState<string | null>(null);
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
    useEffect(() => {
        if (!deviceTypeModalOpen) return;
        createForm.resetFields();
        createForm.setFieldsValue(getS7DeviceTypeFormValues(editingDeviceTypeItem));
    }, [createForm, deviceTypeModalOpen, editingDeviceTypeItem]);
    const activeConfig = (activeType?.config as S7.Config | undefined) ?? null;
    const activeAreas = activeConfig?.areas ?? EMPTY_S7_AREAS;
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
            if (!activeTypeId || !activeConfig || hasSameGroupedOrder(nextAreas, activeAreas))
                return;
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
                                title="确认删除该数据区？"
                                onConfirm={async () => {
                                    if (!activeTypeId || !activeConfig) return;
                                    await saveMutation.mutateAsync({
                                        id: activeTypeId,
                                        protocol: 'S7',
                                        config: {
                                            ...activeConfig,
                                            areas: activeConfig.areas.filter(
                                                (item) => item.id !== area.id
                                            ),
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
    const openDeviceTypeModal = (item?: Protocol.Item) => {
        setEditingDeviceType(Boolean(item));
        setEditingDeviceTypeItem(item);
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
            if (!editingDeviceTypeItem) return;
            const currentConfig = (editingDeviceTypeItem.config as S7.Config) ?? defaultConfig();
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
                id: editingDeviceTypeItem.id,
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
    const editingArea = activeConfig?.areas.find((area) => area.id === editingAreaId);
    return (
        <ProtocolConfigLayout
            shell={shell}
            title="S7配置"
            typeSummary={(item) => `${(item.config as S7.Config)?.areas?.length || 0}个数据区`}
            onCreateType={() => openDeviceTypeModal()}
            onEditType={openDeviceTypeModal}
            heading={
                <Space wrap>
                    <span>数据区配置</span>
                    {activeType?.enabled ? (
                        <Tag color="green">启用</Tag>
                    ) : (
                        <Tag color="red">禁用</Tag>
                    )}
                    <Tag color={activeConnectionMode === 'TSAP' ? 'cyan' : 'blue'}>
                        {getConnectionModeLabel(activeConnectionMode)}
                    </Tag>
                    <Tag color="geekblue">{activeAreas.length} 个数据区</Tag>
                    <Tag color="purple">{areaGroups.length} 个分组</Tag>
                    {activeConnectionMode === 'TSAP' ? (
                        <Tag>
                            TSAP {activeConnectionPreset?.localTSAP ?? '0100'} /{' '}
                            {activeConnectionPreset?.remoteTSAP ?? '0100'}
                        </Tag>
                    ) : (
                        <>
                            <Tag color="blue">
                                {getConnectionTypeLabel(activeConfig?.connection?.connectionType)}
                            </Tag>
                            <Tag>
                                Rack {activeConnectionPreset?.rack ?? 0} / Slot{' '}
                                {activeConnectionPreset?.slot ?? 1}
                            </Tag>
                        </>
                    )}
                    <Tag>读取间隔 {activeConfig?.readInterval ?? 300}s</Tag>
                </Space>
            }
            actions={
                canAdd && (
                    <Button
                        type="primary"
                        onClick={() => {
                            setEditingAreaId(null);
                            setAreaModalOpen(true);
                        }}
                    >
                        新增数据区
                    </Button>
                )
            }
            body={(derivedPoints) => (
                <>
                    {activeAreas.length === 0 ? (
                        <Empty description="暂无数据区，点击右上角新增数据区" />
                    ) : (
                        <SortableGroupSectionList
                            sections={areaGroups}
                            className="w-full"
                            disabled={saveMutation.isPending || !canEdit}
                            onOrderChange={handleAreaGroupOrderChange}
                        >
                            {(group) => (
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
                                            {group.items.some((area) => area.writable) && (
                                                <Tag color="orange">
                                                    {
                                                        group.items.filter((area) => area.writable)
                                                            .length
                                                    }{' '}
                                                    个可写
                                                </Tag>
                                            )}
                                        </Space>
                                    }
                                >
                                    <SortableGroupItemList
                                        items={group.items}
                                        minItemWidth={320}
                                        disabled={saveMutation.isPending || !canEdit}
                                        empty={<Empty description="暂无数据区" />}
                                        onOrderChange={(nextOrder) =>
                                            handleAreaItemOrderChange(group.key, nextOrder)
                                        }
                                    >
                                        {(area, dragHandle) => renderAreaCard(area, dragHandle)}
                                    </SortableGroupItemList>
                                </SortableGroupSectionFrame>
                            )}
                        </SortableGroupSectionList>
                    )}
                    {derivedPoints}
                </>
            )}
        >
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
        </ProtocolConfigLayout>
    );
};

/**
 * SL651 设备类型 Modal
 */
export interface DeviceTypeModalRef {
    open: (mode: 'create' | 'edit', data?: Protocol.Item) => void;
}
interface Sl651DeviceTypeModalProps {
    onSuccess?: () => void;
    saveMutation: SaveMutation;
}
const Sl651DeviceTypeModal = forwardRef<DeviceTypeModalRef, Sl651DeviceTypeModalProps>(
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
    }
);

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
                    <PointAccessFields />
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
const EMPTY_SL651_FUNCS: FuncWithElements[] = [];
const getFuncId = (func: FuncWithElements) => func.id;
const SL651ConfigPage = () => {
    const shell = useProtocolConfigShell('SL651');
    const { activeType, canAdd, canEdit, canDelete, saveMutation, types, refetch } = shell;
    const activeTypeId = activeType?.id;
    const deviceTypeModalRef = useRef<DeviceTypeModalRef>(null);
    const funcModalRef = useRef<FuncModalRef>(null);
    const elementModalRef = useRef<ElementModalRef>(null);
    const presetValueModalRef = useRef<PresetValueModalRef>(null);
    const dictConfigModalRef = useRef<DictConfigModalRef>(null);
    const responseElementsModalRef = useRef<ResponseElementsModalRef>(null);
    const activeConfig = activeType?.config as SL651.Config | undefined;
    const funcs: FuncWithElements[] = activeConfig?.funcs ?? EMPTY_SL651_FUNCS;
    const saveFuncs = async (nextFuncs: SL651.Func[]) => {
        if (!activeTypeId) return;
        await saveMutation.mutateAsync({
            id: activeTypeId,
            protocol: 'SL651',
            config: { funcs: nextFuncs },
        });
    };
    const handleDeleteFunc = (funcId: string) =>
        saveFuncs(funcs.filter((func) => func.id !== funcId));
    const handleDeleteElement = (funcId: string, eleId: string) =>
        saveFuncs(
            funcs.map((func) =>
                func.id === funcId
                    ? { ...func, elements: func.elements.filter((element) => element.id !== eleId) }
                    : func
            )
        );
    const persistElements = async (
        funcId: string,
        listKind: 'elements' | 'responseElements',
        nextItems: SL651.Element[],
        currentItems: SL651.Element[]
    ) => {
        if (hasSameGroupedOrder(nextItems, currentItems)) return;
        await saveFuncs(
            funcs.map((func) => {
                if (func.id !== funcId) return func;
                return listKind === 'responseElements'
                    ? { ...func, responseElements: nextItems.length > 0 ? nextItems : undefined }
                    : { ...func, elements: nextItems };
            })
        );
        await refetch();
    };
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
        const currentItems = elements ?? [];
        const groups = buildGroupSections(currentItems);
        if (!groups.length) {
            return <Empty description={emptyDescription} />;
        }
        return (
            <SortableGroupSectionList
                sections={groups}
                className="w-full"
                disabled={saveMutation.isPending || !canEdit}
                onOrderChange={(nextOrder) =>
                    persistElements(
                        funcId,
                        listKind,
                        reorderItemsByGroupOrder(currentItems, nextOrder),
                        currentItems
                    )
                }
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
                            minItemWidth={280}
                            disabled={saveMutation.isPending || !canEdit}
                            empty={<Empty description={emptyDescription} />}
                            onOrderChange={(nextOrder) =>
                                persistElements(
                                    funcId,
                                    listKind,
                                    reorderItemsWithinGroupOrder(
                                        currentItems,
                                        group.key,
                                        nextOrder
                                    ),
                                    currentItems
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
    return (
        <ProtocolConfigLayout
            shell={shell}
            title="SL651配置"
            typeSummary={(item) => (item.config as SL651.Config)?.responseMode || 'M1'}
            onCreateType={() => deviceTypeModalRef.current?.open('create')}
            onEditType={(item) => deviceTypeModalRef.current?.open('edit', item)}
            heading="功能码配置"
            actions={
                canAdd && (
                    <Button
                        type="primary"
                        onClick={() =>
                            activeTypeId && funcModalRef.current?.open('create', activeTypeId)
                        }
                    >
                        新增功能码
                    </Button>
                )
            }
            body={(derivedPoints) => (
                <>
                    {funcs.length === 0 ? (
                        <Empty description="暂无功能码，点击右上角新增功能码" />
                    ) : (
                        <VirtualStack
                            items={funcs}
                            getItemKey={getFuncId}
                            estimateSize={520}
                            gap={16}
                        >
                            {renderFuncCard}
                        </VirtualStack>
                    )}
                    {derivedPoints}
                </>
            )}
        >
            <Sl651DeviceTypeModal
                ref={deviceTypeModalRef}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />

            {/* 功能码 Modal */}
            <FuncModal
                ref={funcModalRef}
                types={types}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />

            {/* 要素 Modal */}
            <ElementModal
                ref={elementModalRef}
                types={types}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />

            {/* 预设值 Modal */}
            <PresetValueModal
                ref={presetValueModalRef}
                types={types}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />

            {/* 字典配置 Modal */}
            <DictConfigModal
                ref={dictConfigModalRef}
                types={types}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />

            {/* 应答要素 Modal */}
            <ResponseElementsModal
                ref={responseElementsModalRef}
                types={types}
                onSuccess={refetch}
                saveMutation={saveMutation}
            />
        </ProtocolConfigLayout>
    );
};

export { ModbusConfigPage, S7ConfigPage, SL651ConfigPage };

export type SaveMutation = ReturnType<typeof useProtocolConfigSave>;

const EMPTY_INDUSTRIAL_POINTS: IndustrialPoint[] = [];
function IndustrialConfigPage({ protocol }: { protocol: IndustrialProtocol }) {
    const shell = useProtocolConfigShell(protocol);
    const { activeType: active, canEdit, saveMutation: save, types } = shell;
    const { message } = App.useApp();
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
                          decimals: -1,
                      }),
            }
        );
        pointForm.setFieldValue('visible', !point || config?.pointVisibility?.[point.id] !== false);
        setPointOpen(true);
    };
    const writableCount = config?.points.filter((point) => point.writable).length ?? 0;
    return (
        <ProtocolConfigLayout
            shell={shell}
            title={`${label}配置`}
            typeSummary={(item) => `${(item.config as IndustrialConfig).points?.length ?? 0}个点位`}
            onCreateType={() => openType()}
            onEditType={openType}
            heading={
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
                    <Tag color="blue">{config?.points.length ?? 0} 个点位</Tag>
                    {writableCount > 0 && <Tag color="orange">{writableCount} 个可写</Tag>}
                </Space>
            }
            actions={
                canEdit && (
                    <Button type="primary" onClick={() => openPoint()}>
                        新增点位
                    </Button>
                )
            }
            body={(derivedPoints) => (
                <>
                    <div ref={tableViewport} className="min-h-[320px] shrink-0 overflow-hidden">
                        <Table<IndustrialPoint>
                            key={active?.id}
                            rowKey="id"
                            size="small"
                            dataSource={config?.points ?? EMPTY_INDUSTRIAL_POINTS}
                            pagination={{
                                pageSize: 20,
                                showSizeChanger: false,
                                showTotal: (total) => `共 ${total} 个点位`,
                            }}
                            locale={{
                                emptyText: <Empty description="暂无点位，点击右上角新增点位" />,
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
                                    render: (_, point) => (point.writable ? '可写' : '只读'),
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
                                                                (item) => item.id !== point.id
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
                    {derivedPoints}
                </>
            )}
        >
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
                    <Divider titlePlacement="start" plain className="!my-4">
                        基础信息
                    </Divider>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        {protocol !== 'DLT645' && (
                            <Form.Item
                                name="id"
                                label="点位标识"
                                className="sm:col-span-2"
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
                        <Form.Item name="unit" label="单位">
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
                    <PointAccessFields showWritable />
                </Form>
            </FormModal>
        </ProtocolConfigLayout>
    );
}

export const McConfigPage = () => <IndustrialConfigPage protocol="MC" />;
export const FinsConfigPage = () => <IndustrialConfigPage protocol="FINS" />;
export const Dlt645ConfigPage = () => <IndustrialConfigPage protocol="DLT645" />;

export function ExternalIoConfigPage() {
    const shell = useProtocolConfigShell('ExternalIO');
    const [open, setOpen] = useState(false);
    const {
        data: hardwareModels = [],
        isLoading: loadingModels,
        error: modelsError,
    } = useExternalIoModels(open);

    const [editing, setEditing] = useState<Protocol.Item>();
    const [form] = Form.useForm<{
        name: string;
        enabled: boolean;
        edgeNodeModel: string;
        points: ExternalIoPoint[];
    }>();
    const selectedModel = Form.useWatch('edgeNodeModel', form);
    const hardware = hardwareModels.find((item) => item.model === selectedModel);
    const show = (item?: Protocol.Item) => {
        setEditing(item);
        form.resetFields();
        form.setFieldsValue({
            name: item?.name ?? '',
            enabled: item?.enabled ?? true,
            edgeNodeModel: (item?.config as ExternalIoConfig | undefined)?.edgeNodeModel,
            points: item ? (item.config as ExternalIoConfig).points : [],
        });
        setOpen(true);
    };
    const points = (shell.activeType?.config as ExternalIoConfig | undefined)?.points ?? [];
    return (
        <ProtocolConfigLayout
            shell={shell}
            title="外部 IO 配置"
            typeSummary={(item) =>
                `${(item.config as ExternalIoConfig).edgeNodeModel ?? '未选择型号'} · ${(item.config as ExternalIoConfig).points?.length ?? 0} 个点位`
            }
            onCreateType={() => show()}
            onEditType={show}
            heading={shell.activeType?.name}
            body={(derivedPoints) => (
                <div className="flex flex-col gap-4">
                    <Tag>
                        {(shell.activeType?.config as ExternalIoConfig | undefined)
                            ?.edgeNodeModel ?? '未选择型号'}
                    </Tag>
                    <Table<ExternalIoPoint>
                        rowKey="id"
                        dataSource={points}
                        pagination={false}
                        columns={[
                            { title: '名称', dataIndex: 'name' },
                            { title: '类型', dataIndex: 'kind' },
                            { title: '物理通道', dataIndex: 'channel' },
                            {
                                title: '单位',
                                render: (_, point) => (point.kind === 'AI' ? 'uA' : '—'),
                            },
                        ]}
                    />
                    {derivedPoints}
                </div>
            )}
        >
            <FormModal
                open={open}
                title={editing ? '编辑外部 IO 配置' : '新增外部 IO 配置'}
                onCancel={() => setOpen(false)}
                onOk={() => form.submit()}
                confirmLoading={shell.saveMutation.isPending}
                destroyOnHidden
            >
                <Form
                    form={form}
                    layout="vertical"
                    onFinish={async (values) => {
                        if (!hardware) {
                            form.setFields([
                                { name: 'edgeNodeModel', errors: ['请选择受支持的边缘节点型号'] },
                            ]);
                            return;
                        }
                        const config = {
                            ...(editing?.config as ExternalIoConfig | undefined),
                            edgeNodeModel: values.edgeNodeModel,
                            points: values.points.map((point) => ({
                                ...point,
                                unit: point.kind === 'AI' ? 'uA' : '',
                            })),
                            storagePolicy:
                                (editing?.config as ExternalIoConfig | undefined)?.storagePolicy ??
                                ('report' as const),
                        };
                        if (editing)
                            await shell.saveMutation.mutateAsync({
                                id: editing.id,
                                protocol: 'ExternalIO',
                                name: values.name,
                                enabled: values.enabled,
                                config,
                            });
                        else
                            await shell.saveMutation.mutateAsync({
                                protocol: 'ExternalIO',
                                name: values.name,
                                enabled: values.enabled,
                                config,
                            });
                        setOpen(false);
                    }}
                >
                    <Form.Item name="name" label="配置名称" rules={[{ required: true, max: 64 }]}>
                        <Input />
                    </Form.Item>
                    <Form.Item name="enabled" label="启用" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                    <Form.Item
                        name="edgeNodeModel"
                        label="边缘节点型号"
                        rules={[{ required: true, message: '请选择边缘节点型号' }]}
                        extra="方案仅用于同型号边缘节点，设备绑定时还会检查实际通道能力。"
                    >
                        <Select
                            loading={loadingModels}
                            placeholder="请选择边缘节点型号"
                            options={hardwareModels.map((item) => ({
                                value: item.model,
                                label: item.model,
                            }))}
                            onChange={(value) => {
                                const channels =
                                    hardwareModels.find((item) => item.model === value)?.channels ??
                                    [];
                                const current = form.getFieldValue('points') as ExternalIoPoint[];
                                form.setFieldValue(
                                    'points',
                                    (current ?? []).map((point) => ({
                                        ...point,
                                        channel: channels.some(
                                            (channel) =>
                                                channel.channel === point.channel &&
                                                channel.kind === point.kind
                                        )
                                            ? point.channel
                                            : '',
                                    }))
                                );
                            }}
                        />
                    </Form.Item>
                    {modelsError && (
                        <Alert
                            type="error"
                            showIcon
                            message="边缘节点型号加载失败，请重新打开表单重试。"
                        />
                    )}
                    <Form.List name="points">
                        {(fields, { add, remove }) => (
                            <Space direction="vertical" className="w-full">
                                {fields.map((field) => (
                                    <Space key={field.key} align="baseline" wrap>
                                        <Form.Item name={[field.name, 'id']} hidden>
                                            <Input />
                                        </Form.Item>
                                        <Form.Item
                                            name={[field.name, 'name']}
                                            label="点位名称"
                                            rules={[{ required: true, max: 100 }]}
                                        >
                                            <Input />
                                        </Form.Item>
                                        <Form.Item
                                            name={[field.name, 'kind']}
                                            label="类型"
                                            rules={[{ required: true }]}
                                        >
                                            <Select
                                                style={{ width: 100 }}
                                                onChange={() =>
                                                    form.setFieldValue(
                                                        ['points', field.name, 'channel'],
                                                        ''
                                                    )
                                                }
                                                options={['DI', 'DO', 'AI'].map((value) => ({
                                                    value,
                                                    label: value,
                                                }))}
                                            />
                                        </Form.Item>
                                        <Form.Item noStyle shouldUpdate>
                                            {() => {
                                                const kind = form.getFieldValue([
                                                    'points',
                                                    field.name,
                                                    'kind',
                                                ]);
                                                const channels = (hardware?.channels ?? []).filter(
                                                    (channel) =>
                                                        channel.kind === kind &&
                                                        (kind !== 'DO' || channel.controllable)
                                                );
                                                const options = channels.map((channel) => ({
                                                    value: channel.channel,
                                                    label: channel.channel,
                                                }));
                                                return (
                                                    <Form.Item
                                                        name={[field.name, 'channel']}
                                                        label="物理通道"
                                                        rules={[
                                                            {
                                                                required: true,
                                                                message: '请选择通道',
                                                            },
                                                            {
                                                                validator: (_, value) =>
                                                                    !value ||
                                                                    options.some(
                                                                        (option) =>
                                                                            option.value === value
                                                                    )
                                                                        ? Promise.resolve()
                                                                        : Promise.reject(
                                                                              new Error(
                                                                                  '该型号不支持此通道'
                                                                              )
                                                                          ),
                                                            },
                                                        ]}
                                                    >
                                                        <Select
                                                            style={{ width: 160 }}
                                                            options={options}
                                                            disabled={!hardware}
                                                            placeholder="先选择边缘节点型号"
                                                        />
                                                    </Form.Item>
                                                );
                                            }}
                                        </Form.Item>
                                        <Button danger onClick={() => remove(field.name)}>
                                            删除
                                        </Button>
                                    </Space>
                                ))}
                                <Button
                                    disabled={
                                        !hardware || fields.length >= hardware.channels.length
                                    }
                                    onClick={() =>
                                        add({
                                            id: createUuid(),
                                            name: '',
                                            kind: 'DI',
                                            channel: '',
                                            visible: true,
                                        })
                                    }
                                >
                                    添加点位
                                </Button>
                            </Space>
                        )}
                    </Form.List>
                </Form>
            </FormModal>
        </ProtocolConfigLayout>
    );
}

const MQTT_POINT_TYPES = [
    { value: 'DOUBLE', label: '数值' },
    { value: 'BOOL', label: '布尔' },
    { value: 'STRING', label: '文本' },
];
const EMPTY_MQTT_POINTS: MqttPoint[] = [];
const EMPTY_MQTT_COMMANDS: MqttCommandMessage[] = [];
const EMPTY_MQTT_PROPERTIES: NonNullable<MqttTemplateDraft['parameters']> = [];

function MqttMessageInput({
    value = '',
    onChange,
    points,
    properties,
    label,
    topic = false,
    report = false,
}: {
    value?: string;
    onChange?: (value: string) => void;
    points: readonly MqttPoint[];
    properties: NonNullable<MqttTemplateDraft['parameters']>;
    label: string;
    topic?: boolean;
    report?: boolean;
}) {
    const selection = useRef<[number, number] | null>(null);
    const options = [
        ...properties
            .filter((property) => property.name.trim())
            .map((property) => ({
                label: `固有属性：${property.name}`,
                value: topic
                    ? `{param:${property.name}}`
                    : JSON.stringify(`$param:${property.name}`),
            })),
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
            {topic ? (
                <Input {...input} maxLength={1024} />
            ) : (
                <JsonEditor
                    value={value}
                    onChange={onChange}
                    label={label}
                    format={formatMqttTemplate}
                    onSelectionChange={(from, to) => {
                        selection.current = [from, to];
                    }}
                />
            )}
            <Select<string>
                className="mt-2 w-full"
                aria-label={`${label}插入属性`}
                placeholder="在光标处插入用户定义的属性"
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
    const shell = useProtocolConfigShell('MQTT');
    const { activeType: active, canEdit, saveMutation: save, refetch, selectType } = shell;
    const { message } = App.useApp();
    const payloadPreview = useMqttPayloadPreview();
    const [editing, setEditing] = useState<Protocol.Item>();
    const [open, setOpen] = useState(false);
    const [tab, setTab] = useState('points');
    const [expandedPoints, setExpandedPoints] = useState<string[]>([]);
    const [form] = Form.useForm<MqttTemplateDraft & { name: string; enabled: boolean }>();
    const points: MqttPoint[] =
        Form.useWatch('points', { form, preserve: true }) ?? EMPTY_MQTT_POINTS;
    const commands: MqttCommandMessage[] =
        Form.useWatch('commands', { form, preserve: true }) ?? EMPTY_MQTT_COMMANDS;
    const properties: NonNullable<MqttTemplateDraft['parameters']> =
        Form.useWatch('parameters', { form, preserve: true }) ?? EMPTY_MQTT_PROPERTIES;
    const [formError, setFormError] = useState('');
    const [sampleTopic, setSampleTopic] = useState('');
    const [sampleCode, setSampleCode] = useState('');
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
                      topic: '',
                      reportTemplate: '{}',
                      parameters: [],
                      commands: [],
                      storagePolicy: 'report',
                      timeFormat: 'unix_ms',
                      qos: 1,
                      points: [],
                  };
            if (item) draft.points = compileMqttTemplate(draft).points;
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
        draft.reports ??= [
            {
                id: crypto.randomUUID(),
                name: '上报消息',
                topic: draft.topic,
                reportTemplate: draft.reportTemplate,
                qos: draft.qos,
                timeFormat: draft.timeFormat,
                timeField: draft.timeField,
            },
        ];
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
            selectType(
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
            const result = await payloadPreview.mutateAsync({
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
        {
            title: '消息路径',
            dataIndex: 'field',
            width: 200,
            ellipsis: true,
            render: (value, point) =>
                config?.reports
                    ? config.reports
                          .flatMap((report) =>
                              report.pointFields
                                  .filter((field) => field.pointId === point.id)
                                  .map((field) => `${report.name}：${field.field}`)
                          )
                          .join('；') || '—'
                    : value || '—',
        },
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
    ];
    return (
        <ProtocolConfigLayout
            shell={shell}
            title="MQTT 配置"
            typeSummary={(item) => `${(item.config as MqttConfig).points?.length ?? 0} 个点位`}
            onCreateType={() => edit()}
            onEditType={(item) => edit(item)}
            heading={
                <Space size={4} wrap>
                    <span>属性配置</span>
                    <Tag>MQTT 3.1.1</Tag>
                    <Tag color="blue">{config?.points.length ?? 0} 个点位</Tag>
                    <Tag color="orange">
                        {config?.points.filter((point) => point.writable).length ?? 0} 个可写
                    </Tag>
                </Space>
            }
            actions={
                canEdit && (
                    <Button type="primary" onClick={() => edit(active)}>
                        编辑属性与消息
                    </Button>
                )
            }
            body={(derivedPoints) => (
                <>
                    <div className="mb-3 shrink-0 space-y-1 break-all text-sm">
                        {config?.reports ? (
                            config.reports.map((report) => (
                                <div key={report.id}>
                                    {report.name}：{report.topic}
                                </div>
                            ))
                        ) : (
                            <div>上报 Topic：{config?.topic}</div>
                        )}
                        {config?.commandTopic && <div>控制 Topic：{config.commandTopic}</div>}
                        {config?.commands?.map((command) => (
                            <div key={command.id}>
                                {command.name}：{command.topic}
                            </div>
                        ))}
                    </div>
                    <div ref={viewport} className="min-h-0 flex-1 overflow-auto">
                        <Table
                            key={active?.id}
                            rowKey="id"
                            size="small"
                            columns={columns}
                            dataSource={config?.points ?? EMPTY_MQTT_POINTS}
                            scroll={{ x: 890, y: bodyHeight }}
                            pagination={{
                                pageSize: 20,
                                showSizeChanger: false,
                                showTotal: (total) => `共 ${total} 个点位`,
                            }}
                        />
                        {derivedPoints}
                    </div>
                </>
            )}
        >
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
                                    label: `设备属性（${points.length}）`,
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
                                                            <PointAccessFields
                                                                visibleName={[
                                                                    'pointVisibility',
                                                                    point.id,
                                                                ]}
                                                                initialVisible
                                                            />
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
                                            <Divider titlePlacement="start">固有属性</Divider>
                                            <Form.Item
                                                name="identityProperty"
                                                label="设备标识符"
                                                rules={[
                                                    {
                                                        required: true,
                                                        message: '请选择设备标识固有属性',
                                                    },
                                                ]}
                                                extra="自行定义一个文本固有属性，再选择它作为设备标识符；具体值在设备管理中填写。"
                                            >
                                                <Select
                                                    placeholder="选择用户定义的固有属性"
                                                    options={properties
                                                        .filter(
                                                            (property) =>
                                                                property.dataType === 'STRING' &&
                                                                property.name.trim()
                                                        )
                                                        .map((property) => ({
                                                            value: property.name,
                                                            label: property.name,
                                                        }))}
                                                />
                                            </Form.Item>
                                            <p className="mb-3 text-sm text-slate-500">
                                                在这里定义固有属性名称和类型，具体值在设备管理中逐项填写。控制消息用
                                                "$param:固有属性名" 引用。
                                            </p>
                                            <Form.List name="parameters">
                                                {(fields, { add, remove }) => (
                                                    <>
                                                        <Table
                                                            rowKey="key"
                                                            size="small"
                                                            pagination={false}
                                                            dataSource={fields}
                                                            scroll={{ x: 480 }}
                                                            columns={[
                                                                {
                                                                    title: '固有属性名称',
                                                                    render: (_, field) => (
                                                                        <Form.Item
                                                                            name={[
                                                                                field.name,
                                                                                'name',
                                                                            ]}
                                                                            className="mb-0"
                                                                            rules={[
                                                                                {
                                                                                    required: true,
                                                                                    whitespace: true,
                                                                                    message:
                                                                                        '请输入固有属性名称',
                                                                                },
                                                                            ]}
                                                                        >
                                                                            <Input
                                                                                aria-label={`固有属性 ${field.name + 1} 名称`}
                                                                                maxLength={100}
                                                                                placeholder="例如 stationAddress"
                                                                            />
                                                                        </Form.Item>
                                                                    ),
                                                                },
                                                                {
                                                                    title: '类型',
                                                                    width: 130,
                                                                    render: (_, field) => (
                                                                        <Form.Item
                                                                            name={[
                                                                                field.name,
                                                                                'dataType',
                                                                            ]}
                                                                            className="mb-0"
                                                                            rules={[
                                                                                { required: true },
                                                                            ]}
                                                                        >
                                                                            <Select
                                                                                aria-label={`固有属性 ${field.name + 1} 类型`}
                                                                                options={[
                                                                                    ...MQTT_POINT_TYPES,
                                                                                    {
                                                                                        value: 'JSON',
                                                                                        label: 'JSON',
                                                                                    },
                                                                                ]}
                                                                            />
                                                                        </Form.Item>
                                                                    ),
                                                                },
                                                                {
                                                                    title: '操作',
                                                                    width: 80,
                                                                    render: (_, field) => (
                                                                        <Button
                                                                            type="link"
                                                                            danger
                                                                            onClick={() =>
                                                                                remove(field.name)
                                                                            }
                                                                        >
                                                                            删除
                                                                        </Button>
                                                                    ),
                                                                },
                                                            ]}
                                                        />
                                                        <Button
                                                            className="mt-3"
                                                            disabled={fields.length >= 256}
                                                            onClick={() =>
                                                                add({
                                                                    name: '',
                                                                    dataType: 'STRING',
                                                                })
                                                            }
                                                        >
                                                            添加固有属性
                                                        </Button>
                                                    </>
                                                )}
                                            </Form.List>
                                        </>
                                    ),
                                },
                                {
                                    key: 'report',
                                    label: '上报消息',
                                    forceRender: true,
                                    children: (
                                        <>
                                            <Form.Item name="storagePolicy" label="存储策略">
                                                <Select options={STORAGE_POLICY_OPTIONS} />
                                            </Form.Item>
                                            <Form.List name="reports">
                                                {(fields, { add, remove }) => (
                                                    <>
                                                        <div className="space-y-4">
                                                            {fields.map((field) => (
                                                                <Card
                                                                    key={field.key}
                                                                    size="small"
                                                                    title={`上报消息 ${field.name + 1}`}
                                                                    extra={
                                                                        <Popconfirm
                                                                            title="删除此上报消息？"
                                                                            onConfirm={() => {
                                                                                remove(field.name);
                                                                                invalidatePreview();
                                                                            }}
                                                                            disabled={
                                                                                fields.length === 1
                                                                            }
                                                                        >
                                                                            <Button
                                                                                size="small"
                                                                                type="text"
                                                                                danger
                                                                                disabled={
                                                                                    fields.length ===
                                                                                    1
                                                                                }
                                                                            >
                                                                                删除消息
                                                                            </Button>
                                                                        </Popconfirm>
                                                                    }
                                                                >
                                                                    <Form.Item
                                                                        name={[field.name, 'id']}
                                                                        hidden
                                                                    >
                                                                        <Input />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[field.name, 'name']}
                                                                        label="消息名称"
                                                                        rules={[
                                                                            {
                                                                                required: true,
                                                                                whitespace: true,
                                                                                max: 100,
                                                                                message:
                                                                                    '请输入消息名称',
                                                                            },
                                                                        ]}
                                                                    >
                                                                        <Input
                                                                            aria-label={`上报消息 ${field.name + 1} 名称`}
                                                                            maxLength={100}
                                                                        />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[field.name, 'topic']}
                                                                        label="上报 Topic"
                                                                        extra="Topic 用 {param:固有属性名} 引用设备值；共享 Topic 时，在模板中引用所选设备标识固有属性。"
                                                                    >
                                                                        <MqttMessageInput
                                                                            label={`上报消息 ${field.name + 1} Topic`}
                                                                            points={points}
                                                                            properties={properties}
                                                                            topic
                                                                            report
                                                                        />
                                                                    </Form.Item>
                                                                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                                                                        <Form.Item
                                                                            name={[
                                                                                field.name,
                                                                                'qos',
                                                                            ]}
                                                                            label="订阅 QoS"
                                                                        >
                                                                            <Select
                                                                                options={[
                                                                                    0, 1, 2,
                                                                                ].map((value) => ({
                                                                                    value,
                                                                                    label: `QoS ${value}`,
                                                                                }))}
                                                                            />
                                                                        </Form.Item>
                                                                        <Form.Item
                                                                            name={[
                                                                                field.name,
                                                                                'timeFormat',
                                                                            ]}
                                                                            label="设备时间格式"
                                                                            extra="未配置时间字段路径时，使用接收时间。"
                                                                        >
                                                                            <Select
                                                                                options={[
                                                                                    {
                                                                                        value: 'unix_ms',
                                                                                        label: 'Unix 毫秒',
                                                                                    },
                                                                                    {
                                                                                        value: 'unix_s',
                                                                                        label: 'Unix 秒',
                                                                                    },
                                                                                    {
                                                                                        value: 'iso8601',
                                                                                        label: 'ISO 8601',
                                                                                    },
                                                                                ]}
                                                                            />
                                                                        </Form.Item>
                                                                    </div>
                                                                    <Form.Item
                                                                        name={[
                                                                            field.name,
                                                                            'timeField',
                                                                        ]}
                                                                        label="时间字段路径（可选）"
                                                                        extra="填写消息中由你定义的时间字段，例如 /timestamp。"
                                                                    >
                                                                        <Input placeholder="JSON Pointer，例如 /timestamp" />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[
                                                                            field.name,
                                                                            'reportTemplate',
                                                                        ]}
                                                                        label="上报消息模板"
                                                                        extra="用 $point:属性名 绑定消息属性，$param:固有属性名 标记设备标识。所有 JSON 字段均由你定义。"
                                                                    >
                                                                        <MqttMessageInput
                                                                            label={`上报消息 ${field.name + 1} 模板`}
                                                                            points={points}
                                                                            properties={properties}
                                                                            report
                                                                        />
                                                                    </Form.Item>
                                                                </Card>
                                                            ))}
                                                        </div>
                                                        <Space wrap className="mt-3">
                                                            <Button
                                                                disabled={fields.length >= 32}
                                                                onClick={() =>
                                                                    add({
                                                                        id: crypto.randomUUID(),
                                                                        name: `上报消息 ${fields.length + 1}`,
                                                                        topic: '',
                                                                        reportTemplate: '{}',
                                                                        qos: 1,
                                                                        timeFormat: 'unix_ms',
                                                                    })
                                                                }
                                                            >
                                                                添加上报消息
                                                            </Button>
                                                            <Button onClick={validateMessages}>
                                                                校验消息配置
                                                            </Button>
                                                        </Space>
                                                    </>
                                                )}
                                            </Form.List>
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
                                                                        label="测试设备标识符"
                                                                        extra="共享 Topic 时可留空，查看所有设备记录。"
                                                                    >
                                                                        <Input
                                                                            aria-label="测试设备标识符"
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
                                                                    <JsonEditor
                                                                        label="实际 JSON 消息"
                                                                        value={samplePayload}
                                                                        onChange={(value) => {
                                                                            invalidatePreview();
                                                                            setSamplePayload(value);
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
                                                                    <Form.Item
                                                                        name={[field.name, 'name']}
                                                                        label="消息名称"
                                                                        rules={[
                                                                            {
                                                                                required: true,
                                                                                whitespace: true,
                                                                                max: 100,
                                                                                message:
                                                                                    '请输入消息名称',
                                                                            },
                                                                        ]}
                                                                    >
                                                                        <Input
                                                                            aria-label={`控制消息 ${field.name + 1} 名称`}
                                                                            maxLength={100}
                                                                            placeholder="例如：设置目标温度"
                                                                        />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[field.name, 'topic']}
                                                                        label="控制 Topic"
                                                                        extra="可插入 {param:固有属性名} 或 {point:属性名}，每个占位符独占一层。"
                                                                    >
                                                                        <MqttMessageInput
                                                                            label={`控制消息 ${field.name + 1} Topic`}
                                                                            points={points}
                                                                            properties={properties}
                                                                            topic
                                                                        />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[
                                                                            field.name,
                                                                            'template',
                                                                        ]}
                                                                        label="控制消息模板"
                                                                        extra={
                                                                            '使用 "$point:属性名" 引用本次填写的控制值，"$param:固有属性名" 引用设备固有属性。字段名称、结构和指令关联值均由你定义。'
                                                                        }
                                                                    >
                                                                        <MqttMessageInput
                                                                            label={`控制消息 ${field.name + 1} 模板`}
                                                                            points={points}
                                                                            properties={properties}
                                                                        />
                                                                    </Form.Item>
                                                                    <Form.Item
                                                                        name={[
                                                                            field.name,
                                                                            'response',
                                                                        ]}
                                                                        label="设备回执规则（JSON，可选）"
                                                                        extra="matchFields 与原指令同路径字段比较，须包含用户定义的必填控制属性；关联值应在每次操作中唯一。超时不会自动重发。留空时仅确认 Broker 接收。"
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
                                                                    >
                                                                        <JsonEditor
                                                                            label={`${command?.name || `控制消息 ${field.name + 1}`}回执规则 JSON`}
                                                                            placeholder={
                                                                                '{"topic":"用户定义的设备回执 Topic","matchFields":["/用户定义的关联字段"],"codeField":"/用户定义的结果字段","successCode":0,"timeoutMs":10000}'
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
                                                                    topic: '',
                                                                    template: '{}',
                                                                    requiredPointIds: [],
                                                                });
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
        </ProtocolConfigLayout>
    );
}
