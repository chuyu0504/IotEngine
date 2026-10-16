import type { UseQueryOptions } from '@tanstack/react-query';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback } from 'react';
import { useMutationWithMessage, useSaveMutation } from '@/hooks/use_mutation';
import type { RequestConfig } from '@/lib/http';
import type { PaginatedResult } from '@/types/pagination';
import { getGroupKey, normalizeGroupName } from '@/utils/group_key';
import { fetchAllPages } from '@/utils/pagination';
import { createQueryKeys } from '@/utils/query';
import * as api from './protocol.api';
import { formatTsapValue } from './protocol.schema';
import type {
    Dlt645PointCreateDto,
    DerivedPoint,
    DeviceTypeFormValues,
    DeviceTypeTimingConfig,
    ExpressionTestRequest,
    GroupSection,
    Modbus,
    MqttPreviewRequest,
    PlcConnectionPreset,
    Protocol,
    ProtocolExportItem,
    ProtocolImportResult,
    RegisterGroupSection,
    S7,
    SaveProtocolConfigParams,
    SL651,
} from './protocol.types';

const MAX_PAGE_SIZE = 1000;

/** MQTT 样例报文解析预览，结果只用于编辑弹窗展示。 */
export function useMqttPayloadPreview() {
    return useMutation({ mutationFn: (data: MqttPreviewRequest) => api.previewMqtt(data) });
}

/** 派生值公式试算：按填写的测试值由服务端计算结果与命中的单位条件。 */
export function useProtocolExpressionTest() {
    return useMutation({ mutationFn: (data: ExpressionTestRequest) => api.testExpression(data) });
}

/** 用服务端表达式解析器校验公式语法与变量绑定，失败时抛出服务端给出的原因。 */
export async function validateProtocolExpression(
    expression: string,
    aliases: readonly string[]
): Promise<void> {
    await api.validateExpression({ expression, variables: [...new Set(aliases.filter(Boolean))] });
}

export function protocolPointOptions(item: Protocol.Item): { value: string; label: string }[] {
    const config = item.config;
    const physical =
        item.protocol === 'SL651'
            ? ((config as SL651.Config).funcs ?? [])
                  .filter((func) => func.dir === 'UP')
                  .flatMap((func) => func.elements ?? [])
            : item.protocol === 'Modbus'
              ? ((config as Modbus.Config).registers ?? [])
              : item.protocol === 'S7'
                ? ((config as S7.Config).areas ?? [])
                : ((config as { points?: { id: string; name: string }[] }).points ?? []);
    return physical.map((point) => ({ value: point.id, label: point.name }));
}

export function protocolDerivedPoints(item: Protocol.Item): DerivedPoint[] {
    return (item.config as DeviceTypeTimingConfig).derivedPoints ?? [];
}
export const getAllProtocolConfigs = (params?: Protocol.Query, requestConfig?: RequestConfig) => {
    const { page: _page, pageSize: _pageSize, ...filters } = params ?? {};
    return fetchAllPages(MAX_PAGE_SIZE, (page, pageSize) =>
        api.getList({ ...filters, page, pageSize }, requestConfig)
    );
};

export const buildGroupSections = <
    T extends {
        group?: string;
    },
>(
    items: T[]
): GroupSection<T>[] => {
    const sectionMap = new Map<string, GroupSection<T>>();
    items.forEach((item, index) => {
        const rawGroup = normalizeGroupName(item.group);
        const key = getGroupKey(item.group);
        const label = rawGroup || '未分组';
        const current = sectionMap.get(key);
        if (current) {
            current.count++;
            current.items.push(item);
            return;
        }
        sectionMap.set(key, {
            key,
            label,
            count: 1,
            items: [item],
            firstIndex: index,
        });
    });
    return Array.from(sectionMap.values()).sort((a, b) => {
        if (a.firstIndex !== b.firstIndex) return a.firstIndex - b.firstIndex;
        return a.label.localeCompare(b.label, 'zh-Hans-CN');
    });
};
/** 拖拽结果与当前列表的条目顺序及所属分组完全相同，无需保存。 */
export const hasSameGroupedOrder = <T extends { id: string; group?: string }>(
    next: readonly T[],
    current: readonly T[]
) =>
    next.length === current.length &&
    next.every(
        (item, index) =>
            item.id === current[index]?.id &&
            getGroupKey(item.group) === getGroupKey(current[index]?.group)
    );
export const reorderItemsByGroupOrder = <
    T extends {
        group?: string;
    },
>(
    items: T[],
    orderedKeys: string[]
): T[] => {
    const groupMap = new Map<string, T[]>();
    const originalOrder: string[] = [];
    for (const item of items) {
        const key = getGroupKey(item.group);
        const bucket = groupMap.get(key);
        if (bucket) {
            bucket.push(item);
            continue;
        }
        groupMap.set(key, [item]);
        originalOrder.push(key);
    }
    const nextOrder = [
        ...orderedKeys.filter((key) => groupMap.has(key)),
        ...originalOrder.filter((key) => !orderedKeys.includes(key)),
    ];
    return nextOrder.flatMap((key) => groupMap.get(key) ?? []);
};
export const reorderItemsWithinGroupOrder = <
    T extends {
        id: string;
        group?: string;
    },
>(
    items: T[],
    groupKey: string,
    orderedIds: string[]
): T[] => {
    const sections = buildGroupSections(items);
    const orderedIdSet = new Set(orderedIds);
    return sections.flatMap((section) => {
        if (section.key !== groupKey) {
            return section.items;
        }
        const itemMap = new Map(section.items.map((item) => [item.id, item]));
        const nextItems: T[] = [];
        for (const id of orderedIds) {
            const item = itemMap.get(id);
            if (item) {
                nextItems.push(item);
            }
        }
        for (const item of section.items) {
            if (!orderedIdSet.has(item.id)) {
                nextItems.push(item);
            }
        }
        return nextItems;
    });
};

export const protocolQueryKeys = {
    ...createQueryKeys('protocol-configs'),
    list: (params?: Protocol.Query) => ['protocol-configs', 'list', params] as const,
};

export function useProtocolConfigList(
    params?: Protocol.Query,
    options?: Omit<UseQueryOptions<Protocol.Item[]>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: protocolQueryKeys.list(params),
        queryFn: ({ signal }) => getAllProtocolConfigs(params, { signal }),
        ...options,
    });
}
export function useProtocolConfigOptions(
    protocol: Protocol.Type,
    options?: Omit<UseQueryOptions<PaginatedResult<Protocol.Option>>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: [...protocolQueryKeys.all, 'options', protocol],
        queryFn: ({ signal }) => api.getOptions(protocol, signal),
        ...options,
    });
}
export function useProtocolConfigDetail(
    id: string | undefined,
    options?: Omit<UseQueryOptions<Protocol.Item>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: protocolQueryKeys.detail(id ?? ''),
        queryFn: ({ signal }) => api.getDetail(id as string, signal),
        enabled: Boolean(id),
        ...options,
    });
}
export function useProtocolConfigSave() {
    return useSaveMutation<SaveProtocolConfigParams, Protocol.CreateDto, Protocol.UpdateDto>({
        createFn: api.create,
        updateFn: api.update,
        toUpdatePayload: (data) => {
            const { id: _id, protocol: _protocol, ...rest } = data;
            return rest as Protocol.UpdateDto;
        },
        createMessage: '创建成功',
        updateMessage: '更新成功',
        invalidateKeys: [protocolQueryKeys.all],
    });
}
export function useDlt645PointCreate() {
    return useMutationWithMessage({
        mutationFn: ({ id, point }: { id: string; point: Dlt645PointCreateDto }) =>
            api.createDlt645Point(id, point),
        successMessage: '新增点位成功',
        invalidateKeys: [protocolQueryKeys.all],
    });
}
export function useProtocolConfigDelete() {
    return useMutationWithMessage({
        mutationFn: api.remove,
        successMessage: '删除成功',
        invalidateKeys: [protocolQueryKeys.all],
    });
}

/**
 * 协议配置导入保存与导出数据查询
 * 支持 SL651、Modbus 和 S7 配置
 */
const MAX_NAME_LENGTH = 64;
/** 生成不冲突的名称 */
function resolveNameConflict(name: string, existingNames: Set<string>): string {
    if (!existingNames.has(name)) return name;
    let index = 1;
    while (true) {
        const suffix = index === 1 ? ' (导入)' : ` (导入) ${index}`;
        const baseName = name.slice(0, Math.max(0, MAX_NAME_LENGTH - suffix.length)).trimEnd();
        const candidate = `${baseName}${suffix}`;
        if (!existingNames.has(candidate)) return candidate;
        index++;
    }
}
export function useProtocolConfigImport(protocol: Protocol.Type) {
    const queryClient = useQueryClient();
    return useCallback(
        async (items: Protocol.CreateDto[]): Promise<ProtocolImportResult> => {
            // 数据库按全协议范围约束名称唯一，必须加载全部协议名称后再处理冲突。
            const existingList = await getAllProtocolConfigs(undefined, { _silent: true });
            const existingNames = new Set(existingList.map((config) => config.name));
            const result: ProtocolImportResult = {
                total: items.length,
                success: 0,
                renamed: [],
                failed: [],
            };
            for (const item of items) {
                const finalName = resolveNameConflict(item.name, existingNames);
                if (finalName !== item.name) {
                    result.renamed.push(`${item.name} → ${finalName}`);
                }
                try {
                    await api.create(
                        {
                            ...item,
                            protocol,
                            name: finalName,
                        },
                        { _silent: true }
                    );
                    existingNames.add(finalName);
                    result.success++;
                } catch (e) {
                    result.failed.push({
                        name: item.name,
                        reason: e instanceof Error ? e.message : '未知错误',
                    });
                }
            }
            // 刷新缓存
            await queryClient.invalidateQueries({ queryKey: protocolQueryKeys.all });
            return result;
        },
        [protocol, queryClient]
    );
}

export async function loadProtocolExportData(
    protocol: Protocol.Type
): Promise<ProtocolExportItem[]> {
    const configs = await getAllProtocolConfigs({ protocol }, { _silent: true });
    return configs.map(({ id: _id, created_at: _c, updated_at: _u, ...rest }) => rest);
}

export const DEFAULT_PACKET_MERGE_GAP = 100;
export const DEFAULT_PACKET_MAX_QUANTITY = 125;
export const numberOrDefault = (value: unknown, fallback: number) => {
    const numericValue = Number(value);
    return Number.isFinite(numericValue) ? numericValue : fallback;
};
/** 设备类型表单的默认值，也用于兼容缺少新字段的历史配置。 */
export const getModbusDeviceTypeFormValues = (data?: Protocol.Item) => {
    const config = data?.config as Modbus.Config | undefined;
    const packet = normalizePacketConfig(config?.packet);
    return {
        name: data?.name ?? '',
        enabled: data?.enabled ?? true,
        byteOrder: config?.byteOrder ?? 'BIG_ENDIAN',
        readInterval: numberOrDefault(config?.readInterval, 300),
        storagePolicy: config?.storagePolicy ?? 'report',
        commandFastReadDuration: numberOrDefault(config?.commandFastReadDuration, 60),
        commandFastReadInterval: numberOrDefault(config?.commandFastReadInterval, 1),
        packetMergeGap: packet.mergeGap,
        packetMaxQuantity: packet.maxQuantity,
        remark: data?.remark ?? '',
    };
};
export const normalizePacketConfig = (
    packet?: Modbus.PacketConfig
): Required<Modbus.PacketConfig> => {
    const mergeGapRaw = Number(packet?.mergeGap);
    const maxQuantityRaw = Number(packet?.maxQuantity);
    const mergeGap = Number.isFinite(mergeGapRaw)
        ? Math.min(2000, Math.max(0, Math.floor(mergeGapRaw)))
        : DEFAULT_PACKET_MERGE_GAP;
    const maxQuantity = Number.isFinite(maxQuantityRaw)
        ? Math.min(125, Math.max(1, Math.floor(maxQuantityRaw)))
        : DEFAULT_PACKET_MAX_QUANTITY;
    return { mergeGap, maxQuantity };
};
/**
 * 将南桥旧配置中的寄存器类型归一化为管理端/API 使用的标准枚举。
 * 未识别值保留给兜底展示，避免一条历史脏数据导致整个配置页崩溃。
 */
export const normalizeRegisterType = (value: unknown): Modbus.RegisterType | undefined => {
    if (typeof value !== 'string') return undefined;
    switch (value.trim().toUpperCase()) {
        case 'COIL':
            return 'COIL';
        case 'DISCRETE':
        case 'DISCRETE_INPUT':
            return 'DISCRETE_INPUT';
        case 'INPUT':
        case 'INPUT_REGISTER':
            return 'INPUT_REGISTER';
        case 'HOLDING':
        case 'HOLDING_REGISTER':
            return 'HOLDING_REGISTER';
        default:
            return undefined;
    }
};
export const normalizeModbusRegisters = (registers: unknown): Modbus.Register[] => {
    if (!Array.isArray(registers)) return [];
    return registers
        .filter(
            (register): register is Record<string, unknown> =>
                !!register && typeof register === 'object' && !Array.isArray(register)
        )
        .map((value) => {
            const register = value as unknown as Modbus.Register;
            const registerType = normalizeRegisterType(value.registerType);
            return registerType ? { ...register, registerType } : register;
        });
};

export const buildRegisterGroupSections = (
    registers: Modbus.Register[]
): RegisterGroupSection[] => {
    return buildGroupSections(normalizeModbusRegisters(registers)).map((section) => {
        const typeCounts: Record<Modbus.RegisterType, number> = {
            COIL: 0,
            DISCRETE_INPUT: 0,
            HOLDING_REGISTER: 0,
            INPUT_REGISTER: 0,
        };
        for (const register of section.items) {
            const registerType = normalizeRegisterType(register.registerType);
            if (registerType) typeCounts[registerType]++;
        }
        return {
            key: section.key,
            label: section.label,
            count: section.count,
            registers: section.items,
            typeCounts,
        };
    });
};
/**
 * S7 协议配置
 */

export const defaultConfig = (): S7.Config => ({
    deviceType: '',
    plcModel: 'S7-1200',
    connection: {
        mode: 'RACK_SLOT',
        rack: 0,
        slot: 1,
        connectionType: 'PG',
        probeMode: 'STANDARD',
        handshakeTimeout: 5000,
        directProbeTimeout: 5000,
    },
    readInterval: 300,
    storagePolicy: 'report',
    commandFastReadDuration: 60,
    commandFastReadInterval: 1,
    areas: [],
});
export const plcModelOptions: PlcConnectionPreset[] = [
    {
        value: 'S7-200',
        label: 'S7-200',
        mode: 'TSAP',
        rack: 0,
        slot: 1,
        localTSAP: '4D57',
        remoteTSAP: '4D57',
    },
    {
        value: 'S7-300',
        label: 'S7-300',
        mode: 'RACK_SLOT',
        rack: 0,
        slot: 2,
        localTSAP: '0100',
        remoteTSAP: '0102',
    },
    {
        value: 'S7-400',
        label: 'S7-400',
        mode: 'RACK_SLOT',
        rack: 0,
        slot: 3,
        localTSAP: '0100',
        remoteTSAP: '0103',
    },
    {
        value: 'S7-1200',
        label: 'S7-1200',
        mode: 'RACK_SLOT',
        rack: 0,
        slot: 1,
        localTSAP: '0100',
        remoteTSAP: '0101',
    },
    {
        value: 'S7-1500',
        label: 'S7-1500',
        mode: 'RACK_SLOT',
        rack: 0,
        slot: 1,
        localTSAP: '0100',
        remoteTSAP: '0101',
    },
];
export const getPlcPreset = (plcModel: S7.PlcModel) =>
    plcModelOptions.find((option) => option.value === plcModel) ??
    plcModelOptions.find((option) => option.value === 'S7-1200') ??
    plcModelOptions[0];
export const getConnectionTypeCode = (connectionType?: S7.ConnectionType) => {
    if (connectionType === 'OP') return 0x02;
    if (connectionType === 'S7_BASIC') return 0x03;
    return 0x01;
};
export const buildRemoteTsapFromRackSlot = (
    rack: number,
    slot: number,
    connectionType?: S7.ConnectionType
) =>
    ((getConnectionTypeCode(connectionType) << 8) + rack * 0x20 + slot)
        .toString(16)
        .toUpperCase()
        .padStart(4, '0');
export const inferConnectionMode = (
    plcModel: S7.PlcModel,
    connection?: S7.Connection
): S7.ConnectionMode => {
    if (plcModel === 'S7-200') {
        return 'TSAP';
    }
    if (connection?.mode === 'TSAP' || connection?.mode === 'RACK_SLOT') {
        return connection.mode;
    }
    if (connection?.localTSAP || connection?.remoteTSAP) {
        return 'TSAP';
    }
    return getPlcPreset(plcModel).mode;
};
export const getConnectionFormValues = (plcModel: S7.PlcModel, connection?: S7.Connection) => {
    const preset = getPlcPreset(plcModel);
    const connectionMode = inferConnectionMode(plcModel, connection);
    const connectionType = connection?.connectionType ?? 'PG';
    const rack = connection?.rack ?? preset.rack;
    const slot = connection?.slot ?? preset.slot;
    const localTSAP =
        formatTsapValue(connection?.localTSAP) ??
        (connectionMode === 'TSAP' && preset.mode === 'TSAP' ? preset.localTSAP : '0100');
    const remoteTSAP =
        formatTsapValue(connection?.remoteTSAP) ??
        (connectionMode === 'TSAP' && preset.mode === 'TSAP'
            ? preset.remoteTSAP
            : buildRemoteTsapFromRackSlot(rack, slot, connectionType));
    return {
        connectionMode,
        connectionType,
        rack,
        slot,
        localTSAP,
        remoteTSAP,
        probeMode: connection?.probeMode ?? 'STANDARD',
        handshakeTimeout: connection?.handshakeTimeout ?? 5000,
        directProbeTimeout: connection?.directProbeTimeout ?? 5000,
    };
};
/** 设备类型表单默认值与编辑回填值。 */
export const getS7DeviceTypeFormValues = (data?: Protocol.Item): DeviceTypeFormValues => {
    const config = data?.config as S7.Config | undefined;
    const plcModel = config?.plcModel ?? 'S7-1200';
    return {
        deviceType: data?.name ?? '',
        plcModel,
        ...getConnectionFormValues(plcModel, config?.connection),
        readInterval: numberOrDefault(config?.readInterval, 300),
        storagePolicy: config?.storagePolicy ?? 'report',
        commandFastReadDuration: numberOrDefault(config?.commandFastReadDuration, 60),
        commandFastReadInterval: numberOrDefault(config?.commandFastReadInterval, 1),
        enabled: data?.enabled ?? true,
        remark: data?.remark ?? '',
    };
};
export const buildConnectionConfig = (values: DeviceTypeFormValues): S7.Connection => {
    const preset = getPlcPreset(values.plcModel);
    const forceTsap = values.plcModel === 'S7-200' || values.connectionMode === 'TSAP';
    if (forceTsap) {
        return {
            mode: 'TSAP',
            connectionType: values.connectionType,
            rack: undefined,
            slot: undefined,
            localTSAP: formatTsapValue(values.localTSAP) ?? preset.localTSAP,
            remoteTSAP: formatTsapValue(values.remoteTSAP) ?? preset.remoteTSAP,
            probeMode: values.probeMode,
            handshakeTimeout: values.handshakeTimeout,
            directProbeTimeout: values.directProbeTimeout,
        };
    }
    return {
        mode: 'RACK_SLOT',
        connectionType: values.connectionType,
        rack: values.rack,
        slot: values.slot,
        localTSAP: undefined,
        remoteTSAP: undefined,
        probeMode: values.probeMode,
        handshakeTimeout: values.handshakeTimeout,
        directProbeTimeout: values.directProbeTimeout,
    };
};
export const normalizeAreaTypeForPlcModel = (
    plcModel?: S7.PlcModel,
    areaType?: S7.AreaType
): S7.AreaType | undefined => {
    if (!areaType) {
        return areaType;
    }
    if (plcModel === 'S7-200' && areaType === 'DB') {
        return 'V';
    }
    return areaType;
};
export const areaDataTypeSizeMap: Record<S7.AreaDataType, number> = {
    BOOL: 1,
    INT8: 1,
    UINT8: 1,
    INT16: 2,
    UINT16: 2,
    INT32: 4,
    UINT32: 4,
    FLOAT: 4,
    LREAL: 8,
    STRING: 1,
};
export const getDataTypeSize = (dataType?: S7.AreaDataType) =>
    dataType ? (areaDataTypeSizeMap[dataType] ?? 1) : 1;
export const writableAreaTypes: S7.AreaType[] = ['DB', 'V', 'MK', 'PA'];
export const bitOnlyAreaTypes: S7.AreaType[] = ['PE', 'PA'];
export const normalizeS7DataType = (value?: string): S7.AreaDataType => {
    if (value === 'LREAL' || value === 'DOUBLE') {
        return 'LREAL';
    }
    if (
        value === 'BOOL' ||
        value === 'INT8' ||
        value === 'UINT8' ||
        value === 'INT16' ||
        value === 'UINT16' ||
        value === 'INT32' ||
        value === 'UINT32' ||
        value === 'FLOAT' ||
        value === 'STRING'
    ) {
        return value;
    }
    return 'INT16';
};
export const supportsS7Decimals = (dataType?: S7.AreaDataType) =>
    dataType === 'FLOAT' || dataType === 'LREAL';
export const supportsBitAddress = (areaType?: S7.AreaType, dataType?: S7.AreaDataType) =>
    dataType === 'BOOL' && !!areaType && areaType !== 'CT' && areaType !== 'TM';
/** 设备类型表单默认值与编辑回填值。 */
export const getSl651DeviceTypeFormValues = (data?: Protocol.Item) => {
    const config = data?.config as SL651.Config | undefined;
    return {
        name: data?.name ?? '',
        enabled: data?.enabled ?? true,
        responseMode: config?.responseMode ?? 'M1',
        storagePolicy: config?.storagePolicy ?? 'report',
        remark: data?.remark ?? '',
    };
};
