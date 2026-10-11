import type { Link } from '../link/link.types';
import type { Protocol, StoragePolicy } from '../protocol/protocol.types';
/**
 * 设备分组类型定义
 */
export type DeviceGroupStatus = 'enabled' | 'disabled';
/** 设备分组列表项 */
export interface DeviceGroupItem {
    id: string;
    name: string;
    parent_id?: string | null;
    status: DeviceGroupStatus;
    sort_order: number;
    remark?: string;
    created_at?: string;
    updated_at?: string;
    can_share?: boolean;
}
/** 设备分组树节点 */
export interface DeviceGroupTreeItem extends DeviceGroupItem {
    children?: DeviceGroupTreeItem[];
    /** 分组下的设备数量（设备事件 groups 快照返回） */
    deviceCount?: number;
}
/** 创建设备分组 DTO */
export interface CreateDeviceGroupDto {
    name: string;
    parent_id?: string | null;
    status?: DeviceGroupStatus;
    sort_order?: number;
    remark?: string;
}
/** 更新设备分组 DTO */
export interface UpdateDeviceGroupDto {
    name?: string;
    parent_id?: string | null;
    status?: DeviceGroupStatus;
    sort_order?: number;
    remark?: string;
}
/** 设备分组命名空间 */
export declare namespace DeviceGroup {
    type TreeItem = DeviceGroupTreeItem;
    type CreateDto = CreateDeviceGroupDto;
    type UpdateDto = UpdateDeviceGroupDto;
}

/**
 * 设备管理类型定义
 */
/** 设备状态 */
export type DeviceStatus = 'enabled' | 'disabled';
/** Modbus 通信模式（仅当链路是 TCP Server 且协议是 Modbus 时使用） */
export type ModbusMode = 'TCP' | 'RTU';
/** 心跳包模式 */
export type HeartbeatMode = 'OFF' | 'HEX' | 'ASCII';
/** 注册包模式 */
export type RegistrationMode = 'OFF' | 'HEX' | 'ASCII';
/** 设备资源访问级别 */
export type DeviceAccessLevel = 'owner' | 'operate' | 'view' | 'none';
/** 设备分享权限 */
export type DeviceShareAccessLevel = 'view' | 'operate';
/** 心跳包配置 */
export interface HeartbeatConfig {
    mode: HeartbeatMode;
    content?: string;
}
/** 注册包配置（仅平台直采 TCP Server 的 DTU 透传设备使用） */
export interface RegistrationConfig {
    mode: RegistrationMode;
    content?: string;
}
export type EdgeTransport = 'serial' | 'tcp';
export type EdgeMode = 'TCP Client' | 'TCP Server';
export type SerialParity = 'none' | 'even' | 'odd';
export interface EdgeStatus {
    state?: string;
    reason?: string;
    clientCount?: number;
    lastActivityAt?: string;
}
export interface EdgeConnection {
    edge_node_id?: string;
    edge_node_name?: string;
    edge_node_imei?: string;
    edge_transport?: EdgeTransport;
    edge_interface?: string;
    edge_mode?: EdgeMode;
    edge_ip?: string;
    edge_port?: number;
    edgeStatus?: EdgeStatus;
    serial_baud_rate?: number;
    serial_data_bits?: number;
    serial_stop_bits?: number;
    serial_parity?: SerialParity;
}
/** 设备选项（下拉列表） */
export interface DeviceOption {
    id: string;
    name: string;
    device_code: string;
    can_edit?: boolean;
    can_delete?: boolean;
    can_share?: boolean;
    can_command?: boolean;
    access_level?: DeviceAccessLevel;
}
/** 创建设备 DTO */
export interface CreateDeviceDto {
    edge_connection?: Link.SaveDto;
    name: string;
    device_code: string;
    link_id?: string;
    target_id?: string;
    protocol_config_id: string;
    group_id?: string | null;
    status?: DeviceStatus;
    /** 在线超时时间（秒），默认 300 秒（5分钟） */
    online_timeout?: number;
    /** 是否允许远控（下发指令），默认 true */
    remote_control?: boolean;
    /** Modbus 通信模式（仅当链路是 TCP Server 且协议是 Modbus 时使用） */
    modbus_mode?: ModbusMode;
    /** Modbus 从站地址（1-247），默认 1 */
    slave_id?: number;
    /** 设备时区（默认 +08:00） */
    timezone?: string;
    /** 心跳包配置 */
    heartbeat?: HeartbeatConfig;
    /** 注册包配置（仅平台直采 TCP Server 的 DTU 透传设备使用） */
    registration?: RegistrationConfig;
    remark?: string;
}
/** 更新设备 DTO */
export interface UpdateDeviceDto {
    edge_connection?: Link.SaveDto;
    name?: string;
    device_code?: string;
    link_id?: string;
    target_id?: string;
    protocol_config_id?: string;
    group_id?: string | null;
    status?: DeviceStatus;
    /** 在线超时时间（秒） */
    online_timeout?: number;
    /** 是否允许远控（下发指令） */
    remote_control?: boolean;
    /** Modbus 通信模式（仅当链路是 TCP Server 且协议是 Modbus 时使用） */
    modbus_mode?: ModbusMode;
    /** Modbus 从站地址（1-247），默认 1 */
    slave_id?: number;
    /** 设备时区 */
    timezone?: string;
    /** 心跳包配置 */
    heartbeat?: HeartbeatConfig;
    /** 注册包配置（仅平台直采 TCP Server 的 DTU 透传设备使用） */
    registration?: RegistrationConfig;
    remark?: string;
}
/** 设备静态数据（不包含实时数据） */
export interface DeviceStaticData extends EdgeConnection {
    debug_enabled?: boolean;
    link_debug_enabled?: boolean;
    // 基本信息
    id: string;
    name: string;
    /** 全局唯一设备编码 */
    device_code: string;
    link_id: string;
    target_id?: string;
    protocol_config_id: string;
    status: DeviceStatus;
    group_id?: string | null;
    online_timeout?: number;
    remote_control?: boolean;
    read_interval?: number;
    storage_policy?: StoragePolicy;
    /** 协议配置中的展示要素数量，用于稳定设备卡片布局 */
    element_count?: number;
    modbus_mode?: ModbusMode;
    slave_id?: number;
    timezone?: string;
    heartbeat?: HeartbeatConfig;
    registration?: RegistrationConfig;
    remark?: string;
    created_by?: string;
    created_at?: string;
    // 关联信息
    link_name?: string;
    link_mode?: Link.Mode;
    protocol_name?: string;
    protocol_type?: Protocol.Type;
    // 协议配置（按协议类型有条件返回）
    commandOperations?: CommandOperation[];
    imageOperations?: ImageOperation[];
    // 资源权限
    can_edit?: boolean;
    can_delete?: boolean;
    can_share?: boolean;
    can_command?: boolean;
    access_level?: DeviceAccessLevel;
}
/** 设备实时数据 */
export interface DeviceRealtimeSnapshot {
    id: string;
    device_code?: string;
    reportTime?: string;
    /** 设备南向链路是否已连接；不代表设备在线 */
    connected?: boolean;
    /** 设备南向链路状态；不代表设备在线 */
    connectionState?: 'connected' | 'disconnected';
    elements?: DeviceElement[];
    edge_node_id?: string;
    edge_transport?: EdgeTransport;
    edgeStatus?: EdgeStatus;
    image?: {
        data: string;
    };
    // 资源权限（realtime 接口同样返回）
    can_edit?: boolean;
    can_delete?: boolean;
    can_share?: boolean;
    can_command?: boolean;
    access_level?: DeviceAccessLevel;
}
// ========== 实时数据相关类型 ==========
/** 设备要素数据 */
export interface DeviceElement {
    visible?: boolean;
    quality?: string;
    id?: string;
    name: string;
    value: string | number | null;
    unit?: string;
    /** 缩放系数（用于没有显式小数位时推导展示精度） */
    scale?: number;
    /** 小数位数（-1 或 undefined 表示原始值） */
    decimals?: number;
    /** 分组名称（由协议配置映射，用于设备管理页聚合展示） */
    group?: string;
    /** 编码类型 */
    encode?: string;
    /** 字典配置（用于映射值显示） */
    dictConfig?: {
        mapType: 'VALUE' | 'BIT';
        items: Array<{
            key: string;
            label: string;
            value?: string;
            dependsOn?: {
                operator: 'AND' | 'OR';
                conditions: Array<{
                    bitIndex: string;
                    bitValue: string;
                }>;
            };
        }>;
    };
}
/** 控制操作要素预设值 */
export interface CommandOperationElementOption {
    label: string;
    value: string;
}
/** 控制操作要素 */
export interface CommandOperationElement {
    required?: boolean;
    elementId: string;
    name: string;
    value: string;
    unit?: string;
    /** 预设值选项（从协议配置继承） */
    options?: CommandOperationElementOption[];
    /** Modbus 寄存器类型 */
    registerType?: string;
    /** Modbus / S7 数据类型 */
    dataType?: string;
    /** S7 STRING 等类型的字节长度 */
    size?: number;
    /** SL651 编码类型 */
    encode?: string;
    /** SL651 数据长度（字节） */
    length?: number;
    /** SL651 BCD 小数位数 */
    digits?: number;
}
/** 控制操作 */
export interface CommandOperation {
    mqttMessageId?: string;
    name: string;
    elements: CommandOperationElement[];
}
/** 图片操作要素 */
export interface ImageOperationElement {
    elementId: string;
    name: string;
    encode: string;
    unit?: string;
}
/** 图片操作 */
export interface ImageOperation {
    name: string;
    elements: ImageOperationElement[];
    /** 最新图片数据 */
    latestImage?: {
        data: string;
        size?: number;
    };
}
/** 设备实时数据（包含管理字段） = 静态数据 + 实时字段 */
export interface DeviceOverview extends DeviceStaticData {
    /** 设备南向链路是否已连接；不代表设备在线 */
    connected?: boolean;
    /** 设备南向链路状态；不代表设备在线 */
    connectionState?: 'connected' | 'disconnected';
    reportTime?: string;
    elements?: DeviceElement[];
    image?: {
        data: string;
    };
}
/** 设备分享条目 */
export interface DeviceShareItem {
    id: string;
    subject_type: 'user' | 'department';
    subject_id: string;
    subject_name: string;
    access_level: DeviceShareAccessLevel;
    source_type?: 'device' | 'group';
    source_group_id?: string;
    source_group_name?: string;
    inherited?: boolean;
    created_at?: string;
    updated_at?: string;
}
/** 可选分享对象 */
export interface DeviceShareTarget {
    subject_type: 'user' | 'department';
    subject_id: string;
    subject_name: string;
}
/** 设备分享全量替换参数 */
export interface ReplaceDeviceSharesDto {
    shares: Array<{
        subject_type: 'user' | 'department';
        subject_id: string;
        access_level: DeviceShareAccessLevel;
    }>;
}
/** 指令下发参数 */
export interface CommandPayload {
    mqttMessageId?: string;
    idempotency_key?: string;
    elements: Array<{
        elementId: string;
        value: string;
    }>;
}
export interface CommandCreateResult {
    command_ids: string[];
    status: 'ACCEPTED';
}
export interface CommandStatusResult {
    command_id: string;
    device_id: string;
    device_code: string;
    protocol: Protocol.Type;
    status:
        | 'ACCEPTED'
        | 'DISPATCHING'
        | 'AWAITING_RESULT'
        | 'SUCCEEDED'
        | 'REJECTED'
        | 'UNKNOWN'
        | 'READBACK_MISMATCH'
        | 'FAILED';
    reason?: string;
    created_at_ms?: number;
    completed_at_ms?: number;
    actual_values?: Array<{
        element_id: string;
        name: string;
        kind: 'BOOL' | 'SIGNED' | 'UNSIGNED' | 'DOUBLE' | 'STRING' | 'BYTES' | 'UNSPECIFIED';
        value: string;
        unit: string;
    }>;
}
export interface CommandStatusesResult {
    complete: boolean;
    statuses: CommandStatusResult[];
}
/** 数据库存储的单个历史测点值 */
export interface HistoryPointValue {
    name?: string;
    value: unknown;
    unit?: string;
}
/** 单次设备上报的历史记录 */
export interface HistoryRecord {
    id: string;
    protocol: Protocol.Type;
    reportTime: string;
    source: string;
    functionCode?: string;
    values: Record<string, HistoryPointValue>;
    rawPayloadHex: string[];
}
/** 管理端历史记录查询参数 */
export interface DeviceHistoryQuery {
    page: number;
    pageSize: number;
    startTime: string;
    endTime: string;
}
type DeviceCommandOperation = CommandOperation;
type DeviceCommandOperationElement = CommandOperationElement;
type DeviceShareAccessLevelType = DeviceShareAccessLevel;
type DeviceShareItemType = DeviceShareItem;
type DeviceShareTargetType = DeviceShareTarget;
type ReplaceDeviceSharesDtoType = ReplaceDeviceSharesDto;
type DeviceCommandCreateResult = CommandCreateResult;
type DeviceCommandStatusesResult = CommandStatusesResult;
type DeviceHistoryRecord = HistoryRecord;
type DeviceHistoryPointValue = HistoryPointValue;
type DeviceHistoryRecordQuery = DeviceHistoryQuery;
/** 设备模块命名空间 */
export namespace Device {
    export type Option = DeviceOption;
    export type CreateDto = CreateDeviceDto;
    export type UpdateDto = UpdateDeviceDto;
    export type RealtimeSnapshot = DeviceRealtimeSnapshot;
    export type Element = DeviceElement;
    export type Overview = DeviceOverview;
    export type Command = CommandPayload;
    export type CommandCreateResult = DeviceCommandCreateResult;
    export type CommandStatusesResult = DeviceCommandStatusesResult;
    export type CommandOperation = DeviceCommandOperation;
    export type CommandOperationElement = DeviceCommandOperationElement;
    export type ShareAccessLevel = DeviceShareAccessLevelType;
    export type ShareItem = DeviceShareItemType;
    export type ShareTarget = DeviceShareTargetType;
    export type ReplaceSharesDto = ReplaceDeviceSharesDtoType;
    export type HistoryRecord = DeviceHistoryRecord;
    export type HistoryPointValue = DeviceHistoryPointValue;
    export type HistoryRecordQuery = DeviceHistoryRecordQuery;
}
