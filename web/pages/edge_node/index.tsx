import {
    ApartmentOutlined,
    CheckOutlined,
    CloudServerOutlined,
    CodeOutlined,
    DeleteOutlined,
    DownloadOutlined,
    EditOutlined,
    EyeOutlined,
    GlobalOutlined,
    PlusOutlined,
    ReloadOutlined,
    SyncOutlined,
    UploadOutlined,
} from '@ant-design/icons';
import { FitAddon } from '@xterm/addon-fit';
import { WebglAddon } from '@xterm/addon-webgl';
import { Terminal } from '@xterm/xterm';
import {
    Alert,
    App,
    Button,
    Descriptions,
    Drawer,
    Empty,
    Flex,
    Form,
    Input,
    InputNumber,
    Modal,
    Popconfirm,
    Progress,
    Result,
    Select,
    Skeleton,
    Space,
    Switch,
    Table,
    Tabs,
    Tag,
    Tooltip,
    TreeSelect,
    Upload,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { memo, useCallback, useEffect, useMemo, useState } from 'react';
import { FormModal } from '@/components/form_modal';
import type { GroupFormValues } from '@/components/group_form_modal';
import { GroupTreePanel } from '@/components/group_tree_panel';
import { LiveQueryError } from '@/components/live_query_error';
import { VirtualGrid } from '@/components/virtual_grid';
import { usePermissions } from '@/hooks/use_permission';
import {
    dtuTraceHex,
    useDtuChannels,
    useDtuDelete,
    useDtuSave,
    useEdgeGroupDelete,
    useEdgeGroupSave,
    useEdgeGroupTree,
    useEdgeIo,
    useEdgeVpn,
    useEdgeVpnPeerCreate,
    useEdgeVpnPeerRevoke,
    useEdgeVpnPeerSync,
    useEdgeVpnRouteUpdate,
    useIoControl,
    windowsClientDownloadUrl,
} from './edge_node.service';
import type { Edge, EdgeVpn } from './edge_node.types';
import '@xterm/xterm/css/xterm.css';
import type { CSSProperties, ReactNode } from 'react';
import { useRef } from 'react';
import type { DeviceCardItem } from '@/components/device_card';
import DeviceCard from '@/components/device_card';
import { PageContainer } from '@/components/page_container';
import { formatDateTime } from '@/utils/date_time';
import { createUuid } from '@/utils/uuid';
import { validateForm } from '@/utils/validation';
import {
    dtuAsciiPacketSchema,
    dtuChannelSchema,
    firmwareUpgradeSchema,
    networkInterfaceSchema,
    networkSchema,
    nodeNameSchema,
    serialSettingsSchema,
} from './edge_node.schema';
import {
    buildEdgeNodeGroupView,
    normalizeReportedNetwork,
    physicalNetworkInterfaces,
    serialPayloadHex,
    useAssignEdgeNodeGroup,
    useConfigureEdgeNetwork,
    useDeviceConfigSyncMutation,
    useEdgeDeleteMutation,
    useEdgeDetail,
    useEdgeInventory,
    useEdgeLogs,
    useEnrollmentMutation,
    useFetchEdgeConfiguration,
    useFirmwareUpgradeMutation,
    useMobileOperatorName,
    useRenameEdgeNode,
    useSerialDebug,
    useSetEdgeLogLevel,
    useTerminalSession,
} from './edge_node.service';

const EMPTY_GROUPS: Edge.GroupTreeItem[] = [];
const EMPTY_NODES: Edge.Node[] = [];
const getNodeKey = (node: Edge.Node) => node.id;
const toGroupFormValues = (group: Edge.GroupTreeItem): GroupFormValues => ({
    id: group.id,
    name: group.name,
    parentId: group.parentId || undefined,
    sortOrder: group.sortOrder,
    status: group.status,
    remark: group.remark,
});

const statusLabels: Record<
    string,
    {
        color: string;
        text: string;
    }
> = {
    pending: { color: 'processing', text: '等待节点上报公钥' },
    active: { color: 'success', text: '已启用' },
    revoked: { color: 'error', text: '已撤销' },
    error: { color: 'error', text: '错误' },
    disabled: { color: 'default', text: '已停用' },
};
function vpnStatusTag(status: string) {
    const item = statusLabels[status] ?? { color: 'default', text: status || '-' };
    return <Tag color={item.color}>{item.text}</Tag>;
}
function peerName(node: Edge.Node) {
    return `${node.name || node.hostname || node.imei} VPN`;
}
type RouteFormValues = Pick<EdgeVpn.RouteDto, 'virtualCidr'>;
const VPN_MODAL_Z_INDEX = 1100;
export function EdgeVpnPanel({ node, scope }: { node: Edge.Node; scope: Edge.EventScope }) {
    const { has } = usePermissions();
    const canQuery = has('iot:vpn:query');
    const canAdd = has('iot:vpn:add');
    const canEdit = has('iot:vpn:edit');
    const canRevoke = has('iot:vpn:revoke');
    const dataQuery = useEdgeVpn(scope, canQuery && Boolean(scope.vpn));
    const data = dataQuery.data;
    const peer = data?.peers.find((item) => item.peerType === 'edge' && item.status !== 'revoked');
    const network = data?.networks[0];
    const [routeOpen, setRouteOpen] = useState(false);
    const [editingRoute, setEditingRoute] = useState<EdgeVpn.Route | undefined>();
    const [routeForm] = Form.useForm<RouteFormValues>();
    const peerCreate = useEdgeVpnPeerCreate();
    const peerSync = useEdgeVpnPeerSync();
    const peerRevoke = useEdgeVpnPeerRevoke();
    const routeUpdate = useEdgeVpnRouteUpdate();
    const bridgeNetworks = node.networks?.filter(
        (item) => item.bridge && item.ipv4 && item.prefixLength >= 1 && item.prefixLength <= 30
    );
    useEffect(() => {
        if (!routeOpen) return;
        if (editingRoute) routeForm.setFieldsValue({ virtualCidr: editingRoute.virtualCidr });
    }, [editingRoute, routeForm, routeOpen]);
    const routeColumns = useMemo<ColumnsType<EdgeVpn.Route>>(
        () => [
            { title: 'LAN 接口', dataIndex: 'lanInterface', width: 120 },
            { title: '真实网段', dataIndex: 'targetCidr', width: 140 },
            { title: '虚拟网段', dataIndex: 'virtualCidr', width: 140 },
            {
                title: '模式',
                dataIndex: 'mode',
                width: 90,
                render: (value: EdgeVpn.RouteMode) => (value === 'nat' ? 'NAT' : '路由'),
            },
            {
                title: '状态',
                dataIndex: 'status',
                width: 150,
                render: (value: string, item) =>
                    item.lastError ? (
                        <span title={item.lastError}>{vpnStatusTag(value)}</span>
                    ) : (
                        vpnStatusTag(value)
                    ),
            },
            {
                title: '操作',
                key: 'actions',
                width: 130,
                render: (_, item) => (
                    <Space size={0}>
                        {canEdit && (
                            <Button
                                type="link"
                                size="small"
                                onClick={() => {
                                    setEditingRoute(item);
                                    setRouteOpen(true);
                                }}
                            >
                                编辑
                            </Button>
                        )}
                    </Space>
                ),
            },
        ],
        [canEdit]
    );
    if (!canQuery) {
        return <Alert type="warning" showIcon message="您没有 VPN 查询权限" />;
    }
    const supportsVpn = node.capability.vpn?.supportsVpn === true;
    const hasBridgeNetwork = Boolean(bridgeNetworks?.length);
    const closeRoute = () => {
        if (!routeUpdate.isPending) {
            setRouteOpen(false);
            setEditingRoute(undefined);
        }
    };
    return (
        <div className="space-y-4">
            {!supportsVpn && (
                <Alert
                    type="warning"
                    showIcon
                    message="当前节点未上报 WireGuard 能力"
                    description="请将节点代理升级到支持 VPN 的版本；旧节点仍可正常使用其他功能。"
                />
            )}
            {supportsVpn && !hasBridgeNetwork && (
                <Alert
                    type="warning"
                    showIcon
                    message="当前节点没有可映射的桥接 LAN"
                    description="请先让 EdgeNode 上报桥接网段，VPN 会根据该网段自动生成等长的虚拟映射。"
                />
            )}
            <LiveQueryError
                error={dataQuery.error ?? dataQuery.connectionError}
                retry={dataQuery.refetch}
                loading={dataQuery.isFetching}
            />
            <Flex justify="space-between" align="center" gap={12} wrap>
                <div>
                    <div className="font-medium text-slate-800">节点 VPN 能力 · iot-server</div>
                    <div className="mt-1 text-xs text-slate-500">
                        使用 iot-server 的 WireGuard Server。真实 LAN、桥接接口和掩码由节点上报；
                        开启能力后预先分配节点地址和虚拟网段；客户端开启 VPN
                        并选中该节点时才启动隧道， 最后一个客户端停止使用后关闭隧道并保留分配结果。
                    </div>
                </div>
                <Space wrap>
                    <Button
                        icon={<ReloadOutlined />}
                        loading={dataQuery.isFetching}
                        onClick={() => void dataQuery.refetch()}
                    >
                        刷新
                    </Button>
                    {!peer && canAdd && (
                        <Button
                            type="primary"
                            icon={<CloudServerOutlined />}
                            loading={peerCreate.isPending}
                            disabled={!supportsVpn || !data?.networks.length || !hasBridgeNetwork}
                            onClick={() =>
                                peerCreate.mutate({
                                    peerType: 'edge',
                                    edgeNodeId: node.id,
                                    name: peerName(node),
                                })
                            }
                        >
                            开启 VPN 能力
                        </Button>
                    )}
                </Space>
            </Flex>

            {dataQuery.isLoading ? (
                <Skeleton active paragraph={{ rows: 4 }} />
            ) : !data?.networks.length ? (
                <Empty description="默认 iot-server VPN 网络尚未就绪" />
            ) : (
                <>
                    <Descriptions bordered size="small" column={{ xs: 1, sm: 2, lg: 4 }}>
                        <Descriptions.Item label="VPN 网络">
                            {network?.name ?? '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="Overlay">
                            {network?.overlayCidr ?? '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="节点地址">
                            {peer?.assignedIpv4 ?? '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="VPN 能力">
                            {peer ? <Tag color="green">已开启</Tag> : <Tag>未开启</Tag>}
                        </Descriptions.Item>
                        <Descriptions.Item label="Peer 配置状态">
                            {peer ? vpnStatusTag(peer.status) : '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="Agent / WireGuard" span={2}>
                            {node.capability.vpn?.agentVersion || '-'} /{' '}
                            {node.capability.vpn?.wireguardVersion || '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="配置版本">
                            {peer?.configRevision ?? '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="公钥">
                            {node.capability.vpn?.publicKey || peer?.publicKey || '-'}
                        </Descriptions.Item>
                    </Descriptions>

                    {peer && (
                        <Flex justify="end" gap={8} wrap>
                            {canEdit && (
                                <Button
                                    icon={<SyncOutlined />}
                                    loading={peerSync.isPending}
                                    onClick={() => peerSync.mutate(peer.id)}
                                >
                                    重新下发配置
                                </Button>
                            )}
                            {canRevoke && peer.status !== 'revoked' && (
                                <Popconfirm
                                    title="确认关闭当前节点的 VPN 能力吗？"
                                    description="关闭后客户端无法使用该节点，当前 Peer 和地址分配将被撤销。"
                                    onConfirm={() => peerRevoke.mutate(peer.id)}
                                >
                                    <Button danger loading={peerRevoke.isPending}>
                                        关闭 VPN 能力
                                    </Button>
                                </Popconfirm>
                            )}
                        </Flex>
                    )}

                    <Flex justify="space-between" align="center" gap={12}>
                        <span className="text-sm font-medium text-slate-800">LAN 路由映射</span>
                    </Flex>
                    <Table
                        rowKey="id"
                        size="small"
                        pagination={false}
                        columns={routeColumns}
                        dataSource={peer ? data.routes : []}
                        locale={{ emptyText: peer ? '暂无路由映射' : '请先开启节点 VPN 能力' }}
                        scroll={{ x: 'max-content' }}
                    />
                </>
            )}

            <FormModal
                open={routeOpen}
                title="修改虚拟网段"
                onCancel={closeRoute}
                onOk={() => routeForm.submit()}
                confirmLoading={routeUpdate.isPending}
                destroyOnHidden
                zIndex={VPN_MODAL_Z_INDEX}
            >
                <Form
                    form={routeForm}
                    layout="vertical"
                    onFinish={(values) => {
                        if (!editingRoute) return;
                        routeUpdate.mutate(
                            { id: editingRoute.id, data: values },
                            { onSuccess: closeRoute }
                        );
                    }}
                >
                    <Descriptions bordered size="small" column={1} className="mb-4">
                        <Descriptions.Item label="桥接接口">
                            {editingRoute?.lanInterface ?? '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="真实 LAN">
                            {editingRoute?.targetCidr ?? '-'}
                        </Descriptions.Item>
                        <Descriptions.Item label="模式">NAT（自动）</Descriptions.Item>
                    </Descriptions>
                    <p className="mb-4 text-xs text-slate-500">
                        真实 LAN、桥接接口、掩码和 NAT 模式由 EdgeNode
                        自动确定；只允许修改虚拟网段网络号， 且必须与真实 LAN
                        使用相同掩码并保持全局唯一。
                    </p>
                    <Form.Item
                        name="virtualCidr"
                        label="虚拟 LAN 网段"
                        rules={[
                            { required: true, message: '请输入虚拟 LAN 网段' },
                            {
                                pattern: /^172\.\d{1,3}\.\d{1,3}\.\d{1,3}\/\d{1,2}$/,
                                message: '请输入 172.0.0.0/8 范围内的网络 CIDR',
                            },
                        ]}
                    >
                        <Input placeholder="172.168.1.0/24" />
                    </Form.Item>
                </Form>
            </FormModal>
        </div>
    );
}

type NetworkDraftItem = Edge.NetworkConfig & {
    sourceName?: string;
    original: boolean;
    dirty: boolean;
    up?: boolean;
};
const EDGE_CARD_GRID_CLASS = 'grid grid-cols-1 gap-3 xl:grid-cols-2 2xl:grid-cols-4';
/** Keeps one column on narrow screens and up to four on wide ones, like the former grid. */
const EDGE_CARD_MIN_WIDTH = 280;
const EDGE_DETAIL_DRAWER_Z_INDEX = 1000;
const EDGE_ACTION_MODAL_Z_INDEX = EDGE_DETAIL_DRAWER_Z_INDEX + 100;
function groupSelectOptions(groups: Edge.GroupTreeItem[]): {
    value: string;
    title: string;
    disabled: boolean;
    children?: ReturnType<typeof groupSelectOptions>;
}[] {
    return groups.map((group) => ({
        value: group.id,
        title: group.name,
        disabled: group.status === 'disabled',
        children: group.children?.length ? groupSelectOptions(group.children) : undefined,
    }));
}
function statusTag(status: string) {
    const map: Record<
        string,
        {
            color: string;
            text: string;
        }
    > = {
        pending: { color: 'processing', text: '待处理' },
        approved: { color: 'success', text: '已批准' },
        rejected: { color: 'error', text: '已拒绝' },
        applied: { color: 'success', text: '已应用' },
        succeeded: { color: 'success', text: '成功' },
        accepted: { color: 'processing', text: '已接收' },
        running: { color: 'processing', text: '执行中' },
        failed: { color: 'error', text: '失败' },
        idle: { color: 'default', text: '未下发' },
    };
    const item = map[status] ?? { color: 'default', text: status || '-' };
    return <Tag color={item.color}>{item.text}</Tag>;
}
function logLevelTag(level: string) {
    const map: Record<string, string> = {
        debug: 'default',
        info: 'processing',
        warn: 'warning',
        error: 'error',
    };
    return <Tag color={map[level] ?? 'default'}>{level || '-'}</Tag>;
}
function networkDraftFromReported(
    item: Edge.Network,
    interfaces: Edge.NetworkInterface[]
): NetworkDraftItem {
    const binding = normalizeReportedNetwork(item, interfaces);
    return {
        operation: 'upsert',
        name: item.name,
        sourceName: item.name,
        mode: item.mode === 'static' ? 'static' : 'dhcp',
        device: binding.device,
        bridge: binding.bridge,
        bridgePorts: binding.bridgePorts,
        ip: item.mode === 'static' ? item.ipv4 : '',
        prefixLength: item.mode === 'static' ? item.prefixLength : 0,
        gateway: item.mode === 'static' ? item.gateway : '',
        original: true,
        dirty: false,
        up: item.up,
    };
}
function formatBytes(value: number) {
    if (value < 1024) return `${value} B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
    return `${(value / 1024 / 1024).toFixed(1)} MiB`;
}
function validConfigTimestamp(value: number) {
    return value >= Date.UTC(2020, 0, 1);
}
function formatConfigVersions(active: number, desired: number) {
    const activeValid = validConfigTimestamp(active);
    const desiredValid = validConfigTimestamp(desired);
    if (activeValid && desiredValid && active === desired) return String(active);
    if (activeValid && desiredValid) {
        return `已应用 ${active} · 目标 ${desired}`;
    }
    if (desiredValid) return `${desired}（等待节点应用）`;
    if (activeValid) return `已应用 ${active}`;
    return '--';
}
function simStateText(state: Edge.Mobile['simState']) {
    const values: Record<Edge.Mobile['simState'], string> = {
        unknown: '未知',
        ready: '就绪',
        not_inserted: '未插卡',
        pin_required: '需要 PIN',
        puk_required: '需要 PUK',
        blocked: '已锁定',
    };
    return values[state] ?? '未知';
}
function mobileState(node: Edge.Node) {
    const mobile = node.mobile;
    if (!mobile.available) return '未检测到';
    if (mobile.simState !== 'ready') return `SIM ${simStateText(mobile.simState)}`;
    if (mobile.connected) return `已连接${mobile.ipv4 ? ` · ${mobile.ipv4}` : ''}`;
    return mobile.registered ? '已注册，未拨号' : '未注册';
}
function compactBytes(value: number) {
    if (value < 1024) return `${value}B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)}KiB`;
    if (value < 1024 * 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)}MiB`;
    return `${(value / 1024 / 1024 / 1024).toFixed(1)}GiB`;
}
function trafficPair(traffic: Edge.TcpTraffic | undefined, period: 'daily' | 'monthly') {
    const format = (bytes: string | undefined) =>
        bytes === undefined || bytes === '' ? '-' : compactBytes(Number(bytes));
    return period === 'daily'
        ? `${format(traffic?.dailyUploadBytes)}|${format(traffic?.dailyDownloadBytes)}`
        : `${format(traffic?.monthlyUploadBytes)}|${format(traffic?.monthlyDownloadBytes)}`;
}
function buildNodeCardItems(
    node: Edge.Node,
    operatorName: (value: string) => string
): DeviceCardItem[] {
    const config = node.status.config;
    const outbox = node.status.outbox;
    const mobile = node.mobile;
    return [
        { key: 'hostname', label: '主机名', children: node.hostname || '-' },
        { key: 'architecture', label: '系统架构', children: node.architecture || '-' },
        { key: 'openwrt', label: 'OpenWrt', children: node.openwrtRelease || '-' },
        { key: 'operator', label: '运营商', children: operatorName(mobile.operator) || '-' },
        { key: 'mobileState', label: '4G 状态', children: mobileState(node) },
        {
            key: 'mobileSignal',
            label: '4G 信号',
            children: mobile.available
                ? `${mobile.signal.percent}%${mobile.signal.rssiDbm !== -1 ? ` · ${mobile.signal.rssiDbm} dBm` : ''}`
                : '-',
        },
        {
            key: 'iccid',
            label: 'ICCID',
            span: 2,
            children: mobile.iccid || '-',
        },
        {
            key: 'configVersion',
            label: '配置版本',
            children: formatConfigVersions(config.activeVersion, config.desiredVersion),
        },
        {
            key: 'outbox',
            label: '待传缓存',
            children: `${outbox.records ?? 0} 条 / ${formatBytes(outbox.bytes ?? 0)}`,
        },
        {
            key: 'dailyTraffic',
            label: '今日流量',
            children: trafficPair(node.status.tcpTraffic, 'daily'),
        },
        {
            key: 'monthlyTraffic',
            label: '本月流量',
            children: trafficPair(node.status.tcpTraffic, 'monthly'),
        },
        {
            key: 'dailyVpn',
            label: '今日 VPN',
            children: trafficPair(node.status.vpnTraffic, 'daily'),
        },
        {
            key: 'monthlyVpn',
            label: '本月 VPN',
            children: trafficPair(node.status.vpnTraffic, 'monthly'),
        },
        {
            key: 'vpnVirtualCidrs',
            label: 'VPN 网段',
            span: 2,
            children: node.vpnVirtualCidrs?.length ? node.vpnVirtualCidrs.join('、') : '-',
        },
    ];
}
/** One inventory card; memoized so a node delta re-renders only that node's card. */
const EdgeNodeCard = memo(
    ({
        node,
        operatorName,
        onOpen,
    }: {
        node: Edge.Node;
        operatorName: (value: string) => string;
        onOpen: (node: Edge.Node) => void;
    }) => {
        const status = node.status;
        const nodeTitle = node.name || node.hostname || '未命名节点';
        const firmwareActive =
            node.firmware.state === 'accepted' || node.firmware.state === 'running';
        return (
            <div className="flex flex-col">
                <DeviceCard
                    onClick={() => onOpen(node)}
                    ariaLabel={`查看边缘节点 ${node.name || node.hostname || node.imei}`}
                    title={
                        <div className="flex w-full min-w-0 flex-col gap-0.5">
                            <Flex
                                justify="space-between"
                                align="start"
                                gap={10}
                                className="w-full min-w-0"
                            >
                                <Tooltip title={nodeTitle}>
                                    <span className="min-w-0 flex-1 truncate whitespace-nowrap pr-1 text-left leading-5">
                                        {nodeTitle}
                                    </span>
                                </Tooltip>
                                <span className="flex shrink-0 items-center gap-1 [&_.ant-tag]:!m-0">
                                    {statusTag(node.enrollmentStatus)}
                                    <Tag
                                        color={status.online ? 'success' : 'default'}
                                        className="!mr-0 shrink-0 !rounded-md !px-2"
                                    >
                                        {status.online ? '在线' : '离线'}
                                    </Tag>
                                </span>
                            </Flex>
                            <div className="flex w-full min-w-0 items-center justify-between gap-2 text-xs font-normal leading-4 text-slate-400">
                                <Tooltip title={node.imei}>
                                    <span className="min-w-0 truncate text-left">
                                        IMEI：{node.imei}
                                    </span>
                                </Tooltip>
                                <span className="shrink-0 truncate">
                                    上报：{formatDateTime(status.lastSeenAt)}
                                </span>
                            </div>
                        </div>
                    }
                    subtitle={
                        <span className="flex min-w-0 items-center gap-1">
                            <Tag color="blue" className="!mr-0 !rounded-md">
                                {node.model || '未知型号'}
                            </Tag>
                            <Tag color="purple" className="!mr-0 !rounded-md">
                                {node.softwareVersion || '未知版本'}
                            </Tag>
                        </span>
                    }
                    items={buildNodeCardItems(node, operatorName)}
                    column={2}
                    extra={
                        <div className="flex w-full flex-col gap-1.5">
                            <Progress
                                className={firmwareActive ? undefined : 'invisible'}
                                percent={firmwareActive ? node.firmware.progressPercent : 0}
                                size="small"
                                status="active"
                                format={() =>
                                    firmwareActive
                                        ? `${formatBytes(node.firmware.downloadedBytes)} / ${formatBytes(node.firmware.totalBytes)}`
                                        : '\u00a0'
                                }
                            />
                            <Flex
                                align="center"
                                justify="center"
                                gap={6}
                                className="w-full text-slate-500"
                            >
                                <EyeOutlined />
                                <span>查看详情</span>
                            </Flex>
                        </div>
                    }
                />
            </div>
        );
    }
);
export function SerialDebugModal({
    nodeId,
    path,
    title,
    onClose,
}: {
    nodeId: string;
    path: string;
    title: string;
    onClose: () => void;
}) {
    const debug = useSerialDebug(nodeId, path);
    const { message } = App.useApp();
    const [form] = Form.useForm<Edge.SerialSettings>();
    const values = Form.useWatch([], form) as Edge.SerialSettings | undefined;
    const [display, setDisplay] = useState<'hex' | 'text'>('hex');
    const [sendMode, setSendMode] = useState<'hex' | 'text'>('hex');
    const [input, setInput] = useState('');
    const [ending, setEnding] = useState('');
    const [follow, setFollow] = useState(true);
    const logRef = useRef<HTMLDivElement>(null);
    useEffect(() => {
        form.setFieldsValue(debug.settings);
    }, [form, debug.settings]);
    useEffect(() => {
        if (follow && debug.frames.length && logRef.current)
            logRef.current.scrollTop = logRef.current.scrollHeight;
    }, [debug.frames, follow]);
    const ready = debug.connection === 'ready';
    const changed =
        values &&
        Object.keys(debug.settings).some(
            (key) =>
                values[key as keyof Edge.SerialSettings] !==
                debug.settings[key as keyof Edge.SerialSettings]
        );
    const run = (operation: () => void) => {
        try {
            operation();
        } catch (error) {
            void message.error(error instanceof Error ? error.message : '操作失败');
        }
    };
    return (
        <FormModal
            open
            title={`串口调试 · ${title} · ${path}`}
            zIndex={EDGE_ACTION_MODAL_Z_INDEX}
            onCancel={onClose}
            styles={{
                body: { display: 'flex', flexDirection: 'column', gap: 12, overflow: 'hidden' },
            }}
            footer={
                <div className="flex flex-col gap-2 text-left">
                    <Space wrap>
                        <Select
                            aria-label="发送格式"
                            value={sendMode}
                            onChange={setSendMode}
                            options={[
                                { value: 'hex', label: 'HEX 发送' },
                                { value: 'text', label: '文本发送（UTF-8）' },
                            ]}
                        />
                        {sendMode === 'text' && (
                            <Select
                                aria-label="发送换行"
                                value={ending}
                                onChange={setEnding}
                                options={[
                                    { value: '', label: '无换行' },
                                    { value: '\r', label: 'CR' },
                                    { value: '\n', label: 'LF' },
                                    { value: '\r\n', label: 'CRLF' },
                                ]}
                            />
                        )}
                        <span className="text-xs text-slate-500">单次最多 1024 字节</span>
                    </Space>
                    <Input.TextArea
                        aria-label="发送内容"
                        value={input}
                        onChange={(event) => setInput(event.target.value)}
                        autoSize={{ minRows: 2, maxRows: 3 }}
                        maxLength={8192}
                        placeholder={
                            sendMode === 'hex' ? '01 03 00 00 00 02 C4 0B' : '输入要发送的文本'
                        }
                    />
                    <Flex justify="space-between" align="center" gap={8} wrap>
                        <span className="text-xs text-slate-500">
                            {debug.manual
                                ? changed
                                    ? '参数已修改，请先应用参数'
                                    : '自动采集已暂停，可以手动发送'
                                : '先暂停自动采集，再手动发送'}
                        </span>
                        <Space>
                            <Button onClick={onClose}>关闭</Button>
                            <Button
                                type="primary"
                                loading={debug.pending}
                                disabled={!ready || !debug.manual || !!changed || !input}
                                onClick={() =>
                                    run(() =>
                                        debug.send('write', {
                                            hex: serialPayloadHex(input, sendMode, ending),
                                        })
                                    )
                                }
                            >
                                发送
                            </Button>
                        </Space>
                    </Flex>
                </div>
            }
        >
            <Flex justify="space-between" align="center" gap={8} wrap>
                <Tag color={!ready ? 'default' : debug.manual ? 'orange' : 'green'}>
                    {debug.connection === 'connecting'
                        ? '正在连接'
                        : debug.connection === 'closed'
                          ? '已断开'
                          : debug.manual
                            ? '手动调试'
                            : '监听中'}
                </Tag>
                <Space wrap>
                    {debug.connection === 'closed' ? (
                        <Button onClick={debug.reconnect}>重新连接</Button>
                    ) : (
                        <Button
                            disabled={!ready}
                            loading={debug.pending}
                            onClick={() =>
                                run(() =>
                                    debug.manual
                                        ? debug.send('monitor')
                                        : debug.send('manual', debug.settings)
                                )
                            }
                        >
                            {debug.manual ? '恢复自动采集' : '暂停自动采集'}
                        </Button>
                    )}
                </Space>
            </Flex>
            {debug.notice && (
                <Alert
                    type={debug.connection === 'closed' ? 'warning' : 'info'}
                    showIcon
                    title={debug.notice}
                />
            )}
            <Form
                form={form}
                layout="vertical"
                initialValues={debug.settings}
                disabled={!ready || !debug.manual || debug.pending}
                className="shrink-0"
                onFinish={(raw) => {
                    const parsed = validateForm(form, serialSettingsSchema, raw);
                    if (parsed) run(() => debug.send('manual', parsed));
                }}
            >
                <div className="grid grid-cols-3 gap-x-3 sm:grid-cols-5">
                    <Form.Item label="波特率" name="baudRate" className="mb-2">
                        <Select
                            options={[
                                300, 600, 1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200,
                                230400, 460800,
                            ].map((value) => ({ value, label: String(value) }))}
                        />
                    </Form.Item>
                    <Form.Item label="数据位" name="dataBits" className="mb-2">
                        <Select
                            options={[5, 6, 7, 8].map((value) => ({ value, label: String(value) }))}
                        />
                    </Form.Item>
                    <Form.Item label="停止位" name="stopBits" className="mb-2">
                        <Select
                            options={[
                                { value: 1, label: '1' },
                                { value: 2, label: '2' },
                            ]}
                        />
                    </Form.Item>
                    <Form.Item label="校验" name="parity" className="mb-2">
                        <Select
                            options={[
                                { value: 'none', label: '无' },
                                { value: 'even', label: '偶校验' },
                                { value: 'odd', label: '奇校验' },
                            ]}
                        />
                    </Form.Item>
                    <Form.Item label="RS485" name="rs485" valuePropName="checked" className="mb-2">
                        <Switch />
                    </Form.Item>
                </div>
                {debug.manual && (
                    <Button
                        htmlType="submit"
                        size="small"
                        disabled={!changed}
                        loading={debug.pending}
                    >
                        应用串口参数
                    </Button>
                )}
            </Form>
            <Flex justify="space-between" align="center" gap={8} wrap className="shrink-0">
                <Space wrap>
                    <Select
                        aria-label="接收显示格式"
                        size="small"
                        value={display}
                        onChange={setDisplay}
                        options={[
                            { value: 'hex', label: 'HEX 显示' },
                            { value: 'text', label: '文本显示' },
                        ]}
                    />
                    <Switch
                        size="small"
                        checked={follow}
                        onChange={setFollow}
                        aria-label="自动滚动"
                    />
                    <span className="text-xs">自动滚动</span>
                    <Button size="small" onClick={debug.clear}>
                        清空记录
                    </Button>
                </Space>
                <span className="text-xs text-slate-500">
                    TX {debug.counts.tx} B · RX {debug.counts.rx} B
                    {debug.counts.dropped > 0 ? ` · 丢弃 ${debug.counts.dropped} B` : ''}
                </span>
            </Flex>
            <div
                ref={logRef}
                role="log"
                aria-label="串口收发记录"
                className="min-h-24 flex-1 overflow-auto rounded-md bg-slate-950 p-3 font-mono text-xs text-slate-100"
            >
                {debug.frames.length === 0 ? (
                    <span className="text-slate-400">等待串口数据…</span>
                ) : (
                    debug.frames.map((frame) => (
                        <div key={frame.id} className="mb-1 grid grid-cols-[auto_auto_1fr] gap-2">
                            <span className="text-slate-400">
                                {new Date(frame.timestamp).toLocaleTimeString('zh-CN', {
                                    hour12: false,
                                })}
                                .{String(frame.timestamp % 1000).padStart(3, '0')}
                            </span>
                            <span
                                className={
                                    frame.direction === 'TX' ? 'text-amber-300' : 'text-emerald-300'
                                }
                            >
                                {frame.direction}
                            </span>
                            <span className="whitespace-pre-wrap break-all">
                                {display === 'hex' ? frame.hex.match(/../g)?.join(' ') : frame.text}
                            </span>
                        </div>
                    ))
                )}
            </div>
            <span className="shrink-0 text-xs text-slate-500">
                保留最近 200 条记录。关闭调试或会话超时后，节点恢复自动采集。
            </span>
        </FormModal>
    );
}

function TerminalModal({
    nodeId,
    open,
    onClose,
}: {
    nodeId?: string;
    open: boolean;
    onClose: () => void;
}) {
    const terminalHostRef = useRef<HTMLDivElement | null>(null);
    const [screen, setScreen] = useState<Edge.TerminalScreen | null>(null);
    const session = useTerminalSession(nodeId, screen);
    const { input, resize } = session;
    useEffect(() => {
        const host = terminalHostRef.current;
        if (!open || !nodeId || !host) return;
        let disposed = false;
        let fitFrame: number | undefined;
        const terminal = new Terminal({
            cursorBlink: true,
            fontFamily: "'Cascadia Mono', 'SFMono-Regular', Consolas, 'Liberation Mono', monospace",
            fontSize: 14,
            lineHeight: 1.2,
            scrollback: 2000,
            theme: {
                background: '#020617',
                foreground: '#d1fae5',
                cursor: '#6ee7b7',
                cursorAccent: '#020617',
                selectionBackground: '#164e63',
                black: '#0f172a',
                brightBlack: '#64748b',
                green: '#34d399',
                brightGreen: '#6ee7b7',
            },
        });
        const fitAddon = new FitAddon();
        terminal.loadAddon(fitAddon);
        terminal.open(host);
        try {
            const webgl = new WebglAddon();
            webgl.onContextLoss(() => webgl.dispose());
            terminal.loadAddon(webgl);
        } catch {
            // The default renderer remains available without WebGL.
        }
        const scheduleFit = () => {
            if (disposed || fitFrame !== undefined) return;
            fitFrame = window.requestAnimationFrame(() => {
                fitFrame = undefined;
                if (!host.clientWidth || !host.clientHeight) return;
                fitAddon.fit();
                resize(terminal.cols, terminal.rows);
            });
        };
        const resizeObserver = new ResizeObserver(scheduleFit);
        resizeObserver.observe(host);
        const focus = () => {
            if (disposed || document.visibilityState !== 'visible') return;
            scheduleFit();
            terminal.focus();
        };
        window.addEventListener('focus', focus);
        document.addEventListener('visibilitychange', focus);
        scheduleFit();
        const typed = terminal.onData(input);
        setScreen({
            size: () => ({ columns: terminal.cols, rows: terminal.rows }),
            reset: () => {
                terminal.reset();
                terminal.options.disableStdin = false;
            },
            write: (bytes, written) => {
                if (!disposed) terminal.write(bytes, written);
            },
            ready: () => {
                if (disposed) return;
                terminal.focus();
                scheduleFit();
            },
            closed: () => {
                if (!disposed) terminal.options.disableStdin = true;
            },
        });
        return () => {
            disposed = true;
            setScreen(null);
            if (fitFrame !== undefined) window.cancelAnimationFrame(fitFrame);
            resizeObserver.disconnect();
            window.removeEventListener('focus', focus);
            document.removeEventListener('visibilitychange', focus);
            typed.dispose();
            terminal.dispose();
        };
    }, [nodeId, open, input, resize]);
    return (
        <Modal
            open={open}
            zIndex={EDGE_ACTION_MODAL_Z_INDEX}
            onCancel={onClose}
            footer={null}
            width="min(960px, 94vw)"
            title={
                <Space>
                    <span>{`Web 终端 · ${session.state}`}</span>
                    {session.ended && (
                        <Button size="small" onClick={session.reconnect}>
                            重新连接
                        </Button>
                    )}
                </Space>
            }
            forceRender
            destroyOnHidden
            styles={{
                body: {
                    height: 'min(620px, calc(90dvh - 72px))',
                    padding: 0,
                    background: '#020617',
                },
            }}
            style={{ top: 'max(16px, 5dvh)', paddingBottom: 0 }}
        >
            <div
                className="h-full w-full overflow-hidden rounded-md bg-slate-950 p-3"
                role="application"
                aria-label="边缘节点终端"
            >
                <div ref={terminalHostRef} className="h-full min-h-0 w-full overflow-hidden" />
            </div>
        </Modal>
    );
}
function NodeIoTab({ scope, canControl }: { scope: Edge.EventScope; canControl: boolean }) {
    const { data, error, connectionError, isLoading, isFetching, refetch } = useEdgeIo(scope);
    const control = useIoControl(scope.nodeId ?? '');
    const [accepted, setAccepted] = useState<Record<string, string>>({});
    const columns: ColumnsType<Edge.IoChannel> = [
        { title: '通道', dataIndex: ['state', 'channel'], width: 100 },
        { title: '类型', dataIndex: ['state', 'kind'], width: 80 },
        {
            title: '状态',
            width: 130,
            render: (_, row) =>
                !row.state.available ? (
                    <Tag>不可用</Tag>
                ) : row.state.value === undefined ? (
                    '尚未上报'
                ) : row.state.kind === 'AI' ? (
                    `${row.state.value} μA`
                ) : (
                    <Tag color={row.state.value === '1' ? 'success' : 'default'}>
                        {row.state.kind === 'DO'
                            ? row.state.value === '1'
                                ? '开启'
                                : '关闭'
                            : row.state.value}
                    </Tag>
                ),
        },
        {
            title: '设备绑定',
            width: 190,
            render: (_, row) =>
                row.bindings.length ? (
                    row.bindings.map((binding) => binding.deviceName).join('、')
                ) : (
                    <span className="text-slate-500">未绑定</span>
                ),
        },
        {
            title: '控制',
            width: 150,
            render: (_, row) => {
                if (row.bindings.length) return <Tag>已绑定设备，只读</Tag>;
                if (row.state.kind !== 'DO') return '只读';
                const waiting = Boolean(
                    row.pending ||
                        (accepted[row.state.channel] &&
                            row.lastControl?.id !== accepted[row.state.channel])
                );
                const disabled =
                    !canControl ||
                    !data?.online ||
                    !data.current ||
                    !data.controlSupported ||
                    !row.state.available ||
                    !row.state.controllable ||
                    Boolean(connectionError || error) ||
                    isLoading ||
                    isFetching ||
                    control.isPending ||
                    waiting;
                return (
                    <Popconfirm
                        title={`确认${row.state.value === '1' ? '关闭' : '开启'} ${row.state.channel}？`}
                        disabled={disabled}
                        onConfirm={async () => {
                            const result = await control.mutateAsync({
                                channel: row.state.channel,
                                value: row.state.value !== '1',
                                idempotencyKey: createUuid(),
                            });
                            setAccepted((current) => ({
                                ...current,
                                [row.state.channel]: result.id,
                            }));
                        }}
                    >
                        <Button
                            size="small"
                            disabled={disabled}
                            loading={
                                (waiting && !row.lastControl?.uncertain) ||
                                (control.isPending &&
                                    control.variables?.channel === row.state.channel)
                            }
                        >
                            {row.lastControl?.uncertain
                                ? '结果待确认'
                                : row.state.value === '1'
                                  ? '关闭'
                                  : '开启'}
                        </Button>
                    </Popconfirm>
                );
            },
        },
        {
            title: '最近操作',
            width: 220,
            render: (_, row) =>
                row.lastControl ? (
                    <Tooltip title={row.lastControl.message}>
                        <Tag
                            color={
                                row.lastControl.uncertain
                                    ? 'warning'
                                    : row.lastControl.status === 'succeeded'
                                      ? 'success'
                                      : row.lastControl.status === 'failed'
                                        ? 'error'
                                        : 'processing'
                            }
                        >
                            {row.lastControl.uncertain
                                ? '结果待确认'
                                : row.lastControl.status === 'succeeded'
                                  ? '执行成功'
                                  : row.lastControl.status === 'failed'
                                    ? '执行失败'
                                    : '等待执行结果'}
                        </Tag>
                        {row.lastControl.message}
                    </Tooltip>
                ) : (
                    '—'
                ),
        },
    ];
    return (
        <Flex vertical gap={12}>
            <LiveQueryError error={error} retry={refetch} loading={isLoading} />
            {!data?.controlSupported && data && (
                <Alert
                    type="info"
                    showIcon
                    message="当前固件尚未提供节点 IO 实时状态与独立控制，请升级固件。"
                />
            )}
            {data && (!data.online || !data.current) && (
                <Alert
                    type="warning"
                    showIcon
                    message={
                        data.online
                            ? '等待当前会话的 IO 状态，暂不可控制。'
                            : '节点已离线，以下为最近状态。'
                    }
                />
            )}
            <span className="text-xs text-slate-500">
                已绑定设备的通道仅可查看，请在设备页面操作。模拟量显示原始电流值。
            </span>
            <Table
                rowKey={(row) => row.state.channel}
                size="small"
                loading={isLoading}
                columns={columns}
                dataSource={data?.channels}
                pagination={false}
                scroll={{ x: 870, y: 360 }}
            />
        </Flex>
    );
}

export function EdgeNodePage() {
    const { has } = usePermissions();
    const canQuery = has('iot:edge:query');
    const canEdit = has('iot:edge:edit');
    const canConfig = has('iot:edge:config');
    const canFirmware = has('iot:edge:firmware');
    const canTerminal = has('iot:edge:terminal');
    const [searchText, setSearchText] = useState('');
    const scrollContainerRef = useRef<HTMLDivElement>(null);
    const [keyword, setKeyword] = useState('');
    const [status, setStatus] = useState<Edge.EnrollmentStatus>();
    const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
    const [selectedId, setSelectedId] = useState<string>();
    const [detailTab, setDetailTab] = useState('networks');
    const [renamingNode, setRenamingNode] = useState<Edge.Node>();
    const [groupingNode, setGroupingNode] = useState<Edge.Node>();
    const [networkNode, setNetworkNode] = useState<Edge.Node>();
    const [networkDraft, setNetworkDraft] = useState<NetworkDraftItem[]>([]);
    const [networkRollbackTimeoutSec, setNetworkRollbackTimeoutSec] = useState(60);
    const [editingNetwork, setEditingNetwork] = useState<NetworkDraftItem>();
    const [firmwareNode, setFirmwareNode] = useState<Edge.Node>();
    const [terminalNode, setTerminalNode] = useState<Edge.Node>();
    const [serialDebug, setSerialDebug] = useState<{
        nodeId: string;
        path: string;
        title: string;
    }>();
    const [logLevel, setLogLevel] = useState<Edge.LogLevel>();
    const [nodeLogLevel, setNodeLogLevel] = useState<Edge.LogSetting>();
    const [networkOpen, setNetworkOpen] = useState(false);
    const [firmwareOpen, setFirmwareOpen] = useState(false);
    const [firmwareUploadProgress, setFirmwareUploadProgress] =
        useState<Edge.FirmwareUploadProgress>();
    const [terminalOpen, setTerminalOpen] = useState(false);
    const [networkForm] = Form.useForm<Edge.NetworkConfig>();
    const [firmwareForm] = Form.useForm<Edge.FirmwareUpgradeDto>();
    const [nameForm] = Form.useForm<Edge.NameDto>();
    const [groupForm] = Form.useForm<Edge.GroupDto>();
    const networkMode = Form.useWatch('mode', networkForm);
    const networkBridge = Form.useWatch('bridge', networkForm);
    const firmwareFile = Form.useWatch('file', firmwareForm);
    const { message, modal } = App.useApp();
    const eventScope: Edge.EventScope = {
        dtu: Boolean(selectedId && detailTab === 'dtu'),
        io: Boolean(selectedId && detailTab === 'io'),
        nodeId: selectedId,
        logs: selectedId && detailTab === 'events' ? { limit: 48, level: logLevel } : undefined,
        vpn: Boolean(selectedId && detailTab === 'vpn' && has('iot:vpn:query')),
    };
    const {
        data,
        isLoading,
        isFetching,
        refetch,
        error: inventoryError,
        connectionError: inventoryConnectionError,
    } = useEdgeInventory(canQuery, eventScope);
    const { data: edgeGroups = EMPTY_GROUPS, isLoading: groupsLoading } = useEdgeGroupTree();
    const groupSave = useEdgeGroupSave();
    const groupDelete = useEdgeGroupDelete();
    const groupView = useMemo(
        () =>
            buildEdgeNodeGroupView(
                edgeGroups,
                data ?? EMPTY_NODES,
                selectedGroupId,
                keyword,
                status
            ),
        [edgeGroups, data, selectedGroupId, keyword, status]
    );
    const nodeCountByGroup = useMemo(() => {
        const counts = new Map<string, number>();
        for (const node of data ?? EMPTY_NODES)
            counts.set(node.groupId, (counts.get(node.groupId) ?? 0) + 1);
        return counts;
    }, [data]);
    const groupTitle = useCallback(
        (group: Edge.GroupTreeItem) =>
            `${group.name} (${nodeCountByGroup.get(group.id) ?? 0})${group.status === 'disabled' ? ' · 已停用' : ''}`,
        [nodeCountByGroup]
    );
    const operatorName = useMobileOperatorName();
    const fetchConfiguration = useFetchEdgeConfiguration();
    const nodes = groupView.filtered;
    const scrollScope = `${selectedGroupId ?? 'all'}:${keyword}:${status ?? 'all'}`;
    useEffect(() => {
        void scrollScope;
        if (scrollContainerRef.current) scrollContainerRef.current.scrollTop = 0;
    }, [scrollScope]);
    const edgeGroupOptions = useMemo(() => groupSelectOptions(edgeGroups), [edgeGroups]);
    const {
        data: detail,
        isLoading: detailLoading,
        error: detailError,
        connectionError: detailConnectionError,
        refetch: retryDetail,
    } = useEdgeDetail(eventScope);
    const {
        data: eventLogs,
        isFetching: eventLogsLoading,
        refetch: refreshEventLogs,
        error: eventLogsError,
        connectionError: eventLogsConnectionError,
    } = useEdgeLogs(
        eventScope,
        Boolean(
            selectedId && detailTab === 'events' && detail?.status.online && detail.capability.logs
        )
    );
    const enrollment = useEnrollmentMutation();
    const edgeDelete = useEdgeDeleteMutation();
    const nodeName = useRenameEdgeNode();
    const nodeGroup = useAssignEdgeNodeGroup();
    const network = useConfigureEdgeNetwork();
    const deviceConfigSync = useDeviceConfigSyncMutation();
    const firmwareUpgrade = useFirmwareUpgradeMutation();
    const logLevelControl = useSetEdgeLogLevel();
    useEffect(() => {
        if (!selectedId) return;
        const exists = data?.some((node) => node.id === selectedId) ?? true;
        if (!exists) setSelectedId(undefined);
    }, [data, selectedId]);
    useEffect(() => {
        if (detail) setNodeLogLevel(detail.status.log?.level);
    }, [detail]);
    const showDetail = useCallback((node: Edge.Node) => {
        setDetailTab('networks');
        setLogLevel(undefined);
        setSelectedId(node.id);
    }, []);
    if (!canQuery) {
        return (
            <PageContainer>
                <Result status="403" title="无权限" subTitle="您没有查询边缘节点的权限" />
            </PageContainer>
        );
    }
    const approveEnrollment = (node: Edge.Node) => {
        modal.confirm({
            zIndex: EDGE_ACTION_MODAL_Z_INDEX,
            title: `批准 IMEI ${node.imei} 注册？`,
            content: '批准后当前连接会立即转为已批准会话，节点将上报能力并自动同步当前设备配置。',
            onOk: () =>
                enrollment.mutateAsync({
                    id: node.id,
                    status: 'approved',
                    name: node.name || node.hostname,
                }),
        });
    };
    const deleteEnrollment = (node: Edge.Node) => {
        modal.confirm({
            zIndex: EDGE_ACTION_MODAL_Z_INDEX,
            title: `删除 IMEI ${node.imei} 的注册申请？`,
            content: '删除后当前连接会断开；节点再次连接时会重新进入待处理状态。',
            okButtonProps: { danger: true },
            okText: '删除',
            onOk: () =>
                edgeDelete.mutateAsync(node.id).then(() => {
                    if (selectedId === node.id) setSelectedId(undefined);
                }),
        });
    };
    const showNetworkManager = async (node: Edge.Node) => {
        const current = node.networks && node.interfaces ? node : await fetchConfiguration(node.id);
        setNetworkDraft(
            (current.networks ?? []).map((item) =>
                networkDraftFromReported(item, current.interfaces ?? [])
            )
        );
        setNetworkRollbackTimeoutSec(60);
        setEditingNetwork(undefined);
        setNetworkOpen(false);
        setNetworkNode(current);
    };
    const showNetworkEditor = (item?: NetworkDraftItem) => {
        const interfaces = networkNode?.interfaces ?? [];
        const firstDevice = physicalNetworkInterfaces(interfaces)[0]?.name;
        networkForm.setFieldsValue(
            item
                ? {
                      operation: 'upsert',
                      name: item.name,
                      mode: item.mode,
                      device: item.device,
                      bridge: item.bridge,
                      bridgePorts: item.bridgePorts,
                      ip: item.ip,
                      prefixLength: item.prefixLength,
                      gateway: item.gateway,
                  }
                : {
                      operation: 'upsert',
                      name: '',
                      mode: 'dhcp',
                      device: firstDevice,
                      bridge: false,
                      bridgePorts: [],
                      ip: '',
                      prefixLength: 0,
                      gateway: '',
                  }
        );
        setEditingNetwork(item);
        setNetworkOpen(true);
    };
    const submitNetworkDraft = () => {
        if (!networkNode) return;
        const parsed = networkSchema.safeParse({
            interfaces: networkDraft.map(
                ({ original, dirty, up, sourceName, ...item }): Edge.NetworkConfig => {
                    if (item.operation === 'delete') {
                        return { ...item, name: sourceName ?? item.name };
                    }
                    return sourceName && sourceName !== item.name
                        ? { ...item, previousName: sourceName }
                        : item;
                }
            ),
            rollbackTimeoutSec: networkRollbackTimeoutSec,
        });
        if (!parsed.success) {
            void message.error(parsed.error.issues[0]?.message ?? '网络配置校验失败');
            return;
        }
        network.mutate(
            { id: networkNode.id, data: parsed.data },
            {
                onSuccess: () => {
                    setNetworkNode(undefined);
                    setNetworkDraft([]);
                },
            }
        );
    };
    const showRename = (node: Edge.Node) => {
        nameForm.setFieldsValue({ name: node.name || node.hostname });
        setRenamingNode(node);
    };
    const showGroup = (node: Edge.Node) => {
        groupForm.setFieldsValue({ groupId: node.groupId || '' });
        setGroupingNode(node);
    };
    const showFirmware = (node: Edge.Node) => {
        firmwareUpgrade.reset();
        setFirmwareUploadProgress(undefined);
        firmwareForm.resetFields();
        firmwareForm.setFieldsValue({ keepSettings: true });
        setFirmwareNode(node);
        setFirmwareOpen(true);
    };
    const interfaceColumns: ColumnsType<Edge.NetworkInterface> = [
        { title: '接口', dataIndex: 'name' },
        { title: 'MAC', dataIndex: 'mac', render: (value) => value || '-' },
        {
            title: 'IPv4',
            render: (_, item) => (item.ipv4 ? `${item.ipv4}/${item.prefixLength}` : '-'),
        },
        {
            title: '状态',
            dataIndex: 'up',
            render: (value) => (
                <Tag color={value ? 'success' : 'default'}>{value ? 'UP' : 'DOWN'}</Tag>
            ),
        },
        {
            title: '类型',
            render: (_, item) => (item.bridge ? `网桥 ${item.bridgePorts.join(', ')}` : '网口'),
        },
    ];
    const networkInfoColumns: ColumnsType<Edge.Network> = [
        { title: '逻辑接口', dataIndex: 'name', width: 120 },
        {
            title: '地址方式',
            dataIndex: 'mode',
            width: 100,
            render: (value: Edge.Network['mode']) =>
                value === 'dhcp' ? 'DHCP' : value === 'static' ? '静态 IPv4' : '-',
        },
        {
            title: '网卡 / 网桥成员',
            render: (_, item) =>
                item.bridge ? item.bridgePorts.join(', ') || '-' : item.device || '-',
        },
        {
            title: 'IPv4',
            render: (_, item) => (item.ipv4 ? `${item.ipv4}/${item.prefixLength}` : '-'),
        },
        {
            title: '状态',
            dataIndex: 'up',
            width: 80,
            render: (value) => (
                <Tag color={value ? 'success' : 'default'}>{value ? 'UP' : 'DOWN'}</Tag>
            ),
        },
    ];
    const networkDraftColumns: ColumnsType<NetworkDraftItem> = [
        {
            title: '逻辑接口',
            dataIndex: 'name',
            width: 160,
            render: (name: string, item) =>
                item.sourceName && item.sourceName !== name ? `${item.sourceName} → ${name}` : name,
        },
        {
            title: '地址方式',
            dataIndex: 'mode',
            width: 100,
            render: (value: NetworkDraftItem['mode'], item) =>
                item.operation === 'delete'
                    ? '-'
                    : value === 'dhcp'
                      ? 'DHCP'
                      : value === 'static'
                        ? '静态 IPv4'
                        : '-',
        },
        {
            title: '网卡 / 网桥成员',
            render: (_, item) =>
                item.bridge ? item.bridgePorts?.join(', ') || '-' : item.device || '-',
        },
        {
            title: 'IPv4',
            render: (_, item) =>
                item.operation !== 'delete' && item.ip ? `${item.ip}/${item.prefixLength}` : '-',
        },
        {
            title: '修改状态',
            width: 100,
            render: (_, item) => {
                if (item.operation === 'delete') return <Tag color="error">待删除</Tag>;
                if (!item.original) return <Tag color="success">待新增</Tag>;
                if (item.dirty) return <Tag color="processing">待更新</Tag>;
                return <Tag>未修改</Tag>;
            },
        },
        {
            title: '操作',
            width: 140,
            render: (_, item) => (
                <Space size={4}>
                    {item.operation === 'delete' ? (
                        <Button
                            type="link"
                            size="small"
                            onClick={() => {
                                const sourceName = item.sourceName ?? item.name;
                                const original = networkNode?.networks?.find(
                                    (candidate) => candidate.name === sourceName
                                );
                                if (!original) return;
                                setNetworkDraft((current) =>
                                    current.map((candidate) =>
                                        (candidate.sourceName ?? candidate.name) === sourceName
                                            ? networkDraftFromReported(
                                                  original,
                                                  networkNode?.interfaces ?? []
                                              )
                                            : candidate
                                    )
                                );
                            }}
                        >
                            恢复
                        </Button>
                    ) : (
                        <>
                            <Button
                                type="link"
                                size="small"
                                onClick={() => showNetworkEditor(item)}
                            >
                                编辑
                            </Button>
                            <Popconfirm
                                title={`将逻辑接口 ${item.name} 标记为待删除？`}
                                description="这里只修改草稿，点击“保存并下发全部配置”后节点才会应用。"
                                onConfirm={() =>
                                    setNetworkDraft((current) =>
                                        item.original
                                            ? current.map((candidate) =>
                                                  (candidate.sourceName ?? candidate.name) ===
                                                  (item.sourceName ?? item.name)
                                                      ? {
                                                            ...candidate,
                                                            operation: 'delete',
                                                            dirty: true,
                                                        }
                                                      : candidate
                                              )
                                            : current.filter(
                                                  (candidate) =>
                                                      (candidate.sourceName ?? candidate.name) !==
                                                      item.name
                                              )
                                    )
                                }
                            >
                                <Button type="link" size="small" danger>
                                    删除
                                </Button>
                            </Popconfirm>
                        </>
                    )}
                </Space>
            ),
        },
    ];
    const assignedNetworkDevices = new Set(
        networkDraft
            .filter(
                (item) =>
                    item.operation === 'upsert' &&
                    (item.sourceName ?? item.name) !==
                        (editingNetwork?.sourceName ?? editingNetwork?.name)
            )
            .flatMap((item) => (item.bridge ? item.bridgePorts : item.device ? [item.device] : []))
    );
    const networkInterfaces = networkNode?.interfaces ?? [];
    const networkDeviceOptions = physicalNetworkInterfaces(networkInterfaces).map((item) => ({
        value: item.name,
        label: item.displayName ? `${item.displayName} (${item.name})` : item.name,
        disabled: assignedNetworkDevices.has(item.name),
    }));
    const serialColumns: ColumnsType<Edge.SerialPort> = [
        { title: '串口', dataIndex: 'path' },
        { title: '名称', dataIndex: 'displayName' },
        { title: '可读写', dataIndex: 'available', render: (value) => (value ? '是' : '否') },
        { title: 'RS485', dataIndex: 'rs485', render: (value) => (value ? '是' : '未确认') },
        {
            title: '操作',
            width: 100,
            render: (_, port) =>
                canTerminal && (
                    <Tooltip
                        title={
                            !detail?.capability.serialDebug
                                ? '需要支持串口调试的新版固件'
                                : !detail.status.online
                                  ? '节点离线'
                                  : !port.available
                                    ? '串口不可用'
                                    : undefined
                        }
                    >
                        <Button
                            type="link"
                            size="small"
                            disabled={
                                !detail?.capability.serialDebug ||
                                !detail.status.online ||
                                !port.available
                            }
                            onClick={() =>
                                detail &&
                                setSerialDebug({
                                    nodeId: detail.id,
                                    path: port.path,
                                    title: detail.name || detail.imei,
                                })
                            }
                        >
                            串口调试
                        </Button>
                    </Tooltip>
                ),
        },
    ];
    const taskColumns: ColumnsType<Edge.Task> = [
        { title: '类型', dataIndex: 'taskType' },
        { title: '状态', dataIndex: 'status', render: statusTag },
        {
            title: '进度',
            width: 220,
            render: (_, item) =>
                item.taskType === 'firmware' ? (
                    <Progress
                        percent={item.progressPercent}
                        size="small"
                        status={
                            item.status === 'failed'
                                ? 'exception'
                                : item.status === 'succeeded'
                                  ? 'success'
                                  : 'active'
                        }
                        format={(percent) =>
                            `${percent ?? 0}% · ${formatBytes(item.downloadedBytes)} / ${formatBytes(item.totalBytes)}`
                        }
                    />
                ) : (
                    '-'
                ),
        },
        { title: '结果', dataIndex: 'message', render: (value) => value || '-' },
        {
            title: '创建时间',
            dataIndex: 'createdAt',
            render: (value) => formatDateTime(value),
        },
    ];
    const logColumns: ColumnsType<Edge.LogLine> = [
        {
            title: '时间',
            dataIndex: 'time',
            width: 170,
            render: (value) => formatDateTime(value),
        },
        {
            title: '级别',
            dataIndex: 'level',
            width: 90,
            render: logLevelTag,
        },
        { title: '来源', dataIndex: 'source', width: 110, render: (value) => value || '-' },
        { title: '事件', dataIndex: 'message', width: 180, render: (value) => value || '-' },
        { title: '详情', dataIndex: 'detail', render: (value) => value || '-' },
    ];
    const renderNodeCards = (items: Edge.Node[]) => (
        <VirtualGrid
            items={items}
            getItemKey={getNodeKey}
            minItemWidth={EDGE_CARD_MIN_WIDTH}
            maxColumns={4}
            estimateSize={420}
            className="mt-4"
        >
            {(node) => (
                <EdgeNodeCard
                    key={node.id}
                    node={node}
                    operatorName={operatorName}
                    onOpen={showDetail}
                />
            )}
        </VirtualGrid>
    );
    const renderSectionStats = (
        items: Edge.Node[],
        aggregate?: {
            total: number;
            online: number;
            offline: number;
        }
    ) => {
        const stats = aggregate ?? {
            total: items.length,
            online: items.filter((node) => node.status.online).length,
            offline: items.filter((node) => !node.status.online).length,
        };
        return (
            <Space size={6} wrap>
                <Tag color="blue">{stats.total} 个</Tag>
                {stats.online > 0 && <Tag color="green">{stats.online} 在线</Tag>}
                {stats.offline > 0 && <Tag color="red">{stats.offline} 离线</Tag>}
            </Space>
        );
    };
    const renderGroupSection = (group: Edge.GroupTreeItem, depth = 0): ReactNode => {
        const stats = groupView.stats.get(group.id);
        if (!stats?.total) return null;
        const direct = groupView.direct.get(group.id) ?? [];
        const children = (group.children ?? []).filter(
            (child) => (groupView.stats.get(child.id)?.total ?? 0) > 0
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
                    {renderSectionStats(direct, stats)}
                </Flex>
                {direct.length > 0 && renderNodeCards(direct)}
                {children.length > 0 && (
                    <Space direction="vertical" className="mt-4 w-full" size="middle">
                        {children.map((child) => renderGroupSection(child, depth + 1))}
                    </Space>
                )}
            </section>
        );
    };
    const renderUngroupedSection = (items: Edge.Node[]) =>
        items.length > 0 && (
            <section className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                <Flex justify="space-between" align="center" gap={12} wrap>
                    <div className="min-w-0">
                        <div className="text-sm font-semibold text-slate-800">未分组</div>
                        <div className="mt-1 text-xs text-slate-500">
                            没有绑定节点分组的卡片会统一在这里展示
                        </div>
                    </div>
                    {renderSectionStats(items)}
                </Flex>
                {renderNodeCards(items)}
            </section>
        );
    return (
        <PageContainer
            header={
                <div className="flex flex-wrap items-center justify-between gap-2">
                    <h3 className="m-0 text-base font-medium">边缘节点</h3>
                    <Space wrap>
                        <Button
                            icon={<DownloadOutlined />}
                            href={windowsClientDownloadUrl}
                            download
                        >
                            下载 Windows 客户端
                        </Button>
                        <GroupTreePanel
                            groups={edgeGroups}
                            loading={groupsLoading}
                            selectedGroupId={selectedGroupId}
                            onSelect={setSelectedGroupId}
                            allLabel="全部节点"
                            ungroupedCount={groupView.ungroupedCount}
                            groupTitle={groupTitle}
                            canManage={canEdit}
                            formNoun="边缘节点分组"
                            remarkMaxLength={500}
                            deleteHint="请先移出该分组的子分组和边缘节点。"
                            saving={groupSave.isPending}
                            toFormValues={toGroupFormValues}
                            onSave={(values, onSaved) =>
                                groupSave.mutate(values, { onSuccess: onSaved })
                            }
                            onDelete={(group) => groupDelete.mutateAsync(group.id)}
                        />
                        <Input.Search
                            allowClear
                            className="w-[240px]"
                            placeholder="IMEI / 名称 / 型号"
                            enterButton
                            value={searchText}
                            onChange={(event) => {
                                setSearchText(event.target.value);
                                if (!event.target.value) setKeyword('');
                            }}
                            onSearch={(value) => setKeyword(value.trim())}
                        />
                        <Select
                            allowClear
                            className="w-[130px]"
                            placeholder="注册状态"
                            value={status}
                            onChange={setStatus}
                            options={[
                                { value: 'pending', label: '待处理' },
                                { value: 'approved', label: '已批准' },
                            ]}
                        />
                        <Tooltip title="刷新">
                            <Button
                                icon={<ReloadOutlined />}
                                loading={isFetching}
                                onClick={() => void refetch()}
                            />
                        </Tooltip>
                    </Space>
                </div>
            }
        >
            <LiveQueryError
                error={
                    inventoryError ??
                    detailError ??
                    (detailTab === 'events' ? eventLogsError : null) ??
                    inventoryConnectionError ??
                    detailConnectionError ??
                    (detailTab === 'events' ? eventLogsConnectionError : null)
                }
                retry={
                    inventoryError || inventoryConnectionError
                        ? refetch
                        : detailError || detailConnectionError
                          ? retryDetail
                          : refreshEventLogs
                }
                loading={isFetching}
            />
            <div ref={scrollContainerRef} className="h-full overflow-y-auto overflow-x-hidden">
                {isLoading && nodes.length === 0 ? (
                    <div className={EDGE_CARD_GRID_CLASS}>
                        {['first', 'second', 'third', 'fourth'].map((key) => (
                            <div key={key} className="rounded-lg bg-white px-3.5 py-3">
                                <Skeleton active paragraph={{ rows: 4 }} />
                            </div>
                        ))}
                    </div>
                ) : nodes.length === 0 ? (
                    <div className="py-16">
                        <Empty description={keyword ? '未找到匹配的边缘节点' : '暂无边缘节点'} />
                    </div>
                ) : (
                    <Space direction="vertical" className="w-full" size="large">
                        {groupView.roots.length > 0 ? (
                            <>
                                {groupView.roots.map((group) => renderGroupSection(group))}
                                {selectedGroupId === null &&
                                    renderUngroupedSection(groupView.ungrouped)}
                            </>
                        ) : selectedGroupId === 'ungrouped' ? (
                            renderUngroupedSection(nodes)
                        ) : (
                            <section className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                                <Flex justify="space-between" align="center" gap={12} wrap>
                                    <div className="text-sm font-semibold text-slate-800">
                                        全部节点
                                    </div>
                                    {renderSectionStats(nodes)}
                                </Flex>
                                {renderNodeCards(nodes)}
                            </section>
                        )}
                    </Space>
                )}
            </div>

            <Drawer
                open={Boolean(selectedId)}
                zIndex={EDGE_DETAIL_DRAWER_Z_INDEX}
                onClose={() => setSelectedId(undefined)}
                title={
                    detail
                        ? `${detail.name || detail.hostname || '边缘节点'} · ${detail.imei}`
                        : '边缘节点详情'
                }
                extra={
                    detail ? (
                        <Space>
                            {canEdit && (
                                <>
                                    <Button
                                        icon={<EditOutlined />}
                                        onClick={() => showRename(detail)}
                                    >
                                        修改名称
                                    </Button>
                                    <Button
                                        icon={<ApartmentOutlined />}
                                        onClick={() => showGroup(detail)}
                                    >
                                        设置分组
                                    </Button>
                                </>
                            )}
                            {detail.enrollmentStatus === 'approved' &&
                                canTerminal &&
                                detail.capability.terminal && (
                                    <Tooltip
                                        title={detail.status.online ? '打开 Web 终端' : '节点离线'}
                                    >
                                        <span>
                                            <Button
                                                icon={<CodeOutlined />}
                                                disabled={!detail.status.online}
                                                onClick={() => {
                                                    setTerminalNode(detail);
                                                    setTerminalOpen(true);
                                                }}
                                            >
                                                Web 终端
                                            </Button>
                                        </span>
                                    </Tooltip>
                                )}
                        </Space>
                    ) : null
                }
                footer={
                    detail?.enrollmentStatus === 'pending' && canEdit ? (
                        <Flex justify="end" gap={8}>
                            <Button
                                danger
                                icon={<DeleteOutlined />}
                                loading={edgeDelete.isPending && edgeDelete.variables === detail.id}
                                onClick={() => deleteEnrollment(detail)}
                            >
                                删除注册申请
                            </Button>
                            <Button
                                type="primary"
                                icon={<CheckOutlined />}
                                loading={
                                    enrollment.isPending && enrollment.variables?.id === detail.id
                                }
                                onClick={() => approveEnrollment(detail)}
                            >
                                批准注册
                            </Button>
                        </Flex>
                    ) : undefined
                }
                size="min(960px, 92vw)"
                loading={detailLoading}
            >
                <LiveQueryError
                    error={detailError ?? detailConnectionError ?? inventoryConnectionError}
                    retry={detailError || detailConnectionError ? retryDetail : refetch}
                    loading={detailLoading}
                />
                {detail && (
                    <>
                        <Descriptions bordered size="small" column={{ xs: 1, sm: 2, lg: 3 }}>
                            <Descriptions.Item label="IMEI">{detail.imei}</Descriptions.Item>
                            <Descriptions.Item label="ICCID">
                                <span className="break-all font-mono">
                                    {detail.mobile.iccid || '-'}
                                </span>
                            </Descriptions.Item>
                            <Descriptions.Item label="状态">
                                {statusTag(detail.enrollmentStatus)}{' '}
                                {detail.status.online ? (
                                    <Tag color="success">在线</Tag>
                                ) : (
                                    <Tag>离线</Tag>
                                )}
                            </Descriptions.Item>
                            <Descriptions.Item label="型号">
                                {detail.model || '-'}
                            </Descriptions.Item>
                            <Descriptions.Item label="软件版本">
                                {detail.softwareVersion || '-'}
                            </Descriptions.Item>
                            <Descriptions.Item label="架构">
                                {detail.architecture || '-'}
                            </Descriptions.Item>
                            <Descriptions.Item label="OpenWrt">
                                {detail.openwrtRelease || '-'}
                            </Descriptions.Item>
                            <Descriptions.Item label="待上报队列">
                                {detail.status.outbox.records} 条 /{' '}
                                {formatBytes(detail.status.outbox.bytes)}
                            </Descriptions.Item>
                            <Descriptions.Item label="今日流量">
                                {trafficPair(detail.status.tcpTraffic, 'daily')}
                            </Descriptions.Item>
                            <Descriptions.Item label="本月流量">
                                {trafficPair(detail.status.tcpTraffic, 'monthly')}
                            </Descriptions.Item>
                            <Descriptions.Item label="今日 VPN">
                                {trafficPair(detail.status.vpnTraffic, 'daily')}
                            </Descriptions.Item>
                            <Descriptions.Item label="本月 VPN">
                                {trafficPair(detail.status.vpnTraffic, 'monthly')}
                            </Descriptions.Item>
                            <Descriptions.Item label="ttyd">
                                {detail.capability.terminal ? (
                                    <Tag color="success">已检测</Tag>
                                ) : (
                                    <Tag>未安装</Tag>
                                )}
                            </Descriptions.Item>
                            <Descriptions.Item label="最近上报">
                                {formatDateTime(detail.status.lastSeenAt)}
                            </Descriptions.Item>
                        </Descriptions>
                        <Tabs
                            className="mt-4"
                            activeKey={detailTab}
                            onChange={setDetailTab}
                            items={[
                                ...(detail.capability.externalIo?.supported
                                    ? [
                                          {
                                              key: 'io',
                                              label: 'IO',
                                              children: (
                                                  <NodeIoTab
                                                      key={detail.id}
                                                      scope={eventScope}
                                                      canControl={canConfig}
                                                  />
                                              ),
                                          },
                                      ]
                                    : []),
                                {
                                    key: 'networks',
                                    label: `逻辑接口 (${detail.networks?.length ?? 0})`,
                                    children: (
                                        <>
                                            <Flex
                                                justify="space-between"
                                                align="center"
                                                gap={12}
                                                className="mb-3"
                                            >
                                                <span className="text-xs text-slate-500">
                                                    查看逻辑接口及其物理网卡绑定关系。
                                                </span>
                                                {detail.enrollmentStatus === 'approved' &&
                                                    canConfig && (
                                                        <Tooltip
                                                            title={
                                                                detail.capability.networkConfig &&
                                                                detail.capability
                                                                    .networkConfigVersion >= 2
                                                                    ? '集中编辑并原子下发网络配置'
                                                                    : `节点代理 ${detail.softwareVersion || '当前版本'} 过旧，请升级至 0.3.0`
                                                            }
                                                        >
                                                            <span>
                                                                <Button
                                                                    icon={<GlobalOutlined />}
                                                                    disabled={
                                                                        !detail.capability
                                                                            .networkConfig ||
                                                                        detail.capability
                                                                            .networkConfigVersion <
                                                                            2
                                                                    }
                                                                    onClick={() =>
                                                                        void showNetworkManager(
                                                                            detail
                                                                        )
                                                                    }
                                                                >
                                                                    管理网络接口
                                                                </Button>
                                                            </span>
                                                        </Tooltip>
                                                    )}
                                            </Flex>
                                            <Table
                                                rowKey="name"
                                                size="small"
                                                pagination={false}
                                                columns={networkInfoColumns}
                                                dataSource={detail.networks ?? []}
                                                scroll={{ x: 'max-content', y: 360 }}
                                            />
                                        </>
                                    ),
                                },
                                {
                                    key: 'interfaces',
                                    label: `物理网卡 (${physicalNetworkInterfaces(detail.interfaces ?? []).length})`,
                                    children: (
                                        <Table
                                            rowKey="name"
                                            size="small"
                                            pagination={false}
                                            columns={interfaceColumns}
                                            dataSource={physicalNetworkInterfaces(
                                                detail.interfaces ?? []
                                            )}
                                            scroll={{ x: 'max-content', y: 360 }}
                                        />
                                    ),
                                },
                                {
                                    key: 'serial',
                                    label: `串口 (${detail.serialPorts?.length ?? 0})`,
                                    children: (
                                        <Table
                                            rowKey="path"
                                            size="small"
                                            pagination={false}
                                            columns={serialColumns}
                                            dataSource={detail.serialPorts ?? []}
                                            scroll={{ x: 'max-content', y: 360 }}
                                        />
                                    ),
                                },
                                {
                                    key: 'dtu',
                                    label: 'DTU 透传',
                                    children:
                                        detailTab === 'dtu' ? (
                                            <DtuPanel node={detail} scope={eventScope} />
                                        ) : null,
                                },
                                {
                                    key: 'config',
                                    label: '设备配置',
                                    children: (
                                        <>
                                            <Flex
                                                justify="space-between"
                                                align="center"
                                                gap={12}
                                                className="mb-3"
                                            >
                                                <span className="text-xs text-slate-500">
                                                    查看平台目标版本与节点实际应用版本。
                                                </span>
                                                {detail.enrollmentStatus === 'approved' &&
                                                    canConfig &&
                                                    detail.capability.deviceConfig && (
                                                        <Button
                                                            icon={<SyncOutlined />}
                                                            loading={
                                                                deviceConfigSync.isPending &&
                                                                deviceConfigSync.variables ===
                                                                    detail.id
                                                            }
                                                            onClick={() =>
                                                                deviceConfigSync.mutate(detail.id)
                                                            }
                                                        >
                                                            同步设备配置
                                                        </Button>
                                                    )}
                                            </Flex>
                                            <Descriptions bordered size="small" column={2}>
                                                <Descriptions.Item label="状态">
                                                    {statusTag(detail.status.config.state)}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="配置版本">
                                                    {formatConfigVersions(
                                                        detail.status.config.activeVersion,
                                                        detail.status.config.desiredVersion
                                                    )}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="节点已应用版本">
                                                    {validConfigTimestamp(
                                                        detail.status.config.activeVersion
                                                    )
                                                        ? detail.status.config.activeVersion
                                                        : '-'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="平台目标版本">
                                                    {validConfigTimestamp(
                                                        detail.status.config.desiredVersion
                                                    )
                                                        ? detail.status.config.desiredVersion
                                                        : '-'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="结果" span={2}>
                                                    {detail.status.config.message || '-'}
                                                </Descriptions.Item>
                                            </Descriptions>
                                        </>
                                    ),
                                },
                                {
                                    key: 'mobile',
                                    label: '移动网络',
                                    children: (
                                        <>
                                            <p className="mb-3 text-xs text-slate-500">
                                                查看 SIM、信号、运营商与拨号状态。
                                            </p>
                                            <Descriptions bordered size="small" column={2}>
                                                <Descriptions.Item label="4G 状态">
                                                    {mobileState(detail)}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="SIM 状态">
                                                    {simStateText(detail.mobile.simState)}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="ICCID" span={2}>
                                                    <span className="break-all font-mono">
                                                        {detail.mobile.iccid || '-'}
                                                    </span>
                                                </Descriptions.Item>
                                                <Descriptions.Item label="运营商">
                                                    {operatorName(detail.mobile.operator) || '-'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="信号">
                                                    {detail.mobile.available
                                                        ? `${detail.mobile.signal.percent}%${
                                                              detail.mobile.signal.rssiDbm !== -1
                                                                  ? ` · ${detail.mobile.signal.rssiDbm} dBm`
                                                                  : ''
                                                          }`
                                                        : '-'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="APN">
                                                    {detail.mobile.apn || '-'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="网络注册">
                                                    {detail.mobile.registered ? '已注册' : '未注册'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="IPv4">
                                                    {detail.mobile.ipv4 || '-'}
                                                </Descriptions.Item>
                                            </Descriptions>
                                        </>
                                    ),
                                },
                                {
                                    key: 'vpn',
                                    label: 'VPN',
                                    children: <EdgeVpnPanel node={detail} scope={eventScope} />,
                                },
                                {
                                    key: 'tasks',
                                    label: '任务记录',
                                    children: (
                                        <Table
                                            rowKey="id"
                                            size="small"
                                            pagination={false}
                                            columns={taskColumns}
                                            dataSource={(detail.tasks ?? []).filter(
                                                (item) => item.taskType !== 'firmware'
                                            )}
                                            scroll={{ x: 'max-content', y: 360 }}
                                            locale={{ emptyText: '暂无任务记录' }}
                                        />
                                    ),
                                },
                                {
                                    key: 'firmware',
                                    label: '固件',
                                    children: (
                                        <>
                                            <Flex
                                                justify="space-between"
                                                align="center"
                                                gap={12}
                                                className="mb-3"
                                            >
                                                <span className="text-xs text-slate-500">
                                                    查看当前版本、刷写进度与历史升级记录。
                                                </span>
                                                {detail.enrollmentStatus === 'approved' &&
                                                    canFirmware &&
                                                    detail.capability.firmwareUpdate && (
                                                        <Button
                                                            danger
                                                            icon={<UploadOutlined />}
                                                            onClick={() => showFirmware(detail)}
                                                        >
                                                            上传固件并刷写
                                                        </Button>
                                                    )}
                                            </Flex>
                                            <Descriptions
                                                bordered
                                                size="small"
                                                column={2}
                                                className="mb-3"
                                            >
                                                <Descriptions.Item label="当前版本">
                                                    {detail.softwareVersion || '-'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="升级状态">
                                                    {detail.firmware.state
                                                        ? statusTag(detail.firmware.state)
                                                        : '-'}
                                                </Descriptions.Item>
                                                <Descriptions.Item label="当前进度" span={2}>
                                                    {detail.firmware.state === 'accepted' ||
                                                    detail.firmware.state === 'running' ? (
                                                        <Progress
                                                            percent={
                                                                detail.firmware.progressPercent
                                                            }
                                                            size="small"
                                                            status="active"
                                                            format={(percent) =>
                                                                `${percent ?? 0}% · ${formatBytes(detail.firmware.downloadedBytes)} / ${formatBytes(detail.firmware.totalBytes)}`
                                                            }
                                                        />
                                                    ) : (
                                                        detail.firmware.message || '-'
                                                    )}
                                                </Descriptions.Item>
                                            </Descriptions>
                                            <Table
                                                rowKey="id"
                                                size="small"
                                                pagination={false}
                                                columns={taskColumns}
                                                dataSource={(detail.tasks ?? []).filter(
                                                    (item) => item.taskType === 'firmware'
                                                )}
                                                locale={{ emptyText: '暂无固件升级记录' }}
                                                scroll={{ x: 'max-content', y: 300 }}
                                            />
                                        </>
                                    ),
                                },
                                {
                                    key: 'events',
                                    label: '运行事件',
                                    children: !detail.status.online ? (
                                        <Empty description="节点当前离线，无法读取运行事件" />
                                    ) : !detail.capability.logs ? (
                                        <Empty
                                            description={`节点代理 ${detail.softwareVersion || '当前版本'} 过旧，请升级后查看运行事件`}
                                        />
                                    ) : (
                                        <>
                                            <Flex
                                                justify="space-between"
                                                align="center"
                                                gap={12}
                                                className="mb-3"
                                            >
                                                <Space>
                                                    <Select<Edge.LogSetting>
                                                        className="w-[140px]"
                                                        placeholder="日志级别"
                                                        title="新固件默认 SILENT；其他级别开启后 5 分钟自动恢复，再次设置重新计时。旧固件需升级后支持。"
                                                        value={nodeLogLevel}
                                                        loading={logLevelControl.isPending}
                                                        disabled={!canConfig}
                                                        onSelect={(value) => {
                                                            setNodeLogLevel(value);
                                                            if (selectedId)
                                                                logLevelControl.mutate(
                                                                    {
                                                                        id: selectedId,
                                                                        data: { level: value },
                                                                    },
                                                                    {
                                                                        onSuccess: () =>
                                                                            void refreshEventLogs(),
                                                                        onError: () =>
                                                                            setNodeLogLevel(
                                                                                detail.status.log
                                                                                    ?.level
                                                                            ),
                                                                    }
                                                                );
                                                        }}
                                                        options={[
                                                            { value: 'silent', label: 'SILENT' },
                                                            { value: 'debug', label: 'DEBUG' },
                                                            { value: 'info', label: 'INFO' },
                                                            { value: 'warn', label: 'WARN' },
                                                            { value: 'error', label: 'ERROR' },
                                                        ]}
                                                    />
                                                    <Select<Edge.LogLevel>
                                                        allowClear
                                                        className="w-[140px]"
                                                        placeholder="筛选级别"
                                                        value={logLevel}
                                                        onChange={(value) => setLogLevel(value)}
                                                        options={[
                                                            { value: 'debug', label: 'debug' },
                                                            { value: 'info', label: 'info' },
                                                            { value: 'warn', label: 'warn' },
                                                            { value: 'error', label: 'error' },
                                                        ]}
                                                    />
                                                </Space>
                                                <Button
                                                    icon={<ReloadOutlined />}
                                                    loading={eventLogsLoading}
                                                    onClick={() => void refreshEventLogs()}
                                                >
                                                    刷新
                                                </Button>
                                            </Flex>
                                            <Table
                                                rowKey={(item, index) =>
                                                    `${item.time}-${item.source}-${index ?? 0}`
                                                }
                                                size="small"
                                                pagination={false}
                                                loading={eventLogsLoading}
                                                columns={logColumns}
                                                dataSource={eventLogs?.lines ?? []}
                                                locale={{ emptyText: '暂无运行事件' }}
                                                scroll={{ x: 'max-content', y: 420 }}
                                            />
                                        </>
                                    ),
                                },
                            ]}
                        />
                    </>
                )}
            </Drawer>

            <FormModal
                open={Boolean(networkNode)}
                zIndex={EDGE_ACTION_MODAL_Z_INDEX}
                onCancel={() => {
                    if (networkOpen) {
                        setNetworkOpen(false);
                        setEditingNetwork(undefined);
                    } else {
                        setNetworkNode(undefined);
                        setNetworkDraft([]);
                    }
                }}
                onOk={() => (networkOpen ? networkForm.submit() : submitNetworkDraft())}
                okText={networkOpen ? '保存修改' : '保存并下发全部配置'}
                cancelText={networkOpen ? '返回接口列表' : '取消'}
                confirmLoading={!networkOpen && network.isPending}
                okButtonProps={{
                    disabled: !networkOpen && !networkDraft.some((item) => item.dirty),
                }}
                title={`${networkOpen ? (editingNetwork ? '编辑网络接口' : '添加网络接口') : '网络接口'}${networkNode ? ` · ${networkNode.name || networkNode.imei}` : ''}`}
                forceRender
                destroyOnHidden
            >
                {networkOpen ? (
                    <Form
                        form={networkForm}
                        layout="vertical"
                        onFinish={(values) => {
                            const parsed = validateForm(
                                networkForm,
                                networkInterfaceSchema,
                                values
                            );
                            if (!parsed) return;
                            const draft: NetworkDraftItem = {
                                ...parsed,
                                operation: 'upsert',
                                sourceName: editingNetwork?.sourceName,
                                original: editingNetwork?.original ?? false,
                                dirty: true,
                                up: editingNetwork?.up,
                            };
                            setNetworkDraft((current) =>
                                editingNetwork
                                    ? current.map((item) =>
                                          (item.sourceName ?? item.name) ===
                                          (editingNetwork.sourceName ?? editingNetwork.name)
                                              ? draft
                                              : item
                                      )
                                    : [...current, draft]
                            );
                            setNetworkOpen(false);
                            setEditingNetwork(undefined);
                        }}
                    >
                        <Form.Item name="operation" hidden>
                            <Input />
                        </Form.Item>
                        <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                            <Form.Item label="逻辑接口名称" name="name">
                                <Input maxLength={15} placeholder="例如 lan" />
                            </Form.Item>
                            <Form.Item label="地址方式" name="mode">
                                <Select
                                    options={[
                                        { value: 'dhcp', label: 'DHCP 客户端' },
                                        { value: 'static', label: '静态 IPv4' },
                                    ]}
                                    onChange={(value) => {
                                        if (value === 'dhcp') {
                                            networkForm.setFieldsValue({
                                                ip: '',
                                                prefixLength: 0,
                                                gateway: '',
                                            });
                                        }
                                    }}
                                />
                            </Form.Item>
                        </div>
                        <Form.Item label="创建网桥" name="bridge" valuePropName="checked">
                            <Switch
                                onChange={(checked) =>
                                    networkForm.setFieldsValue(
                                        checked ? { device: '' } : { bridgePorts: [] }
                                    )
                                }
                            />
                        </Form.Item>
                        {networkBridge ? (
                            <Form.Item label="网桥成员" name="bridgePorts">
                                <Select
                                    mode="multiple"
                                    options={networkDeviceOptions}
                                    placeholder="选择一个或多个物理网卡"
                                    optionFilterProp="label"
                                    showSearch
                                />
                            </Form.Item>
                        ) : (
                            <Form.Item label="物理网卡" name="device">
                                <Select
                                    options={networkDeviceOptions}
                                    placeholder="选择物理网卡"
                                    optionFilterProp="label"
                                    showSearch
                                />
                            </Form.Item>
                        )}
                        {networkMode === 'static' && (
                            <>
                                <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                                    <Form.Item label="IPv4 地址" name="ip">
                                        <Input placeholder="192.168.1.1" />
                                    </Form.Item>
                                    <Form.Item label="IPv4 前缀长度" name="prefixLength">
                                        <InputNumber className="w-full" min={1} max={30} />
                                    </Form.Item>
                                </div>
                                <Form.Item label="网关（可选）" name="gateway">
                                    <Input placeholder="192.168.1.254" />
                                </Form.Item>
                            </>
                        )}
                    </Form>
                ) : (
                    <>
                        <Flex justify="space-between" align="center" gap={12} className="mb-3">
                            <span className="text-xs text-slate-500">
                                在此集中编辑全部接口；4G 上联网卡已排除，最后一次性原子下发。
                            </span>
                            <Space>
                                <Button
                                    disabled={!networkDraft.some((item) => item.dirty)}
                                    onClick={() =>
                                        setNetworkDraft(
                                            (networkNode?.networks ?? []).map((item) =>
                                                networkDraftFromReported(
                                                    item,
                                                    networkNode?.interfaces ?? []
                                                )
                                            )
                                        )
                                    }
                                >
                                    撤销全部修改
                                </Button>
                                <Button
                                    type="primary"
                                    icon={<PlusOutlined />}
                                    onClick={() => showNetworkEditor()}
                                >
                                    添加接口
                                </Button>
                            </Space>
                        </Flex>
                        <Table
                            rowKey={(item) => item.sourceName ?? item.name}
                            size="small"
                            pagination={false}
                            columns={networkDraftColumns}
                            dataSource={networkDraft}
                            scroll={{ x: 'max-content', y: 360 }}
                        />
                        <div className="mt-4">
                            <div className="mb-1 text-sm">失联自动回滚（秒）</div>
                            <InputNumber
                                min={30}
                                max={300}
                                value={networkRollbackTimeoutSec}
                                onChange={(value) => setNetworkRollbackTimeoutSec(value ?? 60)}
                            />
                            <div className="mt-1 text-xs text-slate-500">
                                节点先保存并应用整套 UCI 配置；若管理链路未恢复，再还原旧配置。
                            </div>
                        </div>
                    </>
                )}
            </FormModal>

            <FormModal
                open={Boolean(renamingNode)}
                zIndex={EDGE_ACTION_MODAL_Z_INDEX}
                title={`修改节点名称${renamingNode ? ` · ${renamingNode.imei}` : ''}`}
                onCancel={() => setRenamingNode(undefined)}
                onOk={() => nameForm.submit()}
                confirmLoading={nodeName.isPending}
                forceRender
                destroyOnHidden
            >
                <Form
                    form={nameForm}
                    layout="vertical"
                    onFinish={(values) => {
                        const parsed = validateForm(nameForm, nodeNameSchema, values);
                        if (parsed && renamingNode)
                            nodeName.mutate(
                                { id: renamingNode.id, data: parsed },
                                { onSuccess: () => setRenamingNode(undefined) }
                            );
                    }}
                >
                    <Form.Item label="节点名称" name="name">
                        <Input maxLength={100} showCount placeholder="请输入节点名称" />
                    </Form.Item>
                </Form>
            </FormModal>

            <FormModal
                open={Boolean(groupingNode)}
                zIndex={EDGE_ACTION_MODAL_Z_INDEX}
                title={`设置节点分组${groupingNode ? ` · ${groupingNode.name || groupingNode.imei}` : ''}`}
                onCancel={() => setGroupingNode(undefined)}
                onOk={() => groupForm.submit()}
                confirmLoading={nodeGroup.isPending}
                forceRender
                destroyOnHidden
            >
                <Form
                    form={groupForm}
                    layout="vertical"
                    onFinish={(values) => {
                        if (!groupingNode) return;
                        nodeGroup.mutate(
                            {
                                id: groupingNode.id,
                                data: { groupId: values.groupId || '' },
                            },
                            { onSuccess: () => setGroupingNode(undefined) }
                        );
                    }}
                >
                    <Form.Item label="所属分组" name="groupId">
                        <TreeSelect
                            allowClear
                            treeDefaultExpandAll
                            treeData={edgeGroupOptions}
                            placeholder="不选则为未分组"
                        />
                    </Form.Item>
                </Form>
            </FormModal>

            <FormModal
                open={firmwareOpen}
                zIndex={EDGE_ACTION_MODAL_Z_INDEX}
                title={`上传固件并刷写${firmwareNode ? ` · ${firmwareNode.name || firmwareNode.imei}` : ''}`}
                onCancel={() => {
                    if (firmwareUpgrade.isPending) return;
                    setFirmwareOpen(false);
                    setFirmwareNode(undefined);
                    setFirmwareUploadProgress(undefined);
                    firmwareUpgrade.reset();
                }}
                onOk={() => firmwareForm.submit()}
                okButtonProps={{ danger: true, disabled: !firmwareFile }}
                cancelButtonProps={{ disabled: firmwareUpgrade.isPending }}
                closable={!firmwareUpgrade.isPending}
                keyboard={!firmwareUpgrade.isPending}
                maskClosable={!firmwareUpgrade.isPending}
                confirmLoading={firmwareUpgrade.isPending}
                destroyOnHidden
            >
                <Form
                    form={firmwareForm}
                    layout="vertical"
                    onFinish={(values) => {
                        const parsed = validateForm(firmwareForm, firmwareUpgradeSchema, values);
                        if (parsed && firmwareNode) {
                            setFirmwareUploadProgress({
                                loadedBytes: 0,
                                totalBytes: parsed.file.size,
                                percent: 0,
                            });
                            firmwareUpgrade.mutate(
                                {
                                    id: firmwareNode.id,
                                    data: parsed,
                                    onProgress: setFirmwareUploadProgress,
                                },
                                {
                                    onSuccess: () => {
                                        setFirmwareOpen(false);
                                        setFirmwareNode(undefined);
                                        setFirmwareUploadProgress(undefined);
                                    },
                                }
                            );
                        }
                    }}
                >
                    <Form.Item
                        label="当前节点固件文件"
                        name="file"
                        getValueFromEvent={(event) => event?.fileList?.[0]?.originFileObj}
                    >
                        <Upload
                            beforeUpload={() => false}
                            maxCount={1}
                            accept=".bin,.img"
                            disabled={firmwareUpgrade.isPending}
                        >
                            <Button disabled={firmwareUpgrade.isPending}>选择固件</Button>
                        </Upload>
                    </Form.Item>
                    <p className="text-xs text-slate-500">
                        最大 128 MiB。上传完成后平台计算 SHA-256，并立即只向当前节点下发；
                        刷写重启后，固件版本以节点实际报告为准。
                    </p>
                    {firmwareUploadProgress && (
                        <div className="mb-5 rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
                            <Flex justify="space-between" gap={12} className="mb-2 text-xs">
                                <span className="font-medium text-slate-700">
                                    {firmwareUpgrade.isError
                                        ? '上传失败，可直接重试'
                                        : firmwareUploadProgress.percent >= 100
                                          ? '上传完成，平台正在校验并创建刷写任务'
                                          : '正在上传固件到平台'}
                                </span>
                                <span className="shrink-0 text-slate-500">
                                    {formatBytes(firmwareUploadProgress.loadedBytes)} /{' '}
                                    {formatBytes(firmwareUploadProgress.totalBytes)}
                                </span>
                            </Flex>
                            <Progress
                                percent={firmwareUploadProgress.percent}
                                status={firmwareUpgrade.isError ? 'exception' : 'active'}
                                size="small"
                            />
                        </div>
                    )}
                    <Form.Item label="保留 UCI 配置" name="keepSettings" valuePropName="checked">
                        <Switch />
                    </Form.Item>
                    <p className="text-xs text-red-500">
                        节点将下载固件、校验大小和 SHA-256，然后调用
                        sysupgrade；请确认固件与目标硬件完全匹配。
                    </p>
                </Form>
            </FormModal>
            {serialDebug && (
                <SerialDebugModal {...serialDebug} onClose={() => setSerialDebug(undefined)} />
            )}
            <TerminalModal
                nodeId={terminalNode?.id}
                open={terminalOpen}
                onClose={() => {
                    setTerminalOpen(false);
                    setTerminalNode(undefined);
                }}
            />
        </PageContainer>
    );
}

export default EdgeNodePage;

function DtuPacketInput({
    value = '',
    onChange,
    id,
}: {
    value?: string;
    onChange?: (value: string) => void;
    id?: string;
}) {
    const [format, setFormat] = useState<'hex' | 'ascii'>('hex');
    const [error, setError] = useState('');
    const bytes = value.match(/.{2}/g) ?? [];
    const asciiAvailable =
        /^(?:[0-9a-fA-F]{2})*$/.test(value) &&
        bytes.every((byte) => Number.parseInt(byte, 16) <= 127);
    const currentFormat = format === 'ascii' && asciiAvailable ? 'ascii' : 'hex';
    return (
        <div className="flex flex-col gap-2">
            <Select<'hex' | 'ascii'>
                className="w-28"
                value={currentFormat}
                options={[
                    { value: 'hex', label: 'HEX' },
                    { value: 'ascii', label: 'ASCII', disabled: !asciiAvailable },
                ]}
                onChange={(next) => {
                    setFormat(next);
                    setError('');
                }}
            />
            <Input.TextArea
                id={id}
                rows={2}
                maxLength={currentFormat === 'ascii' ? 256 : 512}
                status={error ? 'error' : undefined}
                value={
                    currentFormat === 'ascii'
                        ? bytes
                              .map((byte) => String.fromCharCode(Number.parseInt(byte, 16)))
                              .join('')
                        : value
                }
                onChange={(event) => {
                    if (currentFormat === 'hex') {
                        setError('');
                        onChange?.(event.target.value);
                        return;
                    }
                    const parsed = dtuAsciiPacketSchema.safeParse(event.target.value);
                    if (!parsed.success) {
                        setError(parsed.error.issues[0]?.message ?? 'ASCII 内容无效');
                        return;
                    }
                    setError('');
                    onChange?.(parsed.data);
                }}
            />
            {error && <span className="text-xs text-red-500">{error}</span>}
        </div>
    );
}

function DtuPanel({ node, scope }: { node: Edge.Node; scope: Edge.EventScope }) {
    const { has } = usePermissions();
    const query = useDtuChannels(scope);
    const save = useDtuSave(node.id);
    const remove = useDtuDelete(node.id);
    const [editing, setEditing] = useState(false);
    const [traceChannel, setTraceChannel] = useState<string>();
    const [form] = Form.useForm<Edge.DtuChannel>();
    const mode = Form.useWatch('southMode', form);
    const canWrite = has('iot:edge:config') && query.data?.supported;
    const channels = query.data?.channels ?? [];
    const selected = channels.find((item) => item.channelId === traceChannel);
    const edit = (channel?: Edge.DtuChannel) => {
        form.resetFields();
        form.setFieldsValue(
            channel ?? {
                channelId: createUuid(),
                name: '',
                enabled: true,
                southMode: 'tcp_server',
                southHost: '0.0.0.0',
                southPort: 5000,
                northHost: '',
                northPort: 9000,
                serialPath: node.serialPorts?.[0]?.path,
                baudRate: 9600,
                dataBits: 8,
                stopBits: 1,
                parity: 'none',
                rs485: node.serialPorts?.[0]?.rs485 ?? false,
                maxClients: 8,
                queueBytes: 32768,
                serialFrameMs: 20,
                uplinkOnly: false,
                registrationHex: '',
                heartbeatHex: '',
                heartbeatIntervalSec: 0,
                debugEnabled: false,
            }
        );
        setEditing(true);
    };
    const states: Record<string, string> = {
        connected: '已连接',
        connecting: '连接中',
        reconnecting: '等待重连',
        listening: '监听中',
    };
    const columns: ColumnsType<Edge.DtuChannel> = [
        {
            title: '通道',
            dataIndex: 'name',
            width: 150,
            render: (name, row) => (
                <Space>
                    <span>{name}</span>
                    {!row.enabled && <Tag>停用</Tag>}
                </Space>
            ),
        },
        {
            title: '南向设备',
            width: 210,
            render: (_, row) => (
                <div>
                    <div>
                        {row.southMode === 'serial'
                            ? row.serialPath
                            : `${row.southMode === 'tcp_server' ? 'TCP Server' : 'TCP Client'} ${row.southHost}:${row.southPort}`}
                    </div>
                    <span className="text-xs text-slate-500">
                        {row.enabled
                            ? (states[row.status?.southState ?? ''] ?? '等待应用')
                            : '已停用'}{' '}
                        · {row.status?.clientCount ?? 0} 个连接
                    </span>
                </div>
            ),
        },
        {
            title: '北向 TCP 服务器',
            width: 190,
            render: (_, row) => (
                <div>
                    {row.northHost}:{row.northPort}
                    <div className="text-xs text-slate-500">
                        {row.enabled
                            ? (states[row.status?.northState ?? ''] ?? '等待应用')
                            : '已停用'}
                    </div>
                </div>
            ),
        },
        {
            title: '转发字节',
            width: 160,
            render: (_, row) => (
                <div>
                    上行 {row.status?.upstreamBytes ?? 0}
                    <br />
                    下行 {row.status?.downstreamBytes ?? 0}
                    <div className="text-xs text-slate-500">
                        缓冲 {row.status?.queuedBytes ?? 0} B
                    </div>
                </div>
            ),
        },
        {
            title: '操作',
            width: 210,
            fixed: 'right',
            render: (_, row) => (
                <Space size={0}>
                    <Button type="link" disabled={!canWrite} onClick={() => edit(row)}>
                        配置
                    </Button>
                    <Button type="link" onClick={() => setTraceChannel(row.channelId)}>
                        调试
                    </Button>
                    <Popconfirm
                        title="删除此透传通道？"
                        onConfirm={() => remove.mutateAsync(row.channelId)}
                    >
                        <Button type="link" danger disabled={!canWrite}>
                            删除
                        </Button>
                    </Popconfirm>
                </Space>
            ),
        },
    ];
    return (
        <div className="flex min-h-0 flex-col gap-3">
            <LiveQueryError
                error={query.error ?? query.connectionError}
                retry={query.refetch}
                loading={query.isFetching}
            />
            {query.data && !query.data.supported && (
                <Alert type="warning" title="当前固件未声明 DTU 能力，请升级支持透传的固件" />
            )}
            <Flex justify="flex-end" align="center">
                <Space>
                    <Button onClick={() => void query.refetch()}>刷新</Button>
                    <Button
                        type="primary"
                        icon={<PlusOutlined />}
                        disabled={!canWrite || channels.length >= 8}
                        onClick={() => edit()}
                    >
                        新增通道
                    </Button>
                </Space>
            </Flex>
            <Table
                rowKey="channelId"
                size="small"
                loading={query.isLoading}
                dataSource={channels}
                columns={columns}
                pagination={false}
                scroll={{ x: 960, y: 320 }}
            />
            <FormModal
                title="透传通道配置"
                open={editing}
                onCancel={() => setEditing(false)}
                confirmLoading={save.isPending}
                onOk={async () => {
                    const value = validateForm(form, dtuChannelSchema, form.getFieldsValue(true));
                    if (!value) return;
                    await save.mutateAsync(value);
                    setEditing(false);
                }}
            >
                <Form form={form} layout="vertical">
                    <Form.Item name="channelId" hidden>
                        <Input />
                    </Form.Item>
                    <Form.Item name="name" label="通道名称" required>
                        <Input maxLength={100} />
                    </Form.Item>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="enabled" label="启用透传" valuePropName="checked">
                            <Switch />
                        </Form.Item>
                        <Form.Item name="southMode" label="南向连接">
                            <Select
                                options={[
                                    { value: 'serial', label: '串口' },
                                    { value: 'tcp_client', label: 'TCP Client（连接设备）' },
                                    { value: 'tcp_server', label: 'TCP Server（设备接入）' },
                                ]}
                            />
                        </Form.Item>
                    </div>
                    {mode === 'serial' ? (
                        <>
                            <Form.Item name="serialPath" label="独占串口" required>
                                <Select
                                    options={node.serialPorts?.map((port) => ({
                                        value: port.path,
                                        label: `${port.displayName || port.path} (${port.path})`,
                                    }))}
                                />
                            </Form.Item>
                            <div className="grid grid-cols-2 gap-x-4">
                                <Form.Item name="baudRate" label="波特率">
                                    <Select
                                        options={[
                                            300, 600, 1200, 2400, 4800, 9600, 19200, 38400, 57600,
                                            115200, 230400,
                                        ].map((value) => ({ value, label: String(value) }))}
                                    />
                                </Form.Item>
                                <Form.Item name="dataBits" label="数据位">
                                    <Select
                                        options={[5, 6, 7, 8].map((value) => ({
                                            value,
                                            label: String(value),
                                        }))}
                                    />
                                </Form.Item>
                                <Form.Item name="stopBits" label="停止位">
                                    <Select
                                        options={[
                                            { value: 1, label: '1' },
                                            { value: 2, label: '2' },
                                        ]}
                                    />
                                </Form.Item>
                                <Form.Item name="parity" label="校验">
                                    <Select
                                        options={[
                                            { value: 'none', label: '无' },
                                            { value: 'even', label: '偶校验' },
                                            { value: 'odd', label: '奇校验' },
                                        ]}
                                    />
                                </Form.Item>
                                <Form.Item name="rs485" label="RS485" valuePropName="checked">
                                    <Switch />
                                </Form.Item>
                                <Form.Item name="serialFrameMs" label="空闲组帧间隔（ms）">
                                    <InputNumber min={0} max={1000} className="w-full" />
                                </Form.Item>
                            </div>
                        </>
                    ) : (
                        <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                            <Form.Item
                                name="southHost"
                                label={mode === 'tcp_server' ? '监听地址' : '设备地址'}
                                required
                            >
                                <Input />
                            </Form.Item>
                            <Form.Item name="southPort" label="南向端口" required>
                                <InputNumber min={1} max={65535} className="w-full" />
                            </Form.Item>
                        </div>
                    )}
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="northHost" label="北向 TCP 服务器" required>
                            <Input placeholder="域名或 IP" />
                        </Form.Item>
                        <Form.Item name="northPort" label="北向端口" required>
                            <InputNumber min={1} max={65535} className="w-full" />
                        </Form.Item>
                    </div>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="maxClients" label="TCP Server 客户端上限">
                            <InputNumber min={1} max={16} className="w-full" />
                        </Form.Item>
                        <Form.Item name="queueBytes" label="每个方向/客户端的缓冲（字节）">
                            <InputNumber min={4096} max={65536} step={4096} className="w-full" />
                        </Form.Item>
                    </div>
                    <Form.Item name="registrationHex" label="注册包">
                        <DtuPacketInput />
                    </Form.Item>
                    <Form.Item name="heartbeatHex" label="心跳包">
                        <DtuPacketInput />
                    </Form.Item>
                    <Form.Item name="heartbeatIntervalSec" label="心跳间隔（秒，0 关闭）">
                        <InputNumber min={0} max={86400} className="w-full" />
                    </Form.Item>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="uplinkOnly" label="仅上行" valuePropName="checked">
                            <Switch />
                        </Form.Item>
                        <Form.Item name="debugEnabled" label="独立透传调试" valuePropName="checked">
                            <Switch />
                        </Form.Item>
                    </div>
                </Form>
            </FormModal>
            <FormModal
                title={`透传调试 · ${selected?.name ?? ''}`}
                open={Boolean(traceChannel)}
                onCancel={() => setTraceChannel(undefined)}
                footer={<Button onClick={() => setTraceChannel(undefined)}>关闭窗口</Button>}
            >
                {selected && (
                    <Space orientation="vertical" className="w-full" size="middle">
                        <Flex justify="space-between">
                            <span>透传调试</span>
                            <Switch
                                checked={selected.debugEnabled ?? false}
                                disabled={!canWrite}
                                loading={save.isPending}
                                onChange={(debugEnabled) =>
                                    save.mutate({ ...selected, debugEnabled })
                                }
                            />
                        </Flex>
                        {selected.status?.error && (
                            <Alert type="warning" title={selected.status.error} />
                        )}
                        <span className="text-xs text-slate-500">
                            省略的调试事件：{selected.status?.omittedTraces ?? 0}
                        </span>
                        {selected.debugEnabled ? (
                            (selected.status?.traces ?? []).map((trace) => (
                                <div
                                    key={`${trace.monotonicMs}-${trace.sequence}`}
                                    className="rounded border border-slate-200 p-3"
                                >
                                    <Space>
                                        <Tag>{trace.direction}</Tag>
                                        <span>客户端 {trace.clientSlot ?? 0}</span>
                                        <span>{trace.totalBytes ?? 0} B</span>
                                    </Space>
                                    <pre className="mt-2 whitespace-pre-wrap break-all text-xs">
                                        {dtuTraceHex(trace.payload)}
                                    </pre>
                                </div>
                            ))
                        ) : (
                            <Empty description="透传调试已关闭" />
                        )}
                    </Space>
                )}
            </FormModal>
        </div>
    );
}
