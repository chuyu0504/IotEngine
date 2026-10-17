import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useMutationWithMessage, useSaveMutation } from '@/hooks/use_mutation';
import { useSnapshotQuery } from '@/hooks/use_snapshot_query';
import { DebugOperationError } from '@/lib/edge_debug_connection';
import { refreshAccessToken } from '@/pages/login/login.service';
import { getMessageInstance } from '@/providers/message_context_bridge';
import { useAuthStore } from '@/store/auth_store';
import { fetchAllPages } from '@/utils/pagination';
import {
    acknowledgeTerminalOutput,
    authenticateDebugConnection,
    captureLogs,
    closeSerialDebug,
    closeTerminal,
    configureDebugErrorNotification,
    configureNetwork,
    createEdgeGroup,
    createEdgeVpnPeer,
    deleteDtuChannel,
    deleteEdgeGroup,
    deleteEnrollment,
    edgeDebugConnection,
    getEdgeDetail,
    getEdgeGroups,
    getEdgeInventory,
    getEdgeVpnState,
    getLogs,
    getPlmnNames,
    getSerialEvents,
    getTerminalEvents,
    getVpnNetworks,
    keepTerminalAlive,
    observeDtuChannels,
    observeEdgeDetail,
    openSerialDebug,
    openTerminal,
    queryEdgeList,
    renameEdge,
    resizeTerminal,
    reuseFirmware,
    revokeEdgeVpnPeer,
    saveDtuChannel,
    sendSerialCommand,
    setEdgeGroup,
    setEnrollment,
    setLogLevel,
    syncDeviceConfig,
    syncEdgeVpnPeer,
    updateEdgeGroup,
    updateEdgeVpnRoute,
    uploadFirmware,
    writeTerminal,
} from './edge_node.api';
import {
    firmwareUpgradeSchema,
    serialDebugEventSchema,
    serialSettingsSchema,
    terminalEventsSchema,
} from './edge_node.schema';
import type { Edge, EdgeVpn } from './edge_node.types';
import { edgeLiveQueryKeys, edgeQueryKeys, plmnNamesQueryKey } from './edge_node.types';

/** Windows client installer download. */
export const windowsClientDownloadUrl = '/downloads/iot-egine-Setup-x64.exe';

const buildGroupTree = (items: Edge.GroupItem[]) => {
    const index = new Map<string, Edge.GroupTreeItem>();
    const roots: Edge.GroupTreeItem[] = [];
    for (const item of items) index.set(item.id, { ...item, children: [] });
    for (const item of index.values()) {
        const parent = item.parentId ? index.get(item.parentId) : undefined;
        if (parent) parent.children?.push(item);
        else roots.push(item);
    }
    return roots;
};

/** Resolves a reported PLMN code (MCC+MNC) to its operator name; other values pass through. */
export function mobileOperatorName(value: string, names: Readonly<Record<string, string>>) {
    const operator = value.trim();
    if (!/^\d{5,6}$/.test(operator)) return operator;
    return (
        names[operator] ??
        (operator.length === 6 ? names[operator.slice(0, 5)] : undefined) ??
        operator
    );
}

const EMPTY_PLMN_NAMES: Readonly<Record<string, string>> = {};

/** Operator name resolver; codes show unchanged until the static PLMN table has loaded. */
export function useMobileOperatorName() {
    const { data: names = EMPTY_PLMN_NAMES } = useQuery({
        queryKey: plmnNamesQueryKey,
        queryFn: ({ signal }) => getPlmnNames(signal),
        staleTime: Infinity,
        gcTime: Infinity,
    });
    return useCallback((value: string) => mobileOperatorName(value, names), [names]);
}

export function serialPayloadHex(value: string, mode: 'hex' | 'text', ending = ''): string {
    if (mode === 'hex') {
        const hex = value.replace(/\s/g, '');
        if (!hex || hex.length % 2 || !/^[0-9a-f]+$/i.test(hex))
            throw new Error('HEX 数据必须是完整字节，例如 01 03 00 00 00 02 C4 0B');
        if (hex.length > 2048) throw new Error('每次最多发送 1024 字节');
        return hex.toUpperCase();
    }
    const bytes = new TextEncoder().encode(value + ending);
    if (!bytes.length || bytes.length > 1024) throw new Error('每次发送 1–1024 字节');
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0'))
        .join('')
        .toUpperCase();
}

const defaultSerialSettings: Edge.SerialSettings = {
    baudRate: 9600,
    dataBits: 8,
    stopBits: 1,
    parity: 'none',
    rs485: false,
};

const serialNotices: Record<string, string> = {
    'platform connection closed': '节点与平台的连接已断开',
    'device configuration changed; reopen serial debug': '设备配置已更新，请重新打开串口调试',
    'another serial session is active or this session expired': '已有其他调试会话或当前会话已过期',
    'acquisition worker unavailable': '节点采集进程暂不可用，请稍后重新连接',
    'this platform already has a serial debug session': '当前平台已有串口调试会话，请先关闭原窗口',
    'serial port is not advertised': '该串口未在节点上启用',
    'serial debug session expired or acquisition restarted':
        '调试会话已过期或采集进程已重启，请重新连接',
    'serial debug closed': '串口调试已关闭',
    'serial write still pending': '上一次发送尚未完成，请稍后操作',
    'invalid serial settings': '串口参数无效',
    'another platform controls this serial port': '其他平台正在手动调试此串口',
    'device transaction pending; retry after it completes': '设备正在执行操作，请完成后再暂停采集',
    'cannot configure serial port': '无法应用串口参数，已恢复自动采集',
    'pause automatic acquisition before manual sending': '请先暂停自动采集，再手动发送',
    'serial debug lease expired': '调试会话超时，已恢复自动采集',
    'cannot open serial port': '无法打开串口，请检查节点设备',
    'serial write failed; some bytes may have been sent':
        '发送失败，部分字节可能已发送，请核实后再操作',
    'serial device disconnected': '串口设备已断开',
    'serial read failed': '串口读取失败',
};

export function useSerialDebug(nodeId: string, path: string) {
    const [connection, setConnection] = useState<'connecting' | 'ready' | 'closed'>('connecting');
    const [manual, setManual] = useState(false);
    const [settings, setSettings] = useState<Edge.SerialSettings>(defaultSerialSettings);
    const [frames, setFrames] = useState<Edge.SerialFrame[]>([]);
    const [counts, setCounts] = useState({ tx: 0, rx: 0, dropped: 0 });
    const [notice, setNotice] = useState('');
    const [pending, setPending] = useState(false);
    const [attempt, setAttempt] = useState(0);
    const sessionRef = useRef<{
        busy: boolean;
        send: (command: Record<string, unknown>) => Promise<void>;
        close: () => void;
    } | null>(null);
    const sequenceRef = useRef(1);
    const pendingRef = useRef<{ id: number; timer: ReturnType<typeof setTimeout> } | null>(null);
    const readyRef = useRef(false);

    useEffect(() => {
        void attempt;
        let disposed = false;
        let lastSequence = 0;
        let rx = 0;
        let tx = 0;
        let dropped = 0;
        let lastDropped = 0;
        let rowId = 0;
        let received = false;
        let queued: Edge.SerialFrame[] = [];
        const decoders = { RX: new TextDecoder(), TX: new TextDecoder() };
        sequenceRef.current = 1;
        readyRef.current = false;
        setConnection('connecting');
        setManual(false);
        setNotice('');
        setFrames([]);
        setCounts({ tx: 0, rx: 0, dropped: 0 });
        const finishPending = () => {
            if (pendingRef.current) clearTimeout(pendingRef.current.timer);
            pendingRef.current = null;
            setPending(false);
        };
        finishPending();
        const flush = setInterval(() => {
            if (disposed || !received) return;
            const batch = queued;
            queued = [];
            received = false;
            if (batch.length) setFrames((previous) => [...previous, ...batch].slice(-200));
            setCounts({ tx, rx, dropped });
        }, 80);
        const heartbeat = setInterval(() => {
            const session = sessionRef.current;
            if (readyRef.current && session && !session.busy && !pendingRef.current)
                void session
                    .send({ action: 'keepalive', requestId: ++sequenceRef.current })
                    .catch(() => session.close());
        }, 10000);
        openSerialDebug(nodeId, path)
            .then(({ id }) => {
                if (disposed) {
                    void closeSerialDebug(nodeId, id).catch(() => {});
                    return;
                }
                let active = true;
                let release: (() => void) | undefined;
                const opening = setTimeout(() => {
                    setNotice('串口打开超时，请检查节点状态');
                    session.close();
                }, 20000);
                const session = {
                    busy: false,
                    async send(command: Record<string, unknown>) {
                        if (!active || session.busy)
                            throw new Error('串口指令正在发送，请稍后再试');
                        session.busy = true;
                        try {
                            await sendSerialCommand(nodeId, id, command);
                        } finally {
                            session.busy = false;
                        }
                    },
                    close() {
                        if (!active) return;
                        active = false;
                        clearTimeout(opening);
                        release?.();
                        if (sessionRef.current === session) sessionRef.current = null;
                        void closeSerialDebug(nodeId, id).catch(() => {});
                        if (disposed) return;
                        readyRef.current = false;
                        setConnection('closed');
                        setManual(false);
                        if (pendingRef.current)
                            setNotice('连接中断，最后一次操作结果未知；请勿直接重复发送');
                        finishPending();
                    },
                };
                sessionRef.current = session;
                const receive = (raw: unknown) => {
                    if (disposed || !active) return;
                    try {
                        const event = serialDebugEventSchema.parse(raw);
                        if (event.sequence !== undefined) {
                            if (event.sequence <= lastSequence) return;
                            if (lastSequence && event.sequence > lastSequence + 1)
                                setNotice(
                                    '部分监听记录因链路拥塞被省略；收发计数仅包含已收到的数据'
                                );
                            lastSequence = event.sequence;
                        }
                        if (event.droppedBytes !== undefined && event.droppedBytes > lastDropped) {
                            dropped += event.droppedBytes - lastDropped;
                            lastDropped = event.droppedBytes;
                            received = true;
                        }
                        if (event.kind === 'state' || event.kind === 'error') {
                            clearTimeout(opening);
                            readyRef.current = true;
                            setConnection('ready');
                            setManual(event.manual ?? false);
                            const parsed = serialSettingsSchema.safeParse(event.settings);
                            if (parsed.success)
                                setSettings((previous) =>
                                    JSON.stringify(previous) === JSON.stringify(parsed.data)
                                        ? previous
                                        : parsed.data
                                );
                        }
                        if (
                            event.kind === 'state' ||
                            event.kind === 'sent' ||
                            event.kind === 'error'
                        ) {
                            if (pendingRef.current?.id === event.requestId) finishPending();
                            if (event.kind === 'error')
                                setNotice(
                                    serialNotices[event.message ?? ''] ||
                                        event.message ||
                                        '节点拒绝了操作'
                                );
                            if (event.kind === 'sent') setNotice('数据已写入串口');
                        }
                        if (
                            event.kind === 'data' &&
                            event.hex &&
                            (event.direction === 'RX' || event.direction === 'TX')
                        ) {
                            const bytes = Uint8Array.from(event.hex.match(/../g) ?? [], (hex) =>
                                Number.parseInt(hex, 16)
                            );
                            if (event.direction === 'RX') rx += bytes.length;
                            else tx += bytes.length;
                            queued.push({
                                id: ++rowId,
                                timestamp: event.timestamp ?? Date.now(),
                                direction: event.direction,
                                hex: event.hex,
                                text: decoders[event.direction].decode(bytes, { stream: true }),
                            });
                            if (queued.length > 200) queued = queued.slice(-200);
                            received = true;
                        }
                        if (event.kind === 'closed') {
                            setNotice(
                                serialNotices[event.message ?? ''] ||
                                    event.message ||
                                    '串口调试已结束'
                            );
                            session.close();
                        }
                    } catch {
                        setNotice('串口调试数据格式错误，连接已关闭');
                        session.close();
                    }
                };
                release = getSerialEvents(nodeId, id).subscribe({
                    next: (snapshot) => {
                        if (!Array.isArray(snapshot.events)) {
                            setNotice('串口事件格式错误');
                            session.close();
                            return;
                        }
                        for (const event of snapshot.events) receive(event);
                    },
                    error: (error) => {
                        if (!disposed) setNotice(error.message);
                        session.close();
                    },
                });
                if (!active) release();
            })
            .catch(() => {
                if (!disposed) {
                    setConnection('closed');
                    setNotice('无法打开串口调试，请检查节点状态和固件能力');
                }
            });
        return () => {
            disposed = true;
            readyRef.current = false;
            clearInterval(flush);
            clearInterval(heartbeat);
            if (pendingRef.current) clearTimeout(pendingRef.current.timer);
            pendingRef.current = null;
            sessionRef.current?.close();
            sessionRef.current = null;
        };
    }, [nodeId, path, attempt]);

    const send = (
        action: 'manual' | 'monitor' | 'write',
        payload: Partial<Edge.SerialSettings> & { hex?: string } = {}
    ) => {
        const session = sessionRef.current;
        if (!readyRef.current || !session || pendingRef.current)
            throw new Error('串口尚未就绪或正在等待操作结果');
        if (session.busy) throw new Error('串口指令正在发送，请稍后再试');
        if (action === 'manual') serialSettingsSchema.parse(payload);
        const requestId = ++sequenceRef.current;
        const timer = setTimeout(() => {
            pendingRef.current = null;
            setPending(false);
            setNotice('操作确认超时，结果未知；请重新连接核实，避免重复发送');
            session.close();
        }, 20000);
        pendingRef.current = { id: requestId, timer };
        setPending(true);
        setNotice('');
        void session.send({ action, requestId, ...payload }).catch((error: unknown) => {
            if (sessionRef.current !== session) return;
            session.close();
            setNotice(
                error instanceof Error ? error.message : '串口指令发送失败，请确认结果后再操作'
            );
        });
    };
    return {
        connection,
        manual,
        settings,
        frames,
        counts,
        notice,
        pending,
        send,
        reconnect: () => setAttempt((value) => value + 1),
        clear: () => setFrames([]),
    };
}
export function buildEdgeNodeGroupView(
    groups: Edge.GroupTreeItem[],
    nodes: Edge.Node[],
    selectedId: string | null,
    keyword: string,
    status?: Edge.EnrollmentStatus
) {
    const index = new Map<string, Edge.GroupTreeItem>();
    const visit = (items: Edge.GroupTreeItem[]) =>
        items.forEach((group) => {
            index.set(group.id, group);
            visit(group.children ?? []);
        });
    visit(groups);
    const selected = selectedId ? index.get(selectedId) : undefined;
    const scope = new Set<string>();
    const collect = (group: Edge.GroupTreeItem) => {
        scope.add(group.id);
        group.children?.forEach(collect);
    };
    if (selected) collect(selected);
    const query = keyword.trim().toLowerCase();
    const filtered = nodes.filter((node) => {
        if (selectedId === 'ungrouped' && node.groupId) return false;
        if (selectedId && selectedId !== 'ungrouped' && !scope.has(node.groupId ?? ''))
            return false;
        if (status && node.enrollmentStatus !== status) return false;
        return (
            !query ||
            [node.name, node.imei, node.model, node.hostname].some((value) =>
                value?.toLowerCase().includes(query)
            )
        );
    });
    const direct = new Map<string, Edge.Node[]>();
    for (const node of filtered) {
        const id = node.groupId ?? '';
        const items = direct.get(id) ?? [];
        items.push(node);
        direct.set(id, items);
    }
    const stats = new Map<
        string,
        {
            total: number;
            online: number;
            offline: number;
        }
    >();
    const count = (group: Edge.GroupTreeItem) => {
        const own = direct.get(group.id) ?? [];
        const value = {
            total: own.length,
            online: own.filter((node) => node.status.online).length,
            offline: 0,
        };
        for (const child of group.children ?? []) {
            const childStats = count(child);
            value.total += childStats.total;
            value.online += childStats.online;
        }
        value.offline = value.total - value.online;
        stats.set(group.id, value);
        return value;
    };
    groups.forEach(count);
    return {
        filtered,
        direct,
        stats,
        roots: selectedId === 'ungrouped' ? [] : selectedId ? (selected ? [selected] : []) : groups,
        ungrouped: filtered.filter((node) => !node.groupId),
        ungroupedCount: nodes.filter((node) => !node.groupId).length,
    };
}

const platformVirtualInterfaces = new Set(['lo', 'wg']);
export function physicalNetworkInterfaces(interfaces: Edge.NetworkInterface[]) {
    const subinterfaceParents = new Set<string>();
    for (const item of interfaces) {
        let parent = item.name;
        while (parent.includes('.')) {
            parent = parent.slice(0, parent.lastIndexOf('.'));
            if (parent) subinterfaceParents.add(parent);
        }
    }
    return interfaces.filter(
        (item) =>
            !item.bridge &&
            !subinterfaceParents.has(item.name) &&
            !platformVirtualInterfaces.has(item.name)
    );
}
export function normalizeReportedNetwork(
    network: Edge.Network,
    interfaces: Edge.NetworkInterface[]
) {
    const reportedBridge = interfaces.find(
        (candidate) => candidate.name === network.device && candidate.bridge
    );
    const bridge = network.bridge || Boolean(reportedBridge);
    return {
        bridge,
        bridgePorts: bridge
            ? network.bridgePorts.length > 0
                ? network.bridgePorts
                : (reportedBridge?.bridgePorts ?? [])
            : [],
        device: bridge ? '' : network.device,
    };
}

export const queryEdgeSelectionList = (signal?: AbortSignal) =>
    fetchAllPages(100, (page, pageSize) => queryEdgeList({ page, pageSize }, signal));
export const useEdgeSelectionList = (enabled: boolean) =>
    useQuery({
        queryKey: edgeQueryKeys.selection(),
        queryFn: ({ signal }) => queryEdgeSelectionList(signal),
        enabled,
        staleTime: 0,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useEdgeInventory = (enabled = true, scope?: Edge.EventScope) =>
    useSnapshotQuery({
        queryKey: edgeLiveQueryKeys.inventory(),
        streamKey: scope,
        queryFn: () => getEdgeInventory(scope),
        enabled,
        placeholderData: keepPreviousData,
    });
export const useEdgeConfigurationDetail = (nodeId?: string) =>
    useQuery({
        queryKey: edgeQueryKeys.configuration(nodeId),
        queryFn: ({ signal }) => getEdgeDetail(nodeId ?? '', signal),
        enabled: Boolean(nodeId),
        refetchOnWindowFocus: false,
    });
/** Reads the current reported configuration of a node over HTTP, sharing its query cache. */
export function useFetchEdgeConfiguration() {
    const client = useQueryClient();
    return useCallback(
        (nodeId: string) =>
            client.fetchQuery({
                queryKey: edgeQueryKeys.configuration(nodeId),
                queryFn: ({ signal }) => getEdgeDetail(nodeId, signal),
            }),
        [client]
    );
}
export const useEdgeDetail = (scope: Edge.EventScope) =>
    useSnapshotQuery({
        queryKey: edgeLiveQueryKeys.detail(scope.nodeId),
        streamKey: scope,
        queryFn: () => observeEdgeDetail(scope),
        enabled: Boolean(scope.nodeId),
    });
export const useEdgeGroupTree = () =>
    useQuery({
        queryKey: edgeQueryKeys.groups(),
        queryFn: async ({ signal }) => buildGroupTree(await getEdgeGroups(signal)),
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useEdgeLogs = (scope: Edge.EventScope, enabled = true) => {
    const id = scope.nodeId;
    const result = useSnapshotQuery({
        queryKey: edgeLiveQueryKeys.logs(id, scope.logs),
        streamKey: scope,
        queryFn: () => getLogs(scope),
        enabled: enabled && Boolean(id && scope.logs),
    });
    useEffect(() => {
        if (enabled && id) void captureLogs(id).catch(() => undefined);
    }, [enabled, id]);
    return {
        ...result,
        refetch: async () => {
            if (!id) return;
            if (result.error) await result.refetch({ throwOnError: true });
            await captureLogs(id);
        },
    };
};
export function useEnrollmentMutation() {
    return useMutationWithMessage({
        mutationFn: (value: { id: string; status: 'approved'; name?: string }) =>
            setEnrollment(value.id, value.status, value.name),
        successMessage: '注册状态已更新',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useEdgeDeleteMutation() {
    return useMutationWithMessage({
        mutationFn: deleteEnrollment,
        successMessage: '注册申请已删除，节点需要重新注册',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useRenameEdgeNode() {
    return useMutationWithMessage({
        mutationFn: (value: { id: string; data: Edge.NameDto }) => renameEdge(value.id, value.data),
        successMessage: '节点名称已更新',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useAssignEdgeNodeGroup() {
    return useMutationWithMessage({
        mutationFn: (value: { id: string; data: Edge.GroupDto }) =>
            setEdgeGroup(value.id, value.data),
        successMessage: '节点分组已更新',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useEdgeGroupSave() {
    return useSaveMutation<
        Edge.GroupSaveDto & {
            id?: string;
        },
        Edge.GroupSaveDto,
        Edge.GroupSaveDto
    >({
        createFn: createEdgeGroup,
        updateFn: updateEdgeGroup,
        toUpdatePayload: ({ id: _id, ...data }) => data,
        createMessage: '边缘节点分组已创建',
        updateMessage: '边缘节点分组已更新',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useEdgeGroupDelete() {
    return useMutationWithMessage({
        mutationFn: deleteEdgeGroup,
        successMessage: '边缘节点分组已删除',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useConfigureEdgeNetwork() {
    return useMutationWithMessage({
        mutationFn: (value: { id: string; data: Edge.NetworkDto }) =>
            configureNetwork(value.id, value.data),
        successMessage: '网络配置已下发',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useDeviceConfigSyncMutation() {
    return useMutationWithMessage({
        mutationFn: syncDeviceConfig,
        successMessage: '设备配置已生成并下发',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export async function upgradeFirmware(
    id: string,
    data: Edge.FirmwareUpgradeDto,
    onProgress?: (progress: Edge.FirmwareUploadProgress) => void
): Promise<void> {
    const value = firmwareUpgradeSchema.parse(data);
    const digest = await crypto.subtle.digest('SHA-256', await value.file.arrayBuffer());
    const sha256 = Array.from(new Uint8Array(digest), (byte) =>
        byte.toString(16).padStart(2, '0')
    ).join('');
    const { reused } = await reuseFirmware(id, sha256, value.file.size, value.keepSettings);
    if (reused) {
        onProgress?.({ loadedBytes: value.file.size, totalBytes: value.file.size, percent: 100 });
        return;
    }
    await uploadFirmware(id, value, onProgress);
}

export function useFirmwareUpgradeMutation() {
    return useMutationWithMessage({
        mutationFn: (value: {
            id: string;
            data: Edge.FirmwareUpgradeDto;
            onProgress?: (progress: Edge.FirmwareUploadProgress) => void;
        }) => upgradeFirmware(value.id, value.data, value.onProgress),
        successMessage: '固件已上传，刷写任务已下发给当前节点',
        invalidateKeys: [edgeQueryKeys.all],
    });
}
export function useSetEdgeLogLevel() {
    return useMutationWithMessage({
        mutationFn: (value: { id: string; data: Edge.LogLevelDto }) =>
            setLogLevel(value.id, value.data),
        successMessage: '日志等级已下发',
        invalidateKeys: [edgeQueryKeys.all],
    });
}

export const useEdgeVpn = (scope: Edge.EventScope, enabled: boolean) => {
    const networks = useQuery({
        queryKey: edgeQueryKeys.vpnNetworks(),
        queryFn: ({ signal }) => getVpnNetworks(signal),
        enabled,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
    const state = useSnapshotQuery({
        queryKey: edgeLiveQueryKeys.vpn(scope.nodeId),
        streamKey: scope,
        queryFn: () => getEdgeVpnState(scope),
        enabled: enabled && Boolean(scope.nodeId && scope.vpn),
    });
    const networkList = networks.data?.list;
    const data = useMemo(
        () => (state.data && networkList ? { ...state.data, networks: networkList } : undefined),
        [state.data, networkList]
    );
    const refetchState = state.refetch;
    const refetchNetworks = networks.refetch;
    const refetch = useCallback(
        () => Promise.all([refetchState(), refetchNetworks()]),
        [refetchState, refetchNetworks]
    );
    return {
        data,
        error: state.error ?? networks.error,
        isLoading: state.isLoading || networks.isLoading,
        isFetching: state.isFetching || networks.isFetching,
        refetch,
    };
};
const vpnInvalidations = [edgeQueryKeys.all];
export const useEdgeVpnPeerCreate = () =>
    useMutationWithMessage({
        mutationFn: createEdgeVpnPeer,
        successMessage: '节点 VPN 已启用，正在下发桥接网段映射',
        invalidateKeys: vpnInvalidations,
    });
export const useEdgeVpnPeerSync = () =>
    useMutationWithMessage({
        mutationFn: syncEdgeVpnPeer,
        successMessage: 'VPN 配置已重新下发',
        invalidateKeys: vpnInvalidations,
    });
export const useEdgeVpnPeerRevoke = () =>
    useMutationWithMessage({
        mutationFn: revokeEdgeVpnPeer,
        successMessage: 'VPN Peer 已撤销',
        invalidateKeys: vpnInvalidations,
    });
export const useEdgeVpnRouteUpdate = () =>
    useMutationWithMessage({
        mutationFn: ({ id, data }: { id: string; data: Partial<EdgeVpn.RouteDto> }) =>
            updateEdgeVpnRoute(id, data),
        successMessage: '虚拟网段已更新，正在下发配置',
        invalidateKeys: vpnInvalidations,
    });

const TERMINAL_INPUT_LIMIT = 256 * 1024;
const TERMINAL_OUTPUT_LIMIT = 1024 * 1024;
const TERMINAL_CHUNK_BYTES = 16384;

/**
 * Web terminal session on `screen`: open, 20s keepalive, serialized resize, 8ms input batching,
 * in-order output rendered once across resubscription, acknowledged only after rendering, and close.
 */
export function useTerminalSession(nodeId: string | undefined, screen: Edge.TerminalScreen | null) {
    const [state, setState] = useState('正在连接…');
    const [ended, setEnded] = useState(false);
    const [attempt, setAttempt] = useState(0);
    const controlRef = useRef<{
        input: (data: string) => void;
        resize: (columns: number, rows: number) => void;
    } | null>(null);

    useEffect(() => {
        void attempt;
        if (!nodeId || !screen) return;
        const targetNodeId = nodeId;
        screen.reset();
        setEnded(false);
        setState('正在启动终端…');
        let disposed = false;
        let finished = false;
        let ready = false;
        let sessionId: string | undefined;
        let release: (() => void) | undefined;
        let heartbeat: ReturnType<typeof setInterval> | undefined;
        let inputTimer: ReturnType<typeof setTimeout> | undefined;
        let lastSize = '';
        let resizing = false;
        let pendingSize: { columns: number; rows: number } | undefined;
        let lastReceivedSequence = 0;
        let lastRenderedSequence = 0;
        let lastConfirmedSequence = 0;
        let pendingCloseReason: string | undefined;
        let sending = false;
        let rendering = false;
        let inputBytes = 0;
        let outputBytes = 0;
        const inputQueue: Uint8Array[] = [];
        const outputQueue: { bytes: Uint8Array; sequence: number }[] = [];
        const encoder = new TextEncoder();
        const openingTimeout = setTimeout(() => finish('终端启动超时'), 15000);
        function finish(reason: string) {
            if (finished) return;
            finished = true;
            ready = false;
            release?.();
            release = undefined;
            clearTimeout(openingTimeout);
            clearInterval(heartbeat);
            clearTimeout(inputTimer);
            inputQueue.length = 0;
            outputQueue.length = 0;
            inputBytes = 0;
            outputBytes = 0;
            screen?.closed();
            if (sessionId) void closeTerminal(targetNodeId, sessionId).catch(() => undefined);
            if (!disposed) {
                setState(reason);
                setEnded(true);
            }
        }
        function fail(error: unknown) {
            finish(error instanceof Error ? error.message : '终端通信失败');
        }
        async function flushResize() {
            if (resizing || !ready || !sessionId || finished) return;
            resizing = true;
            try {
                while (!finished && pendingSize) {
                    const size = pendingSize;
                    pendingSize = undefined;
                    await resizeTerminal(targetNodeId, sessionId, size.columns, size.rows);
                }
            } catch (error) {
                fail(error);
            } finally {
                resizing = false;
            }
        }
        async function flushInput() {
            inputTimer = undefined;
            if (sending || !ready || !sessionId || finished) return;
            sending = true;
            try {
                while (!finished && inputQueue.length > 0) {
                    const next = inputQueue[0];
                    const chunk = next.slice(0, TERMINAL_CHUNK_BYTES);
                    if (chunk.length === next.length) inputQueue.shift();
                    else inputQueue[0] = next.slice(chunk.length);
                    inputBytes -= chunk.length;
                    await writeTerminal(targetNodeId, sessionId, chunk);
                }
            } catch (error) {
                fail(error);
            } finally {
                sending = false;
            }
        }
        function acknowledgeOutput(sequence: number) {
            if (disposed || finished || !sessionId) return;
            void acknowledgeTerminalOutput(targetNodeId, sessionId, sequence)
                .then(
                    () => {
                        if (disposed || finished || sequence <= lastConfirmedSequence) return;
                        // A replay ACK can finish a lost original request. Resume only once;
                        // its late reply must not release a newer frame's pending render.
                        lastConfirmedSequence = sequence;
                        rendering = false;
                        renderOutput();
                    },
                    (error: unknown) => {
                        if (sequence > lastConfirmedSequence) fail(error);
                    }
                )
                .catch(fail);
        }
        function renderOutput() {
            if (disposed || finished || rendering) return;
            if (outputQueue.length === 0) {
                if (pendingCloseReason !== undefined) finish(pendingCloseReason);
                return;
            }
            const item = outputQueue.shift();
            if (!item || !sessionId || !screen) return;
            rendering = true;
            screen.write(item.bytes, () => {
                if (disposed || finished || !sessionId) {
                    rendering = false;
                    return;
                }
                outputBytes -= item.bytes.length;
                if (item.sequence > 0) {
                    lastRenderedSequence = item.sequence;
                    acknowledgeOutput(item.sequence);
                } else {
                    void Promise.resolve()
                        .then(() => {
                            rendering = false;
                            renderOutput();
                        })
                        .catch(fail);
                }
            });
        }
        function receive(value: unknown) {
            if (disposed || finished) return;
            try {
                for (const event of terminalEventsSchema.parse(value).events) {
                    if (event.kind === 'ready') {
                        ready = true;
                        clearTimeout(openingTimeout);
                        setState('已连接');
                        screen?.ready();
                        void flushInput();
                    } else if (event.kind === 'close') {
                        ready = false;
                        screen?.closed();
                        inputQueue.length = 0;
                        inputBytes = 0;
                        pendingCloseReason = event.reason || '终端已关闭';
                        renderOutput();
                        break;
                    } else {
                        const bytes = Uint8Array.from(atob(event.content), (character) =>
                            character.charCodeAt(0)
                        );
                        if (
                            bytes.length > TERMINAL_CHUNK_BYTES ||
                            (event.sequence > 0 && event.sequence > lastReceivedSequence + 1)
                        )
                            throw new Error('终端数据顺序无效，请重新连接');
                        if (event.sequence > 0) {
                            if (event.sequence <= lastReceivedSequence) {
                                // A resubscription replays retained frames, including pending writes.
                                // Only completed renders can safely repeat the cumulative ACK.
                                if (event.sequence <= lastRenderedSequence)
                                    acknowledgeOutput(lastRenderedSequence);
                                continue;
                            }
                            lastReceivedSequence = event.sequence;
                        }
                        if (outputBytes + bytes.length > TERMINAL_OUTPUT_LIMIT)
                            throw new Error('终端输出超过处理能力，连接已关闭，请重新连接');
                        outputQueue.push({ bytes, sequence: event.sequence });
                        outputBytes += bytes.length;
                        renderOutput();
                    }
                }
            } catch (error) {
                fail(error);
            }
        }
        const control = {
            input(data: string) {
                if (finished) return;
                const bytes = encoder.encode(data);
                if (inputBytes + bytes.length > TERMINAL_INPUT_LIMIT) {
                    finish('终端输入队列已满，连接已关闭，请重新连接');
                    return;
                }
                inputQueue.push(bytes);
                inputBytes += bytes.length;
                if (inputTimer === undefined) inputTimer = setTimeout(() => void flushInput(), 8);
            },
            resize(columns: number, rows: number) {
                const size = `${columns}:${rows}`;
                if (!ready || !sessionId || finished || lastSize === size) return;
                lastSize = size;
                pendingSize = { columns, rows };
                void flushResize();
            },
        };
        controlRef.current = control;
        const size = screen.size();
        void openTerminal(targetNodeId, size.columns, size.rows)
            .then(({ id }) => {
                if (disposed || finished) {
                    void closeTerminal(targetNodeId, id).catch(() => undefined);
                    return;
                }
                sessionId = id;
                release = getTerminalEvents(targetNodeId, id).subscribe({
                    next: receive,
                    error: fail,
                });
                heartbeat = setInterval(() => {
                    if (!finished && !resizing)
                        void keepTerminalAlive(targetNodeId, id).catch(fail);
                }, 20000);
            })
            .catch(fail);
        return () => {
            disposed = true;
            if (controlRef.current === control) controlRef.current = null;
            finish('终端已关闭');
        };
    }, [nodeId, screen, attempt]);

    const input = useCallback((data: string) => controlRef.current?.input(data), []);
    const resize = useCallback(
        (columns: number, rows: number) => controlRef.current?.resize(columns, rows),
        []
    );
    const reconnect = useCallback(() => setAttempt((value) => value + 1), []);
    return { state, ended, input, resize, reconnect };
}

async function restoreDebugConnectionSession(): Promise<void> {
    const token = useAuthStore.getState().token;
    if (!token) return;
    try {
        await authenticateDebugConnection(token);
    } catch (error) {
        if (useAuthStore.getState().token !== token) throw error;
        if (
            error instanceof DebugOperationError &&
            error.code === 11005 &&
            (await refreshAccessToken())
        )
            return;
        if (error instanceof DebugOperationError) useAuthStore.getState().clearAuth();
        throw error;
    }
}

export function configureEdgeDebugConnection() {
    configureDebugErrorNotification((message) => {
        void getMessageInstance()?.error(message);
    });
    edgeDebugConnection.configureSessionRestore(restoreDebugConnectionSession);
}

export const useDtuChannels = (scope: Edge.EventScope) =>
    useSnapshotQuery({
        queryKey: edgeLiveQueryKeys.dtu(scope.nodeId),
        streamKey: scope,
        queryFn: () => observeDtuChannels(scope),
        enabled: Boolean(scope.nodeId && scope.dtu),
    });
// DTU channels are pushed over SSE after each write; there is no HTTP cache to invalidate.
export const useDtuSave = (id: string) =>
    useMutationWithMessage({
        mutationFn: (data: Edge.DtuChannel) => saveDtuChannel(id, data),
        successMessage: '透传配置已保存并提交下发',
    });
export const useDtuDelete = (id: string) =>
    useMutationWithMessage({
        mutationFn: (channelId: string) => deleteDtuChannel(id, channelId),
        successMessage: '透传通道已删除并提交下发',
    });
export function dtuTraceHex(payload = '') {
    try {
        return Array.from(atob(payload), (byte) => byte.charCodeAt(0).toString(16).padStart(2, '0'))
            .join(' ')
            .toUpperCase();
    } catch {
        return '';
    }
}
