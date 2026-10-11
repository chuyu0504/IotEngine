import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query';
import { useEffect, useRef, useState } from 'react';
import { useMutationWithMessage, useSaveMutation } from '@/hooks/use_mutation';
import { useSnapshotQuery } from '@/hooks/use_snapshot_query';
import { refreshAccessToken } from '@/pages/login/login.service';
import { getMessageInstance } from '@/providers/message_context_bridge';
import { useAuthStore } from '@/store/auth_store';
import {
    authenticateDebugConnection,
    captureLogs,
    closeSerialDebug,
    configureDebugErrorNotification,
    configureNetwork,
    createEdgeGroup,
    createEdgeVpnPeer,
    DebugOperationError,
    deleteDtuChannel,
    deleteEdgeGroup,
    deleteEnrollment,
    edgeDebugConnection,
    getDtuChannels,
    getEdgeDetail,
    getEdgeGroups,
    getEdgeInventory,
    getEdgeVpnState,
    getLogs,
    getSerialEvents,
    getVpnNetworks,
    observeDtuChannels,
    observeEdgeDetail,
    openSerialDebug,
    queryEdgeList,
    renameEdge,
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
} from './edge_node.api';
import {
    firmwareUpgradeSchema,
    serialDebugEventSchema,
    serialSettingsSchema,
} from './edge_node.schema';
import type { Edge, EdgeVpn } from './edge_node.types';
import { edgeQueryKeys, edgeVpnQueryKeys } from './edge_node.types';

export { getWindowsClientDownloadUrl } from './edge_node.api';

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

export { mobileOperatorName } from '@/utils/mobile_operator';

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

export async function queryEdgeSelectionList(signal?: AbortSignal): Promise<Edge.Node[]> {
    const first = await queryEdgeList({ page: 1, pageSize: 100 }, signal);
    const nodes = new Map(first.list.map((node) => [node.id, node]));
    for (let page = 2; page <= Math.ceil(first.total / 100); page++) {
        const result = await queryEdgeList({ page, pageSize: 100 }, signal);
        for (const node of result.list) nodes.set(node.id, node);
    }
    return [...nodes.values()];
}
export const useEdgeSelectionList = (enabled: boolean) =>
    useQuery({
        queryKey: [...edgeQueryKeys.all, 'selection'],
        queryFn: ({ signal }) => queryEdgeSelectionList(signal),
        enabled,
        staleTime: 0,
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useEdgeInventory = (enabled = true, scope?: Edge.EventScope) =>
    useSnapshotQuery({
        queryKey: [...edgeQueryKeys.all, 'inventory'],
        streamKey: scope,
        queryFn: () => getEdgeInventory(scope),
        enabled,
        placeholderData: keepPreviousData,
    });
export const useEdgeConfigurationDetail = (nodeId?: string) =>
    useQuery({
        queryKey: [...edgeQueryKeys.detail(nodeId ?? ''), 'configuration'],
        queryFn: ({ signal }) => getEdgeDetail(nodeId ?? '', signal),
        enabled: Boolean(nodeId),
        refetchInterval: false,
        refetchOnWindowFocus: false,
    });
export const useEdgeDetail = (scope: Edge.EventScope) =>
    useSnapshotQuery({
        queryKey: edgeQueryKeys.detail(scope.nodeId),
        streamKey: scope,
        queryFn: () => observeEdgeDetail(scope),
        enabled: Boolean(scope.nodeId),
    });
export const useEdgeGroupTree = () =>
    useQuery({
        queryKey: edgeQueryKeys.groups(),
        queryFn: async ({ signal }) => buildGroupTree(await getEdgeGroups(signal)),
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
export const useEdgeLogs = (scope: Edge.EventScope, enabled = true) => {
    const id = scope.nodeId;
    const result = useSnapshotQuery({
        queryKey: [...edgeQueryKeys.logs(id, scope.logs), scope],
        queryFn: () => getLogs(scope),
        enabled: enabled && Boolean(id && scope.logs),
        staleTime: 0,
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
        queryKey: [...edgeVpnQueryKeys.all, 'networks'],
        queryFn: ({ signal }) => getVpnNetworks(signal),
        enabled,
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
    const state = useSnapshotQuery({
        queryKey: [...edgeVpnQueryKeys.node(scope.nodeId), scope],
        queryFn: () => getEdgeVpnState(scope),
        enabled: enabled && Boolean(scope.nodeId && scope.vpn),
    });
    return {
        ...state,
        data:
            state.data && networks.data
                ? { ...state.data, networks: networks.data.list }
                : undefined,
        isLoading: state.isLoading || networks.isLoading,
        isFetching: state.isFetching || networks.isFetching,
        refetch: () => Promise.all([state.refetch(), networks.refetch()]),
    };
};
const vpnInvalidations = [edgeVpnQueryKeys.all, edgeQueryKeys.all];
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

export {
    acknowledgeTerminalOutput,
    closeTerminal,
    getEdgeDetail,
    getTerminalEvents,
    keepTerminalAlive,
    openTerminal,
    resizeTerminal,
    writeTerminal,
} from './edge_node.api';

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

export function useDtuChannels(scope: Edge.EventScope) {
    const client = useQueryClient();
    const [streamError, setStreamError] = useState<Error>();
    const [attempt, setAttempt] = useState(0);
    const token = useAuthStore((state) => state.token);
    const signature = JSON.stringify(scope);
    const id = scope.nodeId ?? '';
    const result = useQuery({
        queryKey: ['edge-dtu', id],
        queryFn: ({ signal }) => getDtuChannels(id, signal),
        enabled: Boolean(id && scope.dtu),
        refetchInterval: false,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
    useEffect(() => {
        void attempt;
        const current: Edge.EventScope = JSON.parse(signature);
        if (!current.nodeId || !current.dtu || !token) return;
        setStreamError(undefined);
        return observeDtuChannels(current).subscribe({
            next: (value) => {
                client.setQueryData(['edge-dtu', current.nodeId], value);
                setStreamError(undefined);
            },
            error: setStreamError,
        });
    }, [client, signature, attempt, token]);
    return { ...result, streamError, retryStream: () => setAttempt((value) => value + 1) };
}
export const useDtuSave = (id: string) =>
    useMutationWithMessage({
        mutationFn: (data: Edge.DtuChannel) => saveDtuChannel(id, data),
        successMessage: '透传配置已保存并提交下发',
        invalidateKeys: [['edge-dtu', id]],
    });
export const useDtuDelete = (id: string) =>
    useMutationWithMessage({
        mutationFn: (channelId: string) => deleteDtuChannel(id, channelId),
        successMessage: '透传通道已删除并提交下发',
        invalidateKeys: [['edge-dtu', id]],
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
