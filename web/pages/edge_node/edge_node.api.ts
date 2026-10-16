import {
    type DebugRequestOptions,
    DebugTransport,
    debugTarget,
    EdgeDebugConnection,
} from '@/lib/edge_debug_connection';
import request from '@/lib/http';
import { createAuthLiveSubscriptions, createSseSnapshotStream } from '@/lib/snapshot_request';
import type { PaginatedResult } from '@/types/pagination';
import {
    dtuChannelSchema,
    edgeGroupSchema,
    edgeIdSchema,
    edgeListQuerySchema,
    firmwareUpgradeSchema,
    logLevelSchema,
    logsQuerySchema,
    networkSchema,
    nodeGroupSchema,
    nodeNameSchema,
    serialDebugOpenSchema,
    terminalSessionSchema,
    terminalSizeSchema,
} from './edge_node.schema';
import type { Edge, EdgeVpn } from './edge_node.types';

/** PLMN code → operator name table, served as a static asset from `web/public`. */
export async function getPlmnNames(signal?: AbortSignal): Promise<Record<string, string>> {
    const response = await fetch('/plmn_names.json', { signal });
    if (!response.ok) throw new Error('运营商名称表加载失败');
    return response.json();
}

const edgePath = (id: string) => `/v1/edge/${edgeIdSchema.parse(id)}`;
const eventParams = (scope?: Edge.EventScope) => ({
    nodeId: scope?.nodeId ? edgeIdSchema.parse(scope.nodeId) : undefined,
    logs: scope?.logs ? true : undefined,
    vpn: scope?.vpn ? true : undefined,
    dtu: scope?.dtu ? true : undefined,
    ...(scope?.logs ? logsQuerySchema.parse(scope.logs) : {}),
});
export const getEdgeInventory = (scope?: Edge.EventScope) =>
    createSseSnapshotStream<Edge.Node[]>('/v1/edge/events', eventParams(scope), 'nodes');
export const observeEdgeDetail = (scope: Edge.EventScope) =>
    createSseSnapshotStream<Edge.Node>('/v1/edge/events', eventParams(scope), 'detail');
export const queryEdgeList = (query?: Edge.Query, signal?: AbortSignal) =>
    request.get<PaginatedResult<Edge.Node>>('/v1/edge', {
        params: edgeListQuerySchema.parse(query ?? {}),
        signal,
    });
export const getEdgeDetail = (id: string, signal?: AbortSignal) =>
    request.get<Edge.Node>(edgePath(id), { signal });
export const getEdgeGroups = (signal?: AbortSignal) =>
    request.get<Edge.GroupItem[]>('/v1/edge/groups', { signal });
export const createEdgeGroup = (data: Edge.GroupSaveDto) =>
    request.post<void>('/v1/edge/groups', edgeGroupSchema.parse(data));
export const updateEdgeGroup = (id: string, data: Edge.GroupSaveDto) =>
    request.put<void>(`/v1/edge/groups/${edgeIdSchema.parse(id)}`, edgeGroupSchema.parse(data));
export const deleteEdgeGroup = (id: string) =>
    request.delete<void>(`/v1/edge/groups/${edgeIdSchema.parse(id)}`);
export const getLogs = (scope: Edge.EventScope) =>
    createSseSnapshotStream<Edge.Logs>('/v1/edge/events', eventParams(scope), 'logs');
export const captureLogs = (id: string) => request.post<void>(`${edgePath(id)}/logs/capture`);
export const setLogLevel = (id: string, data: Edge.LogLevelDto) =>
    request.put<void>(`${edgePath(id)}/logs/level`, logLevelSchema.parse(data));
export const setEnrollment = (id: string, status: 'approved', name?: string) =>
    request.put<void>(`${edgePath(id)}/enrollment`, { status, name });
export const deleteEnrollment = (id: string) => request.delete<void>(edgePath(id));
export const renameEdge = (id: string, data: Edge.NameDto) =>
    request.put<void>(`${edgePath(id)}/name`, nodeNameSchema.parse(data));
export const setEdgeGroup = (id: string, data: Edge.GroupDto) =>
    request.put<void>(`${edgePath(id)}/group`, nodeGroupSchema.parse(data));
export const configureNetwork = (id: string, data: Edge.NetworkDto) =>
    request.post<void>(`${edgePath(id)}/network`, networkSchema.parse(data));
export const syncDeviceConfig = (id: string) => request.post<void>(`${edgePath(id)}/sync`);
export const reuseFirmware = (
    id: string,
    sha256: string,
    sizeBytes: number,
    keepSettings: boolean
) =>
    request.post<Edge.FirmwareReuseResult>(`${edgePath(id)}/firmware/reuse`, {
        sha256,
        sizeBytes,
        keepSettings,
    });

export const uploadFirmware = async (
    id: string,
    data: Edge.FirmwareUpgradeDto,
    onProgress?: (progress: Edge.FirmwareUploadProgress) => void
): Promise<void> => {
    const value = firmwareUpgradeSchema.parse(data);
    await request.post<void>(`${edgePath(id)}/firmware`, value.file, {
        params: {
            fileName: value.file.name,
            sizeBytes: value.file.size,
            keepSettings: value.keepSettings,
        },
        headers: { 'Content-Type': 'application/octet-stream' },
        timeout: 600000,
        onUploadProgress: (loadedBytes, totalBytes) =>
            onProgress?.({
                loadedBytes,
                totalBytes,
                percent: totalBytes ? Math.round((loadedBytes * 100) / totalBytes) : 0,
            }),
    });
};
export const openSerialDebug = (id: string, path: string) =>
    requestDebugOperation<{ id: string }>('edge.serial.open', {
        ...serialDebugOpenSchema.parse({ path }),
        id: edgeIdSchema.parse(id),
    });

const nodeSession = (id: string, sessionId: string) => ({
    id: edgeIdSchema.parse(id),
    sessionId: edgeIdSchema.parse(sessionId),
});
export const getSerialEvents = (id: string, sessionId: string) =>
    createDebugSnapshotStream<{ events: unknown[]; cursor: number }>(
        'edge.serial.events.subscribe',
        nodeSession(id, sessionId)
    );
export const sendSerialCommand = (
    id: string,
    sessionId: string,
    command: Record<string, unknown>
) =>
    requestDebugOperation(
        'edge.serial.command',
        { ...nodeSession(id, sessionId), command },
        { _silent: true, timeout: 10000 }
    );
export const closeSerialDebug = (id: string, sessionId: string) =>
    requestDebugOperation('edge.serial.close', nodeSession(id, sessionId), {
        _silent: true,
        timeout: 5000,
    });

export const openTerminal = async (id: string, columns: number, rows: number) =>
    terminalSessionSchema.parse(
        await requestDebugOperation('edge.terminal.open', {
            id: edgeIdSchema.parse(id),
            ...terminalSizeSchema.parse({ columns, rows }),
        })
    );
export const getTerminalEvents = (id: string, sessionId: string) =>
    createDebugSnapshotStream<unknown>(
        'edge.terminal.events.subscribe',
        nodeSession(id, sessionId)
    );
export const writeTerminal = (id: string, sessionId: string, bytes: Uint8Array) => {
    if (bytes.length === 0 || bytes.length > 16384)
        throw new Error('终端单次输入必须为 1–16384 字节');
    return requestDebugOperation(
        'edge.terminal.write',
        {
            ...nodeSession(id, sessionId),
            content: btoa(String.fromCharCode(...bytes)),
        },
        { _silent: true, timeout: 10000 }
    );
};
export const resizeTerminal = (id: string, sessionId: string, columns: number, rows: number) =>
    requestDebugOperation(
        'edge.terminal.resize',
        {
            ...nodeSession(id, sessionId),
            ...terminalSizeSchema.parse({ columns, rows }),
        },
        { _silent: true, timeout: 10000 }
    );
export const acknowledgeTerminalOutput = (id: string, sessionId: string, sequence: number) =>
    requestDebugOperation(
        'edge.terminal.output.ack',
        { ...nodeSession(id, sessionId), sequence },
        { _silent: true, timeout: 10000 }
    );
export const keepTerminalAlive = (id: string, sessionId: string) =>
    requestDebugOperation('edge.terminal.keepalive', nodeSession(id, sessionId), {
        _silent: true,
        timeout: 10000,
    });
export const closeTerminal = (id: string, sessionId: string) =>
    requestDebugOperation('edge.terminal.close', nodeSession(id, sessionId), {
        _silent: true,
        timeout: 5000,
    });

export const getVpnNetworks = (signal?: AbortSignal) =>
    request.get<PaginatedResult<EdgeVpn.Network>>('/v1/vpn/networks', {
        params: { page: 1, pageSize: 100, status: 'enabled' },
        signal,
    });
export const getEdgeVpnState = (scope: Edge.EventScope) =>
    createSseSnapshotStream<Pick<EdgeVpn.Overview, 'peers' | 'routes'>>(
        '/v1/edge/events',
        eventParams(scope),
        'vpn'
    );
export const createEdgeVpnPeer = (data: EdgeVpn.PeerCreateDto) =>
    request.post<{ id: string }>('/v1/vpn/peers', data);
export const syncEdgeVpnPeer = (peerId: string) =>
    request.post<void>(`/v1/vpn/peers/${edgeIdSchema.parse(peerId)}/sync`);
export const revokeEdgeVpnPeer = (peerId: string) =>
    request.post<void>(`/v1/vpn/peers/${edgeIdSchema.parse(peerId)}/revoke`);
export const updateEdgeVpnRoute = (routeId: string, data: Partial<EdgeVpn.RouteDto>) =>
    request.patch<void>(`/v1/vpn/routes/${edgeIdSchema.parse(routeId)}`, data);

export function authenticateDebugConnection(token: string) {
    return requestDebugOperation<void>(
        'edge.debug.authenticate',
        { token },
        { anonymous: true, _silent: true }
    );
}

let reportDebugError: ((message: string) => void) | undefined;

export function configureDebugErrorNotification(report: (message: string) => void) {
    reportDebugError = report;
}

async function requestDebugOperation<T = unknown>(
    event: string,
    data: unknown = {},
    options: DebugRequestOptions & { _silent?: boolean } = {}
): Promise<T> {
    try {
        return await edgeDebugConnection.request<T>(event, data, options);
    } catch (error) {
        if (!options._silent && !(error instanceof DOMException && error.name === 'AbortError'))
            reportDebugError?.(error instanceof Error ? error.message : '请求失败');
        throw error;
    }
}

export const edgeDebugConnection = new EdgeDebugConnection(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return new WebSocket(`${protocol}//${window.location.host}/v1/edge/debug`);
});
const debugSubscriptions = createAuthLiveSubscriptions(new DebugTransport(edgeDebugConnection));

function createDebugSnapshotStream<T>(event: string, data: unknown = {}) {
    return debugSubscriptions.create<T>(debugTarget(event, data), event);
}

export const observeDtuChannels = (scope: Edge.EventScope) =>
    createSseSnapshotStream<Edge.DtuChannels>('/v1/edge/events', eventParams(scope), 'dtu');
export const saveDtuChannel = (id: string, data: Edge.DtuChannel) =>
    request.put<void>(`${edgePath(id)}/dtu`, dtuChannelSchema.parse(data));
export const deleteDtuChannel = (id: string, channelId: string) =>
    request.delete<void>(`${edgePath(id)}/dtu/${edgeIdSchema.parse(channelId)}`);
