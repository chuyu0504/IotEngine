import request, { refreshSession } from '@/lib/http';
import { createSseSnapshotStream } from '@/lib/snapshot_request';
import { type SnapshotObserver, SnapshotStream } from '@/lib/snapshot_stream';
import { useAuthStore } from '@/store/auth_store';
import type { PaginatedResult } from '@/types/pagination';
import { createUuid } from '@/utils/uuid';
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
import type {
    DebugRequestOptions,
    DebugSubscriptionObserver,
    Edge,
    EdgeVpn,
} from './edge_node.types';

export const getWindowsClientDownloadUrl = () => '/downloads/iot-egine-Setup-x64.exe';
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

export class DebugConnectionError extends Error {
    constructor(
        message: string,
        readonly mayHaveExecuted = false
    ) {
        super(message);
    }
}
export class DebugOperationError extends Error {
    constructor(
        readonly code: number,
        message: string
    ) {
        super(message);
    }
}
interface PendingDebugRequest {
    event: string;
    data: unknown;
    anonymous: boolean;
    sent: boolean;
    resolve: (data: unknown) => void;
    reject: (error: Error) => void;
    dispose: () => void;
}
interface ActiveDebugSubscription {
    event: string;
    data: unknown;
    observer: DebugSubscriptionObserver;
    id?: string;
}

/** Serial and terminal commands share a dedicated connection; writes are never replayed. */
export class EdgeDebugConnection {
    private socket?: WebSocket;
    private closingSocket?: WebSocket;
    private retry?: ReturnType<typeof setTimeout>;
    private handshake?: ReturnType<typeof setTimeout>;
    private ready = false;
    private backoff = 1000;
    private reconnectAttempts = 0;
    private readyAt?: number;
    private restoreSession: () => Promise<void> = async () => {};
    private readonly requests = new Map<string, PendingDebugRequest>();
    private readonly subscriptions = new Set<ActiveDebugSubscription>();
    private readonly subscriptionIds = new Map<string, ActiveDebugSubscription>();

    constructor(private readonly connect: () => WebSocket) {}
    configureSessionRestore(restore: () => Promise<void>) {
        this.restoreSession = restore;
    }

    request<T = unknown>(
        event: string,
        data: unknown = {},
        options: DebugRequestOptions = {}
    ): Promise<T> {
        if (options.signal?.aborted)
            return Promise.reject(new DOMException('Aborted', 'AbortError'));
        const id = createUuid();
        return new Promise<T>((resolve, reject) => {
            const cancel = (error: Error) => {
                const pending = this.requests.get(id);
                if (!pending) return;
                this.requests.delete(id);
                pending.dispose();
                reject(error);
            };
            // Cancelling a local wait cannot undo an already dispatched write.
            const abort = () =>
                cancel(
                    this.requests.get(id)?.sent
                        ? new DebugConnectionError('等待已取消，请确认操作结果后再重试', true)
                        : new DOMException('Aborted', 'AbortError')
                );
            const timer = setTimeout(
                () =>
                    cancel(
                        new DebugConnectionError(
                            '请求超时，请确认操作结果后再重试',
                            this.requests.get(id)?.sent
                        )
                    ),
                options.timeout ?? 30000
            );
            options.signal?.addEventListener('abort', abort, { once: true });
            const pending: PendingDebugRequest = {
                event,
                data,
                anonymous: options.anonymous ?? false,
                sent: false,
                resolve: (value) => resolve(value as T),
                reject,
                dispose: () => {
                    clearTimeout(timer);
                    options.signal?.removeEventListener('abort', abort);
                },
            };
            this.requests.set(id, pending);
            this.ensureSocket();
            this.sendRequest(id, pending);
        });
    }
    subscribe(event: string, data: unknown, observer: DebugSubscriptionObserver): () => void {
        const subscription = { event, data, observer };
        this.subscriptions.add(subscription);
        this.ensureSocket();
        this.sendSubscription(subscription);
        return () => this.release(subscription);
    }
    reset(reason = '登录状态已变更') {
        this.reconnectAttempts = 0;
        this.backoff = 1000;
        this.readyAt = undefined;
        clearTimeout(this.retry);
        clearTimeout(this.handshake);
        this.retry = undefined;
        this.ready = false;
        this.closingSocket = this.socket;
        const subscriptions = [...this.subscriptions];
        this.subscriptions.clear();
        this.subscriptionIds.clear();
        this.rejectRequests(reason);
        for (const { observer } of subscriptions)
            this.notifyError(observer, new DebugConnectionError(reason));
        // Retain the closing socket until its close event; never open a second one.
        this.socket?.close(1000, 'session reset');
    }
    private notifyError(observer: DebugSubscriptionObserver, error: Error) {
        try {
            observer.error(error);
        } catch {
            /* Consumer failures stay isolated. */
        }
    }
    private rejectRequests(reason: string, sentOnly = false) {
        for (const [id, request] of this.requests) {
            if (sentOnly && !request.sent) continue;
            this.requests.delete(id);
            request.dispose();
            request.reject(new DebugConnectionError(reason, request.sent));
        }
    }
    private send(id: string, event: string, data: unknown): boolean {
        if (this.socket?.readyState !== 1 || this.socket === this.closingSocket) return false;
        try {
            this.socket.send(JSON.stringify({ id, event, data }));
            return true;
        } catch {
            this.socket.close();
            return false;
        }
    }
    private sendRequest(id: string, pending: PendingDebugRequest) {
        if (pending.sent || (!this.ready && !pending.anonymous)) return;
        pending.sent = this.send(id, pending.event, pending.data);
    }
    private sendSubscription(subscription: ActiveDebugSubscription) {
        if (!this.ready || subscription.id) return;
        const id = createUuid();
        subscription.id = id;
        this.subscriptionIds.set(id, subscription);
        if (!this.send(id, subscription.event, subscription.data)) {
            subscription.id = undefined;
            this.subscriptionIds.delete(id);
        }
    }
    private release(subscription: ActiveDebugSubscription) {
        if (!this.subscriptions.delete(subscription)) return;
        if (subscription.id) {
            this.subscriptionIds.delete(subscription.id);
            this.send(createUuid(), 'subscription.cancel', { subscriptionId: subscription.id });
            subscription.id = undefined;
        }
    }
    private async opened(socket: WebSocket) {
        if (socket !== this.socket || socket === this.closingSocket) return;
        clearTimeout(this.handshake);
        for (const [id, pending] of this.requests) this.sendRequest(id, pending);
        try {
            await this.restoreSession();
        } catch (error) {
            if (socket !== this.socket || socket === this.closingSocket) return;
            this.rejectRequests('登录状态恢复失败');
            for (const subscription of [...this.subscriptions]) {
                this.release(subscription);
                this.notifyError(
                    subscription.observer,
                    error instanceof Error ? error : new Error('登录状态恢复失败')
                );
            }
            return;
        }
        if (socket !== this.socket || socket === this.closingSocket || socket.readyState !== 1)
            return;
        this.ready = true;
        this.readyAt = Date.now();
        for (const [id, pending] of this.requests) this.sendRequest(id, pending);
        for (const subscription of this.subscriptions) this.sendSubscription(subscription);
    }
    private received(socket: WebSocket, data: unknown) {
        if (socket !== this.socket || socket === this.closingSocket || socket.readyState !== 1)
            return;
        try {
            if (typeof data !== 'string') throw new Error('消息不是 JSON');
            const frame = JSON.parse(data);
            if (
                !frame ||
                typeof frame !== 'object' ||
                Array.isArray(frame) ||
                typeof frame.id !== 'string' ||
                Object.keys(frame).length !== 2 ||
                Object.hasOwn(frame, 'data') === Object.hasOwn(frame, 'error')
            )
                throw new Error('响应格式无效');
            let error: DebugOperationError | undefined;
            if (Object.hasOwn(frame, 'error')) {
                if (
                    !frame.error ||
                    Object.keys(frame.error).length !== 2 ||
                    !Number.isInteger(frame.error.code) ||
                    typeof frame.error.message !== 'string'
                )
                    throw new Error('错误格式无效');
                error = new DebugOperationError(frame.error.code, frame.error.message);
            }
            const pending = this.requests.get(frame.id);
            if (pending) {
                this.requests.delete(frame.id);
                pending.dispose();
                if (error) pending.reject(error);
                else pending.resolve(frame.data);
                return;
            }
            const subscription = this.subscriptionIds.get(frame.id);
            if (!subscription) return;
            if (error) {
                this.release(subscription);
                this.notifyError(subscription.observer, error);
            } else {
                try {
                    subscription.observer.next(frame.data);
                } catch (failure) {
                    this.release(subscription);
                    this.notifyError(
                        subscription.observer,
                        failure instanceof Error ? failure : new Error('订阅数据处理失败')
                    );
                }
            }
        } catch {
            this.rejectRequests('服务响应协议错误');
            socket.close(1002, 'invalid response');
        }
    }
    private ensureSocket() {
        if (this.socket || this.retry) return;
        let socket: WebSocket;
        try {
            socket = this.connect();
        } catch {
            this.rejectRequests('无法建立服务连接');
            this.scheduleReconnect();
            return;
        }
        this.socket = socket;
        this.ready = false;
        this.handshake = setTimeout(() => socket.close(), 15000);
        socket.onopen = () => {
            void this.opened(socket);
        };
        socket.onmessage = (event) => this.received(socket, event.data);
        socket.onerror = () => socket.close();
        socket.onclose = () => {
            if (this.socket !== socket) return;
            clearTimeout(this.handshake);
            this.socket = undefined;
            this.ready = false;
            if (this.readyAt !== undefined && Date.now() - this.readyAt >= 30000) {
                this.reconnectAttempts = 0;
                this.backoff = 1000;
            }
            this.readyAt = undefined;
            const reset = this.closingSocket === socket;
            this.closingSocket = undefined;
            this.subscriptionIds.clear();
            for (const subscription of this.subscriptions) subscription.id = undefined;
            this.rejectRequests('连接已断开；已发送操作的执行结果可能尚未收到', reset);
            if (reset && (this.requests.size || this.subscriptions.size)) this.ensureSocket();
            else this.scheduleReconnect();
        };
    }
    private scheduleReconnect() {
        if (this.retry || (!this.requests.size && !this.subscriptions.size)) return;
        if (this.reconnectAttempts >= 9) {
            this.rejectRequests('调试连接重试已达上限，请重新打开调试窗口');
            for (const subscription of [...this.subscriptions]) {
                this.release(subscription);
                this.notifyError(
                    subscription.observer,
                    new DebugConnectionError('调试连接重试已达上限，请重新打开调试窗口')
                );
            }
            this.reconnectAttempts = 0;
            this.backoff = 1000;
            return;
        }
        ++this.reconnectAttempts;
        this.retry = setTimeout(() => {
            this.retry = undefined;
            this.ensureSocket();
        }, this.backoff);
        this.backoff = Math.min(15000, this.backoff * 2);
    }
}

export const edgeDebugConnection = new EdgeDebugConnection(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return new WebSocket(`${protocol}//${window.location.host}/v1/edge/debug`);
});

interface DebugSession {
    token: string | null;
    userId: string | null;
}
interface DebugSubscription {
    target: string;
    event: string;
    data: unknown;
    observers: Set<SnapshotObserver<unknown>>;
    value?: unknown;
    received: boolean;
    release?: () => void;
}

/** Serial and terminal output subscriptions follow the authenticated debug connection. */
export class DebugSubscriptions {
    private readonly subscriptions = new Map<string, DebugSubscription>();
    private refreshing?: Promise<boolean>;
    private session: DebugSession;
    private readonly channel: EdgeDebugConnection;

    constructor(
        private readonly options: {
            session: () => DebugSession;
            refresh: () => Promise<boolean>;
            channel?: EdgeDebugConnection;
        }
    ) {
        this.session = options.session();
        this.channel = options.channel ?? edgeDebugConnection;
    }

    sessionChanged() {
        const next = this.options.session();
        if (next.token === this.session.token && next.userId === this.session.userId) return;
        const changedAccount = next.userId !== this.session.userId || !next.token;
        this.session = next;
        for (const entry of this.subscriptions.values()) {
            entry.release?.();
            entry.release = undefined;
            entry.received = false;
            entry.value = undefined;
        }
        if (changedAccount) {
            const entries = [...this.subscriptions.values()];
            this.subscriptions.clear();
            this.channel.reset();
            for (const entry of entries)
                for (const observer of [...entry.observers])
                    this.reportError(observer, new Error('登录状态已变更'));
        } else for (const entry of this.subscriptions.values()) this.start(entry);
    }

    create<T>(event: string, data: unknown = {}): SnapshotStream<T> {
        // Capture JSON values now; later caller mutations cannot change the cache key.
        const serialized = JSON.stringify(data, (_key, value) => {
            if (value && typeof value === 'object' && !Array.isArray(value))
                return Object.fromEntries(
                    Object.keys(value)
                        .sort()
                        .map((key) => [key, value[key]])
                );
            return value;
        });
        const parameters = JSON.parse(serialized);
        const target = `${event}:${serialized}`;
        return new SnapshotStream((observer, options) => {
            this.sessionChanged();
            if (!this.session.token || !/^[a-z][a-z0-9_.]{0,127}$/.test(event)) {
                observer.error(new Error('登录状态或查询事件无效'));
                return () => {};
            }
            let entry = this.subscriptions.get(target);
            if (!entry) {
                entry = { target, event, data: parameters, observers: new Set(), received: false };
                this.subscriptions.set(target, entry);
            }
            const subscribed = entry;
            if (options?.fresh && entry.received) {
                entry.release?.();
                entry.release = undefined;
                entry.received = false;
                entry.value = undefined;
            }
            const untyped: SnapshotObserver<unknown> = {
                next: (value) => observer.next(value as T),
                error: (error) => observer.error(error),
            };
            entry.observers.add(untyped);
            if (entry.received) observer.next(entry.value as T);
            queueMicrotask(() => this.start(subscribed));
            return () => {
                subscribed.observers.delete(untyped);
                if (subscribed.observers.size || this.subscriptions.get(target) !== subscribed)
                    return;
                this.subscriptions.delete(target);
                subscribed.release?.();
            };
        });
    }

    private start(entry: DebugSubscription) {
        const token = this.session.token;
        if (!token || entry.release || this.subscriptions.get(entry.target) !== entry) return;
        entry.release = this.channel.subscribe(entry.event, entry.data, {
            next: (data) => {
                if (token !== this.session.token || this.subscriptions.get(entry.target) !== entry)
                    return;
                entry.value = data;
                entry.received = true;
                for (const observer of [...entry.observers]) {
                    try {
                        observer.next(data);
                    } catch (error) {
                        entry.observers.delete(observer);
                        this.reportError(
                            observer,
                            error instanceof Error ? error : new Error('订阅数据处理失败')
                        );
                    }
                }
                if (!entry.observers.size) {
                    this.subscriptions.delete(entry.target);
                    entry.release?.();
                }
            },
            error: (error) => {
                void this.recover(entry, error, token);
            },
        });
    }

    private fail(entry: DebugSubscription, error: Error) {
        entry.release?.();
        entry.release = undefined;
        entry.received = false;
        entry.value = undefined;
        this.subscriptions.delete(entry.target);
        for (const observer of [...entry.observers]) this.reportError(observer, error);
    }

    private reportError(observer: SnapshotObserver<unknown>, error: Error) {
        try {
            observer.error(error);
        } catch {
            /* Keep other consumers active. */
        }
    }

    private async recover(entry: DebugSubscription, error: Error, token: string) {
        if (token !== this.session.token || this.subscriptions.get(entry.target) !== entry) return;
        entry.release = undefined;
        if (error instanceof DebugOperationError && error.code === 11005) {
            this.refreshing ??= Promise.resolve()
                .then(() => this.options.refresh())
                .then((success) => {
                    this.sessionChanged();
                    return success;
                })
                .catch(() => false)
                .finally(() => {
                    this.refreshing = undefined;
                });
            if (!(await this.refreshing) && token === this.session.token)
                this.fail(entry, new Error('登录状态已失效'));
            else if (this.subscriptions.get(entry.target) === entry) this.start(entry);
            return;
        }
        this.fail(entry, error);
    }
}

const subscriptions = new DebugSubscriptions({
    session: () => {
        const { token, user } = useAuthStore.getState();
        return { token, userId: user?.id ?? null };
    },
    refresh: refreshSession,
});
useAuthStore.subscribe(() => subscriptions.sessionChanged());

function createDebugSnapshotStream<T>(event: string, data: unknown = {}) {
    return subscriptions.create<T>(event, data);
}

export const getDtuChannels = (id: string, signal?: AbortSignal) =>
    request.get<Edge.DtuChannels>(`${edgePath(id)}/dtu`, { signal });
export const observeDtuChannels = (scope: Edge.EventScope) =>
    createSseSnapshotStream<Edge.DtuChannels>('/v1/edge/events', eventParams(scope), 'dtu');
export const saveDtuChannel = (id: string, data: Edge.DtuChannel) =>
    request.put<void>(`${edgePath(id)}/dtu`, dtuChannelSchema.parse(data));
export const deleteDtuChannel = (id: string, channelId: string) =>
    request.delete<void>(`${edgePath(id)}/dtu/${edgeIdSchema.parse(channelId)}`);
