import { createUuid } from '@/utils/uuid';
import type { LiveSink, LiveTransport } from './live_subscriptions';

export interface DebugRequestOptions {
    signal?: AbortSignal;
    timeout?: number;
    anonymous?: boolean;
}
export interface DebugSubscriptionObserver {
    next: (data: unknown) => void;
    error: (error: Error) => void;
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

/** A debug subscription event with its parameters captured as canonical JSON. */
export interface DebugTarget {
    event: string;
    data: unknown;
}

export function debugTarget(event: string, data: unknown = {}): DebugTarget {
    // Later caller mutations cannot change the captured parameters or their source key.
    const serialized = JSON.stringify(data, (_key, value) => {
        if (value && typeof value === 'object' && !Array.isArray(value))
            return Object.fromEntries(
                Object.keys(value)
                    .sort()
                    .map((key) => [key, value[key]])
            );
        return value;
    });
    return { event, data: JSON.parse(serialized) };
}

/** Each debug subscription is one source whose only channel is its event name. */
export class DebugTransport implements LiveTransport<DebugTarget> {
    constructor(private readonly connection: EdgeDebugConnection) {}

    key(target: DebugTarget) {
        return /^[a-z][a-z0-9_.]{0,127}$/.test(target.event)
            ? `${target.event}:${JSON.stringify(target.data)}`
            : undefined;
    }

    open(target: DebugTarget, _context: unknown, sink: LiveSink) {
        return this.connection.subscribe(target.event, target.data, {
            next: (data) => sink.next(target.event, data),
            error: (error) =>
                sink.fail(error, error instanceof DebugOperationError && error.code === 11005),
        });
    }

    /** The authenticated socket belongs to the previous account. */
    accountChanged() {
        this.connection.reset();
    }
}
