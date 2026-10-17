import {
    applySnapshotDelta,
    type SnapshotDelta,
    type SnapshotObserver,
    SnapshotStream,
} from './snapshot_stream';

export interface LiveSession {
    token: string | null;
    userId: string | null;
}

/** Credentials and options a transport uses for one opened source. */
export interface LiveContext {
    token: string;
    /** Channel every source must also carry for `createShared` observers. */
    shared?: string;
}

/** Delivery callbacks of one opened source; calls after it was closed are ignored. */
export interface LiveSink {
    next(channel: string, value: unknown): void;
    /** Entity delta against the channel's latest full value. */
    delta(channel: string, delta: SnapshotDelta): void;
    /** One channel was rejected; the other channels of the source stay live. */
    reject(channel: string, error: Error): void;
    /** Transport is disconnected, but its bounded automatic retry is still active. */
    interrupted(error: Error): void;
    /** The source ended. `unauthorized` asks the registry to refresh the session once. */
    fail(error: Error, unauthorized?: boolean): void;
}

/** Wire behind the registry: SSE over fetch or the dedicated debug WebSocket. */
export interface LiveTransport<Target> {
    /** Source identity; targets with the same key share one source. `undefined` rejects the target. */
    key(target: Target): string | undefined;
    /** Opens a source; the returned function closes it. */
    open(target: Target, context: LiveContext, sink: LiveSink): () => void;
    /** The signed-in account was replaced or cleared. */
    accountChanged?(): void;
}

interface Run {
    live: boolean;
    close: () => void;
}
interface Entry<Target> {
    key: string;
    target: Target;
    observers: Map<SnapshotObserver<unknown>, string>;
    run?: Run;
    failure?: Error;
    connectionError?: Error;
    /** Latest full value per channel; deltas are merged into it for late readers. */
    values: Map<string, unknown>;
    errors: Map<string, Error>;
    /** A session refresh was spent since the last delivered value. */
    refreshed: boolean;
    /** Waiting for that refresh; the source stays closed until it settles. */
    refreshing: boolean;
}

const ACCOUNT_CHANGED = '登录状态已变更';

/** Live sources shared by key, scoped to the current account and to actual observers. */
export class LiveSubscriptions<Target> {
    private readonly entries = new Map<string, Entry<Target>>();
    private session: LiveSession;
    private refreshing?: Promise<boolean>;
    private sharedChannel?: string;
    private readonly sharedObservers = new Set<SnapshotObserver<unknown>>();
    private sharedValue: unknown;
    private sharedReceived = false;
    private sharedError?: Error;
    private sharedConnectionError: Error | null = null;

    constructor(
        private readonly options: {
            session: () => LiveSession;
            refresh: () => Promise<boolean>;
            transport: LiveTransport<Target>;
        }
    ) {
        this.session = options.session();
    }

    sessionChanged() {
        const next = this.options.session();
        if (next.token === this.session.token && next.userId === this.session.userId) return;
        const changedAccount = !next.token || next.userId !== this.session.userId;
        this.session = next;
        this.sharedReceived = false;
        this.sharedValue = undefined;
        this.sharedError = undefined;
        if (changedAccount) {
            for (const observer of this.sharedObservers)
                reportError(observer, new Error(ACCOUNT_CHANGED));
            this.sharedObservers.clear();
        }
        const entries = [...this.entries.values()];
        for (const entry of entries) {
            this.close(entry);
            entry.values.clear();
            entry.errors.clear();
        }
        if (changedAccount) this.options.transport.accountChanged?.();
        for (const entry of entries) {
            if (changedAccount) {
                this.fail(entry, new Error(ACCOUNT_CHANGED));
                if (this.entries.get(entry.key) === entry) this.entries.delete(entry.key);
                entry.observers.clear();
            } else {
                entry.failure = undefined;
                this.start(entry);
            }
        }
        queueMicrotask(() => this.syncShared());
    }

    /** A channel carried by whichever source is live, such as the signed-in user profile. */
    createShared<T>(channel: string): SnapshotStream<T> {
        if (this.sharedChannel && this.sharedChannel !== channel)
            throw new Error('共享订阅来源不能重复配置');
        if (!this.sharedChannel) {
            this.sharedChannel = channel;
            // Open sources must reconnect to carry the shared channel.
            for (const entry of this.entries.values()) {
                this.close(entry);
                entry.values.clear();
                entry.errors.clear();
                queueMicrotask(() => this.start(entry));
            }
        }
        return new SnapshotStream((observer, options) => {
            this.sessionChanged();
            if (!this.session.token) {
                observer.error(new Error('登录状态无效'));
                return () => {};
            }
            const untyped = untypedObserver(observer);
            this.sharedObservers.add(untyped);
            const primary = this.connectionSource();
            const awaitingRecovery = !!(options?.fresh && primary?.connectionError);
            if (primary && options?.fresh && primary.connectionError) {
                this.close(primary);
                primary.failure = undefined;
                queueMicrotask(() => this.start(primary));
            }
            reportConnectionError(untyped, primary?.connectionError ?? null);
            if (this.sharedError) observer.error(this.sharedError);
            else if (this.sharedReceived && !awaitingRecovery) observer.next(this.sharedValue as T);
            queueMicrotask(() => this.syncShared());
            return () => {
                this.sharedObservers.delete(untyped);
                queueMicrotask(() => this.syncShared());
            };
        });
    }

    /**
     * One channel of the source `target` identifies. A fresh read reconnects a failed or
     * interrupted source or channel; a healthy source is current because every change is pushed.
     */
    create<T>(target: Target, channel: string): SnapshotStream<T> {
        const key = this.options.transport.key(target);
        return new SnapshotStream((observer, options) => {
            this.sessionChanged();
            if (!this.session.token || key === undefined) {
                observer.error(new Error('登录状态或订阅目标无效'));
                return () => {};
            }
            let entry = this.entries.get(key);
            if (!entry) {
                entry = {
                    key,
                    target,
                    observers: new Map(),
                    values: new Map(),
                    errors: new Map(),
                    refreshed: false,
                    refreshing: false,
                };
                this.entries.set(key, entry);
            }
            const subscribed = entry;
            const awaitingRecovery = !!(options?.fresh && entry.connectionError);
            if (
                options?.fresh &&
                (entry.failure || entry.connectionError || entry.errors.has(channel))
            ) {
                this.close(entry);
                if (!entry.connectionError || entry.failure || entry.errors.has(channel)) {
                    entry.values.clear();
                    entry.errors.clear();
                }
                entry.failure = undefined;
            }
            const untyped = untypedObserver(observer);
            entry.observers.set(untyped, channel);
            reportConnectionError(untyped, entry.connectionError ?? null);
            const error = entry.failure ?? entry.errors.get(channel);
            if (error) observer.error(error);
            else if (entry.values.has(channel) && !awaitingRecovery)
                observer.next(entry.values.get(channel) as T);
            queueMicrotask(() => {
                this.syncShared();
                this.start(subscribed);
            });
            return () => {
                subscribed.observers.delete(untyped);
                if (subscribed.observers.size || this.entries.get(key) !== subscribed) return;
                this.entries.delete(key);
                this.close(subscribed);
                queueMicrotask(() => this.syncShared());
            };
        });
    }

    private start(entry: Entry<Target>) {
        const token = this.session.token;
        if (
            !token ||
            entry.run ||
            entry.refreshing ||
            entry.failure ||
            this.entries.get(entry.key) !== entry ||
            !entry.observers.size
        )
            return;
        const run: Run = { live: true, close: () => {} };
        entry.run = run;
        const current = () => run.live && entry.run === run;
        run.close = this.options.transport.open(
            entry.target,
            { token, shared: this.sharedChannel },
            {
                next: (channel, value) => {
                    if (current()) this.deliver(entry, channel, value);
                },
                delta: (channel, delta) => {
                    if (current()) this.applyDelta(entry, channel, delta);
                },
                reject: (channel, error) => {
                    if (current()) this.reject(entry, channel, error);
                },
                interrupted: (error) => {
                    if (current()) this.setConnectionError(entry, error);
                },
                fail: (error, unauthorized) => {
                    if (!current()) return;
                    this.close(entry);
                    if (unauthorized && !entry.refreshed) void this.refresh(entry, error, token);
                    else this.fail(entry, error);
                },
            }
        );
        if (!run.live) run.close();
    }

    private close(entry: Entry<Target>) {
        const run = entry.run;
        entry.run = undefined;
        if (!run) return;
        run.live = false;
        run.close();
    }

    private deliver(entry: Entry<Target>, channel: string, value: unknown, delta?: SnapshotDelta) {
        if (!delta) this.setConnectionError(entry, null);
        entry.refreshed = false;
        entry.errors.delete(channel);
        entry.values.set(channel, value);
        if (channel === this.sharedChannel && entry === this.primary()) this.publishShared(value);
        for (const [observer, name] of [...entry.observers]) {
            if (name !== channel) continue;
            try {
                if (delta && observer.delta) observer.delta(delta, value);
                else observer.next(value);
            } catch (error) {
                entry.observers.delete(observer);
                reportError(observer, error instanceof Error ? error : new Error(String(error)));
            }
        }
        if (!entry.observers.size && this.entries.get(entry.key) === entry) {
            this.entries.delete(entry.key);
            this.close(entry);
            queueMicrotask(() => this.syncShared());
        }
    }

    private applyDelta(entry: Entry<Target>, channel: string, delta: SnapshotDelta) {
        if (!entry.values.has(channel)) {
            this.reject(entry, channel, new Error('实时增量缺少基准快照'));
            return;
        }
        const previous = entry.values.get(channel);
        let value: unknown;
        try {
            value = applySnapshotDelta(previous, delta);
        } catch (error) {
            this.reject(entry, channel, error instanceof Error ? error : new Error(String(error)));
            return;
        }
        entry.refreshed = false;
        if (value !== previous) this.deliver(entry, channel, value, delta);
    }

    private reject(entry: Entry<Target>, channel: string, error: Error) {
        entry.values.delete(channel);
        entry.errors.set(channel, error);
        if (channel === this.sharedChannel && entry === this.primary())
            this.publishSharedError(error);
        for (const [observer, name] of [...entry.observers])
            if (name === channel) reportError(observer, error);
    }

    private async refresh(entry: Entry<Target>, error: Error, token: string) {
        entry.refreshed = true;
        this.refreshing ??= this.options
            .refresh()
            .catch(() => false)
            .finally(() => {
                this.refreshing = undefined;
            });
        entry.refreshing = true;
        const refreshed = await this.refreshing;
        entry.refreshing = false;
        this.sessionChanged();
        if (this.entries.get(entry.key) !== entry) return;
        if (!refreshed && this.session.token === token) this.fail(entry, error);
        else this.start(entry);
    }

    private fail(entry: Entry<Target>, error: Error) {
        if (this.entries.get(entry.key) !== entry) return;
        this.close(entry);
        entry.failure = error;
        entry.values.clear();
        entry.errors.clear();
        for (const observer of [...entry.observers.keys()]) reportError(observer, error);
        queueMicrotask(() => this.syncShared());
    }

    private primary() {
        for (const entry of this.entries.values())
            if (entry.observers.size && !entry.failure) return entry;
        return undefined;
    }

    /** Keep a terminally interrupted source retryable without publishing its business failure. */
    private connectionSource() {
        const primary = this.primary();
        if (primary) return primary;
        for (const entry of this.entries.values())
            if (entry.observers.size && entry.connectionError) return entry;
        return undefined;
    }

    private setConnectionError(entry: Entry<Target>, error: Error | null) {
        if ((entry.connectionError ?? null) === error) return;
        entry.connectionError = error ?? undefined;
        for (const observer of [...entry.observers.keys()]) reportConnectionError(observer, error);
        if (entry === this.primary()) this.publishSharedConnectionError(error);
    }

    private publishSharedConnectionError(error: Error | null) {
        if (this.sharedConnectionError === error) return;
        this.sharedConnectionError = error;
        for (const observer of [...this.sharedObservers]) reportConnectionError(observer, error);
    }

    private syncShared() {
        if (!this.sharedChannel) return;
        const primary = this.primary();
        this.publishSharedConnectionError(this.connectionSource()?.connectionError ?? null);
        if (!primary) {
            this.sharedReceived = false;
            this.sharedValue = undefined;
            this.sharedError = undefined;
            return;
        }
        if (!this.sharedObservers.size || !this.session.token) return;
        const error = primary.errors.get(this.sharedChannel);
        if (error) this.publishSharedError(error);
        else if (primary.values.has(this.sharedChannel))
            this.publishShared(primary.values.get(this.sharedChannel));
    }

    private publishShared(value: unknown) {
        if (this.sharedReceived && !this.sharedError && this.sharedValue === value) return;
        this.sharedValue = value;
        this.sharedReceived = true;
        this.sharedError = undefined;
        for (const observer of [...this.sharedObservers]) {
            try {
                observer.next(value);
            } catch (error) {
                reportError(observer, error instanceof Error ? error : new Error(String(error)));
            }
        }
    }

    private publishSharedError(error: Error) {
        if (this.sharedError === error) return;
        this.sharedError = error;
        this.sharedReceived = false;
        this.sharedValue = undefined;
        for (const observer of [...this.sharedObservers]) reportError(observer, error);
    }
}

function untypedObserver<T>(observer: SnapshotObserver<T>): SnapshotObserver<unknown> {
    const delta = observer.delta;
    return {
        next: (value) => observer.next(value as T),
        error: (error) => observer.error(error),
        connectionError: observer.connectionError,
        ...(delta
            ? { delta: (change: SnapshotDelta, value: unknown) => delta(change, value as T) }
            : {}),
    };
}

function reportError(observer: SnapshotObserver<unknown>, error: Error) {
    try {
        observer.error(error);
    } catch {
        /* Other subscribers remain independent. */
    }
}

function reportConnectionError(observer: SnapshotObserver<unknown>, error: Error | null) {
    try {
        observer.connectionError?.(error);
    } catch {
        /* Connection-state observers remain independent of business delivery. */
    }
}
