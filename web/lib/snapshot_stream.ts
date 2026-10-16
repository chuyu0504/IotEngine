/** Entity change of a collection snapshot: rows (keyed by `id`) to replace and ids to drop. */
export interface SnapshotDelta {
    upsert: readonly unknown[];
    remove: readonly string[];
}

export interface SnapshotObserver<T> {
    next: (value: T) => void;
    error: (error: Error) => void;
    /**
     * Receives entity deltas with the merged snapshot instead of `next`; observers without it
     * are given the merged snapshot through `next`.
     */
    delta?: (delta: SnapshotDelta, value: T) => void;
}

export interface SnapshotReadOptions {
    fresh?: boolean;
}

/**
 * Applies an entity delta to a row array or a `{ list, total }` page. Rows the delta does not
 * name keep their identity; an unchanged snapshot is returned as is. Upserts of ids that are not
 * present are ignored because the server sends a full snapshot whenever membership grows.
 */
export function applySnapshotDelta<T>(value: T, delta: SnapshotDelta): T {
    if (Array.isArray(value)) return mergeRows(value, delta) as T;
    if (typeof value === 'object' && value !== null && 'list' in value) {
        const page = value as { list: unknown; total?: unknown };
        if (Array.isArray(page.list)) {
            const list = mergeRows(page.list, delta);
            if (list === page.list) return value;
            const removed = page.list.length - list.length;
            return {
                ...page,
                list,
                ...(typeof page.total === 'number'
                    ? { total: Math.max(0, page.total - removed) }
                    : {}),
            } as T;
        }
    }
    throw new Error('实时增量不适用于当前快照');
}

function mergeRows(rows: readonly unknown[], delta: SnapshotDelta): readonly unknown[] {
    const upserts = new Map<string, unknown>();
    for (const row of delta.upsert) upserts.set(rowId(row), row);
    const removed = new Set(delta.remove);
    let changed = false;
    const merged: unknown[] = [];
    for (const row of rows) {
        const id = rowId(row);
        if (removed.has(id)) {
            changed = true;
            continue;
        }
        const replacement = upserts.get(id);
        if (replacement === undefined || replacement === row) merged.push(row);
        else {
            changed = true;
            merged.push(replacement);
        }
    }
    return changed ? merged : rows;
}

function rowId(row: unknown): string {
    if (typeof row === 'object' && row !== null && 'id' in row && typeof row.id === 'string')
        return row.id;
    throw new Error('实时增量行缺少 id');
}

/** A lazy live query. Awaiting it reads its first authorized snapshot. */
export class SnapshotStream<T> implements PromiseLike<T> {
    constructor(
        readonly subscribe: (
            observer: SnapshotObserver<T>,
            options?: SnapshotReadOptions
        ) => () => void
    ) {}

    static value<T>(value: T): SnapshotStream<T> {
        return new SnapshotStream((observer) => {
            observer.next(value);
            return () => {};
        });
    }

    static combine<T extends readonly unknown[]>(
        sources: { [K in keyof T]: SnapshotStream<T[K]> }
    ): SnapshotStream<T> {
        return new SnapshotStream((observer, options) => {
            const values: unknown[] = new Array(sources.length);
            const ready = new Set<number>();
            const releases = sources.map((source, index) =>
                source.subscribe(
                    {
                        next: (value) => {
                            values[index] = value;
                            ready.add(index);
                            if (ready.size === sources.length)
                                observer.next([...values] as unknown as T);
                        },
                        error: observer.error,
                    },
                    options
                )
            );
            return () => {
                for (const release of releases) release();
            };
        });
    }

    switchMap<U>(transform: (value: T) => SnapshotStream<U>): SnapshotStream<U> {
        return new SnapshotStream((observer, options) => {
            let releaseChild: (() => void) | undefined;
            const release = this.subscribe(
                {
                    next: (value) => {
                        releaseChild?.();
                        releaseChild = undefined;
                        try {
                            releaseChild = transform(value).subscribe(observer, options);
                        } catch (error) {
                            observer.error(
                                error instanceof Error ? error : new Error(String(error))
                            );
                        }
                    },
                    error: observer.error,
                },
                options
            );
            return () => {
                release();
                releaseChild?.();
            };
        });
    }

    map<U>(transform: (value: T) => U): SnapshotStream<U> {
        return new SnapshotStream((observer, options) =>
            this.subscribe(
                {
                    next: (value) => {
                        try {
                            observer.next(transform(value));
                        } catch (error) {
                            observer.error(
                                error instanceof Error ? error : new Error(String(error))
                            );
                        }
                    },
                    error: observer.error,
                },
                options
            )
        );
    }

    filter(predicate: (value: T) => boolean): SnapshotStream<T> {
        return new SnapshotStream((observer, options) =>
            this.subscribe(
                {
                    next: (value) => {
                        try {
                            if (predicate(value)) observer.next(value);
                        } catch (error) {
                            observer.error(
                                error instanceof Error ? error : new Error(String(error))
                            );
                        }
                    },
                    error: observer.error,
                },
                options
            )
        );
    }

    first(signal?: AbortSignal, options?: SnapshotReadOptions): Promise<T> {
        return new Promise((resolve, reject) => {
            let release: (() => void) | undefined;
            let finished = false;
            const cleanup = () => {
                finished = true;
                release?.();
                signal?.removeEventListener('abort', abort);
            };
            const abort = () => {
                cleanup();
                reject(signal?.reason ?? new DOMException('Aborted', 'AbortError'));
            };
            if (signal?.aborted) {
                abort();
                return;
            }
            signal?.addEventListener('abort', abort, { once: true });
            release = this.subscribe(
                {
                    next: (value) => {
                        cleanup();
                        resolve(value);
                    },
                    error: (error) => {
                        cleanup();
                        reject(error);
                    },
                },
                options
            );
            if (finished) release();
        });
    }

    // biome-ignore lint/suspicious/noThenProperty: Implements PromiseLike so awaiting a resource reads and releases its first snapshot.
    then<TResult1 = T, TResult2 = never>(
        fulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
        rejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null
    ): Promise<TResult1 | TResult2> {
        return this.first().then(fulfilled, rejected);
    }
}
