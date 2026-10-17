import { describe, expect, test } from 'bun:test';
import { SnapshotStream, type SnapshotObserver } from '../web/lib/snapshot_stream';

describe('SSE query resource ownership', () => {
    test('command completion keeps one subscription until a terminal snapshot', async () => {
        let target!: SnapshotObserver<{complete:boolean}>;
        let released=0;
        const source=new SnapshotStream<{complete:boolean}>(observer=>{
            target=observer;return ()=>{released++;};
        });
        const result=source.filter(value=>value.complete).first();
        target.next({complete:false});
        expect(released).toBe(0);
        target.next({complete:true});
        expect(await result).toEqual({complete:true});
        expect(released).toBe(1);
    });
    test('one-shot snapshot unsubscribes even when delivered synchronously', async () => {
        let released = 0;
        const source = new SnapshotStream<number>((observer) => {
            observer.next(42);
            return () => { released++; };
        });
        expect(await source).toBe(42);
        expect(released).toBe(1);
    });

    test('mapping applies to every live snapshot and keeps cleanup ownership', () => {
        let observer!: SnapshotObserver<number>;
        let released = 0;
        const source = new SnapshotStream<number>((sink) => {
            observer = sink;
            return () => { released++; };
        });
        const snapshots: number[] = [];
        const release = source.map((value) => value * 2).subscribe({
            next: (value) => snapshots.push(value), error: (error) => { throw error; },
        });
        observer.next(2);
        observer.next(3);
        expect(snapshots).toEqual([4, 6]);
        release();
        expect(released).toBe(1);
    });

    test('mapped streams receive complete snapshots instead of raw entity deltas', () => {
        let observer!: SnapshotObserver<Array<{ id: string }>>;
        const source = new SnapshotStream<Array<{ id: string }>>((sink) => {
            observer = sink;
            return () => {};
        });
        const counts: number[] = [];
        source.map((rows) => rows.length).subscribe({
            next: (value) => counts.push(value),
            delta: () => { throw new Error('mapped values cannot apply row deltas'); },
            error: (error) => { throw error; },
        });
        expect(observer.delta).toBeUndefined();
        observer.next([{ id: 'a' }, { id: 'b' }]);
        expect(counts).toEqual([2]);
    });

    test('aborting a pending first snapshot releases its subscription', async () => {
        let released = 0;
        const source = new SnapshotStream<number>(() => () => { released++; });
        const abort = new AbortController();
        const result = source.first(abort.signal);
        abort.abort(new Error('cancelled'));
        await expect(result).rejects.toThrow('cancelled');
        expect(released).toBe(1);
    });

    test('schema errors propagate rather than retaining a silently invalid snapshot', () => {
        const source = new SnapshotStream<number>((observer) => {
            observer.next(1);
            return () => {};
        });
        const errors: string[] = [];
        source.map(() => { throw new Error('invalid point'); }).subscribe({
            next: () => { throw new Error('unexpected value'); },
            error: (error) => errors.push(error.message),
        });
        expect(errors).toEqual(['invalid point']);
    });

    test('command completion stays subscribed through mapped and filtered connection recovery', async () => {
        const targets: SnapshotObserver<{ complete: boolean }>[] = [];
        let released = 0;
        const source = new SnapshotStream<{ complete: boolean }>((observer) => {
            targets.push(observer);
            return () => { released++; };
        }).map(value => value).filter(value => value.complete);
        const states: Array<Error | null> = [], errors: Error[] = [];
        const release = source.subscribe({
            next: () => {},
            error: error => errors.push(error),
            connectionError: error => states.push(error),
        });
        const pending = source.first();
        const interrupted = new Error('connection lost');
        for (const target of targets) {
            target.next({ complete: false });
            target.connectionError?.(interrupted);
        }
        expect(released).toBe(0);
        for (const target of targets) {
            target.connectionError?.(null);
            target.next({ complete: true });
        }
        expect(await pending).toEqual({ complete: true });
        expect(released).toBe(1);
        release();
        expect(errors).toEqual([]);
        expect(states).toEqual([interrupted, null]);
    });

    test('combine keeps the connection error until every interrupted source recovers', () => {
        const observers: SnapshotObserver<number>[] = [];
        const source = () => new SnapshotStream<number>((observer) => {
            observers.push(observer);
            return () => {};
        });
        const states: Array<Error | null> = [], values: unknown[] = [], errors: Error[] = [];
        const release = SnapshotStream.combine([source(), source()] as const).subscribe({
            next: value => values.push(value),
            error: error => errors.push(error),
            connectionError: error => states.push(error),
        });
        const first = new Error('first lost'), second = new Error('second lost');
        observers[0].next(1); observers[1].next(2);
        observers[0].connectionError?.(first);
        observers[1].connectionError?.(second);
        observers[1].connectionError?.(null);
        expect(states).toEqual([first]);
        observers[0].connectionError?.(null);
        expect(states).toEqual([first, null]);
        expect(values).toEqual([[1, 2]]);
        expect(errors).toEqual([]);
        release();
    });

    test('switchMap combines parent and child connection state and releases replaced children', () => {
        let parent!: SnapshotObserver<number>;
        const children: SnapshotObserver<number>[] = [];
        let released = 0;
        const source = new SnapshotStream<number>((observer) => {
            parent = observer;
            return () => {};
        });
        const states: Array<Error | null> = [], values: number[] = [];
        const release = source.switchMap(() => new SnapshotStream<number>((observer) => {
            children.push(observer);
            return () => { released++; };
        })).subscribe({
            next: value => values.push(value),
            error: error => { throw error; },
            connectionError: error => states.push(error),
        });
        parent.next(1);
        children[0].next(10);
        const upstream = new Error('parent lost'), child = new Error('child lost');
        parent.connectionError?.(upstream);
        children[0].connectionError?.(child);
        parent.connectionError?.(null);
        expect(states).toEqual([null, upstream, child]);
        parent.next(2);
        expect(released).toBe(1);
        expect(states).toEqual([null, upstream, child, null]);
        children[1].next(20);
        expect(values).toEqual([10, 20]);
        release();
        expect(released).toBe(2);
    });
});
