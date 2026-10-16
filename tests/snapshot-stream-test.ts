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
});
