import { useVirtualizer } from '@tanstack/react-virtual';
import {
    type CSSProperties,
    type ReactNode,
    useCallback,
    useLayoutEffect,
    useMemo,
    useRef,
    useState,
} from 'react';

const findVerticalScrollParent = (element: HTMLElement) => {
    let parent = element.parentElement;
    while (parent) {
        const overflowY = window.getComputedStyle(parent).overflowY;
        if (overflowY === 'auto' || overflowY === 'scroll') return parent;
        parent = parent.parentElement;
    }
    return null;
};

/** 列表相对最近纵向滚动容器的偏移，随容器及祖先尺寸变化重新测量。 */
function useScrollAnchor() {
    const rootRef = useRef<HTMLDivElement>(null);
    const [scrollElement, setScrollElement] = useState<HTMLElement | null>(null);
    const [scrollMargin, setScrollMargin] = useState(0);
    useLayoutEffect(() => {
        if (rootRef.current) setScrollElement(findVerticalScrollParent(rootRef.current));
    }, []);
    const updateScrollMargin = useCallback(() => {
        const root = rootRef.current;
        if (!root || !scrollElement) return;
        const nextMargin =
            root.getBoundingClientRect().top -
            scrollElement.getBoundingClientRect().top +
            scrollElement.scrollTop;
        setScrollMargin((current) => (Math.abs(current - nextMargin) < 0.5 ? current : nextMargin));
    }, [scrollElement]);
    useLayoutEffect(() => {
        const root = rootRef.current;
        if (!root || !scrollElement) return;
        updateScrollMargin();
        const observer = new ResizeObserver(updateScrollMargin);
        observer.observe(scrollElement);
        for (
            let ancestor = root.parentElement;
            ancestor && ancestor !== scrollElement;
            ancestor = ancestor.parentElement
        )
            observer.observe(ancestor);
        window.addEventListener('resize', updateScrollMargin);
        return () => {
            observer.disconnect();
            window.removeEventListener('resize', updateScrollMargin);
        };
    }, [scrollElement, updateScrollMargin]);
    return { rootRef, scrollElement, scrollMargin };
}

export interface VirtualStackProps<T> {
    items: readonly T[];
    getItemKey: (item: T) => string;
    children: (item: T) => ReactNode;
    className?: string;
    estimateSize?: number;
    /** 行间距（px）。 */
    gap?: number;
    overscan?: number;
}

/** 在最近的纵向滚动容器内虚拟渲染的单列列表。 */
export function VirtualStack<T>({
    items,
    getItemKey,
    children,
    className,
    estimateSize = 320,
    gap = 16,
    overscan = 4,
}: VirtualStackProps<T>) {
    const { rootRef, scrollElement, scrollMargin } = useScrollAnchor();
    const virtualizer = useVirtualizer<HTMLElement, HTMLDivElement>({
        count: items.length,
        getScrollElement: () => scrollElement,
        estimateSize: () => estimateSize,
        getItemKey: (index) => getItemKey(items[index]),
        overscan,
        scrollMargin,
    });
    return (
        <div className={className}>
            <div
                ref={rootRef}
                className="relative w-full"
                style={{ height: virtualizer.getTotalSize() }}
            >
                {virtualizer.getVirtualItems().map((virtualItem) => (
                    <div
                        key={virtualItem.key}
                        ref={virtualizer.measureElement}
                        data-index={virtualItem.index}
                        className="absolute left-0 top-0 w-full"
                        style={{
                            transform: `translateY(${virtualItem.start - scrollMargin}px)`,
                            paddingBottom: gap,
                        }}
                    >
                        {children(items[virtualItem.index])}
                    </div>
                ))}
            </div>
        </div>
    );
}

export interface VirtualGridProps<T> extends Omit<VirtualStackProps<T>, 'children'> {
    /** 单列最小宽度（px），列数由容器宽度决定。 */
    minItemWidth: number;
    maxColumns?: number;
    /** 单项占用的列数，超过当前列数时按当前列数计。 */
    getItemSpan?: (item: T) => number;
    /** 行内网格的附加类名，如 `items-stretch`。 */
    rowClassName?: string;
    /** `span` 为该项实际占用的列数，调用方据此设置 `gridColumn`。 */
    children: (item: T, span: number) => ReactNode;
}

interface GridRow<T> {
    key: string;
    cells: { item: T; span: number }[];
}

function packRows<T>(
    items: readonly T[],
    columnCount: number,
    getItemKey: (item: T) => string,
    getItemSpan?: (item: T) => number
): GridRow<T>[] {
    const rows: GridRow<T>[] = [];
    let cells: GridRow<T>['cells'] = [];
    let occupied = 0;
    const flush = () => {
        rows.push({ key: cells.map((cell) => getItemKey(cell.item)).join(':'), cells });
        cells = [];
        occupied = 0;
    };
    for (const item of items) {
        const span = Math.min(columnCount, Math.max(1, getItemSpan?.(item) ?? 1));
        if (cells.length && occupied + span > columnCount) flush();
        cells.push({ item, span });
        occupied += span;
        if (occupied >= columnCount) flush();
    }
    if (cells.length) flush();
    return rows;
}

const getRowKey = <T,>(row: GridRow<T>) => row.key;

/** 按容器宽度分列、按行虚拟渲染的卡片网格。 */
export function VirtualGrid<T>({
    items,
    getItemKey,
    children,
    className,
    rowClassName,
    minItemWidth,
    maxColumns = Number.POSITIVE_INFINITY,
    getItemSpan,
    estimateSize = 220,
    gap = 12,
    overscan,
}: VirtualGridProps<T>) {
    const widthRef = useRef<HTMLDivElement>(null);
    const [columnCount, setColumnCount] = useState(1);
    useLayoutEffect(() => {
        const root = widthRef.current;
        if (!root) return;
        const updateColumnCount = () => {
            const width = root.getBoundingClientRect().width;
            const next = Math.min(
                maxColumns,
                Math.max(1, Math.floor((width + gap) / (minItemWidth + gap)))
            );
            setColumnCount((current) => (current === next ? current : next));
        };
        updateColumnCount();
        const observer = new ResizeObserver(updateColumnCount);
        observer.observe(root);
        return () => observer.disconnect();
    }, [gap, maxColumns, minItemWidth]);
    const rows = useMemo(
        () => packRows(items, columnCount, getItemKey, getItemSpan),
        [columnCount, getItemKey, getItemSpan, items]
    );
    const rowStyle = useMemo<CSSProperties>(
        () => ({
            display: 'grid',
            gridTemplateColumns: `repeat(${columnCount}, minmax(0, 1fr))`,
            gap,
        }),
        [columnCount, gap]
    );
    return (
        <div ref={widthRef} className={className}>
            <VirtualStack
                items={rows}
                getItemKey={getRowKey}
                estimateSize={estimateSize}
                gap={gap}
                overscan={overscan}
            >
                {(row) => (
                    <div className={rowClassName} style={rowStyle}>
                        {row.cells.map((cell) => children(cell.item, cell.span))}
                    </div>
                )}
            </VirtualStack>
        </div>
    );
}
