import { HolderOutlined } from '@ant-design/icons';
import type { DragEndEvent } from '@dnd-kit/core';
import { closestCenter, DndContext, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import {
    arrayMove,
    rectSortingStrategy,
    SortableContext,
    useSortable,
    verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { Button, Flex, Space } from 'antd';
import type { CSSProperties, ReactNode } from 'react';
import { memo, useCallback, useEffect, useMemo, useState } from 'react';
import { VirtualGrid, VirtualStack } from '@/components/virtual_grid';

/** 数据变化后保留用户拖出的顺序：删除已不存在的键，新键追加到末尾。 */
const reconcileOrder = (previousOrder: string[], currentKeys: string[]) => {
    if (currentKeys.length === 0) return [];
    if (previousOrder.length === 0) return currentKeys;
    const currentKeySet = new Set(currentKeys);
    const filtered = previousOrder.filter((key) => currentKeySet.has(key));
    if (filtered.length === 0) return currentKeys;
    const filteredSet = new Set(filtered);
    const nextOrder = [...filtered, ...currentKeys.filter((key) => !filteredSet.has(key))];
    const unchanged =
        previousOrder.length === nextOrder.length &&
        previousOrder.every((key, index) => key === nextOrder[index]);
    return unchanged ? previousOrder : nextOrder;
};

/** 按给定键顺序排序；不在顺序中的条目保持原相对位置排在后面。 */
const sortByOrder = <T,>(items: T[], orderedKeys: string[], getKey: (item: T) => string): T[] => {
    const orderIndex = new Map(orderedKeys.map((key, index) => [key, index]));
    const originalIndex = new Map(items.map((item, index) => [getKey(item), index]));
    return [...items].sort((left, right) => {
        const leftOrder = orderIndex.get(getKey(left));
        const rightOrder = orderIndex.get(getKey(right));
        if (leftOrder !== undefined && rightOrder !== undefined) return leftOrder - rightOrder;
        if (leftOrder !== undefined) return -1;
        if (rightOrder !== undefined) return 1;
        return (originalIndex.get(getKey(left)) ?? 0) - (originalIndex.get(getKey(right)) ?? 0);
    });
};

/** 维护可拖拽排序的键顺序，拖拽结束后回调新的完整顺序。 */
function useSortableOrder(currentKeys: string[], onOrderChange?: (nextOrder: string[]) => void) {
    const [order, setOrder] = useState<string[]>(currentKeys);
    useEffect(() => {
        setOrder((previousOrder) => reconcileOrder(previousOrder, currentKeys));
    }, [currentKeys]);
    const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 8 } }));
    const handleDragEnd = useCallback(
        ({ active, over }: DragEndEvent) => {
            if (!over || active.id === over.id) return;
            const activeIndex = order.indexOf(String(active.id));
            const overIndex = order.indexOf(String(over.id));
            if (activeIndex < 0 || overIndex < 0) return;
            const nextOrder = arrayMove(order, activeIndex, overIndex);
            setOrder(nextOrder);
            onOrderChange?.(nextOrder);
        },
        [onOrderChange, order]
    );
    return { order, sensors, handleDragEnd };
}

function useSortableFrameStyle(id: string, disabled: boolean, style?: CSSProperties) {
    const sortable = useSortable({ id, disabled });
    const frameStyle: CSSProperties = {
        transform: CSS.Transform.toString(sortable.transform),
        transition: sortable.transition || undefined,
        opacity: sortable.isDragging ? 0.75 : 1,
        zIndex: sortable.isDragging ? 1 : undefined,
        ...style,
    };
    const dragHandle = (
        <Button
            type="text"
            size="small"
            icon={<HolderOutlined />}
            disabled={disabled}
            aria-label="拖拽排序"
            title="拖拽排序"
            className="!cursor-grab active:!cursor-grabbing"
            style={{ touchAction: 'none' }}
            {...sortable.attributes}
            {...sortable.listeners}
        />
    );
    return { setNodeRef: sortable.setNodeRef, frameStyle, dragHandle };
}

interface SortableGroupSectionFrameProps {
    id: string;
    title: ReactNode;
    description?: ReactNode;
    meta?: ReactNode;
    actions?: ReactNode;
    children: ReactNode;
    disabled?: boolean;
    className?: string;
    bodyClassName?: string;
    style?: CSSProperties;
}

/** 可拖拽分组的外框：标题、统计、操作和拖拽手柄。 */
export const SortableGroupSectionFrame = memo(
    ({
        id,
        title,
        description,
        meta,
        actions,
        children,
        disabled = false,
        className,
        bodyClassName,
        style,
    }: SortableGroupSectionFrameProps) => {
        const { setNodeRef, frameStyle, dragHandle } = useSortableFrameStyle(id, disabled, style);
        return (
            <section ref={setNodeRef} className={className} style={frameStyle}>
                <Flex justify="space-between" align="center" gap={12} wrap className="mb-3">
                    <div className="min-w-0">
                        <div className="text-sm font-semibold text-slate-800">{title}</div>
                        {description ? (
                            <div className="mt-1 text-xs text-slate-500">{description}</div>
                        ) : null}
                    </div>
                    <Space size={6} wrap>
                        {meta}
                        {actions}
                        {dragHandle}
                    </Space>
                </Flex>
                <div className={bodyClassName}>{children}</div>
            </section>
        );
    }
);

interface SortableGroupItemFrameProps {
    id: string;
    children: (dragHandle: ReactNode) => ReactNode;
    disabled?: boolean;
    className?: string;
    style?: CSSProperties;
}

/** 可拖拽条目外框，拖拽手柄交给调用方放入卡片内。 */
export const SortableGroupItemFrame = memo(
    ({ id, children, disabled = false, className, style }: SortableGroupItemFrameProps) => {
        const { setNodeRef, frameStyle, dragHandle } = useSortableFrameStyle(id, disabled, style);
        return (
            <div ref={setNodeRef} className={className} style={frameStyle}>
                {children(dragHandle)}
            </div>
        );
    }
);

const getSectionKey = (section: { key: string }) => section.key;

interface SortableGroupSectionListProps<T extends { key: string }> {
    sections: T[];
    children: (section: T) => ReactNode;
    empty?: ReactNode;
    className?: string;
    disabled?: boolean;
    onOrderChange?: (nextOrder: string[]) => void;
}

/** 纵向虚拟渲染、可拖拽调整顺序的分组列表。 */
export function SortableGroupSectionList<T extends { key: string }>({
    sections,
    children,
    empty,
    className,
    disabled = false,
    onOrderChange,
}: SortableGroupSectionListProps<T>) {
    const currentKeys = useMemo(() => sections.map(getSectionKey), [sections]);
    const { order, sensors, handleDragEnd } = useSortableOrder(currentKeys, onOrderChange);
    const orderedSections = useMemo(
        () => sortByOrder(sections, order, getSectionKey),
        [sections, order]
    );
    if (orderedSections.length === 0) return empty ?? null;
    return (
        <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={disabled ? undefined : handleDragEnd}
        >
            <SortableContext
                items={orderedSections.map(getSectionKey)}
                strategy={verticalListSortingStrategy}
            >
                <VirtualStack
                    items={orderedSections}
                    getItemKey={getSectionKey}
                    className={className ?? 'w-full'}
                    estimateSize={420}
                    gap={24}
                >
                    {children}
                </VirtualStack>
            </SortableContext>
        </DndContext>
    );
}

const getItemId = (item: { id: string }) => item.id;

interface SortableGroupItemListProps<T extends { id: string }> {
    items: T[];
    children: (item: T, dragHandle: ReactNode) => ReactNode;
    empty?: ReactNode;
    /** 卡片最小宽度（px），列数随容器宽度变化。 */
    minItemWidth: number;
    disabled?: boolean;
    onOrderChange?: (nextOrder: string[]) => void;
}

/** 按行虚拟渲染、可拖拽调整顺序的卡片网格。 */
export function SortableGroupItemList<T extends { id: string }>({
    items,
    children,
    empty,
    minItemWidth,
    disabled = false,
    onOrderChange,
}: SortableGroupItemListProps<T>) {
    const currentIds = useMemo(() => items.map(getItemId), [items]);
    const { order, sensors, handleDragEnd } = useSortableOrder(currentIds, onOrderChange);
    const orderedItems = useMemo(() => sortByOrder(items, order, getItemId), [items, order]);
    if (orderedItems.length === 0) return empty ?? null;
    return (
        <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={disabled ? undefined : handleDragEnd}
        >
            <SortableContext items={orderedItems.map(getItemId)} strategy={rectSortingStrategy}>
                <VirtualGrid
                    items={orderedItems}
                    getItemKey={getItemId}
                    className="w-full"
                    minItemWidth={minItemWidth}
                    estimateSize={220}
                    gap={12}
                >
                    {(item) => (
                        <SortableGroupItemFrame
                            key={item.id}
                            id={item.id}
                            disabled={disabled}
                            className="h-full"
                        >
                            {(dragHandle) => children(item, dragHandle)}
                        </SortableGroupItemFrame>
                    )}
                </VirtualGrid>
            </SortableContext>
        </DndContext>
    );
}
