import { Tooltip } from 'antd';
import { memo, type ReactNode, useMemo } from 'react';
import { getGroupKey, normalizeGroupName, UNGROUPED_GROUP_KEY } from '@/utils/group_key';

export interface DeviceCardItem {
    key: string | number;
    label: string;
    children: ReactNode;
    span?: number;
    group?: string;
    tooltipLabel?: string;
}

interface DeviceCardSection {
    key: string;
    label: string;
    items: DeviceCardItem[];
}

interface DeviceCardProps {
    title: ReactNode;
    subtitle?: ReactNode;
    items: DeviceCardItem[];
    column?: number;
    extra?: ReactNode;
    onClick?: () => void;
    ariaLabel?: string;
}

const buildSections = (items: DeviceCardItem[]): DeviceCardSection[] => {
    const sections = new Map<string, DeviceCardSection>();
    for (const item of items) {
        const group = normalizeGroupName(item.group);
        const key = getGroupKey(group);
        const current = sections.get(key);
        if (current) current.items.push(item);
        else sections.set(key, { key, label: group || '未分组', items: [item] });
    }
    return [...sections.values()];
};

const DeviceValues = ({
    items,
    wide = false,
    columns,
}: {
    items: DeviceCardItem[];
    wide?: boolean;
    columns?: number;
}) => (
    <dl
        className="m-0 grid items-start gap-x-2 gap-y-1"
        style={{
            gridTemplateColumns:
                columns === 2
                    ? 'repeat(2, minmax(0, 1fr))'
                    : `repeat(auto-fit, minmax(min(100%, ${wide ? '100px' : '104px'}), 1fr))`,
        }}
    >
        {items.map((item) => (
            <div
                key={item.key}
                className="min-w-0 rounded bg-slate-50/70 px-1 py-0.5 text-xs leading-4"
                style={item.span && item.span > 1 ? { gridColumn: '1 / -1' } : undefined}
            >
                <Tooltip title={item.tooltipLabel ?? item.label}>
                    <dt
                        className={`min-w-0 text-center text-[11px] leading-4 text-slate-500 ${item.span && item.span > 1 ? 'whitespace-normal' : 'truncate whitespace-nowrap'}`}
                    >
                        {item.label}
                    </dt>
                </Tooltip>
                <dd
                    className={`m-0 min-w-0 text-center text-[13px] font-semibold leading-5 tabular-nums text-slate-950 ${item.span && item.span > 1 ? 'whitespace-normal break-all' : 'truncate whitespace-nowrap'}`}
                    title={
                        typeof item.children === 'string' || typeof item.children === 'number'
                            ? String(item.children)
                            : undefined
                    }
                >
                    {item.children}
                </dd>
            </div>
        ))}
    </dl>
);

const DeviceCard = ({
    title,
    subtitle,
    items,
    column = 4,
    extra,
    onClick,
    ariaLabel,
}: DeviceCardProps) => {
    const sections = useMemo(() => buildSections(items), [items]);
    const hasGroupSections = sections.some((section) => section.key !== UNGROUPED_GROUP_KEY);

    const content = (
        <>
            <div className="min-w-0 text-[15px] font-semibold leading-5 text-slate-950">
                {title}
            </div>
            {subtitle && <div className="text-xs leading-5 text-slate-500">{subtitle}</div>}
            <div className="h-px bg-slate-100" />
            <div className="flex flex-col gap-1">
                {hasGroupSections ? (
                    sections.map((section) => (
                        <section
                            key={section.key}
                            className="flex flex-col border-t border-slate-100 pt-1 first:border-0 first:pt-0"
                        >
                            <div className="flex shrink-0 items-center gap-2">
                                <span className="min-w-0 truncate whitespace-nowrap text-[11px] font-semibold leading-5 text-slate-600">
                                    {section.label}
                                </span>
                                <span className="h-px flex-1 bg-slate-100" />
                            </div>
                            <DeviceValues
                                items={section.items}
                                wide={column >= 8}
                                columns={column}
                            />
                        </section>
                    ))
                ) : (
                    <DeviceValues items={items} wide={column >= 8} columns={column} />
                )}
            </div>
            {extra && (
                <div className="mt-auto border-t border-slate-100 pt-1.5 text-[11px] leading-4 text-slate-500">
                    {extra}
                </div>
            )}
        </>
    );
    const className =
        'flex h-full w-full flex-col gap-1 rounded-lg border border-slate-200 bg-white px-3 py-2 text-left shadow-[0_8px_24px_rgba(15,23,42,0.08)] transition-all hover:shadow-[0_12px_28px_rgba(15,23,42,0.12)]';

    if (onClick) {
        return (
            <button
                type="button"
                className={`${className} cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2`}
                aria-label={ariaLabel}
                onClick={onClick}
            >
                {content}
            </button>
        );
    }

    return <div className={className}>{content}</div>;
};

export default memo(DeviceCard);
