import { Empty, Flex, Skeleton, Space, Tag, Tooltip, Tree } from 'antd';
import type { ReactNode } from 'react';

export interface DeviceTypeTreeItem {
    id: string;
    name: string;
    enabled: boolean;
    remark?: string;
}

interface DeviceTypeTreeProps<T extends DeviceTypeTreeItem> {
    items: readonly T[];
    loading: boolean;
    selectedId?: string;
    onSelect: (id: string) => void;
    /** 名称右侧的概要标签内容，如点位数量。 */
    summary: (item: T) => ReactNode;
}

/** 协议设备类型侧栏列表：名称、概要、启用状态，悬停显示备注。 */
export function DeviceTypeTree<T extends DeviceTypeTreeItem>({
    items,
    loading,
    selectedId,
    onSelect,
    summary,
}: DeviceTypeTreeProps<T>) {
    if (loading) return <Skeleton active paragraph={{ rows: 6 }} />;
    if (items.length === 0) return <Empty description="暂无设备类型" />;
    return (
        <Tree
            blockNode
            className="[&_.ant-tree-switcher]:hidden"
            selectedKeys={selectedId ? [selectedId] : []}
            onSelect={(keys) => {
                if (keys.length > 0) onSelect(String(keys[0]));
            }}
            treeData={items.map((item) => ({
                key: item.id,
                title: (
                    <Tooltip title={item.remark || '暂无备注'} placement="right">
                        <Flex
                            justify="space-between"
                            align="center"
                            gap={4}
                            className="h-8 min-w-0 p-1"
                        >
                            <span className="min-w-0 truncate">{item.name}</span>
                            <Space size={4} className="shrink-0">
                                <Tag color="blue">{summary(item)}</Tag>
                                <Tag color={item.enabled ? 'green' : 'red'}>
                                    {item.enabled ? '启用' : '禁用'}
                                </Tag>
                            </Space>
                        </Flex>
                    </Tooltip>
                ),
            }))}
        />
    );
}
