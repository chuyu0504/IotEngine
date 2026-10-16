import {
    ApartmentOutlined,
    DeleteOutlined,
    DownOutlined,
    EditOutlined,
    PlusOutlined,
} from '@ant-design/icons';
import type { MenuProps, TreeProps } from 'antd';
import { App, Button, Dropdown, Popover, Space, Spin, Tree } from 'antd';
import type { DataNode } from 'antd/es/tree';
import { type ReactNode, useMemo, useState } from 'react';
import { type GroupFormValues, GroupFormModal, type GroupTreeNode } from './group_form_modal';

/** 选择「未分组」时 `onSelect` 收到的值。 */
export const UNGROUPED_SELECTION = 'ungrouped';
const ALL_KEY = 'all';

export interface GroupTreeAction {
    key: string;
    label: string;
    icon: ReactNode;
    onClick: () => void;
}

/** 分组树节点，子节点与自身同类型。 */
export type GroupTreeItem<T> = GroupTreeNode & { children?: T[] };

export interface GroupTreePanelProps<T extends GroupTreeItem<T>> {
    groups: readonly T[];
    loading: boolean;
    /** `null` 为全部，`UNGROUPED_SELECTION` 为未分组，其余为分组 ID。 */
    selectedGroupId: string | null;
    onSelect: (groupId: string | null) => void;
    /** 「全部」节点名称，如「全部设备」。 */
    allLabel: string;
    ungroupedCount: number;
    groupTitle: (group: T) => string;
    canManage: boolean;
    /** 表单标题中的对象名称，如「分组」。 */
    formNoun: string;
    remarkMaxLength?: number;
    deleteHint: string;
    saving: boolean;
    toFormValues: (group: T) => GroupFormValues;
    onSave: (values: GroupFormValues, onSaved: () => void) => void;
    onDelete: (group: T) => unknown;
    /** 管理操作之前的附加右键菜单项。 */
    extraActions?: (group: T) => GroupTreeAction[];
}

export function GroupTreePanel<T extends GroupTreeItem<T>>({
    groups,
    loading,
    selectedGroupId,
    onSelect,
    allLabel,
    ungroupedCount,
    groupTitle,
    canManage,
    formNoun,
    remarkMaxLength,
    deleteHint,
    saving,
    toFormValues,
    onSave,
    onDelete,
    extraActions,
}: GroupTreePanelProps<T>) {
    const { modal } = App.useApp();
    const [popoverOpen, setPopoverOpen] = useState(false);
    const [formOpen, setFormOpen] = useState(false);
    const [editing, setEditing] = useState<GroupFormValues | null>(null);
    const [parentId, setParentId] = useState<string | null>(null);
    const groupIndex = useMemo(() => {
        const index = new Map<string, T>();
        const walk = (items: readonly T[]) => {
            for (const item of items) {
                index.set(item.id, item);
                if (item.children?.length) walk(item.children);
            }
        };
        walk(groups);
        return index;
    }, [groups]);
    const treeData = useMemo<DataNode[]>(() => {
        const convert = (items: readonly T[]): DataNode[] =>
            items.map((item) => ({
                key: item.id,
                title: groupTitle(item),
                children: item.children?.length ? convert(item.children) : undefined,
            }));
        return [
            { key: ALL_KEY, title: allLabel, isLeaf: true },
            ...(ungroupedCount > 0
                ? [
                      {
                          key: UNGROUPED_SELECTION,
                          title: `未分组 (${ungroupedCount})`,
                          isLeaf: true,
                      },
                  ]
                : []),
            ...convert(groups),
        ];
    }, [allLabel, groupTitle, groups, ungroupedCount]);
    const selectedLabel =
        selectedGroupId === null
            ? allLabel
            : selectedGroupId === UNGROUPED_SELECTION
              ? '未分组'
              : (groupIndex.get(selectedGroupId)?.name ?? allLabel);
    const selectedKeys = useMemo(() => [selectedGroupId ?? ALL_KEY], [selectedGroupId]);
    const selectNode: TreeProps['onSelect'] = (keys) => {
        if (!keys.length) return;
        const key = String(keys[0]);
        onSelect(key === ALL_KEY ? null : key);
        setPopoverOpen(false);
    };
    const closeForm = () => {
        setFormOpen(false);
        setEditing(null);
    };
    const openCreate = (nextParentId: string | null) => {
        setEditing(null);
        setParentId(nextParentId);
        setFormOpen(true);
    };
    const openEdit = (group: T) => {
        setEditing(toFormValues(group));
        setParentId(null);
        setFormOpen(true);
    };
    const confirmDelete = (group: T) =>
        modal.confirm({
            title: `确认删除分组「${group.name}」？`,
            content: deleteHint,
            okText: '确定删除',
            okButtonProps: { danger: true },
            onOk: () => onDelete(group),
        });
    const actionsOf = (group: T): GroupTreeAction[] => [
        ...(extraActions?.(group) ?? []),
        ...(canManage
            ? [
                  {
                      key: 'addChild',
                      label: '新增子分组',
                      icon: <PlusOutlined />,
                      onClick: () => openCreate(group.id),
                  },
                  {
                      key: 'edit',
                      label: '编辑',
                      icon: <EditOutlined />,
                      onClick: () => openEdit(group),
                  },
                  {
                      key: 'delete',
                      label: '删除',
                      icon: <DeleteOutlined />,
                      onClick: () => confirmDelete(group),
                  },
              ]
            : []),
    ];
    const renderTitle = (node: DataNode) => {
        const title = (
            <span className="block whitespace-normal break-words pr-2">{node.title as string}</span>
        );
        const group = groupIndex.get(String(node.key));
        const actions = group ? actionsOf(group) : [];
        if (!actions.length) return title;
        const items: MenuProps['items'] = actions.map(({ key, label, icon }) => ({
            key,
            label,
            icon,
            danger: key === 'delete',
        }));
        return (
            <Dropdown
                trigger={['contextMenu']}
                menu={{
                    items,
                    onClick: ({ key }) => actions.find((action) => action.key === key)?.onClick(),
                }}
            >
                {title}
            </Dropdown>
        );
    };
    const content = (
        <div className="w-72 max-w-[calc(100vw-32px)]">
            {loading ? (
                <div className="py-6 text-center">
                    <Spin size="small" />
                </div>
            ) : (
                <div className="max-h-[min(68vh,560px)] overflow-y-auto pr-1">
                    <Tree
                        blockNode
                        defaultExpandAll
                        treeData={treeData}
                        selectedKeys={selectedKeys}
                        onSelect={selectNode}
                        titleRender={renderTitle}
                    />
                </div>
            )}
            {canManage && (
                <Button
                    type="text"
                    size="small"
                    block
                    icon={<PlusOutlined />}
                    className="mt-1 !text-gray-500"
                    onClick={() => openCreate(null)}
                >
                    新建分组
                </Button>
            )}
        </div>
    );
    return (
        <>
            <Popover
                content={content}
                trigger="click"
                open={popoverOpen}
                onOpenChange={setPopoverOpen}
                placement="bottomLeft"
            >
                <Button icon={<ApartmentOutlined />}>
                    <Space size={4}>
                        {selectedLabel}
                        <DownOutlined className="!text-[10px] text-gray-400" />
                    </Space>
                </Button>
            </Popover>
            <GroupFormModal
                open={formOpen}
                noun={formNoun}
                editing={editing}
                parentId={parentId}
                groups={groups}
                saving={saving}
                remarkMaxLength={remarkMaxLength}
                onCancel={closeForm}
                onFinish={(values) => onSave(values, closeForm)}
            />
        </>
    );
}
