import type { TreeDataNode } from 'antd';
import {
    App,
    Button,
    Form,
    Input,
    InputNumber,
    Pagination,
    Result,
    Select,
    Space,
    Table,
    Tree,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useEffect, useMemo, useState } from 'react';
import { FormModal } from '@/components/form_modal';
import { PageContainer } from '@/components/page_container';
import { StatusTag } from '@/components/status_tag';
import { useDebounceFn } from '@/hooks/use_debounce_fn';
import { usePermissions } from '@/hooks/use_permission';
import { validateForm } from '@/utils/validation';
import { useUserOptions } from '../user/user.service';
import type { User } from '../user/user.types';
import { createDeptSchema, updateDeptSchema } from './dept.schema';
import { useDeptDelete, useDeptList, useDeptOptions, useDeptSave } from './dept.service';
import type { Dept } from './dept.types';

const { Search } = Input;
const EMPTY_DEPARTMENTS: Dept.Option[] = [];
const EMPTY_USERS: User.Option[] = [];
type DeptFormValues = Dept.CreateDto & {
    id?: string;
};
export function SystemDeptPage() {
    const [keyword, setKeyword] = useState('');
    const [selectedParentId, setSelectedParentId] = useState<string>();
    const [expandedTreeKeys, setExpandedTreeKeys] = useState<(string | number)[]>(['all']);
    const [pagination, setPagination] = useState({ page: 1, pageSize: 10 });
    const [editing, setEditing] = useState<Dept.Item | null>(null);
    const [open, setOpen] = useState(false);
    const [form] = Form.useForm<DeptFormValues>();
    const { modal } = App.useApp();
    const { has } = usePermissions();
    const canQuery = has('system:dept:query');
    const canAdd = has('system:dept:add');
    const canEdit = has('system:dept:edit');
    const canDelete = has('system:dept:delete');
    const { run: search } = useDebounceFn((value: string) => {
        setKeyword(value);
        setPagination((current) => ({ ...current, page: 1 }));
    }, 300);
    const { data, isLoading } = useDeptList(
        {
            ...pagination,
            keyword: keyword || undefined,
            parent_id: selectedParentId,
        },
        { enabled: canQuery }
    );
    const { data: departmentData } = useDeptOptions({ enabled: canQuery || canAdd || canEdit });
    const departments = departmentData ?? EMPTY_DEPARTMENTS;
    const { data: users = EMPTY_USERS } = useUserOptions({ enabled: canAdd || canEdit });
    const save = useDeptSave();
    const remove = useDeptDelete();
    const parentOptions = useMemo(
        () =>
            departments
                .filter((item) => item.id !== editing?.id)
                .map((item) => ({ value: item.id, label: item.name })),
        [departments, editing]
    );
    const leaderOptions = useMemo(
        () => users.map((item) => ({ value: item.id, label: item.nickname || item.username })),
        [users]
    );
    const departmentTree = useMemo<TreeDataNode[]>(() => {
        const childrenByParent = new Map<string, Dept.Option[]>();
        for (const department of departments) {
            const siblings = childrenByParent.get(department.parent_id) ?? [];
            siblings.push(department);
            childrenByParent.set(department.parent_id, siblings);
        }
        const buildNodes = (parentId: string, ancestors = new Set<string>()): TreeDataNode[] =>
            (childrenByParent.get(parentId) ?? [])
                .filter((department) => !ancestors.has(department.id))
                .sort((left, right) => left.name.localeCompare(right.name, 'zh-CN'))
                .map((department) => {
                    const nextAncestors = new Set(ancestors).add(department.id);
                    const children = buildNodes(department.id, nextAncestors);
                    return {
                        key: `dept-${department.id}`,
                        title: department.name,
                        children: children.length ? children : undefined,
                    };
                });
        return [
            {
                key: 'all',
                title: `全部部门 (${departments.length})`,
                children: buildNodes(''),
            },
        ];
    }, [departments]);
    const selectedDepartment = useMemo(
        () => departments.find((department) => department.id === selectedParentId),
        [departments, selectedParentId]
    );
    useEffect(() => {
        if (!departmentData) return;
        setExpandedTreeKeys([
            'all',
            ...departmentData.map((department) => `dept-${department.id}`),
        ]);
    }, [departmentData]);
    const showCreate = () => {
        setEditing(null);
        form.resetFields();
        form.setFieldsValue({ parent_id: '', leader_id: '', sort_order: 0, status: 'enabled' });
        setOpen(true);
    };
    const showEdit = (dept: Dept.Item) => {
        setEditing(dept);
        form.setFieldsValue({ ...dept });
        setOpen(true);
    };
    const submit = (values: DeptFormValues) => {
        if (editing) {
            const validated = validateForm(form, updateDeptSchema, values);
            if (!validated) return;
            save.mutate({ ...validated, id: editing.id } as DeptFormValues, {
                onSuccess: () => setOpen(false),
            });
            return;
        }
        const validated = validateForm(form, createDeptSchema, values);
        if (validated) save.mutate(validated, { onSuccess: () => setOpen(false) });
    };
    const confirmDelete = (dept: Dept.Item) =>
        modal.confirm({
            title: `确认删除部门「${dept.name}」吗？`,
            content: '存在子部门时不能删除。',
            okButtonProps: { danger: true },
            onOk: () => remove.mutate(dept.id),
        });
    if (!canQuery)
        return (
            <PageContainer>
                <Result status="403" title="无权限" subTitle="您没有查询部门的权限" />
            </PageContainer>
        );
    const columns: ColumnsType<Dept.Item> = [
        { title: '部门名称', dataIndex: 'name' },
        { title: '部门编码', dataIndex: 'code', render: (value) => value || '-' },
        { title: '上级部门', dataIndex: 'parent_name', render: (value) => value || '-' },
        { title: '负责人', dataIndex: 'leader_name', render: (value) => value || '-' },
        { title: '排序', dataIndex: 'sort_order', width: 80 },
        {
            title: '状态',
            dataIndex: 'status',
            render: (value: Dept.Status) => <StatusTag status={value} />,
        },
        {
            title: '操作',
            key: 'actions',
            width: 140,
            render: (_, dept) => (
                <Space>
                    {canEdit && (
                        <Button type="link" onClick={() => showEdit(dept)}>
                            编辑
                        </Button>
                    )}
                    {canDelete && (
                        <Button type="link" danger onClick={() => confirmDelete(dept)}>
                            删除
                        </Button>
                    )}
                </Space>
            ),
        },
    ];
    return (
        <PageContainer
            header={
                <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                        <h2 className="m-0 text-lg font-semibold text-slate-900">部门管理</h2>
                        <p className="m-0 mt-1 text-xs text-slate-500">
                            左侧选择部门，右侧查看直属下级部门
                        </p>
                    </div>
                    <Space wrap>
                        <Search
                            allowClear
                            placeholder="部门名称 / 编码"
                            onChange={(event) => search(event.target.value)}
                            className="w-[240px]"
                        />
                        {canAdd && (
                            <Button type="primary" onClick={showCreate}>
                                新建部门
                            </Button>
                        )}
                    </Space>
                </div>
            }
            footer={
                <div className="flex justify-end">
                    <Pagination
                        {...pagination}
                        total={data?.total || 0}
                        showSizeChanger
                        showTotal={(total) => `共 ${total} 条`}
                        onChange={(page, pageSize) => setPagination({ page, pageSize })}
                    />
                </div>
            }
        >
            <div className="flex h-full min-h-0 flex-col gap-4 lg:flex-row">
                <aside className="flex max-h-56 shrink-0 flex-col overflow-hidden rounded-lg border border-slate-200 bg-slate-50/70 lg:max-h-none lg:w-60">
                    <div className="border-b border-slate-200 bg-white px-4 py-3">
                        <div className="font-medium text-slate-800">部门结构</div>
                        <div className="mt-0.5 text-xs text-slate-500">选择节点筛选直属部门</div>
                    </div>
                    <div className="min-h-0 flex-1 overflow-auto p-2">
                        <Tree
                            blockNode
                            showLine
                            treeData={departmentTree}
                            expandedKeys={expandedTreeKeys}
                            selectedKeys={[
                                selectedParentId === undefined ? 'all' : `dept-${selectedParentId}`,
                            ]}
                            onExpand={(keys) => setExpandedTreeKeys(keys.map(String))}
                            onSelect={(keys) => {
                                const key = String(keys[0] ?? 'all');
                                setSelectedParentId(
                                    key === 'all' ? undefined : key.replace('dept-', '')
                                );
                                setPagination((current) => ({ ...current, page: 1 }));
                            }}
                        />
                    </div>
                </aside>
                <section className="min-h-0 min-w-0 flex-1 overflow-hidden">
                    <div className="mb-2 flex h-7 items-center text-sm text-slate-500">
                        当前：
                        <span className="font-medium text-slate-700">
                            {selectedDepartment?.name ?? '全部部门'}
                        </span>
                    </div>
                    <Table
                        rowKey="id"
                        columns={columns}
                        dataSource={data?.list || []}
                        loading={isLoading}
                        pagination={false}
                        sticky
                        scroll={{ x: 'max-content', y: 'calc(100vh - 330px)' }}
                    />
                </section>
            </div>
            <FormModal
                open={open}
                title={editing ? '编辑部门' : '新建部门'}
                onCancel={() => setOpen(false)}
                onOk={() => form.submit()}
                confirmLoading={save.isPending}
                destroyOnHidden
            >
                <Form form={form} layout="vertical" onFinish={submit}>
                    <Form.Item name="id" hidden>
                        <Input />
                    </Form.Item>
                    <Form.Item label="部门名称" name="name">
                        <Input maxLength={128} />
                    </Form.Item>
                    <Form.Item label="部门编码" name="code">
                        <Input maxLength={64} />
                    </Form.Item>
                    <Form.Item label="上级部门" name="parent_id">
                        <Select options={[{ value: '', label: '顶级部门' }, ...parentOptions]} />
                    </Form.Item>
                    <Form.Item label="负责人" name="leader_id">
                        <Select
                            showSearch
                            optionFilterProp="label"
                            options={[{ value: '', label: '未指定' }, ...leaderOptions]}
                        />
                    </Form.Item>
                    <Form.Item label="排序" name="sort_order">
                        <InputNumber min={0} className="!w-full" />
                    </Form.Item>
                    <Form.Item label="状态" name="status">
                        <Select
                            options={[
                                { value: 'enabled', label: '启用' },
                                { value: 'disabled', label: '禁用' },
                            ]}
                        />
                    </Form.Item>
                </Form>
            </FormModal>
        </PageContainer>
    );
}

export default SystemDeptPage;
