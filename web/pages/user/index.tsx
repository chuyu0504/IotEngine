import { App, Button, Form, Input, Pagination, Result, Select, Space, Table, Tag } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useMemo, useState } from 'react';
import { FormModal } from '@/components/form_modal';
import { PageContainer } from '@/components/page_container';
import { StatusTag } from '@/components/status_tag';
import { useDebounceFn } from '@/hooks/use_debounce_fn';
import { usePermissions } from '@/hooks/use_permission';
import { validateForm } from '@/utils/validation';
import { useDeptOptions } from '../dept/dept.service';
import type { Dept } from '../dept/dept.types';
import { useRoleOptions } from '../role/role.service';
import type { Role } from '../role/role.types';
import { createUserSchema, updateUserSchema } from './user.schema';
import { useUserDelete, useUserList, useUserSave } from './user.service';
import type { User } from './user.types';

/**
 * 用户管理
 */
const { Search } = Input;
const EMPTY_ROLE_OPTIONS: Role.Option[] = [];
const EMPTY_DEPT_OPTIONS: Dept.Option[] = [];
interface UserFormValues {
    id?: string;
    username: string;
    password?: string;
    nickname?: string;
    phone?: string;
    email?: string;
    status: User.Status;
    department_id?: string;
    role_ids?: string[];
}
const SystemUserPage = () => {
    const [keyword, setKeyword] = useState('');
    const [pagination, setPagination] = useState({ page: 1, pageSize: 10 });
    const [modalVisible, setModalVisible] = useState(false);
    const [editing, setEditing] = useState<User.Item | null>(null);
    const [form] = Form.useForm<UserFormValues>();
    const { modal } = App.useApp();
    const { has } = usePermissions();
    const canQuery = has('system:user:query');
    const canAdd = has('system:user:add');
    const canEdit = has('system:user:edit');
    const canDelete = has('system:user:delete');
    const canQueryDept = has('system:dept:query');
    const doSearch = (value: string) => {
        setKeyword(value);
        setPagination((prev) => ({ ...prev, page: 1 }));
    };
    const { run: debouncedSearch } = useDebounceFn(doSearch, 300);
    const { data: userPage, isLoading: loadingUsers } = useUserList(
        {
            page: pagination.page,
            pageSize: pagination.pageSize,
            keyword: keyword || undefined,
        },
        { enabled: canQuery }
    );
    const { data: roleOptions = EMPTY_ROLE_OPTIONS } = useRoleOptions({
        enabled: canAdd || canEdit,
    });
    const { data: departmentOptions = EMPTY_DEPT_OPTIONS } = useDeptOptions({
        enabled: (canAdd || canEdit) && canQueryDept,
    });
    const roleSelectOptions = useMemo(
        () => roleOptions.map((role) => ({ label: role.name, value: role.id })),
        [roleOptions]
    );
    const departmentSelectOptions = useMemo(
        () =>
            departmentOptions.map((department) => ({
                label: department.name,
                value: department.id,
            })),
        [departmentOptions]
    );
    const saveMutation = useUserSave();
    const deleteMutation = useUserDelete();
    const openCreateModal = () => {
        setEditing(null);
        form.resetFields();
        form.setFieldsValue({ status: 'enabled' });
        setModalVisible(true);
    };
    const openEditModal = (record: User.Item) => {
        setEditing(record);
        form.setFieldsValue({
            id: record.id,
            username: record.username,
            nickname: record.nickname ?? undefined,
            phone: record.phone ?? undefined,
            email: record.email ?? undefined,
            status: record.status,
            department_id: record.department_id || undefined,
            role_ids: record.roles.map((r) => r.id),
        });
        setModalVisible(true);
    };
    const onDelete = (record: User.Item) => {
        modal.confirm({
            title: `确认删除用户「${record.username}」吗？`,
            content: '删除后该用户将无法登录系统。此操作不可撤销。',
            okText: '确定删除',
            cancelText: '取消',
            okButtonProps: { danger: true },
            onOk: () => deleteMutation.mutate(record.id),
        });
    };
    const onFinish = (values: UserFormValues) => {
        const onSuccess = () => {
            setModalVisible(false);
            setEditing(null);
        };
        if (editing) {
            const validated = validateForm(form, updateUserSchema, {
                ...values,
                department_id: values.department_id ?? '',
            });
            if (!validated) return;
            saveMutation.mutate(
                {
                    ...validated,
                    id: editing.id,
                    username: values.username,
                } as User.CreateDto & {
                    id?: string;
                },
                { onSuccess }
            );
            return;
        }
        const validated = validateForm(form, createUserSchema, values);
        if (!validated) return;
        saveMutation.mutate(validated, {
            onSuccess,
        });
    };
    const handlePageChange = (page: number, pageSize: number) => {
        setPagination({
            page,
            pageSize,
        });
    };
    if (!canQuery) {
        return (
            <PageContainer>
                <Result
                    status="403"
                    title="无权限"
                    subTitle="您没有查询用户列表的权限，请联系管理员"
                />
            </PageContainer>
        );
    }
    const columns: ColumnsType<User.Item> = [
        { title: '用户名', dataIndex: 'username', ellipsis: true },
        { title: '昵称', dataIndex: 'nickname', ellipsis: true },
        { title: '手机号', dataIndex: 'phone' },
        { title: '邮箱', dataIndex: 'email', ellipsis: true },
        {
            title: '部门',
            dataIndex: 'department_name',
            render: (value?: string) => value || '-',
        },
        {
            title: '角色',
            dataIndex: 'roles',
            render: (roles: User.Item['roles']) =>
                roles.map((r) => (
                    <Tag key={r.id} color="blue">
                        {r.name}
                    </Tag>
                )),
        },
        {
            title: '状态',
            dataIndex: 'status',
            render: (v: User.Status) => <StatusTag status={v} />,
        },
        {
            title: '操作',
            key: 'actions',
            width: 150,
            fixed: 'right' as const,
            render: (_, record) => {
                const isBuiltinAdmin = record.username === 'admin';
                return (
                    <Space>
                        {canEdit && (
                            <Button
                                type="link"
                                onClick={() => openEditModal(record)}
                                disabled={isBuiltinAdmin}
                            >
                                编辑
                            </Button>
                        )}
                        {canDelete && (
                            <Button
                                type="link"
                                danger
                                onClick={() => onDelete(record)}
                                disabled={isBuiltinAdmin}
                            >
                                删除
                            </Button>
                        )}
                    </Space>
                );
            },
        },
    ];
    return (
        <PageContainer
            header={
                <div className="flex flex-wrap items-center justify-between gap-3">
                    <div>
                        <h2 className="m-0 text-lg font-semibold text-slate-900">用户管理</h2>
                        <p className="m-0 mt-1 text-xs text-slate-500">
                            维护用户资料、角色与账号状态
                        </p>
                    </div>
                    <Space wrap>
                        <Search
                            allowClear
                            placeholder="用户名 / 昵称 / 手机 / 邮箱"
                            onChange={(e) => debouncedSearch(e.target.value)}
                            onSearch={doSearch}
                            className="w-[280px]"
                        />
                        {canAdd && (
                            <Button type="primary" onClick={openCreateModal}>
                                新建用户
                            </Button>
                        )}
                    </Space>
                </div>
            }
            footer={
                <div className="flex justify-end">
                    <Pagination
                        current={pagination.page}
                        pageSize={pagination.pageSize}
                        total={userPage?.total || 0}
                        showSizeChanger
                        showTotal={(total, range) => `${range[0]}-${range[1]} / 共 ${total} 条`}
                        onChange={handlePageChange}
                    />
                </div>
            }
        >
            <Table<User.Item>
                rowKey="id"
                columns={columns}
                dataSource={userPage?.list || []}
                loading={loadingUsers}
                pagination={false}
                size="middle"
                scroll={{ x: 'max-content', y: 'calc(100vh - 280px)' }}
                sticky
            />

            <FormModal
                open={modalVisible}
                title={editing ? '编辑用户' : '新建用户'}
                okText="确定"
                cancelText="取消"
                onCancel={() => {
                    setModalVisible(false);
                    setEditing(null);
                }}
                onOk={() => form.submit()}
                confirmLoading={saveMutation.isPending}
                afterOpenChange={(open) => {
                    if (!open) form.resetFields();
                }}
                destroyOnHidden
            >
                <Form<UserFormValues> form={form} layout="vertical" onFinish={onFinish}>
                    <Form.Item name="id" hidden>
                        <Input />
                    </Form.Item>

                    <Form.Item label="用户名" name="username">
                        <Input placeholder="登录用户名" disabled={!!editing} maxLength={50} />
                    </Form.Item>

                    <Form.Item label="密码" name="password">
                        <Input.Password
                            placeholder={editing ? '留空则不修改密码' : '请输入密码'}
                            maxLength={100}
                        />
                    </Form.Item>

                    <Form.Item label="昵称" name="nickname">
                        <Input placeholder="用户昵称" maxLength={100} />
                    </Form.Item>

                    <Form.Item label="手机号" name="phone">
                        <Input placeholder="7–20 位数字，可包含 +、- 或空格" maxLength={20} />
                    </Form.Item>

                    <Form.Item label="邮箱" name="email">
                        <Input placeholder="电子邮箱" maxLength={100} />
                    </Form.Item>

                    <Form.Item label="角色" name="role_ids">
                        <Select
                            mode="multiple"
                            allowClear={false}
                            placeholder="选择角色"
                            disabled={editing?.username === 'admin'}
                            options={roleSelectOptions}
                        />
                    </Form.Item>

                    <Form.Item label="部门" name="department_id">
                        <Select
                            allowClear
                            showSearch
                            optionFilterProp="label"
                            placeholder={canQueryDept ? '选择部门' : '无部门查询权限'}
                            disabled={!canQueryDept}
                            options={departmentSelectOptions}
                        />
                    </Form.Item>

                    <Form.Item label="状态" name="status">
                        <Select>
                            <Select.Option value="enabled">启用</Select.Option>
                            <Select.Option value="disabled">禁用</Select.Option>
                        </Select>
                    </Form.Item>
                </Form>
            </FormModal>
        </PageContainer>
    );
};

export default SystemUserPage;
