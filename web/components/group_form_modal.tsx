import { Form, Input, InputNumber, Select, TreeSelect } from 'antd';
import { useEffect, useMemo } from 'react';
import { FormModal } from './form_modal';

export type GroupStatus = 'enabled' | 'disabled';

/** 分组树节点的公共形状；各模块 DTO 的字段命名由调用方映射。 */
export interface GroupTreeNode {
    id: string;
    name: string;
    children?: GroupTreeNode[];
}

export interface GroupFormValues {
    id?: string;
    name: string;
    parentId?: string;
    sortOrder: number;
    status: GroupStatus;
    remark?: string;
}

export interface GroupFormModalProps {
    open: boolean;
    /** 弹窗标题中的对象名称，如「分组」「边缘节点分组」。 */
    noun: string;
    editing: GroupFormValues | null;
    parentId: string | null;
    groups: readonly GroupTreeNode[];
    saving: boolean;
    remarkMaxLength?: number;
    onCancel: () => void;
    onFinish: (values: GroupFormValues) => void;
}

interface ParentOption {
    value: string;
    title: string;
    children?: ParentOption[];
}

function parentOptions(nodes: readonly GroupTreeNode[], excludeId?: string): ParentOption[] {
    return nodes
        .filter((node) => node.id !== excludeId)
        .map((node) => ({
            value: node.id,
            title: node.name,
            children: node.children?.length ? parentOptions(node.children, excludeId) : undefined,
        }));
}

const STATUS_OPTIONS = [
    { value: 'enabled', label: '启用' },
    { value: 'disabled', label: '禁用' },
];

export function GroupFormModal({
    open,
    noun,
    editing,
    parentId,
    groups,
    saving,
    remarkMaxLength,
    onCancel,
    onFinish,
}: GroupFormModalProps) {
    const [form] = Form.useForm<GroupFormValues>();
    useEffect(() => {
        if (!open) return;
        form.resetFields();
        form.setFieldsValue(
            editing
                ? { ...editing, parentId: editing.parentId || undefined }
                : { parentId: parentId || undefined, sortOrder: 0, status: 'enabled' }
        );
    }, [editing, form, open, parentId]);
    const treeData = useMemo(() => parentOptions(groups, editing?.id), [groups, editing?.id]);
    return (
        <FormModal
            open={open}
            title={editing ? `编辑${noun}` : `新建${noun}`}
            okText="确定"
            cancelText="取消"
            confirmLoading={saving}
            onCancel={onCancel}
            onOk={() => form.submit()}
            destroyOnClose
        >
            <Form form={form} layout="vertical" onFinish={onFinish} className="mt-4">
                <Form.Item name="id" hidden>
                    <Input />
                </Form.Item>
                <Form.Item
                    label="分组名称"
                    name="name"
                    rules={[{ required: true, message: '请输入分组名称' }]}
                >
                    <Input placeholder="请输入分组名称" maxLength={100} />
                </Form.Item>
                <Form.Item label="上级分组" name="parentId">
                    <TreeSelect
                        allowClear
                        treeData={treeData}
                        placeholder="不选则为顶级分组"
                        treeDefaultExpandAll
                    />
                </Form.Item>
                <Form.Item label="排序" name="sortOrder">
                    <InputNumber className="!w-full" min={0} placeholder="数值越小越靠前" />
                </Form.Item>
                <Form.Item
                    label="状态"
                    name="status"
                    rules={[{ required: true, message: '请选择状态' }]}
                >
                    <Select options={STATUS_OPTIONS} />
                </Form.Item>
                <Form.Item label="备注" name="remark">
                    <Input.TextArea rows={2} maxLength={remarkMaxLength} placeholder="可选备注" />
                </Form.Item>
            </Form>
        </FormModal>
    );
}
