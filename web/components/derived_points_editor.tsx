import {
    Alert,
    App,
    Button,
    Card,
    Divider,
    Empty,
    Flex,
    Form,
    Input,
    InputNumber,
    Popconfirm,
    Select,
    Space,
    Tag,
} from 'antd';
import type { Ref } from 'react';
import { useImperativeHandle, useRef, useState } from 'react';
import { FormModal } from '@/components/form_modal';
import { PointAccessFields } from '@/components/point_access_fields';

export interface DerivedPointValue {
    id: string;
    name: string;
    unit?: string;
    unitMode: 'fixed' | 'conditional';
    unitRules?: { condition: string; unit: string }[];
    sourceAlias?: string;
    kind: 'expression' | 'average' | 'minimum' | 'maximum';
    valueType: 'number' | 'boolean';
    expression?: string;
    inputs: { alias: string; pointId: string }[];
    windowSeconds?: number;
    maxAgeSeconds: number;
    visible: boolean;
}

export interface DerivedPointTestRequest {
    expression: string;
    inputs: { alias: string; value: number }[];
    unit: string;
    unitRules: { condition: string; unit: string }[];
}

export interface DerivedPointTestResult {
    value: number;
    unit: string;
    /** 命中的单位条件序号（从 1 开始），0 表示使用默认单位。 */
    matchedRule: number;
}

/** 与 zod `safeParse` 结果同形，问题路径定位到表单字段。 */
export type DerivedPointValidation =
    | { success: true; data: DerivedPointValue }
    | { success: false; error: { issues: { path: PropertyKey[]; message: string }[] } };

export interface DerivedPointsEditorHandle {
    create: () => void;
}

interface DerivedPointsEditorProps {
    ref?: Ref<DerivedPointsEditorHandle>;
    points: readonly DerivedPointValue[];
    /** 可作为输入的物理点位。 */
    sourceOptions: readonly { value: string; label: string }[];
    editable: boolean;
    saving: boolean;
    /** 保存完整的派生值列表，失败时抛出以保留编辑弹窗。 */
    onSave: (points: DerivedPointValue[]) => Promise<unknown>;
    validatePoint: (point: DerivedPointValue) => DerivedPointValidation;
    /** 校验公式语法与变量绑定，无效时抛出带原因的错误。 */
    validateExpression: (expression: string, aliases: string[]) => Promise<void>;
    testExpression: (request: DerivedPointTestRequest) => Promise<DerivedPointTestResult>;
}

const KIND_LABELS: Record<DerivedPointValue['kind'], string> = {
    expression: '表达式 / 条件',
    average: '窗口平均值',
    minimum: '窗口最小值',
    maximum: '窗口最大值',
};

const KIND_OPTIONS = Object.entries(KIND_LABELS).map(([value, label]) => ({ value, label }));

const newDerivedPoint = (): DerivedPointValue => ({
    id: crypto.randomUUID(),
    name: '',
    kind: 'expression',
    expression: 'x',
    unitMode: 'fixed',
    unit: '',
    valueType: 'number',
    inputs: [{ alias: 'x', pointId: '' }],
    maxAgeSeconds: 300,
    visible: true,
});

/** 派生值列表与编辑弹窗：数据和保存由调用方提供。 */
export function DerivedPointsEditor({
    ref,
    points,
    sourceOptions,
    editable,
    saving,
    onSave,
    validatePoint,
    validateExpression,
    testExpression,
}: DerivedPointsEditorProps) {
    const { message } = App.useApp();
    const [form] = Form.useForm<DerivedPointValue>();
    const [editing, setEditing] = useState(false);
    const [testValues, setTestValues] = useState<Record<string, number | null>>({});
    const [testResult, setTestResult] = useState<{ ok: boolean; text: string }>();
    const [testing, setTesting] = useState(false);
    const testRevision = useRef(0);
    const kind = Form.useWatch('kind', form);
    const unitMode = Form.useWatch('unitMode', form);
    const inputs = Form.useWatch('inputs', form) as DerivedPointValue['inputs'] | undefined;
    const currentId = Form.useWatch('id', form);
    const invalidateTest = () => {
        testRevision.current += 1;
        setTesting(false);
        setTestResult(undefined);
    };
    const checkExpression = async () => {
        const revision = ++testRevision.current;
        setTestResult(undefined);
        setTesting(true);
        try {
            const values: DerivedPointValue = form.getFieldsValue(true);
            const samples = (values.inputs ?? []).map((input) => {
                const value = testValues[input.alias];
                if (value == null || !Number.isFinite(value))
                    throw new Error(`请填写变量 ${input.alias || '（未命名）'} 的测试值`);
                return { alias: input.alias, value };
            });
            const result = await testExpression({
                expression: values.kind === 'expression' ? (values.expression ?? '') : '0',
                inputs: samples,
                unit: values.unit ?? '',
                unitRules: values.unitMode === 'conditional' ? (values.unitRules ?? []) : [],
            });
            if (revision !== testRevision.current) return;
            const resultValue =
                values.valueType === 'boolean' ? String(result.value !== 0) : String(result.value);
            const unitResult = `${result.unit || '无单位'}${values.unitMode === 'conditional' ? (result.matchedRule ? `（命中条件 ${result.matchedRule}）` : '（默认单位）') : ''}`;
            setTestResult({
                ok: true,
                text:
                    values.kind === 'expression'
                        ? `校验通过，计算结果：${resultValue}；单位：${unitResult}`
                        : `单位条件校验通过：${unitResult}。窗口统计结果由实际采样计算。`,
            });
        } catch (error) {
            if (revision === testRevision.current)
                setTestResult({
                    ok: false,
                    text: error instanceof Error ? error.message : '校验失败',
                });
        } finally {
            if (revision === testRevision.current) setTesting(false);
        }
    };
    const expressionRules = [
        {
            validator: async (_: unknown, value?: string) => {
                if (!value?.trim()) throw new Error('请输入表达式');
                const aliases = (
                    (form.getFieldValue('inputs') ?? []) as DerivedPointValue['inputs']
                )
                    .map((input) => input?.alias)
                    .filter((alias): alias is string => Boolean(alias));
                await validateExpression(value, aliases);
            },
        },
    ];
    const options = [
        ...sourceOptions,
        ...points
            .filter((point) => point.id !== currentId)
            .map((point) => ({ value: point.id, label: `${point.name}（派生）` })),
    ];
    const edit = (point?: DerivedPointValue) => {
        invalidateTest();
        setTestValues({});
        form.resetFields();
        form.setFieldsValue(point ?? newDerivedPoint());
        setEditing(true);
    };
    useImperativeHandle(ref, () => ({ create: () => edit() }));
    const submit = async () => {
        const values = await form.validateFields();
        if (values.kind !== 'expression') {
            values.valueType = 'number';
            values.expression = undefined;
        } else values.windowSeconds = undefined;
        if (values.unitMode === 'fixed') values.unitRules = [];
        const result = validatePoint(values);
        if (!result.success) {
            form.setFields(
                result.error.issues.map((issue) => ({
                    name: issue.path as Parameters<typeof form.setFields>[0][number]['name'],
                    errors: [issue.message],
                }))
            );
            message.error(result.error.issues[0]?.message ?? '配置无效');
            return;
        }
        const saved = result.data;
        await onSave(
            points.some((point) => point.id === saved.id)
                ? points.map((point) => (point.id === saved.id ? saved : point))
                : [...points, saved]
        );
        setEditing(false);
    };
    return (
        <>
            <section className="mt-4 rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                <Flex justify="space-between" align="center" gap={8} wrap className="mb-4">
                    <Space>
                        <span className="font-semibold">派生值</span>
                        <Tag color="blue">{points.length} 个</Tag>
                    </Space>
                </Flex>
                {points.length === 0 ? (
                    <Empty
                        image={Empty.PRESENTED_IMAGE_SIMPLE}
                        description="暂无派生值，点击上方新增派生值"
                    />
                ) : (
                    <div className="grid grid-cols-[repeat(auto-fill,minmax(min(240px,100%),1fr))] gap-3">
                        {points.map((point) => (
                            <Card
                                key={point.id}
                                size="small"
                                hoverable
                                className="h-full border-slate-200 shadow-[0_1px_4px_rgba(15,23,42,0.06)]"
                                styles={{ body: { padding: 12 } }}
                            >
                                <Flex
                                    justify="space-between"
                                    gap={12}
                                    align="start"
                                    className="mb-2"
                                >
                                    <div className="min-w-0 flex-1">
                                        <div className="truncate text-sm font-semibold text-slate-800">
                                            {point.name}
                                        </div>
                                        <div className="mt-0.5 text-[12px] text-slate-400">
                                            {KIND_LABELS[point.kind]}
                                        </div>
                                    </div>
                                    {editable && (
                                        <Space size={4} className="shrink-0">
                                            <Button
                                                size="small"
                                                type="link"
                                                onClick={() => edit(point)}
                                            >
                                                编辑
                                            </Button>
                                            <Popconfirm
                                                title="删除此派生值？引用它的公式也需要修改。"
                                                onConfirm={() =>
                                                    onSave(
                                                        points.filter(
                                                            (value) => value.id !== point.id
                                                        )
                                                    )
                                                }
                                            >
                                                <Button
                                                    size="small"
                                                    type="link"
                                                    danger
                                                    loading={saving}
                                                >
                                                    删除
                                                </Button>
                                            </Popconfirm>
                                        </Space>
                                    )}
                                </Flex>
                                <Space size={4} wrap className="mb-2">
                                    <Tag color="purple">派生值</Tag>
                                    <Tag>只读</Tag>
                                    {point.unitMode === 'conditional' ? (
                                        <Tag color="cyan">条件单位</Tag>
                                    ) : (
                                        point.unit && <Tag>{point.unit}</Tag>
                                    )}
                                    {point.kind !== 'expression' && (
                                        <Tag>{point.windowSeconds} 秒窗口</Tag>
                                    )}
                                    <Tag>{point.valueType === 'boolean' ? '布尔值' : '数值'}</Tag>
                                </Space>
                                <div className="mb-3 break-all text-xs leading-5 text-slate-500">
                                    {point.kind === 'expression'
                                        ? point.expression
                                        : `统计输入：${point.sourceAlias}`}
                                </div>
                            </Card>
                        ))}
                    </div>
                )}
            </section>

            <FormModal
                title={points.some((point) => point.id === currentId) ? '编辑派生值' : '新增派生值'}
                confirmLoading={saving}
                open={editing}
                onCancel={() => {
                    invalidateTest();
                    setEditing(false);
                }}
                onOk={submit}
            >
                <Form form={form} layout="vertical" onValuesChange={invalidateTest}>
                    <Form.Item name="id" hidden>
                        <Input />
                    </Form.Item>
                    <div className="grid grid-cols-1 gap-x-4 sm:grid-cols-2">
                        <Form.Item name="name" label="名称" rules={[{ required: true }]}>
                            <Input maxLength={100} />
                        </Form.Item>
                        <Form.Item name="unitMode" label="单位方式">
                            <Select
                                options={[
                                    { value: 'fixed', label: '固定单位' },
                                    { value: 'conditional', label: '条件单位' },
                                ]}
                            />
                        </Form.Item>
                        <Form.Item
                            name="unit"
                            label={unitMode === 'conditional' ? '默认单位（均不匹配时）' : '单位'}
                        >
                            <Input maxLength={32} />
                        </Form.Item>
                        <Form.Item name="kind" label="计算方式">
                            <Select options={KIND_OPTIONS} />
                        </Form.Item>
                        <Form.Item name="valueType" label="结果类型">
                            <Select
                                disabled={kind !== 'expression'}
                                options={[
                                    { value: 'number', label: '数值' },
                                    { value: 'boolean', label: '布尔值' },
                                ]}
                            />
                        </Form.Item>
                    </div>
                    <Form.List name="inputs">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map((field) => (
                                    <div
                                        key={field.key}
                                        className="grid grid-cols-[100px_minmax(0,1fr)_auto] gap-2"
                                    >
                                        <Form.Item
                                            name={[field.name, 'alias']}
                                            label="变量名"
                                            rules={[{ required: true }]}
                                        >
                                            <Input />
                                        </Form.Item>
                                        <Form.Item
                                            name={[field.name, 'pointId']}
                                            label="输入点位"
                                            rules={[{ required: true }]}
                                        >
                                            <Select
                                                showSearch
                                                optionFilterProp="label"
                                                options={options}
                                            />
                                        </Form.Item>
                                        <Button
                                            className="mt-7"
                                            disabled={fields.length <= 1}
                                            onClick={() => remove(field.name)}
                                        >
                                            移除
                                        </Button>
                                    </div>
                                ))}
                                <Button
                                    className="mb-4"
                                    disabled={fields.length >= 16}
                                    onClick={() =>
                                        add({ alias: `x${fields.length + 1}`, pointId: '' })
                                    }
                                >
                                    添加输入
                                </Button>
                            </>
                        )}
                    </Form.List>
                    {kind !== 'expression' && (
                        <Form.Item
                            name="sourceAlias"
                            label="窗口计算输入"
                            rules={[{ required: true }]}
                        >
                            <Select
                                options={(inputs ?? []).map((input) => ({
                                    value: input.alias,
                                    label: input.alias,
                                }))}
                            />
                        </Form.Item>
                    )}
                    {kind === 'expression' ? (
                        <Form.Item
                            name="expression"
                            label="计算公式"
                            dependencies={['inputs']}
                            validateTrigger="onBlur"
                            extra="支持 + - * / %、比较、&& || ! 和 if(条件, 真值, 假值)、min、max、abs、sqrt、round。例如 if(x > 10, x * 2, 0)。"
                            rules={expressionRules}
                        >
                            <Input.TextArea rows={3} maxLength={512} />
                        </Form.Item>
                    ) : (
                        <Form.Item
                            name="windowSeconds"
                            label="滑动窗口（秒）"
                            rules={[{ required: true }]}
                            extra="平均值采用窗口内有效采样的算术平均。"
                        >
                            <InputNumber min={1} max={86400} className="w-full" />
                        </Form.Item>
                    )}
                    <Form.Item
                        name="maxAgeSeconds"
                        label="输入有效期（秒）"
                        rules={[{ required: true }]}
                        extra="输入缺失或超时，结果显示无效，不按 0 计算。"
                    >
                        <InputNumber min={1} max={86400} className="w-full" />
                    </Form.Item>
                    {unitMode === 'conditional' && (
                        <>
                            <Divider titlePlacement="start">单位条件</Divider>
                            <p>
                                按顺序使用第一条成立的条件；单位切换不自动换算数值，需要在数值公式中处理。
                            </p>
                            <Form.List name="unitRules">
                                {(fields, { add, remove, move }) => (
                                    <>
                                        {fields.map((field, index) => (
                                            <Card
                                                key={field.key}
                                                size="small"
                                                className="mb-3"
                                                title={`条件 ${index + 1}`}
                                                extra={
                                                    <Space>
                                                        <Button
                                                            size="small"
                                                            disabled={index === 0}
                                                            onClick={() => move(index, index - 1)}
                                                        >
                                                            上移
                                                        </Button>
                                                        <Button
                                                            size="small"
                                                            onClick={() => remove(field.name)}
                                                        >
                                                            移除
                                                        </Button>
                                                    </Space>
                                                }
                                            >
                                                <Form.Item
                                                    name={[field.name, 'condition']}
                                                    label="条件表达式"
                                                    dependencies={['inputs']}
                                                    validateTrigger="onBlur"
                                                    rules={expressionRules}
                                                >
                                                    <Input
                                                        placeholder="例如 mode == 1"
                                                        maxLength={512}
                                                    />
                                                </Form.Item>
                                                <Form.Item
                                                    name={[field.name, 'unit']}
                                                    label="匹配时单位"
                                                >
                                                    <Input maxLength={32} />
                                                </Form.Item>
                                            </Card>
                                        ))}
                                        <Button
                                            disabled={fields.length >= 8}
                                            onClick={() => add({ condition: '', unit: '' })}
                                        >
                                            添加单位条件
                                        </Button>
                                    </>
                                )}
                            </Form.List>
                        </>
                    )}
                    {(kind === 'expression' || unitMode === 'conditional') && (
                        <Card size="small" title="手动校验" className="my-4">
                            <p className="mb-3 text-xs text-slate-500">
                                填写各变量的测试值，使用与后台计算相同的规则校验；不会保存配置或写入设备。布尔输入用
                                0 / 1 表示。
                            </p>
                            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                                {Array.from(
                                    new Set((inputs ?? []).map((input) => input.alias))
                                ).map((alias) => (
                                    <label
                                        key={alias}
                                        htmlFor={`expression-test-${currentId}-${alias}`}
                                        className="flex flex-col gap-1"
                                    >
                                        <span>{alias || '未命名变量'} 的测试值</span>
                                        <InputNumber
                                            id={`expression-test-${currentId}-${alias}`}
                                            aria-label={`${alias || '未命名变量'} 的测试值`}
                                            value={testValues[alias] ?? null}
                                            className="w-full"
                                            onChange={(value) => {
                                                invalidateTest();
                                                setTestValues((current) => ({
                                                    ...current,
                                                    [alias]: value,
                                                }));
                                            }}
                                        />
                                    </label>
                                ))}
                            </div>
                            <Button className="mt-3" loading={testing} onClick={checkExpression}>
                                试算公式
                            </Button>
                            {testResult && (
                                <Alert
                                    className="mt-3"
                                    showIcon
                                    type={testResult.ok ? 'success' : 'error'}
                                    title={testResult.text}
                                />
                            )}
                        </Card>
                    )}
                    <PointAccessFields />
                </Form>
            </FormModal>
        </>
    );
}
