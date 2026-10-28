import { z } from 'zod';

export const derivedPointSchema = z
    .object({
        id: z.uuid(),
        name: z.string().trim().min(1).max(100),
        unit: z.string().max(32).optional(),
        kind: z.enum(['expression', 'average', 'minimum', 'maximum']),
        unitMode: z.enum(['fixed', 'conditional']),
        unitRules: z
            .array(
                z.object({ condition: z.string().trim().min(1).max(512), unit: z.string().max(32) })
            )
            .max(8)
            .optional(),
        sourceAlias: z.string().optional(),
        valueType: z.enum(['number', 'boolean']),
        expression: z.string().max(512).optional(),
        inputs: z
            .array(
                z.object({
                    alias: z
                        .string()
                        .regex(/^[a-zA-Z_][a-zA-Z0-9_]{0,31}$/)
                        .refine(
                            (value) => value !== 'true' && value !== 'false',
                            '变量名不能使用 true 或 false'
                        ),
                    pointId: z.uuid(),
                })
            )
            .min(1)
            .max(16),
        windowSeconds: z.number().int().min(1).max(86400).optional(),
        maxAgeSeconds: z.number().int().min(1).max(86400),
        visible: z.boolean(),
    })
    .superRefine((point, context) => {
        if (new Set(point.inputs.map((input) => input.alias)).size !== point.inputs.length)
            context.addIssue({ code: 'custom', path: ['inputs'], message: '输入变量名不能重复' });
        if (point.kind === 'expression' && !point.expression?.trim())
            context.addIssue({ code: 'custom', path: ['expression'], message: '请输入公式' });
        if (
            point.kind !== 'expression' &&
            (!point.windowSeconds ||
                !point.inputs.some((input) => input.alias === point.sourceAlias) ||
                point.valueType !== 'number')
        )
            context.addIssue({
                code: 'custom',
                path: ['sourceAlias'],
                message: '请选择窗口计算的输入变量，并设置窗口时长和数值结果类型',
            });
        if (point.unitMode === 'conditional' && !point.unitRules?.length)
            context.addIssue({ code: 'custom', path: ['unitRules'], message: '请添加单位条件' });
    });
