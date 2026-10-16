import { z } from 'zod';
import type { PaginatedResult } from '@/types/pagination';

export const pageParamsSchema = z.object({
    page: z.coerce.number().int().min(1, 'page 必须大于 0').optional(),
    pageSize: z.coerce
        .number()
        .int()
        .min(1, 'pageSize 必须在 1 - 100 之间')
        .max(100, 'pageSize 必须在 1 - 100 之间')
        .optional(),
    keyword: z.string().optional(),
});

/** 读取分页接口的全部页，按 `id` 去重并保持首次出现的顺序。 */
export async function fetchAllPages<T extends { id: string }>(
    pageSize: number,
    fetchPage: (page: number, pageSize: number) => Promise<PaginatedResult<T>>
): Promise<T[]> {
    const first = await fetchPage(1, pageSize);
    const pageCount = Math.max(1, first.totalPages ?? Math.ceil(first.total / pageSize));
    const rest = await Promise.all(
        Array.from({ length: pageCount - 1 }, (_, index) => fetchPage(index + 2, pageSize))
    );
    const items = new Map<string, T>();
    for (const page of [first, ...rest]) {
        for (const item of page.list) if (!items.has(item.id)) items.set(item.id, item);
    }
    return [...items.values()];
}
