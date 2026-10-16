import { keepPreviousData, useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { useMutationWithMessage, useSaveMutation } from '@/hooks/use_mutation';
import { useSnapshotQuery } from '@/hooks/use_snapshot_query';
import { fetchAllPages } from '@/utils/pagination';
import { createQueryKeys } from '@/utils/query';
import { retainNewerRecords } from '@/utils/versioned_records';
import {
    create,
    getDebugPackets,
    getEnums,
    getList,
    getPublicIp,
    queryList,
    remove,
    setDebug as saveDebugSwitch,
    update,
} from './link.api';
import type { Link } from './link.types';

const keys = createQueryKeys('links');
/** `list` 与 `debug` 是 SSE 快照缓存，只随推送更新；写入后只失效 HTTP 查询。 */
const linkQueryKeys = {
    list: (params?: Link.Query) => [...keys.lists(), params] as const,
    debug: (id: string, params?: Link.Query) => [...keys.all, 'debug', id, params] as const,
    enums: [...keys.all, 'enums'] as const,
    publicIp: [...keys.all, 'public-ip'] as const,
    options: keys.options(),
};
const LINK_OPTIONS_PAGE_SIZE = 100;

export function useLinkList(
    params?: Link.Query,
    options?: { enabled?: boolean },
    debugLinkId?: string
) {
    return useSnapshotQuery({
        queryKey: [...linkQueryKeys.list(params), debugLinkId],
        queryFn: () => getList(params, debugLinkId),
        placeholderData: keepPreviousData,
        enabled: options?.enabled ?? true,
    });
}
export function useLinkEnums(options?: { enabled?: boolean }) {
    return useQuery({
        queryKey: linkQueryKeys.enums,
        queryFn: ({ signal }) => getEnums(signal),
        enabled: options?.enabled ?? true,
        staleTime: Number.POSITIVE_INFINITY,
    });
}
export function usePublicIp(options?: { enabled?: boolean }) {
    return useQuery({
        queryKey: linkQueryKeys.publicIp,
        queryFn: ({ signal }) => getPublicIp(signal),
        enabled: options?.enabled ?? true,
        staleTime: 5 * 60 * 1000,
        retry: false,
    });
}
export function useLinkOptions(
    options?: Omit<UseQueryOptions<Link.Item[]>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: linkQueryKeys.options,
        queryFn: ({ signal }) =>
            fetchAllPages(LINK_OPTIONS_PAGE_SIZE, (page, pageSize) =>
                queryList({ page, pageSize }, signal)
            ),
        ...options,
        enabled: options?.enabled ?? true,
    });
}
export function useLinkSave() {
    return useSaveMutation<
        Link.SaveDto & {
            id?: string;
        },
        Link.SaveDto,
        Link.SaveDto
    >({
        createFn: create,
        updateFn: update,
        toUpdatePayload: ({ id: _id, ...data }) => data,
        createMessage: '保存成功',
        updateMessage: '保存成功',
        invalidateKeys: [linkQueryKeys.options],
    });
}
export function useLinkDelete() {
    return useMutationWithMessage({
        mutationFn: remove,
        successMessage: '删除成功',
        invalidateKeys: [linkQueryKeys.options],
    });
}

export function useLinkDebug(id: string, open: boolean, params?: Link.Query) {
    const packets = useSnapshotQuery({
        queryKey: linkQueryKeys.debug(id, params),
        queryFn: () => getDebugPackets(id, params),
        structuralSharing: retainNewerRecords,
        enabled: open,
    });
    const toggle = useMutationWithMessage({
        mutationFn: (enabled: boolean) => saveDebugSwitch(id, enabled),
        successMessage: '调试设置已保存',
    });
    return { packets, toggle };
}
