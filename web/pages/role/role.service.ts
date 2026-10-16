import { createQueryKeys } from '@/utils/query';
import { useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { useMutationWithMessage, useSaveMutation } from '@/hooks/use_mutation';
import type { PaginatedResult } from '@/types/pagination';
import { create, getList, getOptions, remove, update } from './role.api';
import type { Role } from './role.types';
const roleKeys = createQueryKeys('roles');
const roleQueryKeys = {
    ...roleKeys,
    list: (params?: Role.Query) => [...roleKeys.lists(), params] as const,
};

export function useRoleOptions(options?: { enabled?: boolean }) {
    return useQuery({
        queryKey: roleQueryKeys.options(),
        queryFn: ({ signal }) => getOptions(signal),
        staleTime: 5 * 60 * 1000,
        enabled: options?.enabled ?? true,
        refetchOnWindowFocus: false,
    });
}
export function useRoleList(
    params?: Role.Query,
    options?: Omit<UseQueryOptions<PaginatedResult<Role.Item>>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: roleQueryKeys.list(params),
        queryFn: ({ signal }) => getList(params, signal),
        ...options,
        refetchOnWindowFocus: false,
    });
}
export function useRoleSave() {
    return useSaveMutation<
        Role.CreateDto & {
            id?: string;
        },
        Role.CreateDto,
        Role.UpdateDto
    >({
        createFn: create,
        updateFn: update,
        toUpdatePayload: ({ id: _id, ...data }) => data,
        createMessage: '保存成功',
        updateMessage: '保存成功',
        invalidateKeys: [roleQueryKeys.all],
    });
}
export function useRoleDelete() {
    return useMutationWithMessage({
        mutationFn: remove,
        successMessage: '删除成功',
        invalidateKeys: [roleQueryKeys.all],
    });
}
