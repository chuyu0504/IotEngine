import { createQueryKeys } from '@/utils/query';
import { useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { useMutationWithMessage, useSaveMutation } from '@/hooks/use_mutation';
import type { PaginatedResult } from '@/types/pagination';
import { create, getList, getOptions, remove, update } from './user.api';
import type { User } from './user.types';

// ============ Queries ============
type UserListResult = PaginatedResult<User.Item>;
const userKeys = createQueryKeys('users');
const userQueryKeys = {
    ...userKeys,
    list: (params?: User.Query) => [...userKeys.lists(), params] as const,
};

export function useUserList(
    params?: User.Query,
    options?: Omit<UseQueryOptions<UserListResult>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: userQueryKeys.list(params),
        queryFn: ({ signal }) => getList(params, signal),
        ...options,
        refetchOnWindowFocus: false,
    });
}
export function useUserOptions(
    options?: Omit<UseQueryOptions<User.Option[]>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: userQueryKeys.options(),
        queryFn: ({ signal }) => getOptions(undefined, signal),
        staleTime: 5 * 60 * 1000,
        ...options,
        refetchOnWindowFocus: false,
    });
}
// ============ Mutations ============
export function useUserDelete() {
    return useMutationWithMessage({
        mutationFn: remove,
        successMessage: '删除成功',
        invalidateKeys: [userQueryKeys.all],
    });
}
export function useUserSave() {
    return useSaveMutation<
        User.CreateDto & {
            id?: string;
        },
        User.CreateDto,
        User.UpdateDto
    >({
        createFn: create,
        updateFn: update,
        toUpdatePayload: ({
            id: _id,
            username: _username,
            ...data
        }: User.CreateDto & {
            id?: string;
        }) => data as User.UpdateDto,
        createMessage: '保存成功',
        updateMessage: '保存成功',
        invalidateKeys: [userQueryKeys.all],
    });
}
