import type { PageParams } from '@/types/pagination';

/**
 * 用户管理类型定义
 */
// ============ 枚举/状态类型 ============
type UserStatus = 'enabled' | 'disabled';
// ============ 基础类型 ============
interface UserRole {
    id: string;
    name: string;
    code: string;
}
interface UserOption {
    id: string;
    username: string;
    nickname?: string;
    phone?: string;
    email?: string;
}
// ============ 列表项/详情类型 ============
interface UserItem {
    id: string;
    username: string;
    nickname?: string;
    phone?: string;
    email?: string;
    status: UserStatus;
    department_id?: string;
    department_name?: string;
    roles: UserRole[];
    created_at?: string;
    updated_at?: string;
}
// ============ 查询参数 ============
interface UserQuery extends PageParams {
    status?: UserStatus;
}
// ============ DTO 类型 ============
interface CreateUserDto {
    username: string;
    password: string;
    nickname?: string;
    phone?: string;
    email?: string;
    status?: UserStatus;
    department_id?: string;
    role_ids?: string[];
}
interface UpdateUserDto {
    nickname?: string;
    phone?: string;
    email?: string;
    status?: UserStatus;
    password?: string;
    department_id?: string;
    role_ids?: string[];
}
export namespace User {
    export type Status = UserStatus;
    export type Option = UserOption;
    export type Item = UserItem;
    export type Query = UserQuery;
    export type CreateDto = CreateUserDto;
    export type UpdateDto = UpdateUserDto;
}
