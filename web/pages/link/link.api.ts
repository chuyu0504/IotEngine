import request from '@/lib/http';
import { createSseSnapshotStream } from '@/lib/snapshot_request';
import type { DebugAcquisition } from '@/types/packet_debug';
import type { PaginatedResult } from '@/types/pagination';
import { linkDebugSchema, linkIdSchema, linkListQuerySchema, saveLinkSchema } from './link.schema';
import type { Link } from './link.types';

export const setDebug = (id: string, enabled: boolean) =>
    request.put<void>(
        `/v1/link/${linkIdSchema.parse(id)}/debug`,
        linkDebugSchema.parse({ enabled })
    );
const eventParams = (params?: Link.Query, debugLinkId?: string) => ({
    ...linkListQuerySchema.parse(params ?? {}),
    debugLinkId: debugLinkId ? linkIdSchema.parse(debugLinkId) : undefined,
});
export const getDebugPackets = (id: string, params?: Link.Query) =>
    createSseSnapshotStream<DebugAcquisition[]>(
        '/v1/link/events',
        eventParams(params, id),
        'packets'
    );
export const getList = (params?: Link.Query, debugLinkId?: string) =>
    createSseSnapshotStream<PaginatedResult<Link.Item>>(
        '/v1/link/events',
        eventParams(params, debugLinkId),
        'links'
    );
export const queryList = (params?: Link.Query, signal?: AbortSignal) =>
    request.get<PaginatedResult<Link.Item>>('/v1/link', {
        params: linkListQuerySchema.parse(params ?? {}),
        signal,
    });
export const getEnums = (signal?: AbortSignal) =>
    request.get<Link.Enums>('/v1/link/enums', { signal });
export const getPublicIp = (signal?: AbortSignal) =>
    request.get<{ ip: string }>('/v1/link/public-ip', { signal });
export const create = (data: Link.SaveDto) =>
    request.post<void>('/v1/link', saveLinkSchema.parse(data));
export const update = (id: string, data: Link.SaveDto) =>
    request.put<void>(`/v1/link/${linkIdSchema.parse(id)}`, saveLinkSchema.parse(data));
export const remove = (id: string) => request.delete<void>(`/v1/link/${linkIdSchema.parse(id)}`);
