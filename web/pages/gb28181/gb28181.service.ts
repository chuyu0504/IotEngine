import { useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useMutationWithMessage } from '@/hooks/use_mutation';
import { useSnapshotQuery } from '@/hooks/use_snapshot_query';
import * as api from './gb28181.api';
import type { GB28181, PlaybackCandidate, PlaybackCapabilities } from './gb28181.types';

/** `devices` 是 SSE 快照缓存，只随推送更新；写入后只失效录像状态等 HTTP 查询。 */
export const gb28181Keys = {
    health: () => ['gb28181', 'health'] as const,
    devices: () => ['gb28181', 'devices'] as const,
    streams: () => ['gb28181', 'streams'] as const,
    recording: (streamId: string) => ['gb28181', 'streams', streamId, 'recording'] as const,
};
export function useGb28181Health(
    options?: Omit<UseQueryOptions<GB28181.Health>, 'queryKey' | 'queryFn'>
) {
    return useQuery({
        queryKey: gb28181Keys.health(),
        queryFn: ({ signal }) => api.getHealth(signal),
        retry: false,
        ...options,
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
}
export function useGb28181Devices(options?: { enabled?: boolean }) {
    return useSnapshotQuery({
        queryKey: gb28181Keys.devices(),
        queryFn: api.getDevices,
        enabled: options?.enabled ?? true,
    });
}
export function useGb28181CatalogQuery() {
    return useMutationWithMessage({
        mutationFn: api.queryCatalog,
        successMessage: '目录查询已发送',
    });
}
export function useGb28181RenameDevice() {
    return useMutationWithMessage<void, GB28181.DeviceNamePayload>({
        mutationFn: api.renameDevice,
        successMessage: '摄像头名称已更新',
    });
}
export function useGb28181RenameChannel() {
    return useMutationWithMessage<void, GB28181.ChannelNamePayload>({
        mutationFn: api.renameChannel,
        successMessage: '通道名称已更新',
    });
}
export function useGb28181PreviewStart() {
    return useMutationWithMessage<GB28181.PreviewStartResult, GB28181.StartPreviewPayload>({
        mutationFn: api.startPreview,
        successMessage: '预览已发起',
        invalidateKeys: [gb28181Keys.streams()],
    });
}
export function useGb28181PreviewStop() {
    return useMutationWithMessage<GB28181.PreviewStopResult, GB28181.StopPreviewPayload>({
        mutationFn: api.stopPreview,
        successMessage: '会话已停止',
        invalidateKeys: [gb28181Keys.streams()],
    });
}
export function useGb28181Recording(streamId?: string, enabled = true) {
    return useQuery({
        queryKey: gb28181Keys.recording(streamId ?? ''),
        queryFn: ({ signal }) => api.getRecording({ streamId: streamId ?? '' }, signal),
        enabled: enabled && Boolean(streamId),
        refetchOnWindowFocus: false,
        refetchOnReconnect: false,
    });
}
export function useGb28181RecordingStart() {
    return useMutationWithMessage<GB28181.CommandResult, GB28181.StreamPayload>({
        mutationFn: api.startRecording,
        successMessage: '录像已开始',
        invalidateKeys: [gb28181Keys.streams()],
    });
}
export function useGb28181RecordingStop() {
    return useMutationWithMessage<GB28181.CommandResult, GB28181.StreamPayload>({
        mutationFn: api.stopRecording,
        successMessage: '录像已停止',
        invalidateKeys: [gb28181Keys.streams()],
    });
}

interface PtzMotion {
    session: GB28181.PreviewStartResult;
    speed: number;
}

/**
 * 实时预览会话租约：按租约时长的 1/3 续约，离开页面、关闭标签页或切换会话时停止云台动作并释放旧会话。
 */
export function usePreviewLease() {
    const startMutation = useGb28181PreviewStart();
    const stopMutation = useGb28181PreviewStop();
    const { mutate: startPreview } = startMutation;
    const { mutate: stopPreview } = stopMutation;
    const [session, setSession] = useState<GB28181.PreviewStartResult | null>(null);
    const sessionRef = useRef<GB28181.PreviewStartResult | null>(null);
    const motionRef = useRef<PtzMotion | null>(null);
    const release = useCallback(() => {
        const current = sessionRef.current;
        if (!current) return;
        const motion = motionRef.current;
        motionRef.current = null;
        if (motion) {
            void api
                .sendPtz({
                    deviceId: motion.session.device_id,
                    channelId: motion.session.channel_id,
                    action: 'stop',
                    speed: motion.speed,
                })
                .catch(() => undefined);
        }
        sessionRef.current = null;
        api.stopPreviewOnExit(current.session_id);
    }, []);
    useEffect(() => {
        if (!session) return;
        const leaseSeconds = Math.max(3, session.lease_timeout_seconds || 90);
        const renewal = window.setInterval(
            () => {
                void api.renewPreview({ sessionId: session.session_id }).catch(() => undefined);
            },
            Math.max(1000, Math.floor((leaseSeconds * 1000) / 3))
        );
        return () => window.clearInterval(renewal);
    }, [session]);
    useEffect(() => {
        window.addEventListener('pagehide', release);
        window.addEventListener('beforeunload', release);
        return () => {
            window.removeEventListener('pagehide', release);
            window.removeEventListener('beforeunload', release);
            release();
        };
    }, [release]);
    const start = useCallback(
        (target: GB28181.StartPreviewPayload) =>
            startPreview(target, {
                onSuccess: (result) => {
                    const previous = sessionRef.current;
                    if (previous && previous.session_id !== result.session_id) {
                        api.stopPreviewOnExit(previous.session_id);
                    }
                    sessionRef.current = result;
                    setSession(result);
                },
            }),
        [startPreview]
    );
    const stop = useCallback(() => {
        const current = sessionRef.current;
        if (!current) return;
        stopPreview(
            { sessionId: current.session_id },
            {
                onSuccess: () => {
                    if (sessionRef.current?.session_id !== current.session_id) return;
                    motionRef.current = null;
                    sessionRef.current = null;
                    setSession(null);
                },
            }
        );
    }, [stopPreview]);
    /** 云台动作作用于当前会话；`stop` 优先作用于发起动作的会话。没有目标会话时不发送。 */
    const ptz = useCallback(async (action: GB28181.PtzAction, speed: number) => {
        const target =
            action === 'stop'
                ? (motionRef.current?.session ?? sessionRef.current)
                : sessionRef.current;
        motionRef.current = action === 'stop' || !target ? null : { session: target, speed };
        if (!target) return;
        await api.sendPtz({
            deviceId: target.device_id,
            channelId: target.channel_id,
            action,
            speed,
        });
    }, []);
    const ptzPosition = useCallback(
        async (position: Pick<GB28181.PtzPositionPayload, 'pan' | 'tilt' | 'zoom'>) => {
            const target = sessionRef.current;
            if (!target) return;
            await api.sendPtzPosition({
                deviceId: target.device_id,
                channelId: target.channel_id,
                ...position,
            });
        },
        []
    );
    return {
        session,
        start,
        stop,
        starting: startMutation.isPending,
        stopping: stopMutation.isPending,
        ptz,
        ptzPosition,
    };
}

const compact = (candidates: Array<PlaybackCandidate | null>) =>
    candidates.filter((candidate): candidate is PlaybackCandidate => Boolean(candidate?.url));
export function buildPlaybackCandidates(
    urls: GB28181.PlayUrls,
    capabilities: PlaybackCapabilities
): PlaybackCandidate[] {
    const nativeFlv = capabilities.webCodecs
        ? compact([
              urls.ws_flv
                  ? {
                        decoder: 'native-only',
                        engine: 'adaptive-flv',
                        label: 'WS-FLV · WebCodecs',
                        url: urls.ws_flv,
                    }
                  : null,
              urls.http_flv
                  ? {
                        decoder: 'native-only',
                        engine: 'adaptive-flv',
                        label: 'HTTP-FLV · WebCodecs',
                        url: urls.http_flv,
                    }
                  : null,
          ])
        : [];
    const mse = capabilities.mpegts
        ? compact([
              urls.http_ts
                  ? {
                        engine: 'mpegts',
                        label: 'HTTP-TS · 原生解码',
                        mediaType: 'mpegts',
                        url: urls.http_ts,
                    }
                  : null,
              urls.ws_flv
                  ? {
                        engine: 'mpegts',
                        label: 'WS-FLV · 原生解码',
                        mediaType: 'flv',
                        url: urls.ws_flv,
                    }
                  : null,
              urls.http_flv
                  ? {
                        engine: 'mpegts',
                        label: 'HTTP-FLV · 原生解码',
                        mediaType: 'flv',
                        url: urls.http_flv,
                    }
                  : null,
          ])
        : [];
    const softwareFlv = capabilities.softwareVideo
        ? compact([
              urls.ws_flv
                  ? {
                        decoder: 'software-only',
                        engine: 'adaptive-flv',
                        label: 'WS-FLV · Worker软解',
                        url: urls.ws_flv,
                    }
                  : null,
              urls.http_flv
                  ? {
                        decoder: 'software-only',
                        engine: 'adaptive-flv',
                        label: 'HTTP-FLV · Worker软解',
                        url: urls.http_flv,
                    }
                  : null,
          ])
        : [];
    const ordered = capabilities.mseH265
        ? [...mse.slice(0, 1), ...nativeFlv, ...mse.slice(1), ...softwareFlv]
        : [...nativeFlv, ...mse, ...softwareFlv];
    if (capabilities.hls && urls.hls) {
        ordered.push({ engine: 'hls', label: 'HLS', url: urls.hls });
    }
    return ordered;
}
