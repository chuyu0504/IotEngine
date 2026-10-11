import request from '@/lib/http';
import { createSseSnapshotStream } from '@/lib/snapshot_request';
import type { GB28181 } from './gb28181.types';

const devicePath = (id: string) => `/v1/gb28181/devices/${encodeURIComponent(id)}`;
const channelPath = (payload: GB28181.StartPreviewPayload) =>
    `${devicePath(payload.deviceId)}/channels/${encodeURIComponent(payload.channelId)}`;
const previewPath = (sessionId: string) => `/v1/gb28181/previews/${encodeURIComponent(sessionId)}`;
const streamPath = (streamId: string) => `/v1/gb28181/streams/${encodeURIComponent(streamId)}`;

export function stopPreviewOnExit(sessionId: string) {
    void request
        .post<void>(`${previewPath(sessionId)}/stop`, undefined, {
            _silent: true,
            timeout: 5000,
            keepalive: true,
        })
        .catch(() => undefined);
}
export const getHealth = (signal?: AbortSignal) =>
    request.get<GB28181.Health>('/v1/gb28181/health', { signal });
export const getDevices = () =>
    createSseSnapshotStream<GB28181.Items<GB28181.Device>>('/v1/gb28181/devices/events');
export const renameDevice = (payload: GB28181.DeviceNamePayload) =>
    request.put<void>(`${devicePath(payload.deviceId)}/name`, { name: payload.name });
export const renameChannel = (payload: GB28181.ChannelNamePayload) =>
    request.put<void>(`${channelPath(payload)}/name`, { name: payload.name });
export const queryCatalog = (deviceId: string) =>
    request.post<GB28181.CommandResult>(`${devicePath(deviceId)}/catalog/query`);
export const startPreview = (payload: GB28181.StartPreviewPayload) =>
    request.post<GB28181.PreviewStartResult>(`${channelPath(payload)}/preview/start`);
export const stopPreview = (payload: GB28181.StopPreviewPayload) =>
    request.post<GB28181.PreviewStopResult>(`${previewPath(payload.sessionId)}/stop`);
export const renewPreview = (payload: GB28181.StopPreviewPayload) =>
    request.post<GB28181.CommandResult>(`${previewPath(payload.sessionId)}/heartbeat`);
export const sendPtz = (payload: GB28181.PtzPayload) =>
    request.post<GB28181.CommandResult>(
        `${channelPath(payload)}/ptz/${encodeURIComponent(payload.action)}`,
        { speed: payload.speed }
    );
export const sendPtzPosition = (payload: GB28181.PtzPositionPayload) =>
    request.post<GB28181.CommandResult>(`${channelPath(payload)}/ptz/position/set`, {
        pan: payload.pan,
        tilt: payload.tilt,
        zoom: payload.zoom,
    });
export const getRecording = (payload: GB28181.StreamPayload, signal?: AbortSignal) =>
    request.get<GB28181.CommandResult>(`${streamPath(payload.streamId)}/recording`, { signal });
export const startRecording = (payload: GB28181.StreamPayload) =>
    request.post<GB28181.CommandResult>(`${streamPath(payload.streamId)}/recording/start`);
export const stopRecording = (payload: GB28181.StreamPayload) =>
    request.post<GB28181.CommandResult>(`${streamPath(payload.streamId)}/recording/stop`);
