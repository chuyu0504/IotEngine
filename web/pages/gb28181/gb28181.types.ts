export namespace GB28181 {
    export interface Health {
        status: string;
        service: string;
        enabled: boolean;
        started: boolean;
        error: string;
        media_ports: {
            http: number;
            https: number;
            rtsp: number;
            rtsps: number;
            rtmp: number;
            rtmps: number;
            rtc: number;
            srt: number;
        };
        media_capabilities: {
            faac: boolean;
            ffmpeg: boolean;
            hls: boolean;
            mp4: boolean;
            rtp_proxy: boolean;
            srt: boolean;
            sctp: boolean;
            web_rtc: boolean;
            x264: boolean;
            video_stack: boolean;
            tls: boolean;
            recording: boolean;
        };
    }
    export interface Channel {
        id: string;
        name: string;
        reported_name: string;
        custom_name: string;
        manufacturer: string;
        online: boolean;
        ptz_type: number;
        ptz_capable: boolean;
    }
    export interface Device {
        id: string;
        name: string;
        reported_name: string;
        custom_name: string;
        manufacturer: string;
        remote_address: string;
        remote_ip: string;
        remote_port: string;
        registration_source: string;
        last_seen_at: string;
        online: boolean;
        channels: Channel[];
    }
    export interface Items<T> {
        items: T[];
    }
    export interface PlayUrls {
        http_flv: string;
        ws_flv: string;
        http_ts: string;
        hls: string;
        webrtc: string;
        rtsp: string;
        rtmp: string;
    }
    export interface PreviewStartResult {
        sent: boolean;
        session_id: string;
        device_id: string;
        channel_id: string;
        stream_id: string;
        ssrc: string;
        rtp_port: number;
        lease_timeout_seconds: number;
        play_urls: PlayUrls;
    }
    export interface PreviewStopResult {
        stopped: boolean;
        session_id: string;
        stream_id: string;
        bye_sent: boolean;
        rtp_server_closed: boolean;
    }
    export interface CommandResult {
        sent: boolean;
        device_id?: string;
        channel_id?: string;
        action?: string;
        speed?: number;
        recording?: boolean;
    }
    export interface StartPreviewPayload {
        deviceId: string;
        channelId: string;
    }
    export interface StopPreviewPayload {
        sessionId: string;
    }
    export interface PtzPayload {
        deviceId: string;
        channelId: string;
        action: PtzAction;
        speed: number;
    }
    export interface PtzPositionPayload {
        deviceId: string;
        channelId: string;
        pan: number;
        tilt: number;
        zoom: number;
    }
    export interface StreamPayload {
        streamId: string;
    }
    export interface DeviceNamePayload {
        deviceId: string;
        name: string;
    }
    export interface ChannelNamePayload extends DeviceNamePayload {
        channelId: string;
    }
    export type PtzAction = 'left' | 'right' | 'up' | 'down' | 'zoomin' | 'zoomout' | 'stop';
}

export type PlaybackCandidate = {
    decoder?: 'native-only' | 'software-only';
    engine: 'adaptive-flv' | 'hls' | 'mpegts';
    label: string;
    mediaType?: 'flv' | 'mpegts';
    url: string;
};

export type PlaybackCapabilities = {
    hls: boolean;
    mpegts: boolean;
    mseH265: boolean;
    softwareVideo: boolean;
    webCodecs: boolean;
};
