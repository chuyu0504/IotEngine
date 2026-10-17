import { LiveQueryError } from '@/components/live_query_error';
import {
    ArrowDownOutlined,
    ArrowLeftOutlined,
    ArrowRightOutlined,
    ArrowUpOutlined,
    ControlOutlined,
    EditOutlined,
    MinusOutlined,
    PauseCircleOutlined,
    PlayCircleOutlined,
    PlusOutlined,
    ReloadOutlined,
    SendOutlined,
    StopOutlined,
    VideoCameraOutlined,
} from '@ant-design/icons';
import {
    App,
    Badge,
    Button,
    Card,
    Descriptions,
    Divider,
    Form,
    Input,
    InputNumber,
    Popover,
    Result,
    Select,
    Slider,
    Space,
    Statistic,
    Table,
    Tag,
    Tooltip,
    Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import Hls from 'hls.js';
import mpegts from 'mpegts.js';
import type { ReactNode } from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Endpoint, Events } from 'zlmrtc-client';
import { FormModal } from '@/components/form_modal';
import { PageContainer } from '@/components/page_container';
import { usePermission } from '@/hooks/use_permission';
import type { AdaptiveFlvPlayer } from '@/lib/flv_player/adaptive_flv_player';
import { createAdaptiveFlvPlayer } from '@/lib/flv_player/adaptive_flv_player';
import {
    buildPlaybackCandidates,
    usePreviewLease,
    useGb28181CatalogQuery,
    useGb28181Devices,
    useGb28181Health,
    useGb28181Recording,
    useGb28181RecordingStart,
    useGb28181RecordingStop,
    useGb28181RenameChannel,
    useGb28181RenameDevice,
} from './gb28181.service';
import type { GB28181, PlaybackCandidate, PlaybackCapabilities } from './gb28181.types';
export const onlineTag = (online: boolean) => (
    <Badge status={online ? 'processing' : 'default'} text={online ? '在线' : '离线'} />
);
export const displayText = (value?: string | number | null) => {
    if (value === undefined || value === null || value === '') return '--';
    return value;
};
export const remoteEndpoint = (device?: GB28181.Device) => {
    if (!device) return '--';
    const ip = device.remote_ip || device.remote_address;
    const port = device.remote_port;
    if (!ip) return '--';
    return port ? `${ip}:${port}` : ip;
};
export const registrationSourceTag = (source?: string) => {
    if (source === 'mock') {
        return <Tag color="orange">模拟</Tag>;
    }
    return null;
};
export const ptzCapabilityTag = (channel?: GB28181.Channel) => {
    if (!channel || channel.ptz_type === undefined || channel.ptz_type < 0) {
        return <Tag>云台未知</Tag>;
    }
    return channel.ptz_capable ? <Tag color="green">支持云台</Tag> : <Tag color="red">无云台</Tag>;
};

const { Text } = Typography;
type DeviceStats = {
    onlineDevices: number;
    channelCount: number;
    onlineChannels: number;
};
type DeviceListCardProps = {
    devices: GB28181.Device[];
    filteredDevices: GB28181.Device[];
    selectedDevice?: GB28181.Device;
    stats: DeviceStats;
    loading: boolean;
    canRename: boolean;
    onSelect: (device: GB28181.Device) => void;
    onRename: (device: GB28181.Device) => void;
};
export function DeviceListCard({
    devices,
    filteredDevices,
    selectedDevice,
    stats,
    loading,
    canRename,
    onSelect,
    onRename,
}: DeviceListCardProps) {
    const deviceColumns: ColumnsType<GB28181.Device> = [
        {
            title: '状态',
            dataIndex: 'online',
            width: 78,
            render: (online: boolean) => onlineTag(online),
        },
        {
            title: '设备',
            dataIndex: 'name',
            ellipsis: true,
            render: (name: string, record) => (
                <Space direction="vertical" size={2} className="min-w-0">
                    <Space size={4}>
                        <Text strong>{displayText(name)}</Text>
                        {registrationSourceTag(record.registration_source)}
                        {canRename && (
                            <Tooltip title="编辑摄像头名称">
                                <Button
                                    type="text"
                                    size="small"
                                    icon={<EditOutlined />}
                                    aria-label={`编辑摄像头 ${name || record.id} 的名称`}
                                    onClick={(event) => {
                                        event.stopPropagation();
                                        onRename(record);
                                    }}
                                />
                            </Tooltip>
                        )}
                    </Space>
                    <Text type="secondary" className="text-xs">
                        {record.id}
                    </Text>
                    <Text type="secondary" className="text-xs">
                        IP {remoteEndpoint(record)}
                    </Text>
                </Space>
            ),
        },
        {
            title: '通道',
            width: 64,
            align: 'right',
            render: (_, record) => record.channels.length,
        },
    ];
    return (
        <Card
            size="small"
            className="flex h-full min-h-0 flex-col [&_.ant-card-body]:flex [&_.ant-card-body]:min-h-0 [&_.ant-card-body]:flex-1 [&_.ant-card-body]:flex-col"
            title={
                <Space>
                    <VideoCameraOutlined />
                    设备列表
                </Space>
            }
            extra={
                <Tag color="blue">
                    {stats.onlineDevices}/{devices.length}
                </Tag>
            }
        >
            <div className="grid grid-cols-2 gap-3 mb-3">
                <Statistic
                    title="设备在线"
                    value={stats.onlineDevices}
                    suffix={`/ ${devices.length}`}
                    valueStyle={{ fontSize: 20 }}
                />
                <Statistic
                    title="通道在线"
                    value={stats.onlineChannels}
                    suffix={`/ ${stats.channelCount}`}
                    valueStyle={{ fontSize: 20 }}
                />
            </div>
            <div className="min-h-0 flex-1 overflow-auto">
                <Table
                    sticky
                    rowKey="id"
                    size="small"
                    columns={deviceColumns}
                    dataSource={filteredDevices}
                    loading={loading}
                    pagination={{ pageSize: 10, size: 'small' }}
                    onRow={(record) => ({
                        onClick: () => onSelect(record),
                        className:
                            record.id === selectedDevice?.id
                                ? 'cursor-pointer bg-blue-50'
                                : 'cursor-pointer',
                    })}
                />
            </div>
        </Card>
    );
}

const WEBRTC_CONNECT_TIMEOUT_MS = 4000;
const FALLBACK_FIRST_FRAME_TIMEOUT_MS = 5000;
const WEBRTC_CODEC_RETRY_MS = 400;
const WEBRTC_CODEC_RETRY_COUNT = 10;
type PlayerResources = {
    adaptiveFlv?: AdaptiveFlvPlayer;
    candidateCleanup?: () => void;
    hls?: Hls;
    mpegts?: ReturnType<typeof mpegts.createPlayer>;
    rtc?: Endpoint;
    fallbackTimer?: number;
};
const resetVideoElement = (video: HTMLVideoElement) => {
    video.pause();
    video.removeAttribute('src');
    video.srcObject = null;
    video.load();
};
const closePlayerResources = (resources: PlayerResources, video?: HTMLVideoElement | null) => {
    resources.candidateCleanup?.();
    resources.candidateCleanup = undefined;
    if (resources.fallbackTimer) {
        window.clearTimeout(resources.fallbackTimer);
        resources.fallbackTimer = undefined;
    }
    resources.hls?.destroy();
    resources.hls = undefined;
    resources.mpegts?.destroy();
    resources.mpegts = undefined;
    resources.rtc?.close();
    resources.rtc = undefined;
    resources.adaptiveFlv?.close();
    resources.adaptiveFlv = undefined;
    if (video) resetVideoElement(video);
};
const AUXILIARY_VIDEO_CODECS = new Set(['RTX', 'RED', 'ULPFEC', 'FLEXFEC-03']);
type RtcEndpointWithPeerConnection = Endpoint & {
    pc?: RTCPeerConnection | null;
};
type RtcStatsRecord = {
    id?: string;
    type?: string;
    kind?: string;
    mediaType?: string;
    codecId?: string;
    mimeType?: string;
    payloadType?: number;
    packetsReceived?: number;
    framesDecoded?: number;
};
const normalizeVideoCodecName = (value?: string) => {
    if (!value) return undefined;
    let codec = value.trim();
    const mimeMatch = /^video\/([^;\s]+)/i.exec(codec);
    if (mimeMatch?.[1]) {
        codec = mimeMatch[1];
    }
    codec = codec.split(';')[0].split('/')[0].trim().toUpperCase();
    if (!codec || AUXILIARY_VIDEO_CODECS.has(codec)) return undefined;
    if (codec === 'HEVC' || codec === 'HVC1') return 'H265';
    if (codec === 'AVC' || codec === 'AVC1') return 'H264';
    return codec;
};
const parseVideoCodecFromSdp = (sdp?: string) => {
    if (!sdp) return undefined;
    const lines = sdp.split(/\r?\n/).map((line) => line.trim());
    const videoLineIndex = lines.findIndex((line) => line.startsWith('m=video '));
    if (videoLineIndex < 0) return undefined;
    const payloadTypes = lines[videoLineIndex].split(/\s+/).slice(3);
    const payloadCodecs = new Map<string, string>();
    for (let index = videoLineIndex + 1; index < lines.length; index += 1) {
        const line = lines[index];
        if (line.startsWith('m=')) break;
        const match = /^a=rtpmap:(\d+)\s+([^\s]+)/i.exec(line);
        const codec = normalizeVideoCodecName(match?.[2]);
        if (match?.[1] && codec) payloadCodecs.set(match[1], codec);
    }
    for (const payloadType of payloadTypes) {
        const codec = payloadCodecs.get(payloadType);
        if (codec) return codec;
    }
    return undefined;
};
const getEndpointPeerConnection = (endpoint: Endpoint) =>
    (endpoint as RtcEndpointWithPeerConnection).pc ?? null;
const findWebRtcVideoCodecFromStats = async (pc: RTCPeerConnection) => {
    try {
        const report = await pc.getStats();
        const codecsById = new Map<string, string>();
        const codecsByPayloadType = new Map<number, string>();
        report.forEach((value: RtcStatsRecord) => {
            if (value.type !== 'codec') return;
            const codec = normalizeVideoCodecName(value.mimeType);
            if (!codec) return;
            if (value.id) codecsById.set(value.id, codec);
            if (typeof value.payloadType === 'number')
                codecsByPayloadType.set(value.payloadType, codec);
        });
        let selectedCodecId: string | undefined;
        let selectedPayloadType: number | undefined;
        let selectedScore = -1;
        report.forEach((value: RtcStatsRecord) => {
            if (value.type !== 'inbound-rtp') return;
            if (value.kind !== 'video' && value.mediaType !== 'video') return;
            const score = (value.framesDecoded ?? 0) + (value.packetsReceived ?? 0);
            if (score < selectedScore) return;
            selectedScore = score;
            selectedCodecId = value.codecId;
            selectedPayloadType = value.payloadType;
        });
        if (selectedCodecId) {
            const codec = codecsById.get(selectedCodecId);
            if (codec) return codec;
        }
        if (typeof selectedPayloadType === 'number') {
            return codecsByPayloadType.get(selectedPayloadType);
        }
    } catch {
        return undefined;
    }
    return undefined;
};
const findWebRtcVideoCodec = async (endpoint: Endpoint) => {
    const pc = getEndpointPeerConnection(endpoint);
    if (!pc) return undefined;
    return (
        (await findWebRtcVideoCodecFromStats(pc)) ??
        parseVideoCodecFromSdp(pc.remoteDescription?.sdp)
    );
};
const wait = (duration: number) =>
    new Promise<void>((resolve) => {
        window.setTimeout(resolve, duration);
    });
const watchFirstVideoFrame = (video: HTMLVideoElement, callback: () => void) => {
    let done = false;
    const finish = () => {
        if (done) return;
        done = true;
        callback();
    };
    if (typeof video.requestVideoFrameCallback === 'function') {
        const id = video.requestVideoFrameCallback(finish);
        return () => {
            done = true;
            video.cancelVideoFrameCallback(id);
        };
    }
    video.addEventListener('loadeddata', finish, { once: true });
    return () => {
        done = true;
        video.removeEventListener('loadeddata', finish);
    };
};
const detectPlaybackCapabilities = (video: HTMLVideoElement): PlaybackCapabilities => {
    const featureList = mpegts.getFeatureList();
    return {
        hls: Hls.isSupported() || Boolean(video.canPlayType('application/vnd.apple.mpegurl')),
        mpegts: mpegts.isSupported(),
        mseH265: featureList.mseH265Playback === true,
        softwareVideo:
            typeof Worker !== 'undefined' &&
            typeof WebAssembly !== 'undefined' &&
            typeof VideoFrame !== 'undefined',
        webCodecs:
            typeof VideoDecoder !== 'undefined' &&
            typeof EncodedVideoChunk !== 'undefined' &&
            typeof VideoFrame !== 'undefined',
    };
};
export function Gb28181LivePlayer({ session }: { session: GB28181.PreviewStartResult }) {
    const videoRef = useRef<HTMLVideoElement | null>(null);
    const canvasRef = useRef<HTMLCanvasElement | null>(null);
    const resourcesRef = useRef<PlayerResources>({});
    const mutedRef = useRef(true);
    const [status, setStatus] = useState('准备播放');
    const [activeProtocol, setActiveProtocol] = useState<string>();
    const [audioAvailable, setAudioAvailable] = useState(false);
    const [muted, setMuted] = useState(true);
    const [surface, setSurface] = useState<'video' | 'canvas'>('video');
    useEffect(() => {
        let disposed = false;
        let fallbackStarted = false;
        const resources = resourcesRef.current;
        const playUrls = session.play_urls;
        setAudioAvailable(false);
        const playElement = async (label: string) => {
            const video = videoRef.current;
            if (!video || disposed) return;
            setActiveProtocol(label);
            try {
                await video.play();
            } catch {
                if (!disposed) setStatus('等待浏览器允许播放');
            }
        };
        const updateWebRtcVideoCodec = async (
            endpoint: Endpoint,
            onCodec: (codec: string) => void
        ) => {
            for (let attempt = 0; attempt < WEBRTC_CODEC_RETRY_COUNT; attempt += 1) {
                if (disposed || resources.rtc !== endpoint) return;
                const codec = await findWebRtcVideoCodec(endpoint);
                if (disposed || resources.rtc !== endpoint) return;
                if (codec) {
                    onCodec(codec);
                    return;
                }
                if (attempt < WEBRTC_CODEC_RETRY_COUNT - 1) {
                    await wait(WEBRTC_CODEC_RETRY_MS);
                }
            }
            if (!disposed && resources.rtc === endpoint) onCodec('未知编码');
        };
        const playFallbackCandidate = (candidates: PlaybackCandidate[], index: number) => {
            const video = videoRef.current;
            const canvas = canvasRef.current;
            if (!video || !canvas || disposed) return;
            closePlayerResources(resources, video);
            const candidate = candidates[index];
            if (!candidate) {
                setActiveProtocol(undefined);
                setStatus('没有可用播放地址');
                return;
            }
            let candidateClosed = false;
            let advanced = false;
            const startedAt = performance.now();
            const firstFrameTimer = window.setTimeout(() => {
                playNext();
            }, FALLBACK_FIRST_FRAME_TIMEOUT_MS);
            let stopWatchingFrame: (() => void) | undefined;
            const cleanupCandidate = () => {
                candidateClosed = true;
                advanced = true;
                window.clearTimeout(firstFrameTimer);
                stopWatchingFrame?.();
            };
            const playNext = () => {
                if (advanced || disposed) return;
                advanced = true;
                window.clearTimeout(firstFrameTimer);
                stopWatchingFrame?.();
                playFallbackCandidate(candidates, index + 1);
            };
            const firstFrameStatus = (details?: string) => {
                window.clearTimeout(firstFrameTimer);
                const elapsed = Math.max(0, Math.round(performance.now() - startedAt));
                setStatus(`${details ? `${details} · ` : ''}首帧 ${elapsed}ms`);
            };
            resources.candidateCleanup = cleanupCandidate;
            setAudioAvailable(false);
            setActiveProtocol(candidate.label);
            setStatus('连接中');
            if (candidate.engine === 'adaptive-flv') {
                setSurface('canvas');
                void createAdaptiveFlvPlayer({
                    url: candidate.url,
                    canvas,
                    decoder: candidate.decoder ?? 'native-only',
                    muted: mutedRef.current,
                    onAudioAvailable: (available) => {
                        if (!disposed && !candidateClosed) setAudioAvailable(available);
                    },
                    onVideoMode: (info) => {
                        if (!disposed && !candidateClosed) {
                            setStatus(
                                `${info.codec} · ${info.decoder === 'webcodecs' ? '硬解' : 'Worker软解'} · 解码中`
                            );
                        }
                    },
                    onFirstFrame: (info) => {
                        if (disposed || candidateClosed) return;
                        firstFrameStatus(
                            `${info.codec} · ${info.decoder === 'webcodecs' ? '硬解' : 'Worker软解'}`
                        );
                    },
                    onError: playNext,
                })
                    .then((runtime) => {
                        if (candidateClosed || disposed || advanced) runtime.close();
                        else resources.adaptiveFlv = runtime;
                    })
                    .catch(playNext);
                return;
            }
            setSurface('video');
            stopWatchingFrame = watchFirstVideoFrame(video, () => firstFrameStatus());
            if (candidate.engine === 'hls') {
                if (Hls.isSupported()) {
                    const hls = new Hls({
                        backBufferLength: 5,
                        liveMaxLatencyDurationCount: 3,
                        liveSyncDurationCount: 1,
                        lowLatencyMode: true,
                    });
                    resources.hls = hls;
                    hls.attachMedia(video);
                    hls.on(Hls.Events.MEDIA_ATTACHED, () => {
                        hls.loadSource(candidate.url);
                    });
                    hls.on(Hls.Events.MANIFEST_PARSED, () => {
                        setAudioAvailable(hls.audioTracks.length > 0);
                        void playElement(candidate.label);
                    });
                    hls.on(Hls.Events.ERROR, (_event, data) => {
                        if (data.fatal) playNext();
                    });
                    return;
                }
                if (video.canPlayType('application/vnd.apple.mpegurl')) {
                    video.src = candidate.url;
                    video.addEventListener(
                        'loadedmetadata',
                        () => {
                            void playElement(candidate.label);
                        },
                        {
                            once: true,
                        }
                    );
                    video.addEventListener('error', playNext, { once: true });
                    return;
                }
                playNext();
                return;
            }
            if (mpegts.isSupported()) {
                const player = mpegts.createPlayer(
                    {
                        type: candidate.mediaType ?? 'flv',
                        isLive: true,
                        url: candidate.url,
                    },
                    {
                        enableStashBuffer: false,
                        isLive: true,
                        liveBufferLatencyChasing: true,
                        liveBufferLatencyMaxLatency: 1,
                        liveBufferLatencyMinRemain: 0.1,
                        lazyLoad: false,
                    }
                );
                resources.mpegts = player;
                player.on(mpegts.Events.ERROR, playNext);
                player.on(mpegts.Events.MEDIA_INFO, (info) => {
                    if (candidateClosed || disposed) return;
                    setAudioAvailable(info.hasAudio === true);
                    if (info.videoCodec)
                        setStatus(`${info.videoCodec.toUpperCase()} · 原生解码 · 解码中`);
                });
                player.attachMediaElement(video);
                player.load();
                void playElement(candidate.label);
                return;
            }
            playNext();
        };
        const startFallbackPlayback = () => {
            if (disposed || fallbackStarted) return;
            fallbackStarted = true;
            const video = videoRef.current;
            if (!video) return;
            playFallbackCandidate(
                buildPlaybackCandidates(playUrls, detectPlaybackCapabilities(video)),
                0
            );
        };
        const startWebRtcPlayback = () => {
            const video = videoRef.current;
            if (!video || !playUrls.webrtc || typeof RTCPeerConnection === 'undefined') {
                return false;
            }
            closePlayerResources(resources, video);
            setActiveProtocol('WebRTC');
            setSurface('video');
            setStatus('连接中');
            const startedAt = performance.now();
            let codec = '未知编码';
            let firstFrameElapsed: number | undefined;
            const renderWebRtcStatus = () => {
                setStatus(
                    firstFrameElapsed === undefined
                        ? `${codec} · 获取中`
                        : `${codec} · 首帧 ${firstFrameElapsed}ms`
                );
            };
            const stopWatchingFrame = watchFirstVideoFrame(video, () => {
                firstFrameElapsed = Math.max(0, Math.round(performance.now() - startedAt));
                if (resources.fallbackTimer) {
                    window.clearTimeout(resources.fallbackTimer);
                    resources.fallbackTimer = undefined;
                }
                renderWebRtcStatus();
            });
            resources.candidateCleanup = stopWatchingFrame;
            const endpoint = new Endpoint({
                element: video,
                debug: false,
                zlmsdpUrl: playUrls.webrtc,
                simulcast: false,
                useCamera: false,
                audioEnable: true,
                videoEnable: true,
                recvOnly: true,
                resolution: { w: 0, h: 0 },
                usedatachannel: false,
            });
            resources.rtc = endpoint;
            endpoint.on(Events.WEBRTC_ON_REMOTE_STREAMS, () => {
                const stream = video.srcObject;
                setAudioAvailable(
                    stream instanceof MediaStream && stream.getAudioTracks().length > 0
                );
                void playElement('WebRTC').then(() =>
                    updateWebRtcVideoCodec(endpoint, (nextCodec) => {
                        codec = nextCodec;
                        renderWebRtcStatus();
                    })
                );
            });
            endpoint.on(Events.WEBRTC_NOT_SUPPORT, startFallbackPlayback);
            endpoint.on(Events.WEBRTC_ICE_CANDIDATE_ERROR, startFallbackPlayback);
            endpoint.on(Events.WEBRTC_OFFER_ANSWER_EXCHANGE_FAILED, startFallbackPlayback);
            endpoint.on(Events.WEBRTC_ON_CONNECTION_STATE_CHANGE, (state: unknown) => {
                if (['closed', 'disconnected', 'failed'].includes(String(state))) {
                    startFallbackPlayback();
                }
            });
            resources.fallbackTimer = window.setTimeout(() => {
                resources.fallbackTimer = undefined;
                if (video.readyState < video.HAVE_CURRENT_DATA) {
                    startFallbackPlayback();
                }
            }, WEBRTC_CONNECT_TIMEOUT_MS);
            return true;
        };
        setStatus('准备播放');
        if (!startWebRtcPlayback()) {
            startFallbackPlayback();
        }
        return () => {
            disposed = true;
            closePlayerResources(resources, videoRef.current);
        };
    }, [session.play_urls]);
    const toggleMuted = () => {
        const nextMuted = !mutedRef.current;
        mutedRef.current = nextMuted;
        setMuted(nextMuted);
        if (videoRef.current) videoRef.current.muted = nextMuted;
        resourcesRef.current.adaptiveFlv?.setMuted(nextMuted);
    };
    return (
        <>
            <video
                ref={videoRef}
                controls
                autoPlay
                muted={muted}
                playsInline
                className={`h-full w-full bg-black ${surface === 'video' ? 'block' : 'hidden'}`}
            />
            <canvas
                ref={canvasRef}
                className={`h-full w-full bg-black ${surface === 'canvas' ? 'block' : 'hidden'}`}
            />
            <div className="absolute left-3 top-3 flex items-center gap-2 rounded bg-black/60 px-2 py-1 text-xs text-white">
                {activeProtocol && <span>{activeProtocol}</span>}
                <span>{status}</span>
            </div>
            {audioAvailable && (
                <button
                    type="button"
                    onClick={toggleMuted}
                    className="absolute bottom-3 left-3 z-20 rounded bg-black/60 px-2 py-1 text-xs text-white transition hover:bg-black/80"
                >
                    {muted ? '开启声音' : '关闭声音'}
                </button>
            )}
        </>
    );
}

const PTZ_ACTIONS: Array<{
    action: GB28181.PtzAction;
    title: string;
    icon: ReactNode;
    className?: string;
}> = [
    { action: 'up', title: '上', icon: <ArrowUpOutlined />, className: 'col-start-2' },
    { action: 'left', title: '左', icon: <ArrowLeftOutlined />, className: 'col-start-1' },
    { action: 'stop', title: '停止', icon: <StopOutlined />, className: 'col-start-2' },
    { action: 'right', title: '右', icon: <ArrowRightOutlined />, className: 'col-start-3' },
    { action: 'down', title: '下', icon: <ArrowDownOutlined />, className: 'col-start-2' },
];
type PtzPanelProps = {
    speed: number;
    disabled: boolean;
    onSpeedChange: (speed: number) => void;
    onAction: (action: GB28181.PtzAction) => Promise<unknown>;
    onPosition: (
        position: Pick<GB28181.PtzPositionPayload, 'pan' | 'tilt' | 'zoom'>
    ) => Promise<unknown>;
};
export function PtzPanel({ speed, disabled, onSpeedChange, onAction, onPosition }: PtzPanelProps) {
    const [open, setOpen] = useState(false);
    const [pan, setPan] = useState(0);
    const [tilt, setTilt] = useState(0);
    const [zoom, setZoom] = useState(1);
    const [positioning, setPositioning] = useState(false);
    const holdingRef = useRef<GB28181.PtzAction | null>(null);
    const onActionRef = useRef(onAction);
    useEffect(() => {
        onActionRef.current = onAction;
    }, [onAction]);
    const sendAction = useCallback((action: GB28181.PtzAction) => {
        void onActionRef.current(action).catch(() => undefined);
    }, []);
    const stopHolding = useCallback(() => {
        if (!holdingRef.current) return;
        holdingRef.current = null;
        sendAction('stop');
    }, [sendAction]);
    useEffect(() => {
        if (disabled) stopHolding();
    }, [disabled, stopHolding]);
    useEffect(
        () => () => {
            if (!holdingRef.current) return;
            holdingRef.current = null;
            sendAction('stop');
        },
        [sendAction]
    );
    const startHolding = useCallback(
        (action: GB28181.PtzAction) => {
            if (disabled || holdingRef.current) return;
            holdingRef.current = action;
            sendAction(action);
        },
        [disabled, sendAction]
    );
    const panel = (
        <div className="w-[240px] select-none space-y-3 rounded-lg bg-black/70 p-3 text-white shadow-xl backdrop-blur-md">
            <div className="flex items-center justify-between">
                <Text strong className="!text-white">
                    云台控制
                </Text>
                <Text className="!text-white/70 text-xs">速度 {speed}</Text>
            </div>
            <Text className="!text-white/70 block text-center text-xs">
                按住方向或变倍按钮控制，松开即停止
            </Text>
            <div>
                <Slider
                    min={1}
                    max={255}
                    value={speed}
                    onChange={(value) => onSpeedChange(Number(value))}
                />
            </div>
            <div className="grid grid-cols-3 gap-2 w-[210px] mx-auto">
                {PTZ_ACTIONS.map((item) => (
                    <Tooltip title={item.title} key={item.action}>
                        <Button
                            className={item.className}
                            icon={item.icon}
                            disabled={disabled}
                            danger={item.action === 'stop'}
                            type={item.action === 'stop' ? 'primary' : 'default'}
                            onClick={
                                item.action === 'stop'
                                    ? () => {
                                          holdingRef.current = null;
                                          sendAction('stop');
                                      }
                                    : undefined
                            }
                            onPointerDown={
                                item.action === 'stop'
                                    ? undefined
                                    : (event) => {
                                          event.preventDefault();
                                          startHolding(item.action);
                                      }
                            }
                            onPointerUp={item.action === 'stop' ? undefined : stopHolding}
                            onPointerCancel={item.action === 'stop' ? undefined : stopHolding}
                            onPointerLeave={item.action === 'stop' ? undefined : stopHolding}
                        />
                    </Tooltip>
                ))}
            </div>
            <Space.Compact block>
                <Button
                    icon={<PlusOutlined />}
                    disabled={disabled}
                    onPointerDown={(event) => {
                        event.preventDefault();
                        startHolding('zoomin');
                    }}
                    onPointerUp={stopHolding}
                    onPointerCancel={stopHolding}
                    onPointerLeave={stopHolding}
                >
                    变倍+
                </Button>
                <Button
                    icon={<MinusOutlined />}
                    disabled={disabled}
                    onPointerDown={(event) => {
                        event.preventDefault();
                        startHolding('zoomout');
                    }}
                    onPointerUp={stopHolding}
                    onPointerCancel={stopHolding}
                    onPointerLeave={stopHolding}
                >
                    变倍-
                </Button>
            </Space.Compact>
            <Divider className="!my-3 !border-white/20" />
            <Text strong className="!text-white block">
                绝对定位
            </Text>
            <div className="grid grid-cols-3 gap-2">
                <div className="space-y-1">
                    <Text className="!text-white/70 text-xs">水平角</Text>
                    <InputNumber<number>
                        className="w-full"
                        min={0}
                        max={360}
                        precision={2}
                        value={pan}
                        disabled={disabled}
                        onChange={(value) => setPan(value ?? 0)}
                    />
                </div>
                <div className="space-y-1">
                    <Text className="!text-white/70 text-xs">垂直角</Text>
                    <InputNumber<number>
                        className="w-full"
                        min={-30}
                        max={90}
                        precision={2}
                        value={tilt}
                        disabled={disabled}
                        onChange={(value) => setTilt(value ?? 0)}
                    />
                </div>
                <div className="space-y-1">
                    <Text className="!text-white/70 text-xs">变倍</Text>
                    <InputNumber<number>
                        className="w-full"
                        min={1}
                        max={1000}
                        precision={2}
                        value={zoom}
                        disabled={disabled}
                        onChange={(value) => setZoom(value ?? 1)}
                    />
                </div>
            </div>
            <Button
                block
                type="primary"
                disabled={disabled}
                loading={positioning}
                onClick={async () => {
                    setPositioning(true);
                    try {
                        await onPosition({ pan, tilt, zoom });
                    } finally {
                        setPositioning(false);
                    }
                }}
            >
                定位
            </Button>
        </div>
    );
    return (
        <Popover
            trigger="click"
            placement="topRight"
            arrow={false}
            open={open}
            onOpenChange={(nextOpen) => {
                if (!nextOpen) stopHolding();
                setOpen(nextOpen);
            }}
            content={panel}
            styles={{
                container: {
                    padding: 0,
                    background: 'transparent',
                    boxShadow: 'none',
                },
            }}
        >
            <Button
                type="primary"
                icon={<ControlOutlined />}
                disabled={disabled}
                className="!bg-black/65 !shadow-lg backdrop-blur hover:!bg-black/75"
            >
                云台
            </Button>
        </Popover>
    );
}

type SessionDetailsCardProps = {
    selectedDevice?: GB28181.Device;
    selectedChannel?: GB28181.Channel;
    activeSession: GB28181.PreviewStartResult | null;
};
export function SessionDetailsCard({
    selectedDevice,
    selectedChannel,
    activeSession,
}: SessionDetailsCardProps) {
    return (
        <Card size="small" title="当前会话">
            <Descriptions size="small" column={{ xs: 1, lg: 2 }}>
                <Descriptions.Item label="设备">
                    {displayText(selectedDevice?.id)}
                </Descriptions.Item>
                <Descriptions.Item label="通道">
                    {displayText(selectedChannel?.id)}
                </Descriptions.Item>
                <Descriptions.Item label="云台">
                    {ptzCapabilityTag(selectedChannel)}
                </Descriptions.Item>
                <Descriptions.Item label="设备 IP">
                    {remoteEndpoint(selectedDevice)}
                </Descriptions.Item>
                <Descriptions.Item label="状态">
                    {selectedDevice ? onlineTag(selectedDevice.online) : '--'}
                </Descriptions.Item>
                <Descriptions.Item label="会话">
                    {displayText(activeSession?.session_id)}
                </Descriptions.Item>
                <Descriptions.Item label="RTP">
                    {displayText(activeSession?.rtp_port)}
                </Descriptions.Item>
            </Descriptions>
        </Card>
    );
}

const { Title } = Typography;
const EMPTY_DEVICES: GB28181.Device[] = [];
type RenameTarget =
    | {
          kind: 'device';
          deviceId: string;
          reportedName: string;
      }
    | {
          kind: 'channel';
          deviceId: string;
          channelId: string;
          reportedName: string;
      };
export function Gb28181Page() {
    const { message } = App.useApp();
    const canQuery = usePermission('iot:gb28181:query');
    const canControl = usePermission('iot:gb28181:control');
    const canRecord = usePermission('iot:gb28181:record');
    const [keyword, setKeyword] = useState('');
    const [selectedDeviceId, setSelectedDeviceId] = useState<string>();
    const [selectedChannelId, setSelectedChannelId] = useState<string>();
    const lease = usePreviewLease();
    const activeSession = lease.session;
    const [ptzSpeed, setPtzSpeed] = useState(80);
    const [renameTarget, setRenameTarget] = useState<RenameTarget>();
    const [renameValue, setRenameValue] = useState('');
    const healthQuery = useGb28181Health({ enabled: canQuery });
    const devicesQuery = useGb28181Devices({
        enabled: canQuery && healthQuery.data?.enabled === true,
    });
    const catalogMutation = useGb28181CatalogQuery();
    const renameDeviceMutation = useGb28181RenameDevice();
    const renameChannelMutation = useGb28181RenameChannel();
    const recordingStartMutation = useGb28181RecordingStart();
    const recordingStopMutation = useGb28181RecordingStop();
    const devices = devicesQuery.data?.items ?? EMPTY_DEVICES;
    const filteredDevices = useMemo(() => {
        const text = keyword.trim().toLowerCase();
        if (!text) return devices;
        return devices.filter((device) =>
            [
                device.id,
                device.name,
                device.manufacturer,
                device.remote_address,
                device.remote_ip,
                device.remote_port,
            ]
                .filter(Boolean)
                .some((item) => item.toLowerCase().includes(text))
        );
    }, [devices, keyword]);
    const selectedDevice = useMemo(
        () => devices.find((device) => device.id === selectedDeviceId) ?? devices[0],
        [devices, selectedDeviceId]
    );
    const channels = useMemo<GB28181.Channel[]>(() => {
        if (!selectedDevice) return [];
        if (selectedDevice.channels.length > 0) return selectedDevice.channels;
        return [
            {
                id: selectedDevice.id,
                name: selectedDevice.name || selectedDevice.id,
                reported_name: selectedDevice.reported_name || selectedDevice.name,
                custom_name: selectedDevice.custom_name,
                manufacturer: selectedDevice.manufacturer,
                online: selectedDevice.online,
                ptz_type: -1,
                ptz_capable: false,
            },
        ];
    }, [selectedDevice]);
    const selectedChannel = useMemo(
        () => channels.find((channel) => channel.id === selectedChannelId) ?? channels[0],
        [channels, selectedChannelId]
    );
    const recordingQuery = useGb28181Recording(
        activeSession?.stream_id,
        canRecord && healthQuery.data?.media_capabilities.recording === true
    );
    const isRecording = recordingQuery.data?.recording === true;
    const stats = useMemo(() => {
        const onlineDevices = devices.filter((device) => device.online).length;
        const channelCount = devices.reduce((sum, device) => sum + device.channels.length, 0);
        const onlineChannels = devices.reduce(
            (sum, device) => sum + device.channels.filter((channel) => channel.online).length,
            0
        );
        return { onlineDevices, channelCount, onlineChannels };
    }, [devices]);
    const activePtzDevice = activeSession
        ? devices.find((device) => device.id === activeSession.device_id)
        : undefined;
    const activePtzChannel = activePtzDevice?.channels.find(
        (channel) => channel.id === activeSession?.channel_id
    );
    const activeChannelSupportsPtz =
        activePtzChannel?.ptz_type === undefined ||
        activePtzChannel.ptz_type < 0 ||
        activePtzChannel.ptz_capable;
    const ptzDisabled =
        !canControl ||
        !activeSession ||
        !activePtzDevice?.online ||
        activePtzChannel?.online === false ||
        !activeChannelSupportsPtz;
    const channelOptions = channels.map((channel) => ({
        label: `${channel.name || channel.id} (${channel.id})`,
        value: channel.id,
    }));
    const refreshAll = () => {
        healthQuery.refetch();
        devicesQuery.refetch();
    };
    const openRenameDevice = (device: GB28181.Device) => {
        setRenameTarget({
            kind: 'device',
            deviceId: device.id,
            reportedName: device.reported_name || device.id,
        });
        setRenameValue(device.name || device.id);
    };
    const openRenameChannel = () => {
        if (!selectedDevice || !selectedChannel) return;
        setRenameTarget({
            kind: 'channel',
            deviceId: selectedDevice.id,
            channelId: selectedChannel.id,
            reportedName: selectedChannel.reported_name || selectedChannel.id,
        });
        setRenameValue(selectedChannel.name || selectedChannel.id);
    };
    const submitRename = () => {
        if (!renameTarget) return;
        const name = renameValue.trim();
        if (!name) {
            message.warning('名称不能为空');
            return;
        }
        const close = () => setRenameTarget(undefined);
        if (renameTarget.kind === 'device') {
            renameDeviceMutation.mutate(
                { deviceId: renameTarget.deviceId, name },
                { onSuccess: close }
            );
            return;
        }
        renameChannelMutation.mutate(
            {
                deviceId: renameTarget.deviceId,
                channelId: renameTarget.channelId,
                name,
            },
            { onSuccess: close }
        );
    };
    const currentTarget = () => {
        if (!selectedDevice || !selectedChannel) {
            message.warning('请选择在线设备和通道');
            return null;
        }
        return { deviceId: selectedDevice.id, channelId: selectedChannel.id };
    };
    const handleQueryCatalog = () => {
        if (!selectedDevice) {
            message.warning('请选择设备');
            return;
        }
        catalogMutation.mutate(selectedDevice.id);
    };
    const handleStartPreview = () => {
        const target = currentTarget();
        if (!target) return;
        lease.start(target);
    };
    const handleStopSession = () => {
        if (!activeSession) {
            message.warning('当前没有活动会话');
            return;
        }
        lease.stop();
    };
    const handlePtz = async (action: GB28181.PtzAction) => {
        if (action !== 'stop' && !activeSession) {
            message.warning('请先开始实时预览');
            return;
        }
        try {
            await lease.ptz(action, ptzSpeed);
        } catch (error) {
            if (action !== 'stop') {
                message.error(error instanceof Error ? error.message : '云台指令发送失败');
            }
            throw error;
        }
    };
    const handlePtzPosition = async (
        position: Pick<GB28181.PtzPositionPayload, 'pan' | 'tilt' | 'zoom'>
    ) => {
        if (!activeSession) {
            message.warning('请先开始实时预览');
            return;
        }
        try {
            await lease.ptzPosition(position);
            message.success('云台绝对定位指令已发送');
        } catch (error) {
            message.error(error instanceof Error ? error.message : '云台绝对定位指令发送失败');
            throw error;
        }
    };
    const handleStartRecording = () => {
        if (!activeSession) return;
        recordingStartMutation.mutate(
            { streamId: activeSession.stream_id },
            { onSuccess: () => recordingQuery.refetch() }
        );
    };
    const handleStopRecording = () => {
        if (!activeSession) return;
        recordingStopMutation.mutate(
            { streamId: activeSession.stream_id },
            { onSuccess: () => recordingQuery.refetch() }
        );
    };
    if (!canQuery) {
        return (
            <PageContainer>
                <Result
                    status="403"
                    title="无权限"
                    subTitle="您没有 GB28181 查询权限，请联系管理员。"
                />
            </PageContainer>
        );
    }
    if (!healthQuery.isLoading && healthQuery.data?.enabled !== true) {
        return (
            <PageContainer>
                <Result
                    status="404"
                    title="GB28181 未启用"
                    subTitle="服务端关闭后不会提供菜单或业务接口。"
                />
            </PageContainer>
        );
    }
    const pageHeader = (
        <div className="flex flex-wrap items-center justify-between gap-3">
            <Space size={12} wrap>
                <Title level={4} className="!mb-0">
                    视频监控
                </Title>
                {healthQuery.data?.status === 'disabled' && <Tag>模块未启用</Tag>}
                {(healthQuery.isError || healthQuery.data?.status === 'error') && (
                    <Tag color="error">模块异常</Tag>
                )}
            </Space>
            <Space wrap>
                <Input.Search
                    allowClear
                    placeholder="搜索设备、编号、厂商、地址"
                    value={keyword}
                    onChange={(event) => setKeyword(event.target.value)}
                    className="w-[280px]"
                />
                <Button
                    icon={<ReloadOutlined />}
                    loading={devicesQuery.isFetching}
                    onClick={refreshAll}
                >
                    刷新
                </Button>
            </Space>
        </div>
    );
    return (
        <PageContainer header={pageHeader}>
            <LiveQueryError
                error={devicesQuery.error ?? devicesQuery.connectionError}
                retry={devicesQuery.refetch}
                loading={devicesQuery.isFetching}
            />
            <div className="grid h-full min-h-0 grid-cols-1 grid-rows-[minmax(0,2fr)_minmax(0,3fr)] gap-4 overflow-hidden 2xl:grid-cols-[420px_minmax(0,1fr)] 2xl:grid-rows-1">
                <div className="min-h-0 min-w-0 overflow-hidden">
                    <DeviceListCard
                        devices={devices}
                        filteredDevices={filteredDevices}
                        selectedDevice={selectedDevice}
                        stats={stats}
                        loading={devicesQuery.isLoading}
                        canRename={canControl}
                        onSelect={(device) => {
                            setSelectedDeviceId(device.id);
                            setSelectedChannelId(undefined);
                        }}
                        onRename={openRenameDevice}
                    />
                </div>

                <div className="min-h-0 min-w-0 space-y-4 overflow-y-auto pr-1">
                    <Card
                        size="small"
                        title={
                            <Space wrap>
                                <PlayCircleOutlined />
                                实时预览
                                {selectedDevice && (
                                    <Tag>{selectedDevice.name || selectedDevice.id}</Tag>
                                )}
                                {ptzCapabilityTag(selectedChannel)}
                                <Select
                                    size="small"
                                    value={selectedChannel?.id}
                                    placeholder="选择通道"
                                    options={channelOptions}
                                    disabled={!selectedDevice}
                                    className="min-w-[260px]"
                                    onChange={setSelectedChannelId}
                                />
                                <Button
                                    size="small"
                                    icon={<EditOutlined />}
                                    disabled={!canControl || !selectedChannel}
                                    onClick={openRenameChannel}
                                >
                                    编辑通道名
                                </Button>
                            </Space>
                        }
                        extra={
                            <Space wrap>
                                <Button
                                    icon={<SendOutlined />}
                                    disabled={!selectedDevice || !canControl}
                                    loading={catalogMutation.isPending}
                                    onClick={handleQueryCatalog}
                                >
                                    目录查询
                                </Button>
                                <Button
                                    type="primary"
                                    icon={<PlayCircleOutlined />}
                                    disabled={!canControl}
                                    loading={lease.starting}
                                    onClick={handleStartPreview}
                                >
                                    开始预览
                                </Button>
                                <Button
                                    danger
                                    icon={<PauseCircleOutlined />}
                                    disabled={!activeSession || !canControl}
                                    loading={lease.stopping}
                                    onClick={handleStopSession}
                                >
                                    停止
                                </Button>
                                {isRecording && <Tag color="red">录像中</Tag>}
                                <Button
                                    disabled={
                                        !canRecord ||
                                        !activeSession ||
                                        healthQuery.data?.media_capabilities.recording !== true ||
                                        isRecording
                                    }
                                    loading={recordingStartMutation.isPending}
                                    onClick={handleStartRecording}
                                >
                                    开始录像
                                </Button>
                                <Button
                                    disabled={!canRecord || !activeSession || !isRecording}
                                    loading={recordingStopMutation.isPending}
                                    onClick={handleStopRecording}
                                >
                                    停止录像
                                </Button>
                            </Space>
                        }
                    >
                        <div className="relative aspect-video overflow-hidden rounded bg-black">
                            {activeSession ? (
                                <Gb28181LivePlayer
                                    key={activeSession.session_id}
                                    session={activeSession}
                                />
                            ) : (
                                <div className="flex h-full flex-col items-center justify-center text-white/70">
                                    <VideoCameraOutlined className="mb-3 text-5xl" />
                                    <Text className="!text-white/80">
                                        {selectedDevice
                                            ? `${selectedDevice.name || selectedDevice.id} / ${selectedChannel?.name || selectedChannel?.id || '未选择通道'}`
                                            : '请选择设备和通道'}
                                    </Text>
                                </div>
                            )}

                            {activeSession && activeChannelSupportsPtz ? (
                                <div className="absolute bottom-14 right-3 z-20">
                                    <PtzPanel
                                        speed={ptzSpeed}
                                        disabled={ptzDisabled}
                                        onSpeedChange={setPtzSpeed}
                                        onAction={handlePtz}
                                        onPosition={handlePtzPosition}
                                    />
                                </div>
                            ) : null}
                        </div>
                    </Card>

                    <SessionDetailsCard
                        selectedDevice={selectedDevice}
                        selectedChannel={selectedChannel}
                        activeSession={activeSession}
                    />
                </div>
            </div>
            <FormModal
                title={renameTarget?.kind === 'device' ? '编辑摄像头名称' : '编辑通道名称'}
                open={Boolean(renameTarget)}
                okText="保存"
                cancelText="取消"
                confirmLoading={renameDeviceMutation.isPending || renameChannelMutation.isPending}
                onOk={submitRename}
                onCancel={() => setRenameTarget(undefined)}
                destroyOnHidden
            >
                <Form layout="vertical" onFinish={submitRename}>
                    <Form.Item
                        label="名称"
                        required
                        extra={`设备上报名称：${renameTarget?.reportedName || '--'}`}
                    >
                        <Input
                            autoFocus
                            maxLength={255}
                            showCount
                            value={renameValue}
                            placeholder="请输入名称"
                            onChange={(event) => setRenameValue(event.target.value)}
                            onPressEnter={submitRename}
                        />
                    </Form.Item>
                </Form>
            </FormModal>
        </PageContainer>
    );
}

export default Gb28181Page;
