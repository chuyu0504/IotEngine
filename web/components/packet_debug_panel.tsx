import { BugOutlined, FileTextOutlined } from '@ant-design/icons';
import { FitAddon } from '@xterm/addon-fit';
import { Terminal } from '@xterm/xterm';
import { Alert, App, Button, Modal, Space, Tooltip } from 'antd';
import '@xterm/xterm/css/xterm.css';
import { useEffect, useMemo, useRef, useState } from 'react';
import type { DebugAcquisition } from '@/types/packet_debug';
import { formatDebugTerminal } from '@/utils/packet_debug';

interface Props {
    scope: 'device' | 'link';
    protocol: string;
    title: string;
    buttonClassName?: string;
    enabled: boolean;
    open: boolean;
    pending: boolean;
    loading: boolean;
    error?: Error | null;
    acquisitions?: DebugAcquisition[];
    onToggle: () => void;
    onOpen: () => void;
    onClose: () => void;
}
export function PacketDebugPanel(props: Props) {
    const active = props.enabled;
    const toggleTitle = props.enabled ? '关闭调试' : '开启调试';
    useEffect(() => {
        if (props.open && !props.enabled) props.onClose();
    }, [props.open, props.enabled, props.onClose]);
    return (
        <>
            <Space size={2}>
                <Tooltip title={toggleTitle}>
                    <Button
                        size="small"
                        type="text"
                        className={props.buttonClassName}
                        aria-label={toggleTitle}
                        aria-pressed={props.enabled}
                        icon={
                            <BugOutlined
                                style={active ? { color: 'var(--ant-color-primary)' } : undefined}
                            />
                        }
                        loading={props.pending}
                        onClick={props.onToggle}
                    />
                </Tooltip>
                {active && (
                    <Tooltip title="查看调试报文">
                        <Button
                            size="small"
                            type="text"
                            className={props.buttonClassName}
                            aria-label="查看调试报文"
                            icon={<FileTextOutlined />}
                            onClick={props.onOpen}
                        />
                    </Tooltip>
                )}
            </Space>
            <Modal
                title={props.title}
                width="min(1440px, 96vw)"
                centered
                modalRender={(modal) => (
                    <div
                        style={{
                            resize: 'both',
                            overflow: 'hidden',
                            width: '100%',
                            height: '92dvh',
                            minWidth: 'min(640px, 96vw)',
                            maxWidth: '96vw',
                            minHeight: 360,
                            maxHeight: '96dvh',
                        }}
                    >
                        {modal}
                    </div>
                )}
                destroyOnHidden
                styles={{
                    container: {
                        height: '100%',
                        display: 'flex',
                        flexDirection: 'column',
                        overflow: 'hidden',
                    },
                    body: { flex: 1, minHeight: 0, overflow: 'hidden' },
                    header: { flexShrink: 0 },
                    footer: { flexShrink: 0 },
                }}
                open={props.open && active}
                onCancel={props.onClose}
                footer={
                    <Space>
                        <Button loading={props.pending} onClick={props.onToggle}>
                            {props.enabled ? '关闭调试' : '开启调试'}
                        </Button>
                        <Button onClick={props.onClose}>关闭窗口</Button>
                    </Space>
                }
            >
                <div className="flex h-full min-h-0 flex-col gap-3">
                    <Alert
                        type={props.enabled ? 'info' : 'warning'}
                        showIcon
                        title={
                            props.enabled
                                ? '调试开关已开启，空闲 5 分钟后服务端自动关闭'
                                : '调试已关闭'
                        }
                    />
                    {props.error && (
                        <Alert
                            type="error"
                            title="报文读取失败"
                            description={props.error.message}
                        />
                    )}
                    {props.open && active && (
                        <PacketTerminal key={`${props.scope}:${props.title}`} {...props} />
                    )}
                </div>
            </Modal>
        </>
    );
}

function PacketTerminal(props: Props) {
    const host = useRef<HTMLDivElement>(null);
    const terminalRef = useRef<Terminal | null>(null);
    const previousText = useRef('');
    const [paused, setPaused] = useState(false);
    const [ready, setReady] = useState(false);
    const { message } = App.useApp();
    const text = useMemo(
        () => formatDebugTerminal(props.scope, props.protocol, props.acquisitions ?? []),
        [props.scope, props.protocol, props.acquisitions]
    );
    useEffect(() => {
        if (!host.current) return;
        const terminal = new Terminal({
            disableStdin: true,
            convertEol: true,
            scrollback: 100000,
            smoothScrollDuration: window.matchMedia('(prefers-reduced-motion: reduce)').matches
                ? 0
                : 180,
            fontSize: 13,
            lineHeight: 1.5,
            fontFamily: 'Consolas, "Cascadia Mono", "Courier New", monospace',
            cursorBlink: false,
            cursorInactiveStyle: 'none',
            theme: { background: '#0d1117', foreground: '#d6e2ee', selectionBackground: '#284766' },
        });
        const fit = new FitAddon();
        terminal.loadAddon(fit);
        terminal.open(host.current);
        terminalRef.current = terminal;
        const resize = new ResizeObserver(() => {
            if (host.current && host.current.clientWidth > 0 && host.current.clientHeight > 0)
                fit.fit();
        });
        resize.observe(host.current);
        const selection = terminal.onSelectionChange(() => {
            if (terminal.hasSelection()) setPaused(true);
        });
        terminal.attachCustomKeyEventHandler((event) => {
            if (
                event.type === 'keydown' &&
                (event.ctrlKey || event.metaKey) &&
                event.key.toLowerCase() === 'c'
            ) {
                const selected = terminal.getSelection();
                if (selected)
                    void navigator.clipboard
                        .writeText(selected)
                        .catch(() => message.error('复制失败'));
                return false;
            }
            return true;
        });
        setReady(true);
        return () => {
            resize.disconnect();
            selection.dispose();
            terminal.dispose();
            terminalRef.current = null;
            previousText.current = '';
        };
    }, [message]);
    useEffect(() => {
        const terminal = terminalRef.current;
        if (!ready || !terminal || paused) return;
        const content = text || (props.loading ? '正在读取调试日志…' : '暂无调试报文');
        if (content === previousText.current) return;
        const previous = previousText.current;
        previousText.current = content;
        if (previous && content.startsWith(previous)) {
            terminal.write(content.slice(previous.length), () => terminal.scrollToBottom());
        } else {
            // 快照中的状态变化替换原内容，不追加成另一份报文。
            terminal.write(`\x1b[3J\x1b[2J\x1b[H\x1b[?25l${content}`, () =>
                terminal.scrollToBottom()
            );
        }
    }, [text, paused, ready, props.loading]);
    return (
        <>
            <div className="flex flex-wrap items-center justify-between gap-2">
                <span>
                    {props.protocol} · {props.scope === 'device' ? '采集日志' : '链路收发日志'}
                </span>
                <Space>
                    <Button
                        size="small"
                        onClick={async () => {
                            try {
                                await navigator.clipboard.writeText(text);
                                message.success('日志已复制');
                            } catch {
                                message.error('复制失败');
                            }
                        }}
                    >
                        复制全部
                    </Button>
                    <Button
                        size="small"
                        onClick={() => {
                            if (paused) {
                                terminalRef.current?.clearSelection();
                                terminalRef.current?.scrollToBottom();
                            }
                            setPaused(!paused);
                        }}
                    >
                        {paused ? '恢复实时跟随' : '暂停'}
                    </Button>
                </Space>
            </div>
            <div
                className="min-h-0 flex-1 overflow-hidden rounded bg-[#0d1117] p-3"
                onWheel={(event) => {
                    if (event.deltaY < 0) setPaused(true);
                }}
            >
                <div ref={host} className="h-full w-full" />
            </div>
        </>
    );
}
