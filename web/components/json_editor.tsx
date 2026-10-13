import { json, jsonParseLinter } from '@codemirror/lang-json';
import { type Diagnostic, forEachDiagnostic, linter, lintGutter } from '@codemirror/lint';
import CodeMirror, { EditorView, keymap, type ViewUpdate } from '@uiw/react-codemirror';
import { Button, theme } from 'antd';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { formatJson } from '@/utils/json';

const basicSetup = { lineNumbers: true, foldGutter: true, highlightActiveLine: true };
const parseLint = jsonParseLinter();

interface JsonEditorProps {
    value?: string;
    onChange?: (value: string) => void;
    label: string;
    height?: string;
    placeholder?: string;
    readOnly?: boolean;
    format?: (value: string) => string;
    onSelectionChange?: (from: number, to: number) => void;
}

export function JsonEditor({
    value = '',
    onChange,
    label,
    height = '220px',
    placeholder,
    readOnly = false,
    format = formatJson,
    onSelectionChange,
}: JsonEditorProps) {
    const { token } = theme.useToken();
    const editor = useRef<EditorView | null>(null);
    useEffect(
        () => () => {
            editor.current = null;
        },
        []
    );
    const [errors, setErrors] = useState<
        { message: string; from: number; to: number; line: number; column: number }[]
    >([]);
    const formatDocument = useCallback(
        (view: EditorView) => {
            const source = view.state.doc.toString();
            if (readOnly || !source.trim()) return false;
            try {
                const formatted = format(source);
                if (formatted !== source)
                    view.dispatch({ changes: { from: 0, to: source.length, insert: formatted } });
                return true;
            } catch {
                return false;
            }
        },
        [format, readOnly]
    );
    const extensions = useMemo(
        () => [
            json(),
            lintGutter(),
            linter(
                (view) => {
                    if (!view.state.doc.length) return [];
                    const syntax = parseLint(view);
                    if (syntax.length) return syntax;
                    try {
                        format(view.state.doc.toString());
                        return [];
                    } catch (error) {
                        const offset =
                            error instanceof Error &&
                            'offset' in error &&
                            typeof error.offset === 'number'
                                ? error.offset
                                : 0;
                        const from = Math.max(0, Math.min(offset, view.state.doc.length));
                        return [
                            {
                                from,
                                to: Math.min(from + 1, view.state.doc.length),
                                severity: 'error',
                                message: error instanceof Error ? error.message : 'JSON 无效',
                            } satisfies Diagnostic,
                        ];
                    }
                },
                { delay: 200 }
            ),
            keymap.of([{ key: 'Mod-Shift-f', run: formatDocument }]),
            EditorView.contentAttributes.of({ 'aria-label': label, 'aria-multiline': 'true' }),
            EditorView.domEventHandlers({
                blur: (_event, view) => {
                    formatDocument(view);
                },
            }),
        ],
        [format, formatDocument, label]
    );
    const editorTheme = useMemo(
        () =>
            EditorView.theme({
                '&': {
                    backgroundColor: token.colorBgContainer,
                    color: token.colorText,
                    fontSize: '13px',
                },
                '.cm-gutters': {
                    backgroundColor: token.colorFillQuaternary,
                    color: token.colorTextSecondary,
                    borderColor: token.colorBorder,
                },
                '.cm-content': { fontFamily: 'ui-monospace, SFMono-Regular, Consolas, monospace' },
                '.cm-scroller': { overflow: 'auto' },
            }),
        [
            token.colorBgContainer,
            token.colorBorder,
            token.colorFillQuaternary,
            token.colorText,
            token.colorTextSecondary,
        ]
    );
    const update = useCallback(
        (change: ViewUpdate) => {
            const diagnostics: typeof errors = [];
            forEachDiagnostic(change.state, (diagnostic, from, to) => {
                if (diagnostic.severity !== 'error') return;
                const line = change.state.doc.lineAt(from);
                diagnostics.push({
                    message: diagnostic.message,
                    from,
                    to,
                    line: line.number,
                    column: from - line.from + 1,
                });
            });
            setErrors((previous) =>
                JSON.stringify(previous) === JSON.stringify(diagnostics) ? previous : diagnostics
            );
            if (change.selectionSet || change.docChanged) {
                const selection = change.state.selection.main;
                onSelectionChange?.(selection.from, selection.to);
            }
            if (change.transactions.some((transaction) => transaction.isUserEvent('input.paste'))) {
                queueMicrotask(() => {
                    if (editor.current === change.view) formatDocument(change.view);
                });
            }
        },
        [formatDocument, onSelectionChange]
    );
    return (
        <div className="min-w-0">
            <div className="mb-2 flex items-center justify-between gap-2 text-xs text-slate-500">
                <span>JSON · 自动缩进与折叠 · 粘贴或离开编辑区时格式化</span>
                {!readOnly && (
                    <Button
                        size="small"
                        aria-label={`${label}格式化`}
                        onClick={() => {
                            if (editor.current) formatDocument(editor.current);
                        }}
                    >
                        格式化
                    </Button>
                )}
            </div>
            <div className="overflow-hidden rounded border border-slate-200">
                <CodeMirror
                    value={value}
                    height={height}
                    basicSetup={basicSetup}
                    extensions={extensions}
                    theme={editorTheme}
                    placeholder={placeholder}
                    readOnly={readOnly}
                    editable={!readOnly}
                    onChange={onChange}
                    onUpdate={update}
                    onCreateEditor={(view) => {
                        editor.current = view;
                        formatDocument(view);
                    }}
                />
            </div>
            {errors.length > 0 && (
                <div role="alert" className="mt-2 space-y-1 text-xs text-red-600">
                    {errors.map((error) => (
                        <button
                            type="button"
                            key={`${error.from}:${error.message}`}
                            className="block cursor-pointer text-left"
                            onClick={() => {
                                const view = editor.current;
                                if (view) {
                                    view.dispatch({
                                        selection: { anchor: error.from, head: error.to },
                                        scrollIntoView: true,
                                    });
                                    view.focus();
                                }
                            }}
                        >
                            第 {error.line} 行，第 {error.column} 列：{error.message}
                        </button>
                    ))}
                </div>
            )}
        </div>
    );
}
