export type JsonValue =
    | null
    | boolean
    | number
    | string
    | JsonValue[]
    | { [key: string]: JsonValue };

export function parseJson(text: string): JsonValue {
    const value = JSON.parse(text) as JsonValue;
    let position = 0;
    const space = () => {
        while (position < text.length && /\s/.test(text[position])) position++;
    };
    const string = () => {
        const start = position++;
        while (text[position] !== '"') {
            if (text[position] === '\\') position++;
            position++;
        }
        position++;
        return JSON.parse(text.slice(start, position)) as string;
    };
    const fail = (parts: string[], message: string, offset: number): never => {
        const path = `/${parts.map((part) => part.replace(/~/g, '~0').replace(/\//g, '~1')).join('/')}`;
        throw Object.assign(new Error(message), { path, offset });
    };
    const scan = (parts: string[]) => {
        if (parts.length > 32) fail(parts, 'JSON 不能超过 32 层', position);
        space();
        if (text[position] === '{') {
            position++;
            space();
            const keys = new Set<string>();
            while (text[position] !== '}') {
                const offset = position;
                const key = string();
                const path = [...parts, key];
                if (keys.has(key)) fail(path, 'JSON 对象键不能重复', offset);
                keys.add(key);
                space();
                position++;
                scan(path);
                space();
                if (text[position] !== ',') break;
                position++;
                space();
            }
            position++;
        } else if (text[position] === '[') {
            position++;
            space();
            let index = 0;
            while (text[position] !== ']') {
                scan([...parts, String(index++)]);
                space();
                if (text[position] !== ',') break;
                position++;
            }
            position++;
        } else if (text[position] === '"') string();
        else while (position < text.length && !/[\s,}\]]/.test(text[position])) position++;
    };
    scan([]);
    return value;
}

// 仅整理 token 之间的空白，保留大整数、科学计数及字符串转义的原文。
export function formatJson(text: string): string {
    parseJson(text);
    const tokens = text.match(/"(?:\\.|[^"\\])*"|[{}[\],:]|[^\s{}[\],:]+/g) ?? [];
    let depth = 0,
        result = '';
    const newline = () => {
        result += `\n${'    '.repeat(depth)}`;
    };
    tokens.forEach((token, index) => {
        if (token === '{' || token === '[') {
            result += token;
            depth++;
            if (tokens[index + 1] !== (token === '{' ? '}' : ']')) newline();
        } else if (token === '}' || token === ']') {
            depth--;
            if (tokens[index - 1] !== (token === '}' ? '{' : '[')) newline();
            result += token;
        } else if (token === ',') {
            result += ',';
            newline();
        } else if (token === ':') result += ': ';
        else result += token;
    });
    return result;
}
