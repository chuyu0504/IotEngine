import { expect, test } from 'bun:test';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';

const root = resolve(import.meta.dir, '..');
const walk = (dir: string): string[] => readdirSync(dir).flatMap(name => {
    const file = join(dir, name);
    return statSync(file).isDirectory() ? walk(file) : [file];
});
const webFiles = walk(join(root, 'web'));
const sources = webFiles.filter(file => /\.[tj]sx?$/.test(file));
const paths = new Set(sources);
const repoPath = (file: string) => relative(root, file).replaceAll('\\', '/');
const owner = (file: string) => file.includes('/pages/') ? dirname(file) : undefined;
const edges = new Map<string, string[]>();
const unresolved: string[] = [];
for (const file of sources) {
    const imports: string[] = [];
    const content = readFileSync(file, 'utf8');
    for (const match of content.matchAll(/(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s*)['"]([^'"]+)['"]/g)) {
        const spec = match[1];
        if (!spec.startsWith('.') && !spec.startsWith('@/')) continue;
        const base = spec.startsWith('@/') ? resolve(root, 'web', spec.slice(2)) : resolve(dirname(file), spec);
        const target = [base, `${base}.ts`, `${base}.tsx`, `${base}.js`, join(base, 'index.ts'), join(base, 'index.tsx')].find(p => paths.has(p));
        if (target) imports.push(target);
        else if (!webFiles.includes(base)) unresolved.push(`${repoPath(file)} -> ${spec}`);
    }
    edges.set(file, imports);
}

test('page modules use only their role files and have no private subdirectories', () => {
    const violations: string[] = [];
    const pages = join(root, 'web/pages');
    for (const name of readdirSync(pages)) {
        const dir = join(pages, name);
        if (!statSync(dir).isDirectory()) { violations.push(repoPath(dir)); continue; }
        const moduleFiles = walk(dir);
        if (!moduleFiles.includes(join(dir, 'index.tsx'))) violations.push(`${repoPath(dir)}: missing index.tsx`);
        for (const file of moduleFiles) {
            const local = relative(dir, file).replaceAll('\\', '/');
            if (!/^[a-z][a-z0-9_]*$/.test(name) || !['index.tsx', ...['types', 'schema', 'api', 'service'].map(role => `${name}.${role}.ts`)].includes(local)) violations.push(repoPath(file));
        }
    }
    expect(violations).toEqual([]);
});

test('owned frontend source paths use snake_case names', () => {
    const violations = sources.filter(file => !repoPath(file).startsWith('web/generated/')).filter(file =>
        repoPath(file).split('/').some(part => !/^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*$/.test(part))
    ).map(repoPath);
    expect(violations).toEqual([]);
});

test('local source imports resolve after module moves', () => {
    expect(unresolved).toEqual([]);
});

test('shared frontend infrastructure never imports page implementations', () => {
    const violations: string[] = [];
    for (const [file, imports] of edges) {
        if (!/^web\/(components|store|hooks|utils|lib|types|providers|config)\//.test(repoPath(file))) continue;
        for (const target of imports) if (repoPath(target).startsWith('web/pages/')) violations.push(`${repoPath(file)} -> ${repoPath(target)}`);
    }
    expect(violations).toEqual([]);
});

test('page dependencies use public services and types and views never import API clients', () => {
    const violations: string[] = [];
    for (const [file, imports] of edges) {
        if (!repoPath(file).startsWith('web/pages/')) continue;
        for (const target of imports) {
            if (!repoPath(target).startsWith('web/pages/')) continue;
            if ((owner(repoPath(file)) !== owner(repoPath(target)) && !/\.(service|types)\.ts$/.test(target)) || (file.endsWith('.tsx') && target.endsWith('.api.ts'))) violations.push(`${repoPath(file)} -> ${repoPath(target)}`);
        }
    }
    expect(violations).toEqual([]);
});

test('frontend source dependencies are acyclic', () => {
    const visited = new Set<string>();
    const active: string[] = [];
    const visit = (file: string) => {
        if (active.includes(file)) throw new Error([...active.slice(active.indexOf(file)), file].map(repoPath).join(' -> '));
        if (visited.has(file)) return;
        active.push(file);
        for (const target of edges.get(file) ?? []) visit(target);
        active.pop(); visited.add(file);
    };
    for (const file of sources) visit(file);
});

test('public page data types are defined in the owning types file', () => {
    const violations = sources.filter(file => repoPath(file).startsWith('web/pages/') && file.endsWith('.service.ts') && /^export\s+(?:interface|type)\s/m.test(readFileSync(file, 'utf8'))).map(repoPath);
    expect(violations).toEqual([]);
});

test('page views obtain network connections through their services', () => {
    const violations = sources.filter(file =>
        repoPath(file).startsWith('web/pages/') && file.endsWith('.tsx') &&
        /(?:\bnew\s+(?:WebSocket|EventSource)\s*\(|\bfetch\s*\()/.test(readFileSync(file, 'utf8'))
    ).map(repoPath);
    expect(violations).toEqual([]);
});

test('page views do not own query caching or import transport infrastructure', () => {
    const violations: string[] = [];
    for (const [file, imports] of edges) {
        if (!repoPath(file).startsWith('web/pages/') || !file.endsWith('/index.tsx') && !file.endsWith('\\index.tsx')) continue;
        const source = readFileSync(file, 'utf8');
        if (/\b(?:useQuery|useInfiniteQuery|useMutation|useQueryClient|useSnapshotQuery)\s*\(/.test(source)) violations.push(repoPath(file));
        for (const target of imports) {
            if (/^web\/lib\/(?:http|sse|snapshot_request|snapshot_stream|sse_subscriptions)\./.test(repoPath(target))) violations.push(`${repoPath(file)} -> ${repoPath(target)}`);
        }
    }
    expect(violations).toEqual([]);
});

test('module types and schemas are pure and API clients do not own UI or query caching', () => {
    const violations: string[] = [];
    for (const [file, imports] of edges) {
        const path = repoPath(file);
        if (!path.startsWith('web/pages/')) continue;
        const source = readFileSync(file, 'utf8');
        if (/\.(types|schema)\.ts$/.test(path)) {
            for (const target of imports) {
                if (!/^web\/(?:utils|types)\//.test(repoPath(target)) && !target.endsWith('.types.ts')) violations.push(`${path} -> ${repoPath(target)}`);
            }
            if (/from\s+['"](?:react|antd|@tanstack\/react-query)|\b(?:fetch|useEffect|useQuery)\s*\(|\bnew\s+(?:WebSocket|EventSource)\s*\(/.test(source)) violations.push(path);
        }
        if (path.endsWith('.api.ts')) {
            for (const target of imports) {
                if (/^web\/(?:components|layouts|hooks|providers|routes)\//.test(repoPath(target)) || /(?:\.service\.ts|\.tsx)$/.test(target)) violations.push(`${path} -> ${repoPath(target)}`);
            }
            if (/from\s+['"](?:react|antd|@tanstack\/react-query|react-router-dom)/.test(source)) violations.push(path);
        }
    }
    expect(violations).toEqual([]);
});

test('business query polling stays disabled globally and in individual queries', () => {
    const provider = readFileSync(join(root, 'web/providers/tan_stack_query_provider.tsx'), 'utf8');
    expect(provider).toMatch(/refetchInterval:\s*false/);
    const violations = sources.filter(file => [...readFileSync(file, 'utf8').matchAll(/\brefetchInterval\s*:\s*([^,\n}]+)/g)].some(match => match[1].trim() !== 'false')).map(repoPath);
    expect(violations).toEqual([]);
});
