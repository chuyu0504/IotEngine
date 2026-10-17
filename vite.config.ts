import { readFileSync } from 'node:fs';
import { defineConfig, loadEnv, type ProxyOptions } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import cssInjectedByJs from 'vite-plugin-css-injected-by-js';
import { fileURLToPath, URL } from 'node:url';

const rootDir = fileURLToPath(new URL('.', import.meta.url));

/** Only streaming SSE responses need explicit downstream teardown on upstream loss. */
export const configureSseProxy: NonNullable<ProxyOptions['configure']> = (proxy) => {
    proxy.on('proxyRes', (upstream, _request, downstream) => {
        if (!upstream.headers['content-type']?.includes('text/event-stream')) return;
        const end = () => {
            if (!downstream.destroyed && !downstream.writableEnded) downstream.end();
        };
        upstream.once('close', end);
        upstream.once('error', end);
        downstream.once('close', () => {
            upstream.removeListener('close', end);
            upstream.removeListener('error', end);
        });
    });
};

export default defineConfig(({ mode }) => {
    const env = loadEnv(mode, rootDir, '');
    const backendPort = env.PORT?.trim() || '1102';
    const backendUrl = env.DEV_BACKEND_URL?.trim() || `http://127.0.0.1:${backendPort}`;

    return {
        root: fileURLToPath(new URL('./web', import.meta.url)),
        envDir: rootDir,
        plugins: [
            {
                name: 'inline-favicon',
                transformIndexHtml(html) {
                    const icon = readFileSync(new URL('./web/public/favicon.svg', import.meta.url));
                    return html.replace('href="/favicon.svg"', `href="data:image/svg+xml;base64,${icon.toString('base64')}"`);
                },
            },
            tailwindcss(),
            react(),
            cssInjectedByJs(),
        ],
        resolve: {
            alias: {
                '@': fileURLToPath(new URL('./web', import.meta.url)),
            },
        },
        server: {
            host: '0.0.0.0',
            port: 5173,
            proxy: {
                '/v1': {
                    target: backendUrl,
                    changeOrigin: true,
                    ws: true,
                    configure: configureSseProxy,
                },
                '/api': {
                    target: backendUrl,
                    changeOrigin: true,
                    configure: configureSseProxy,
                },
                '/edge': {
                    target: backendUrl,
                    changeOrigin: true,
                    ws: true,
                    configure: configureSseProxy,
                },
            },
        },
        build: {
            outDir: '../build/web',
            emptyOutDir: true,
            chunkSizeWarningLimit: 9999,
            rollupOptions: {
                output: {
                    // Design intent: ship a single JS bundle (no code splitting).
                    codeSplitting: false,
                    hashCharacters: 'hex',
                    entryFileNames: '[hash].js',
                    chunkFileNames: '[hash].js',
                },
            },
        },
    };
});
