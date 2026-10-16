import { readFileSync } from 'node:fs';
import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import cssInjectedByJs from 'vite-plugin-css-injected-by-js';
import { fileURLToPath, URL } from 'node:url';

const rootDir = fileURLToPath(new URL('.', import.meta.url));

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
                },
                '/api': {
                    target: backendUrl,
                    changeOrigin: true,
                },
                '/edge': {
                    target: backendUrl,
                    changeOrigin: true,
                    ws: true,
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
