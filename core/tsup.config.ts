import { defineConfig } from 'tsup'

export default defineConfig([
  // Main library bundle (ESM for Node/Workers)
  {
    entry: ['src/index.ts', 'src/types.ts'],
    format: ['esm'],
    dts: true,
    clean: true,
    sourcemap: true,
    treeshake: true,
    external: ['@cloudflare/workers-types', 'cloudflare:workers'],
    target: 'es2022',
  },
  // Browser SDK bundle (ESM)
  {
    entry: ['src/browser.ts'],
    format: ['esm'],
    dts: true,
    sourcemap: true,
    treeshake: true,
    target: 'es2020',
    outDir: 'dist',
    platform: 'browser',
  },
  // Standalone browser bundle (IIFE for script tag)
  {
    entry: { 'browser.min': 'src/browser.ts' },
    format: ['iife'],
    globalName: 'events',
    minify: true,
    treeshake: true,
    target: 'es2020',
    outDir: 'dist',
    platform: 'browser',
    dts: false,
    esbuildOptions(options) {
      options.footer = { js: 'events = events.default;' }
    },
  },
])
