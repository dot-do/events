import { defineConfig } from 'tsup'

export default defineConfig([
  // CLI entry point with shebang
  {
    entry: { cli: 'src/cli.ts' },
    format: ['esm'],
    dts: true,
    clean: true,
    shims: true,
    banner: {
      js: '#!/usr/bin/env node',
    },
  },
  // Library entry point without shebang
  {
    entry: { index: 'src/index.ts' },
    format: ['esm'],
    dts: true,
    clean: false, // Don't clean since cli build already cleaned
    shims: true,
  },
])
