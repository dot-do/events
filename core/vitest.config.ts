import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersConfig({
  test: {
    globals: true,
    include: ['src/**/*.test.ts', 'tests/**/*.test.ts'],
    poolOptions: {
      workers: {
        singleWorker: true,
        isolatedStorage: false,
        main: './src/tests/test-worker.ts',
        miniflare: {
          compatibilityDate: '2026-02-17',
          compatibilityFlags: ['nodejs_compat'],
          durableObjects: {
            EVENT_EMITTER_TEST: {
              className: 'EventEmitterTestDO',
              useSQLite: true,
            },
          },
        },
      },
    },
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'json-summary', 'html'],
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.test.ts', 'src/**/types.ts', 'src/tests/**/*'],
      thresholds: {
        lines: 80,
        functions: 80,
        branches: 80,
        statements: 80,
      },
    },
  },
})
