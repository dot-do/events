import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        singleWorker: true,
        isolatedStorage: true,
        wrangler: { configPath: './wrangler.jsonc' },
        miniflare: {
          compatibilityDate: '2026-01-25',
          compatibilityFlags: ['nodejs_compat'],
          r2Buckets: ['EVENTS_BUCKET'],
        },
      },
    },
  },
})
