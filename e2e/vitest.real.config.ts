import { defineConfig } from 'vitest/config'

/**
 * Vitest config for real infrastructure E2E tests
 *
 * These tests hit actual Cloudflare/production endpoints, not mocks.
 *
 * Run all E2E tests:
 *   npx vitest run --config e2e/vitest.real.config.ts
 *
 * Run specific test file:
 *   npx vitest run --config e2e/vitest.real.config.ts webhooks-e2e.test.ts
 *
 * Environment variables for webhook secrets:
 *   TEST_GITHUB_SECRET, TEST_STRIPE_SECRET, TEST_SLACK_SECRET,
 *   TEST_LINEAR_SECRET, TEST_SVIX_SECRET, TEST_WORKOS_SECRET
 *
 * Environment variables for endpoints:
 *   EVENTS_DO_URL, EVENTS_TAIL_URL, EVENTS_COMPACTOR_URL
 */
export default defineConfig({
  test: {
    include: ['**/*-e2e.test.ts', '**/real-infra.test.ts'],
    testTimeout: 60000, // 60s timeout for E2E tests
    hookTimeout: 60000, // 60s for setup/teardown
    pool: 'forks', // Use forks, not workers pool
    globals: true,
    environment: 'node',
    reporters: ['verbose'],
    // Retry flaky network tests once
    retry: 1,
    // Run tests sequentially to avoid overwhelming endpoints
    sequence: {
      concurrent: false,
    },
  },
})
