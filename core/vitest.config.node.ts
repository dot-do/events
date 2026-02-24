/**
 * Node.js vitest config for config validation tests.
 * These tests read files from disk and don't need workerd runtime.
 */
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['src/tests/*-entrypoint.test.ts'],
  },
})
