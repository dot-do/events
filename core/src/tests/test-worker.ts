/**
 * Test worker entry point for vitest-pool-workers
 *
 * Exports the EventEmitterTestDO class for miniflare to instantiate,
 * plus a minimal default fetch handler.
 */

export { EventEmitterTestDO } from './test-do.js'

export default {
  fetch: () => new Response('ok'),
}
