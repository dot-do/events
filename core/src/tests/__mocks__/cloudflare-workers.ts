/**
 * Mock for cloudflare:workers module
 *
 * This provides a base DurableObject class that can be extended in tests
 * when running outside of the workerd runtime.
 */

export class DurableObject {
  ctx: unknown
  env: unknown

  constructor(ctx: unknown, env: unknown) {
    this.ctx = ctx
    this.env = env
  }
}
