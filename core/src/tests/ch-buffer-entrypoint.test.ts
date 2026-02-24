/**
 * EVENTS binding entrypoint validation test (RED).
 *
 * Reads the apps/api wrangler.jsonc and verifies the EVENTS service binding
 * has entrypoint: "BufferService". Without it, the BufferService RPC path
 * in ingest.ts is dead code — env.EVENTS is a plain Fetcher, not a typed
 * RPC binding.
 *
 * Also validates the apps/api worker-configuration.d.ts declares EVENTS
 * as a Service type (not Fetcher).
 *
 * NOTE: This file uses the node:fs module, which requires nodejs_compat.
 */

import { describe, it, expect } from 'vitest'
import { readFileSync } from 'fs'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))
// Path from .do/events/core/src/tests/ to apps/api/
const API_DIR = resolve(__dirname, '../../../../../apps/api')

function stripJsoncComments(text: string): string {
  return text.replace(/\/\/.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '')
}

describe('EVENTS binding — BufferService entrypoint', () => {
  it('apps/api wrangler.jsonc EVENTS binding has entrypoint: BufferService', () => {
    const raw = readFileSync(resolve(API_DIR, 'wrangler.jsonc'), 'utf-8')
    const config = JSON.parse(stripJsoncComments(raw))

    const services = config.services as Array<{ binding: string; service: string; entrypoint?: string }>
    const eventsBinding = services.find((s) => s.binding === 'EVENTS')

    expect(eventsBinding).toBeDefined()
    expect(eventsBinding!.service).toBe('events')
    expect(eventsBinding!.entrypoint).toBe('BufferService') // RED: entrypoint is missing
  })

  it('worker-configuration.d.ts declares EVENTS as Service (not Fetcher)', () => {
    const dts = readFileSync(resolve(API_DIR, 'worker-configuration.d.ts'), 'utf-8')

    // After `pnpm types`, EVENTS should be typed as Service<...BufferService>
    // Currently it's `EVENTS: Fetcher /* events */` which means no RPC methods
    const eventsLine = dts.split('\n').find((line) => line.includes('EVENTS'))
    expect(eventsLine).toBeDefined()
    expect(eventsLine).not.toContain('Fetcher') // RED: currently declares as Fetcher
  })
})
