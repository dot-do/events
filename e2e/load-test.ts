#!/usr/bin/env npx tsx
/**
 * Load test for ClickHouseBufferDO via the /ingest endpoint.
 *
 * Usage: npx tsx .do/events/e2e/load-test.ts [rps] [duration_sec]
 *   Default: 100 rps for 10 seconds
 *
 * Measures: response latency (p50/p99), error rate.
 * Requires: deployed events.do or headless.ly worker (staging or production).
 */

const RPS = parseInt(process.argv[2] ?? '100', 10)
const DURATION = parseInt(process.argv[3] ?? '10', 10)
const TARGET = process.env.TARGET_URL ?? 'https://events.do/ingest'

const latencies: number[] = []
let errors = 0
let sent = 0

async function sendEvent() {
  const start = performance.now()
  try {
    const resp = await fetch(TARGET, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify([
        {
          type: 'loadtest',
          event: 'ping',
          ts: new Date().toISOString(),
          data: { rps: RPS, seq: sent },
        },
      ]),
    })
    if (!resp.ok) errors++
  } catch {
    errors++
  }
  latencies.push(performance.now() - start)
  sent++
}

async function run() {
  console.log(`Load test: ${RPS} rps for ${DURATION}s against ${TARGET}`)
  const interval = 1000 / RPS
  const end = Date.now() + DURATION * 1000

  while (Date.now() < end) {
    sendEvent() // fire-and-forget
    await new Promise((r) => setTimeout(r, interval))
  }

  // Wait for in-flight requests
  await new Promise((r) => setTimeout(r, 3000))

  latencies.sort((a, b) => a - b)
  const p50 = latencies[Math.floor(latencies.length * 0.5)]
  const p99 = latencies[Math.floor(latencies.length * 0.99)]

  console.log(`\nResults:`)
  console.log(`  Sent: ${sent}, Errors: ${errors} (${((errors / sent) * 100).toFixed(1)}%)`)
  console.log(`  P50: ${p50?.toFixed(1)}ms, P99: ${p99?.toFixed(1)}ms`)
  console.log(`  Min: ${latencies[0]?.toFixed(1)}ms, Max: ${latencies[latencies.length - 1]?.toFixed(1)}ms`)
}

run()
