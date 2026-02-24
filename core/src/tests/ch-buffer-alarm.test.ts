/**
 * ClickHouseBufferDO alarm safety flush tests (RED).
 *
 * Tests that the DO sets a safety-net alarm when events are buffered
 * without immediate flush, preventing stranded events.
 *
 * These tests require the workerd pool (miniflare) to run the actual DO.
 */

import { describe, it, expect } from 'vitest'
import { env, runDurableObjectAlarm } from 'cloudflare:test'
import { FLUSH_INTERVAL_MS } from '../ch-buffer-do'

const CH_BUFFER = (env as unknown as Record<string, DurableObjectNamespace>).CH_BUFFER

function getStub(shard: number) {
  return CH_BUFFER.get(CH_BUFFER.idFromName(`shard-${shard}`))
}

function makeEvents(count: number) {
  return Array.from({ length: count }, (_, i) => ({
    id: `alarm-test-${Date.now()}-${i}`,
    ns: 'test.alarm',
    ts: new Date().toISOString(),
    type: 'test',
    event: `alarm.test.${i}`,
    source: 'alarm-test',
    data: { index: i },
  }))
}

describe('ClickHouseBufferDO — alarm safety flush', () => {
  it('schedules alarm when events buffer without immediate flush', async () => {
    const stub = getStub(50)

    // First message triggers immediate flush (lastFlushTime starts at 0)
    const resp = await stub.fetch('https://buffer/ws', { headers: { Upgrade: 'websocket' } })
    const ws = resp.webSocket!
    ws.accept()
    ws.send(JSON.stringify(makeEvents(2)))

    // Wait a tiny bit for the first flush to process, then send more events
    // within FLUSH_INTERVAL_MS — these should buffer but NOT flush
    await new Promise((r) => setTimeout(r, 20))
    ws.send(JSON.stringify(makeEvents(3)))
    await new Promise((r) => setTimeout(r, 20))

    // Safety alarm should have been set — runDurableObjectAlarm finds and runs it
    const alarmRan = await runDurableObjectAlarm(stub)
    expect(alarmRan).toBe(true) // RED: no alarm is currently set

    ws.close(1000, 'done')
  })

  it('alarm handler flushes stranded events and DO stays healthy', async () => {
    const stub = getStub(51)

    const resp = await stub.fetch('https://buffer/ws', { headers: { Upgrade: 'websocket' } })
    const ws = resp.webSocket!
    ws.accept()

    // First message flushes immediately (lastFlushTime was 0)
    ws.send(JSON.stringify(makeEvents(1)))
    await new Promise((r) => setTimeout(r, 10))

    // Second message within FLUSH_INTERVAL_MS — buffers only, no flush
    ws.send(JSON.stringify(makeEvents(5)))
    await new Promise((r) => setTimeout(r, 10))

    // Trigger alarm — should flush the 5 stranded events
    const alarmRan = await runDurableObjectAlarm(stub)
    expect(alarmRan).toBe(true) // RED: no alarm is set

    // After alarm, DO should still accept new connections
    ws.close(1000, 'done')
    await new Promise((r) => setTimeout(r, 50))

    const resp2 = await stub.fetch('https://buffer/ws', { headers: { Upgrade: 'websocket' } })
    expect(resp2.status).toBe(101)
    const ws2 = resp2.webSocket!
    ws2.accept()
    ws2.close(1000, 'verify')
  })

  it('alarm does not fire when flush already drained the buffer', async () => {
    const stub = getStub(52)

    const resp = await stub.fetch('https://buffer/ws', { headers: { Upgrade: 'websocket' } })
    const ws = resp.webSocket!
    ws.accept()

    // Send events and wait long enough for time-based flush to trigger
    ws.send(JSON.stringify(makeEvents(2)))
    await new Promise((r) => setTimeout(r, FLUSH_INTERVAL_MS + 50))

    // Send more events — these should also flush immediately (interval elapsed)
    ws.send(JSON.stringify(makeEvents(2)))
    await new Promise((r) => setTimeout(r, FLUSH_INTERVAL_MS + 50))

    // All events were flushed by the time-based path — alarm should be a no-op
    // (either not set, or set but buffer already empty when it fires)
    // Running alarm should not crash
    await runDurableObjectAlarm(stub)

    // DO still healthy
    ws.close(1000, 'done')
    const resp2 = await stub.fetch('https://buffer/ws', { headers: { Upgrade: 'websocket' } })
    expect(resp2.status).toBe(101)
    resp2.webSocket!.accept()
    resp2.webSocket!.close(1000, 'verify')
  })
})
