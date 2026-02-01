/**
 * Query Endpoint Tests
 *
 * Tests POST /query for DuckDB SQL generation.
 */

import { SELF } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'

interface QueryRequest {
  dateRange?: { start: string; end: string }
  doId?: string
  doClass?: string
  eventTypes?: string[]
  collection?: string
  colo?: string
  limit?: number
}

async function postQuery(query: QueryRequest): Promise<{ sql: string }> {
  const response = await SELF.fetch('http://localhost/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(query),
  })
  return response.json() as Promise<{ sql: string }>
}

describe('POST /query - Basic SQL generation', () => {
  it('generates SELECT * FROM read_json_auto', async () => {
    const { sql } = await postQuery({})
    expect(sql).toContain('SELECT *')
    expect(sql).toContain('read_json_auto')
  })

  it('includes filename=true option', async () => {
    const { sql } = await postQuery({})
    expect(sql).toContain('filename=true')
  })

  it('orders by ts DESC', async () => {
    const { sql } = await postQuery({})
    expect(sql).toContain('ORDER BY ts DESC')
  })

  it('uses events/ prefix path', async () => {
    const { sql } = await postQuery({})
    expect(sql).toContain("'events/")
  })
})

describe('POST /query - Date range path optimization', () => {
  it('uses wildcard path when no dateRange', async () => {
    const { sql } = await postQuery({})
    expect(sql).toContain('events/**/')
  })

  it('uses year path with wildcards for months/days/hours when same year', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2024-01-01T00:00:00Z',
        end: '2024-12-31T23:59:59Z',
      },
    })
    // Path: events/YYYY/*/
    // Year is fixed, month and below are wildcards
    expect(sql).toContain('events/2024/')
    expect(sql).toMatch(/events\/2024\/\*\//)
  })

  it('uses year/month path with wildcards for days/hours when same year and month', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2024-03-01T00:00:00Z',
        end: '2024-03-31T23:59:59Z',
      },
    })
    // Path: events/YYYY/MM/*/
    expect(sql).toContain('events/2024/03/')
    expect(sql).toMatch(/events\/2024\/03\/\*\//)
  })

  it('uses year/month/day path with hour wildcard when same day', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2024-03-15T00:00:00Z',
        end: '2024-03-15T23:59:59Z',
      },
    })
    // Path: events/YYYY/MM/DD/*.jsonl (day specified, hour is in filename pattern)
    expect(sql).toContain('events/2024/03/15/')
  })

  it('pads single-digit month and day with zeros', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2024-01-05T00:00:00Z',
        end: '2024-01-05T23:59:59Z',
      },
    })
    expect(sql).toContain('events/2024/01/05/')
  })

  it('uses wildcard for different years', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2023-12-01T00:00:00Z',
        end: '2024-01-31T23:59:59Z',
      },
    })
    expect(sql).toContain('events/*/')
  })

  it('uses year path with wildcards for days across different months', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2024-03-28T00:00:00Z',
        end: '2024-04-05T23:59:59Z',
      },
    })
    // Should use year path with wildcard for month
    expect(sql).toMatch(/events\/2024\/\*\//)
  })
})

describe('POST /query - Filter conditions', () => {
  it('adds doId filter condition', async () => {
    const { sql } = await postQuery({ doId: 'my-do-123' })
    expect(sql).toContain(`"do".id = 'my-do-123'`)
  })

  it('adds doClass filter condition', async () => {
    const { sql } = await postQuery({ doClass: 'CounterDO' })
    expect(sql).toContain(`"do".class = 'CounterDO'`)
  })

  it('adds colo filter condition', async () => {
    const { sql } = await postQuery({ colo: 'DFW' })
    expect(sql).toContain(`"do".colo = 'DFW'`)
  })

  it('adds collection filter condition', async () => {
    const { sql } = await postQuery({ collection: 'users' })
    expect(sql).toContain(`collection = 'users'`)
  })

  it('adds eventTypes IN filter condition', async () => {
    const { sql } = await postQuery({
      eventTypes: ['rpc.call', 'collection.insert'],
    })
    expect(sql).toContain(`type IN ('rpc.call', 'collection.insert')`)
  })

  it('handles single eventType in array', async () => {
    const { sql } = await postQuery({
      eventTypes: ['rpc.call'],
    })
    expect(sql).toContain(`type IN ('rpc.call')`)
  })

  it('adds date range conditions to WHERE clause', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2024-03-15T00:00:00Z',
        end: '2024-03-15T23:59:59Z',
      },
    })
    expect(sql).toContain(`ts >= '2024-03-15T00:00:00Z'`)
    expect(sql).toContain(`ts <= '2024-03-15T23:59:59Z'`)
  })
})

describe('POST /query - Combined filters', () => {
  it('combines multiple filter conditions with AND', async () => {
    const { sql } = await postQuery({
      doClass: 'CounterDO',
      colo: 'DFW',
      eventTypes: ['rpc.call'],
    })

    expect(sql).toContain(`"do".class = 'CounterDO'`)
    expect(sql).toContain(`"do".colo = 'DFW'`)
    expect(sql).toContain(`type IN ('rpc.call')`)
    expect(sql).toContain('AND')
  })

  it('includes WHERE clause when conditions exist', async () => {
    const { sql } = await postQuery({ doId: 'test' })
    expect(sql).toContain('WHERE')
  })

  it('omits WHERE clause when no conditions', async () => {
    const { sql } = await postQuery({})
    expect(sql).not.toContain('WHERE')
  })
})

describe('POST /query - Limit', () => {
  it('adds LIMIT clause when specified', async () => {
    const { sql } = await postQuery({ limit: 100 })
    expect(sql).toContain('LIMIT 100')
  })

  it('omits LIMIT when not specified', async () => {
    const { sql } = await postQuery({})
    expect(sql).not.toContain('LIMIT')
  })

  it('LIMIT comes after ORDER BY', async () => {
    const { sql } = await postQuery({ limit: 50 })
    const orderByIndex = sql.indexOf('ORDER BY')
    const limitIndex = sql.indexOf('LIMIT')
    expect(limitIndex).toBeGreaterThan(orderByIndex)
  })
})

describe('POST /query - Full query structure', () => {
  it('generates complete valid SQL', async () => {
    const { sql } = await postQuery({
      dateRange: {
        start: '2024-03-15T00:00:00Z',
        end: '2024-03-15T23:59:59Z',
      },
      doClass: 'CounterDO',
      eventTypes: ['rpc.call', 'collection.insert'],
      limit: 1000,
    })

    // Verify SQL structure
    expect(sql).toMatch(/^SELECT \*/)
    expect(sql).toContain('FROM read_json_auto')
    expect(sql).toContain('WHERE')
    expect(sql).toContain('ORDER BY ts DESC')
    expect(sql).toContain('LIMIT 1000')
  })

  it('uses .jsonl file extension pattern', async () => {
    const { sql } = await postQuery({})
    expect(sql).toContain('*.jsonl')
  })
})
