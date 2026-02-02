import { Command } from 'commander'
import pc from 'picocolors'

export interface QueryOptions {
  sql?: string
  format?: 'json' | 'table'
  endpoint?: string
  namespace?: string
  limit?: number
}

export interface QueryResult {
  columns: string[]
  rows: unknown[][]
  rowCount: number
  executionTimeMs: number
}

/**
 * Query events using SQL
 */
export async function runQuery(options: QueryOptions = {}): Promise<void> {
  const endpoint = options.endpoint || process.env.EVENTS_ENDPOINT || 'https://events.do'
  const apiKey = process.env.EVENTS_API_KEY
  const format = options.format || 'table'

  if (!options.sql) {
    console.log(pc.red('\n  Error: --sql option is required\n'))
    console.log(pc.dim('  Example: events query --sql "SELECT * FROM events LIMIT 10"\n'))
    process.exit(1)
  }

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
  }

  if (apiKey) {
    headers['Authorization'] = `Bearer ${apiKey}`
  }

  try {
    console.log(pc.dim(`\n  Executing query on ${endpoint}...\n`))

    const response = await fetch(`${endpoint}/query`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        sql: options.sql,
        namespace: options.namespace,
        limit: options.limit,
      }),
    })

    if (!response.ok) {
      const error = await response.json().catch(() => ({ error: response.statusText }))
      console.error(pc.red(`\n  Query error: ${(error as { error?: string }).error || response.statusText}\n`))
      process.exit(1)
    }

    const result = await response.json() as QueryResult | { sql: string; namespace: string | null; isolated: boolean }

    // Check if this is a query builder response (returns SQL) or direct results
    if ('sql' in result && !('rows' in result)) {
      // Query builder response - display the generated SQL
      console.log(pc.cyan('  Generated SQL:\n'))
      console.log(pc.white(`  ${(result as { sql: string }).sql.split('\n').join('\n  ')}\n`))

      if ((result as { namespace: string | null }).namespace) {
        console.log(pc.dim(`  Namespace: ${(result as { namespace: string | null }).namespace}`))
      }
      console.log(pc.dim(`  Isolated: ${(result as { isolated: boolean }).isolated}\n`))
      return
    }

    const queryResult = result as QueryResult

    if (format === 'json') {
      // JSON output
      console.log(JSON.stringify(queryResult, null, 2))
    } else {
      // Table output
      displayTable(queryResult)
    }

    console.log(pc.dim(`\n  ${queryResult.rowCount} rows returned in ${queryResult.executionTimeMs}ms\n`))
  } catch (err) {
    console.error(pc.red(`\n  Error: ${(err as Error).message}\n`))
    process.exit(1)
  }
}

/**
 * Display query results as a formatted table
 */
function displayTable(result: QueryResult): void {
  if (!result.columns || result.columns.length === 0) {
    console.log(pc.yellow('  No columns in result\n'))
    return
  }

  if (!result.rows || result.rows.length === 0) {
    console.log(pc.yellow('  No rows returned\n'))
    return
  }

  // Calculate column widths
  const widths: number[] = result.columns.map((col, i) => {
    let maxWidth = col.length
    for (const row of result.rows) {
      const cellValue = formatCell(row[i])
      maxWidth = Math.max(maxWidth, cellValue.length)
    }
    // Cap at 50 chars for readability
    return Math.min(maxWidth, 50)
  })

  // Print header
  const header = result.columns.map((col, i) => padRight(col, widths[i]!)).join(' | ')
  console.log(pc.cyan(`  ${header}`))
  console.log(pc.dim(`  ${widths.map(w => '-'.repeat(w)).join('-+-')}`))

  // Print rows
  for (const row of result.rows) {
    const line = row.map((cell, i) => {
      const formatted = formatCell(cell)
      return padRight(truncate(formatted, widths[i]!), widths[i]!)
    }).join(' | ')
    console.log(`  ${line}`)
  }
}

/**
 * Format a cell value for display
 */
function formatCell(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL'
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

/**
 * Pad string to the right with spaces
 */
function padRight(str: string, width: number): string {
  if (str.length >= width) return str
  return str + ' '.repeat(width - str.length)
}

/**
 * Truncate string with ellipsis
 */
function truncate(str: string, maxLength: number): string {
  if (str.length <= maxLength) return str
  return str.slice(0, maxLength - 3) + '...'
}

export const queryCommand = new Command('query')
  .description('Query events with SQL')
  .option('-s, --sql <query>', 'SQL query to execute')
  .option('-f, --format <format>', 'Output format: json or table (default: table)', 'table')
  .option('-n, --namespace <namespace>', 'Namespace to query')
  .option('-l, --limit <limit>', 'Maximum number of rows to return', parseInt)
  .option('-e, --endpoint <url>', 'Events.do API endpoint (default: https://events.do)')
  .action(async (opts) => {
    await runQuery(opts)
  })
