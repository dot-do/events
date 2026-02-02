/**
 * Catalog API route handler - /catalog/*
 */

import type { Env } from '../env'
import { authCorsHeaders } from '../utils'

// ============================================================================
// SQL Injection Prevention
// ============================================================================

/**
 * Dangerous SQL keywords that should be rejected in where clauses
 */
const SQL_DANGEROUS_KEYWORDS = [
  'UNION', 'DROP', 'DELETE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER', 'TRUNCATE',
  'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
  'ATTACH', 'DETACH', 'PRAGMA', 'VACUUM', 'COPY', 'LOAD', 'INSTALL',
]

/**
 * Allowed column names for where clause filtering
 */
const ALLOWED_COLUMNS = new Set([
  'id', 'ts', 'type', 'collection', 'provider', 'do_id', 'do_class', 'colo',
  'method', 'status', 'latency', 'size', 'error', 'created_at', 'updated_at',
  'name', 'namespace', 'path', 'format', 'record_count', 'file_size_bytes',
])

/**
 * Validates a where clause for SQL injection vulnerabilities
 */
function validateWhereClause(where: string): { valid: boolean; reason?: string } {
  // Check for SQL comment sequences
  if (where.includes('--') || where.includes('/*') || where.includes('*/')) {
    return { valid: false, reason: 'SQL comments are not allowed' }
  }

  // Check for semicolons (statement terminator)
  if (where.includes(';')) {
    return { valid: false, reason: 'Multiple statements are not allowed' }
  }

  // Check for dangerous keywords (case-insensitive, word boundary)
  const upperWhere = where.toUpperCase()
  for (const keyword of SQL_DANGEROUS_KEYWORDS) {
    // Match keyword as a whole word
    const regex = new RegExp(`\\b${keyword}\\b`, 'i')
    if (regex.test(upperWhere)) {
      return { valid: false, reason: `Keyword '${keyword}' is not allowed` }
    }
  }

  // Check for hex escapes that could bypass string checks
  if (/0x[0-9a-fA-F]+/.test(where)) {
    return { valid: false, reason: 'Hex literals are not allowed' }
  }

  // Check for backslash escapes
  if (where.includes('\\')) {
    return { valid: false, reason: 'Backslash escapes are not allowed' }
  }

  // Extract column names from the where clause and validate them
  // Match patterns like: column = , column >, column <, column LIKE, etc.
  const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|!=|<>|<=?|>=?|LIKE|IN|IS|BETWEEN)\b/gi
  let match
  while ((match = columnPattern.exec(where)) !== null) {
    const column = match[1]
    if (!column) continue
    const columnLower = column.toLowerCase()
    // Skip SQL keywords that might match the pattern
    if (['and', 'or', 'not', 'null', 'true', 'false'].includes(columnLower)) {
      continue
    }
    if (!ALLOWED_COLUMNS.has(columnLower)) {
      return { valid: false, reason: `Column '${columnLower}' is not allowed. Allowed columns: ${[...ALLOWED_COLUMNS].join(', ')}` }
    }
  }

  // Validate overall structure: only allow safe characters and patterns
  // Allow: column names, comparison operators, numbers, quoted strings, AND/OR/NOT/IS/NULL/IN/LIKE/BETWEEN, parens, commas, whitespace
  const safePattern = /^(?:[A-Za-z_][A-Za-z0-9_]*|[0-9]+(?:\.[0-9]+)?|'[^']*'|[<>=!]+|[\s(),]|AND|OR|NOT|IS|NULL|IN|LIKE|BETWEEN|TRUE|FALSE)+$/i
  if (!safePattern.test(where)) {
    return { valid: false, reason: 'Contains disallowed characters or expressions' }
  }

  // Limit maximum length to prevent DoS
  if (where.length > 1000) {
    return { valid: false, reason: 'Where clause exceeds maximum length of 1000 characters' }
  }

  return { valid: true }
}

/**
 * Legacy singleton catalog name for backwards compatibility during migration.
 * New data uses namespace-sharded catalogs.
 */
const LEGACY_CATALOG_NAME = 'events-catalog'

/**
 * Gets the CatalogDO instance for a given namespace.
 * Shards by namespace to avoid SPOF and bottleneck on single DO.
 */
function getCatalog(env: Env, namespace: string) {
  const catalogId = env.CATALOG.idFromName(namespace)
  return env.CATALOG.get(catalogId)
}

/**
 * Gets the legacy singleton CatalogDO for migration purposes.
 */
function getLegacyCatalog(env: Env) {
  const catalogId = env.CATALOG.idFromName(LEGACY_CATALOG_NAME)
  return env.CATALOG.get(catalogId)
}

export async function handleCatalog(request: Request, env: Env, url: URL): Promise<Response> {
  const path = url.pathname.replace('/catalog', '') || '/'

  try {
    // GET /catalog/namespaces - list all namespaces across all shards
    // For backwards compatibility, we check both legacy and known namespaces
    if (path === '/namespaces' && request.method === 'GET') {
      // First check legacy catalog for any existing namespaces
      const legacyCatalog = getLegacyCatalog(env)
      const legacyNamespaces = await legacyCatalog.listNamespaces()

      // For sharded catalogs, we need a registry or query R2 for known namespaces
      // For now, return legacy namespaces plus 'default' if not present
      const namespaces = new Set(legacyNamespaces)
      namespaces.add('default')

      return Response.json({ namespaces: [...namespaces].sort() }, { headers: authCorsHeaders(request, env) })
    }

    // POST /catalog/namespaces - create namespace in the namespace-sharded catalog
    if (path === '/namespaces' && request.method === 'POST') {
      let body: unknown
      try {
        body = await request.json()
      } catch {
        return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const { name, properties } = body as Record<string, unknown>
      if (!name || typeof name !== 'string') {
        return Response.json({ error: 'Missing required field: name (string)' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (properties !== undefined && (typeof properties !== 'object' || properties === null || Array.isArray(properties))) {
        return Response.json({ error: 'Invalid field: properties must be an object' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      // Create namespace in the namespace-sharded catalog
      const catalog = getCatalog(env, name)
      await catalog.createNamespace(name, properties as Record<string, string> | undefined)
      return Response.json({ ok: true, namespace: name }, { headers: authCorsHeaders(request, env) })
    }

    // GET /catalog/tables?namespace=xxx
    if (path === '/tables' && request.method === 'GET') {
      const namespace = url.searchParams.get('namespace') ?? 'default'
      const catalog = getCatalog(env, namespace)

      // First try sharded catalog
      let tables = await catalog.listTables(namespace)

      // If no tables found and this is default namespace, also check legacy catalog
      if (tables.length === 0 && namespace === 'default') {
        const legacyCatalog = getLegacyCatalog(env)
        tables = await legacyCatalog.listTables(namespace)
      }

      return Response.json({ namespace, tables }, { headers: authCorsHeaders(request, env) })
    }

    // POST /catalog/tables - create table in namespace-sharded catalog
    if (path === '/tables' && request.method === 'POST') {
      let body: unknown
      try {
        body = await request.json()
      } catch {
        return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const { namespace, name, schema, location } = body as Record<string, unknown>
      if (!namespace || typeof namespace !== 'string') {
        return Response.json({ error: 'Missing required field: namespace (string)' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (!name || typeof name !== 'string') {
        return Response.json({ error: 'Missing required field: name (string)' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (!Array.isArray(schema)) {
        return Response.json({ error: 'Missing required field: schema (array)' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      for (let i = 0; i < schema.length; i++) {
        const col = schema[i]
        if (!col || typeof col !== 'object' || typeof col.name !== 'string' || typeof col.type !== 'string') {
          return Response.json({ error: `Invalid schema entry at index ${i}: must have name (string) and type (string)` }, { status: 400, headers: authCorsHeaders(request, env) })
        }
      }
      if (location !== undefined && typeof location !== 'string') {
        return Response.json({ error: 'Invalid field: location must be a string' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const catalog = getCatalog(env, namespace)
      const table = await catalog.createTable(
        namespace,
        name,
        schema as { name: string; type: 'string' | 'int32' | 'int64' | 'float' | 'double' | 'boolean' | 'timestamp' | 'json'; nullable?: boolean }[],
        { location: location as string | undefined },
      )
      return Response.json({ ok: true, table }, { headers: authCorsHeaders(request, env) })
    }

    // GET /catalog/table?namespace=xxx&name=yyy
    if (path === '/table' && request.method === 'GET') {
      const namespace = url.searchParams.get('namespace') ?? 'default'
      const name = url.searchParams.get('name')
      if (!name) {
        return Response.json({ error: 'name required' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const catalog = getCatalog(env, namespace)

      // First try sharded catalog
      let table = await catalog.loadTable(namespace, name)

      // If not found and this is default namespace, also check legacy catalog
      if (!table && namespace === 'default') {
        const legacyCatalog = getLegacyCatalog(env)
        table = await legacyCatalog.loadTable(namespace, name)
      }

      if (!table) {
        return Response.json({ error: 'not found' }, { status: 404, headers: authCorsHeaders(request, env) })
      }
      return Response.json({ table }, { headers: authCorsHeaders(request, env) })
    }

    // POST /catalog/commit - commit a snapshot to namespace-sharded catalog
    if (path === '/commit' && request.method === 'POST') {
      let body: unknown
      try {
        body = await request.json()
      } catch {
        return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return Response.json({ error: 'Invalid JSON body' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const { namespace, table, files, cdcBookmark } = body as Record<string, unknown>
      if (!namespace || typeof namespace !== 'string') {
        return Response.json({ error: 'Missing required field: namespace (string)' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (!table || typeof table !== 'string') {
        return Response.json({ error: 'Missing required field: table (string)' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      if (!Array.isArray(files)) {
        return Response.json({ error: 'Missing required field: files (array)' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      for (let i = 0; i < files.length; i++) {
        const f = files[i]
        if (!f || typeof f !== 'object') {
          return Response.json({ error: `Invalid file entry at index ${i}` }, { status: 400, headers: authCorsHeaders(request, env) })
        }
        if (typeof f.path !== 'string') {
          return Response.json({ error: `Missing required field: files[${i}].path (string)` }, { status: 400, headers: authCorsHeaders(request, env) })
        }
        if (f.format !== 'parquet' && f.format !== 'jsonl') {
          return Response.json({ error: `Invalid files[${i}].format: must be 'parquet' or 'jsonl'` }, { status: 400, headers: authCorsHeaders(request, env) })
        }
        if (typeof f.recordCount !== 'number') {
          return Response.json({ error: `Missing required field: files[${i}].recordCount (number)` }, { status: 400, headers: authCorsHeaders(request, env) })
        }
        if (typeof f.fileSizeBytes !== 'number') {
          return Response.json({ error: `Missing required field: files[${i}].fileSizeBytes (number)` }, { status: 400, headers: authCorsHeaders(request, env) })
        }
      }
      if (cdcBookmark !== undefined && typeof cdcBookmark !== 'string') {
        return Response.json({ error: 'Invalid field: cdcBookmark must be a string' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const catalog = getCatalog(env, namespace)
      const snapshot = await catalog.commitSnapshot(
        namespace,
        table,
        files.map((f: { path: string; format: 'parquet' | 'jsonl'; recordCount: number; fileSizeBytes: number }) => ({
          ...f,
          createdAt: new Date().toISOString(),
        })),
        { cdcBookmark: cdcBookmark as string | undefined },
      )
      return Response.json({ ok: true, snapshot }, { headers: authCorsHeaders(request, env) })
    }

    // GET /catalog/query?namespace=xxx&table=yyy
    if (path === '/query' && request.method === 'GET') {
      const namespace = url.searchParams.get('namespace') ?? 'default'
      const name = url.searchParams.get('table')
      const where = url.searchParams.get('where') ?? undefined
      const limit = url.searchParams.get('limit')
      if (!name) {
        return Response.json({ error: 'table required' }, { status: 400, headers: authCorsHeaders(request, env) })
      }

      // Validate where clause to prevent SQL injection
      if (where) {
        const validation = validateWhereClause(where)
        if (!validation.valid) {
          return Response.json(
            { error: `Invalid where clause: ${validation.reason}` },
            { status: 400, headers: authCorsHeaders(request, env) }
          )
        }
      }

      const catalog = getCatalog(env, namespace)

      // First try sharded catalog
      let sql = await catalog.buildQuery(namespace, name, {
        where,
        limit: limit ? parseInt(limit) : undefined,
      })

      // If no result and this is default namespace, also check legacy catalog
      if (!sql && namespace === 'default') {
        const legacyCatalog = getLegacyCatalog(env)
        sql = await legacyCatalog.buildQuery(namespace, name, {
          where,
          limit: limit ? parseInt(limit) : undefined,
        })
      }

      return Response.json({ sql }, { headers: authCorsHeaders(request, env) })
    }

    // GET /catalog/files?namespace=xxx&table=yyy
    if (path === '/files' && request.method === 'GET') {
      const namespace = url.searchParams.get('namespace') ?? 'default'
      const name = url.searchParams.get('table')
      if (!name) {
        return Response.json({ error: 'table required' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const catalog = getCatalog(env, namespace)

      // First try sharded catalog
      let files = await catalog.listFiles(namespace, name)

      // If no files and this is default namespace, also check legacy catalog
      if (files.length === 0 && namespace === 'default') {
        const legacyCatalog = getLegacyCatalog(env)
        files = await legacyCatalog.listFiles(namespace, name)
      }

      return Response.json({ files }, { headers: authCorsHeaders(request, env) })
    }

    return Response.json({ error: 'unknown catalog endpoint', path }, { status: 404, headers: authCorsHeaders(request, env) })
  } catch (e) {
    return Response.json(
      { error: e instanceof Error ? e.message : 'unknown error' },
      { status: 500, headers: authCorsHeaders(request, env) }
    )
  }
}
