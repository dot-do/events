/**
 * Catalog API route handler - /catalog/*
 */

import type { Env } from '../env'
import { authCorsHeaders } from '../utils'

export async function handleCatalog(request: Request, env: Env, url: URL): Promise<Response> {
  const catalogId = env.CATALOG.idFromName('events-catalog')
  const catalog = env.CATALOG.get(catalogId)

  const path = url.pathname.replace('/catalog', '') || '/'

  try {
    // GET /catalog/namespaces
    if (path === '/namespaces' && request.method === 'GET') {
      const namespaces = await catalog.listNamespaces()
      return Response.json({ namespaces }, { headers: authCorsHeaders(request, env) })
    }

    // POST /catalog/namespaces
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
      await catalog.createNamespace(name, properties as Record<string, string> | undefined)
      return Response.json({ ok: true, namespace: name }, { headers: authCorsHeaders(request, env) })
    }

    // GET /catalog/tables?namespace=xxx
    if (path === '/tables' && request.method === 'GET') {
      const namespace = url.searchParams.get('namespace') ?? 'default'
      const tables = await catalog.listTables(namespace)
      return Response.json({ namespace, tables }, { headers: authCorsHeaders(request, env) })
    }

    // POST /catalog/tables
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
      const table = await catalog.createTable(
        namespace,
        name,
        schema as { name: string; type: string; nullable?: boolean }[],
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
      const table = await catalog.loadTable(namespace, name)
      if (!table) {
        return Response.json({ error: 'not found' }, { status: 404, headers: authCorsHeaders(request, env) })
      }
      return Response.json({ table }, { headers: authCorsHeaders(request, env) })
    }

    // POST /catalog/commit - commit a snapshot
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
      const snapshot = await catalog.commitSnapshot(
        namespace,
        table,
        files as { path: string; format: 'parquet' | 'jsonl'; recordCount: number; fileSizeBytes: number }[],
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
      const sql = await catalog.buildQuery(namespace, name, {
        where,
        limit: limit ? parseInt(limit) : undefined,
      })
      return Response.json({ sql }, { headers: authCorsHeaders(request, env) })
    }

    // GET /catalog/files?namespace=xxx&table=yyy
    if (path === '/files' && request.method === 'GET') {
      const namespace = url.searchParams.get('namespace') ?? 'default'
      const name = url.searchParams.get('table')
      if (!name) {
        return Response.json({ error: 'table required' }, { status: 400, headers: authCorsHeaders(request, env) })
      }
      const files = await catalog.listFiles(namespace, name)
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
