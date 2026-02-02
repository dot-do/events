/**
 * Shard management route handlers
 *
 * Endpoints:
 * - GET /shards - Get shard statistics and active shards
 * - GET /shards/config - Get shard coordinator configuration
 * - PUT /shards/config - Update shard coordinator configuration
 * - POST /shards/scale - Force scale to a specific shard count
 * - GET /shards/:shardId - Get specific shard stats
 */

import type { Env } from '../env'
import { corsHeaders } from '../utils'
import { getShardStats, forceScaleShards, getEventWriterDO, getShardCoordinator } from '../event-writer-do'

/**
 * Handle GET /shards - Get shard statistics
 */
export async function handleGetShards(request: Request, env: Env): Promise<Response> {
  const stats = await getShardStats(env)

  if (!stats) {
    return Response.json({
      error: 'Shard coordinator not available',
      message: 'The SHARD_COORDINATOR binding is not configured. Dynamic sharding is disabled.',
      fallbackShards: [0],
    }, { status: 200, headers: corsHeaders() })
  }

  return Response.json({
    ...stats,
    dynamicShardingEnabled: true,
  }, { headers: corsHeaders() })
}

/**
 * Handle GET /shards/config - Get coordinator configuration
 */
export async function handleGetShardConfig(request: Request, env: Env): Promise<Response> {
  const coordinator = getShardCoordinator(env)

  if (!coordinator) {
    return Response.json({
      error: 'Shard coordinator not available',
      dynamicShardingEnabled: false,
    }, { status: 200, headers: corsHeaders() })
  }

  try {
    const config = await coordinator.getConfig()
    return Response.json({
      config,
      dynamicShardingEnabled: true,
    }, { headers: corsHeaders() })
  } catch (err) {
    return Response.json({
      error: 'Failed to get shard config',
      message: String(err),
    }, { status: 500, headers: corsHeaders() })
  }
}

/**
 * Handle PUT /shards/config - Update coordinator configuration
 */
export async function handleUpdateShardConfig(request: Request, env: Env): Promise<Response> {
  const coordinator = getShardCoordinator(env)

  if (!coordinator) {
    return Response.json({
      error: 'Shard coordinator not available',
    }, { status: 400, headers: corsHeaders() })
  }

  let updates: Record<string, unknown>
  try {
    updates = await request.json() as Record<string, unknown>
  } catch (err) {
    return Response.json({
      error: 'Invalid JSON body',
    }, { status: 400, headers: corsHeaders() })
  }

  // Validate config updates
  const validKeys = ['minShards', 'maxShards', 'scaleUpThreshold', 'scaleDownThreshold', 'cooldownMs', 'metricsWindowMs', 'healthCheckIntervalMs']
  const invalidKeys = Object.keys(updates).filter(k => !validKeys.includes(k))
  if (invalidKeys.length > 0) {
    return Response.json({
      error: 'Invalid configuration keys',
      invalidKeys,
      validKeys,
    }, { status: 400, headers: corsHeaders() })
  }

  try {
    const config = await coordinator.updateConfig(updates)
    return Response.json({
      ok: true,
      config,
    }, { headers: corsHeaders() })
  } catch (err) {
    return Response.json({
      error: 'Failed to update shard config',
      message: String(err),
    }, { status: 500, headers: corsHeaders() })
  }
}

/**
 * Handle POST /shards/scale - Force scale to specific shard count
 */
export async function handleForceScale(request: Request, env: Env): Promise<Response> {
  let body: { targetCount?: number }
  try {
    body = await request.json() as { targetCount?: number }
  } catch (err) {
    return Response.json({
      error: 'Invalid JSON body',
    }, { status: 400, headers: corsHeaders() })
  }

  const { targetCount } = body
  if (typeof targetCount !== 'number' || targetCount < 1) {
    return Response.json({
      error: 'Invalid targetCount',
      message: 'targetCount must be a positive integer',
    }, { status: 400, headers: corsHeaders() })
  }

  const result = await forceScaleShards(env, targetCount)

  if (!result) {
    return Response.json({
      error: 'Shard coordinator not available',
    }, { status: 400, headers: corsHeaders() })
  }

  return Response.json({
    ok: true,
    ...result,
  }, { headers: corsHeaders() })
}

/**
 * Handle GET /shards/:shardId - Get specific shard stats
 */
export async function handleGetShardById(request: Request, env: Env, shardId: number): Promise<Response> {
  try {
    const stub = getEventWriterDO(env, shardId)
    const stats = await stub.stats()

    return Response.json({
      ...stats,
      exists: true,
    }, { headers: corsHeaders() })
  } catch (err) {
    return Response.json({
      error: 'Failed to get shard stats',
      message: String(err),
      shardId,
    }, { status: 500, headers: corsHeaders() })
  }
}

/**
 * Handle POST /shards/health-check - Start health checks (manual trigger)
 */
export async function handleStartHealthChecks(request: Request, env: Env): Promise<Response> {
  const coordinator = getShardCoordinator(env)

  if (!coordinator) {
    return Response.json({
      error: 'Shard coordinator not available',
    }, { status: 400, headers: corsHeaders() })
  }

  try {
    await coordinator.startHealthChecks()
    return Response.json({
      ok: true,
      message: 'Health checks started',
    }, { headers: corsHeaders() })
  } catch (err) {
    return Response.json({
      error: 'Failed to start health checks',
      message: String(err),
    }, { status: 500, headers: corsHeaders() })
  }
}

/**
 * Main shard routes handler
 */
export async function handleShardRoutes(request: Request, env: Env, url: URL): Promise<Response | null> {
  const path = url.pathname

  // GET /shards - Get stats
  if (path === '/shards') {
    if (request.method !== 'GET') {
      return Response.json({ error: 'Method not allowed' }, {
        status: 405,
        headers: { ...corsHeaders(), 'Allow': 'GET' }
      })
    }
    return handleGetShards(request, env)
  }

  // /shards/config - supports GET and PUT
  if (path === '/shards/config') {
    if (request.method === 'GET') {
      return handleGetShardConfig(request, env)
    } else if (request.method === 'PUT') {
      return handleUpdateShardConfig(request, env)
    } else {
      return Response.json({ error: 'Method not allowed' }, {
        status: 405,
        headers: { ...corsHeaders(), 'Allow': 'GET, PUT' }
      })
    }
  }

  // POST /shards/scale - Force scale
  if (path === '/shards/scale') {
    if (request.method !== 'POST') {
      return Response.json({ error: 'Method not allowed' }, {
        status: 405,
        headers: { ...corsHeaders(), 'Allow': 'POST' }
      })
    }
    return handleForceScale(request, env)
  }

  // POST /shards/health-check - Start health checks
  if (path === '/shards/health-check') {
    if (request.method !== 'POST') {
      return Response.json({ error: 'Method not allowed' }, {
        status: 405,
        headers: { ...corsHeaders(), 'Allow': 'POST' }
      })
    }
    return handleStartHealthChecks(request, env)
  }

  // GET /shards/:shardId - Get specific shard
  const shardMatch = path.match(/^\/shards\/(\d+)$/)
  if (shardMatch && shardMatch[1]) {
    if (request.method !== 'GET') {
      return Response.json({ error: 'Method not allowed' }, {
        status: 405,
        headers: { ...corsHeaders(), 'Allow': 'GET' }
      })
    }
    const shardId = parseInt(shardMatch[1], 10)
    return handleGetShardById(request, env, shardId)
  }

  return null
}
