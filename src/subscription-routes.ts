/**
 * Subscription Routes - REST API for SubscriptionDO
 *
 * Provides HTTP endpoints for managing event subscriptions:
 * - POST /subscriptions/subscribe - Create new subscription
 * - POST /subscriptions/unsubscribe - Deactivate subscription
 * - GET /subscriptions/list - List subscriptions
 * - GET /subscriptions/status/:id - Get subscription status with stats
 * - GET /subscriptions/:id - Get single subscription
 * - PUT /subscriptions/:id - Update subscription settings
 * - DELETE /subscriptions/:id - Permanently delete subscription
 * - GET /subscriptions/:id/dead-letters - Get dead letters
 * - POST /subscriptions/:id/dead-letters/:deadLetterId/retry - Retry dead letter
 * - GET /subscriptions/deliveries/:id/logs - Get delivery logs
 *
 * Subscriptions are sharded by top-level event type prefix (e.g., "collection",
 * "webhook", "rpc") to distribute load across multiple SubscriptionDO instances.
 */

import type { SubscriptionDO } from '../core/src/subscription'
import { corsHeaders as baseCorsHeaders } from './utils'
import type { Env } from './env'

type SubscriptionEnv = Pick<Env, 'SUBSCRIPTIONS'>

/**
 * CORS headers for cross-origin requests (includes Content-Type for subscription routes)
 */
function corsHeaders(): HeadersInit {
  return {
    ...baseCorsHeaders(),
    'Content-Type': 'application/json',
  }
}

/**
 * Well-known shard prefixes for subscription fanout.
 * Used by list-all operations to query across all shards.
 */
export const KNOWN_SUBSCRIPTION_SHARDS = [
  'collection',
  'rpc',
  'do',
  'ws',
  'webhook',
  'default',
]

/**
 * Extract the shard key from a subscription pattern or event type.
 * Uses the top-level prefix (first segment before '.') as the shard key.
 *
 * Examples:
 *   "collection.insert.*" -> "collection"
 *   "webhook.github.*"    -> "webhook"
 *   "rpc.call"            -> "rpc"
 *   "**"                  -> "default"
 *   ""                    -> "default"
 */
export function getSubscriptionShard(pattern: string): string {
  const prefix = pattern.split('.')[0] || 'default'
  // Wildcard-only patterns go to default shard
  if (prefix === '*' || prefix === '**') return 'default'
  return prefix
}

/**
 * Get a SubscriptionDO stub for the given shard key.
 */
function getSubscriptionStub(env: SubscriptionEnv, shard: string = 'default'): DurableObjectStub<SubscriptionDO> {
  const id = env.SUBSCRIPTIONS.idFromName(shard)
  return env.SUBSCRIPTIONS.get(id)
}

/**
 * Get stubs for all known shards (used for list-all and cross-shard queries).
 */
function getAllSubscriptionStubs(env: SubscriptionEnv): { shard: string; stub: DurableObjectStub<SubscriptionDO> }[] {
  return KNOWN_SUBSCRIPTION_SHARDS.map(shard => ({
    shard,
    stub: getSubscriptionStub(env, shard),
  }))
}

/**
 * Handle subscription-related routes
 * Returns null if the route is not handled
 */
export async function handleSubscriptionRoutes(
  request: Request,
  env: SubscriptionEnv,
  url: URL
): Promise<Response | null> {
  // Only handle /subscriptions routes
  if (!url.pathname.startsWith('/subscriptions')) {
    return null
  }

  const path = url.pathname.replace('/subscriptions', '') || '/'

  try {
    // POST /subscriptions/subscribe
    if (path === '/subscribe' && request.method === 'POST') {
      let body: unknown
      try {
        body = await request.json()
      } catch {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      const { workerId, workerBinding, pattern, rpcMethod, maxRetries, timeoutMs } = body as Record<string, unknown>

      // Validate required fields
      if (!workerId || typeof workerId !== 'string') {
        return Response.json(
          { ok: false, error: 'Missing required field: workerId (string)' },
          { status: 400, headers: corsHeaders() }
        )
      }
      if (!pattern || typeof pattern !== 'string') {
        return Response.json(
          { ok: false, error: 'Missing required field: pattern (string)' },
          { status: 400, headers: corsHeaders() }
        )
      }
      if (!rpcMethod || typeof rpcMethod !== 'string') {
        return Response.json(
          { ok: false, error: 'Missing required field: rpcMethod (string)' },
          { status: 400, headers: corsHeaders() }
        )
      }
      if (workerBinding !== undefined && typeof workerBinding !== 'string') {
        return Response.json(
          { ok: false, error: 'Invalid field: workerBinding must be a string' },
          { status: 400, headers: corsHeaders() }
        )
      }
      if (maxRetries !== undefined && (typeof maxRetries !== 'number' || !Number.isInteger(maxRetries))) {
        return Response.json(
          { ok: false, error: 'Invalid field: maxRetries must be an integer' },
          { status: 400, headers: corsHeaders() }
        )
      }
      if (timeoutMs !== undefined && (typeof timeoutMs !== 'number' || !Number.isInteger(timeoutMs))) {
        return Response.json(
          { ok: false, error: 'Invalid field: timeoutMs must be an integer' },
          { status: 400, headers: corsHeaders() }
        )
      }

      // Route to the correct shard based on pattern prefix
      const shard = getSubscriptionShard(pattern)
      const stub = getSubscriptionStub(env, shard)
      const result = await stub.subscribe({
        workerId,
        workerBinding: workerBinding as string | undefined,
        pattern,
        rpcMethod,
        maxRetries: maxRetries as number | undefined,
        timeoutMs: timeoutMs as number | undefined,
      })
      return Response.json({ ...result, shard }, {
        status: result.ok ? 200 : 400,
        headers: corsHeaders(),
      })
    }

    // POST /subscriptions/unsubscribe
    if (path === '/unsubscribe' && request.method === 'POST') {
      let body: unknown
      try {
        body = await request.json()
      } catch {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      const { subscriptionId, shard: requestedShard } = body as Record<string, unknown>

      if (!subscriptionId || typeof subscriptionId !== 'string') {
        return Response.json(
          { ok: false, error: 'Missing required field: subscriptionId (string)' },
          { status: 400, headers: corsHeaders() }
        )
      }

      // If shard is provided, use it directly; otherwise query all shards
      if (requestedShard && typeof requestedShard === 'string') {
        const stub = getSubscriptionStub(env, requestedShard)
        const result = await stub.unsubscribe(subscriptionId)
        return Response.json(result, { headers: corsHeaders() })
      }

      // Try all shards to find the subscription
      for (const { stub } of getAllSubscriptionStubs(env)) {
        const sub = await stub.getSubscription(subscriptionId)
        if (sub) {
          const result = await stub.unsubscribe(subscriptionId)
          return Response.json(result, { headers: corsHeaders() })
        }
      }

      return Response.json({ ok: false, error: 'Subscription not found' }, { status: 404, headers: corsHeaders() })
    }

    // POST /subscriptions/reactivate
    if (path === '/reactivate' && request.method === 'POST') {
      let body: unknown
      try {
        body = await request.json()
      } catch {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      const { subscriptionId, shard: requestedShard } = body as Record<string, unknown>

      if (!subscriptionId || typeof subscriptionId !== 'string') {
        return Response.json(
          { ok: false, error: 'Missing required field: subscriptionId (string)' },
          { status: 400, headers: corsHeaders() }
        )
      }

      if (requestedShard && typeof requestedShard === 'string') {
        const stub = getSubscriptionStub(env, requestedShard)
        const result = await stub.reactivate(subscriptionId)
        return Response.json(result, { headers: corsHeaders() })
      }

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const sub = await stub.getSubscription(subscriptionId)
        if (sub) {
          const result = await stub.reactivate(subscriptionId)
          return Response.json(result, { headers: corsHeaders() })
        }
      }

      return Response.json({ ok: false, error: 'Subscription not found' }, { status: 404, headers: corsHeaders() })
    }

    // GET /subscriptions/list - queries across all shards (or a specific one)
    if (path === '/list' && request.method === 'GET') {
      const active = url.searchParams.get('active')
      const workerId = url.searchParams.get('workerId')
      const patternPrefix = url.searchParams.get('patternPrefix')
      const limit = url.searchParams.get('limit')
      const offset = url.searchParams.get('offset')
      const shard = url.searchParams.get('shard')

      const filterOpts = {
        active: active !== null ? active === 'true' : undefined,
        workerId: workerId ?? undefined,
        patternPrefix: patternPrefix ?? undefined,
        limit: limit ? parseInt(limit, 10) : undefined,
        offset: offset ? parseInt(offset, 10) : undefined,
      }

      // If a specific shard is requested, only query that one
      if (shard) {
        const stub = getSubscriptionStub(env, shard)
        const result = await stub.listSubscriptions(filterOpts)
        return Response.json({ subscriptions: result, shard }, { headers: corsHeaders() })
      }

      // Query all shards in parallel and merge results
      const allStubs = getAllSubscriptionStubs(env)
      const results = await Promise.all(
        allStubs.map(async ({ shard: shardName, stub }) => {
          const subs = await stub.listSubscriptions(filterOpts)
          return subs.map(sub => ({ ...sub, _shard: shardName }))
        })
      )

      // Merge, sort by createdAt descending, and apply global limit/offset
      let allSubscriptions = results.flat()
      allSubscriptions.sort((a, b) => b.createdAt - a.createdAt)

      const globalOffset = offset ? parseInt(offset, 10) : 0
      const globalLimit = limit ? parseInt(limit, 10) : allSubscriptions.length
      allSubscriptions = allSubscriptions.slice(globalOffset, globalOffset + globalLimit)

      return Response.json({ subscriptions: allSubscriptions }, { headers: corsHeaders() })
    }

    // GET /subscriptions/match?eventType=xxx - route to correct shard + default
    if (path === '/match' && request.method === 'GET') {
      const eventType = url.searchParams.get('eventType')

      if (!eventType) {
        return Response.json(
          { ok: false, error: 'Missing required query param: eventType' },
          { status: 400, headers: corsHeaders() }
        )
      }

      // Route to the shard for this event type, plus the default shard for catch-all patterns
      const shard = getSubscriptionShard(eventType)
      const shards = shard === 'default' ? ['default'] : [shard, 'default']
      const uniqueShards = [...new Set(shards)]

      const results = await Promise.all(
        uniqueShards.map(s => getSubscriptionStub(env, s).findMatchingSubscriptions(eventType))
      )

      return Response.json({ subscriptions: results.flat() }, { headers: corsHeaders() })
    }

    // GET /subscriptions/status/:id - try all shards
    const statusMatch = path.match(/^\/status\/([A-Z0-9]+)$/)
    if (statusMatch && request.method === 'GET') {
      const subscriptionId = statusMatch[1]!

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const result = await stub.getSubscriptionStatus(subscriptionId)
        if (result.subscription) {
          return Response.json(result, { headers: corsHeaders() })
        }
      }

      return Response.json(
        { subscription: null, stats: { pendingDeliveries: 0, failedDeliveries: 0, deadLetters: 0, successRate: 0, totalDelivered: 0, totalAttempts: 0 } },
        { headers: corsHeaders() }
      )
    }

    // GET /subscriptions/deliveries/:id/logs - try all shards
    const logsMatch = path.match(/^\/deliveries\/([A-Z0-9]+)\/logs$/)
    if (logsMatch && request.method === 'GET') {
      const deliveryId = logsMatch[1]!

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const result = await stub.getDeliveryLogs(deliveryId)
        if (result.length > 0) {
          return Response.json({ logs: result }, { headers: corsHeaders() })
        }
      }

      return Response.json({ logs: [] }, { headers: corsHeaders() })
    }

    // GET /subscriptions/:id/dead-letters - try all shards
    const deadLettersMatch = path.match(/^\/([A-Z0-9]+)\/dead-letters$/)
    if (deadLettersMatch && request.method === 'GET') {
      const subscriptionId = deadLettersMatch[1]!
      const limit = url.searchParams.get('limit')

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const sub = await stub.getSubscription(subscriptionId)
        if (sub) {
          const result = await stub.getDeadLetters(subscriptionId, limit ? parseInt(limit, 10) : undefined)
          return Response.json({ deadLetters: result }, { headers: corsHeaders() })
        }
      }

      return Response.json({ deadLetters: [] }, { headers: corsHeaders() })
    }

    // POST /subscriptions/:id/dead-letters/:deadLetterId/retry - try all shards
    const retryMatch = path.match(/^\/([A-Z0-9]+)\/dead-letters\/([A-Z0-9]+)\/retry$/)
    if (retryMatch && request.method === 'POST') {
      const deadLetterId = retryMatch[2]!

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const result = await stub.retryDeadLetter(deadLetterId)
        if (result.ok) {
          return Response.json(result, { status: 200, headers: corsHeaders() })
        }
      }

      return Response.json({ ok: false }, { status: 404, headers: corsHeaders() })
    }

    // GET /subscriptions/:id - try all shards
    const getMatch = path.match(/^\/([A-Z0-9]+)$/)
    if (getMatch && request.method === 'GET') {
      const subscriptionId = getMatch[1]!

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const result = await stub.getSubscription(subscriptionId)
        if (result) {
          return Response.json({ subscription: result }, { headers: corsHeaders() })
        }
      }

      return Response.json(
        { ok: false, error: 'Subscription not found' },
        { status: 404, headers: corsHeaders() }
      )
    }

    // PUT /subscriptions/:id - try all shards
    const updateMatch = path.match(/^\/([A-Z0-9]+)$/)
    if (updateMatch && request.method === 'PUT') {
      const subscriptionId = updateMatch[1]!
      let body: unknown
      try {
        body = await request.json()
      } catch {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        return Response.json({ ok: false, error: 'Invalid JSON body' }, { status: 400, headers: corsHeaders() })
      }
      const { maxRetries, timeoutMs, rpcMethod } = body as Record<string, unknown>
      if (maxRetries !== undefined && (typeof maxRetries !== 'number' || !Number.isInteger(maxRetries))) {
        return Response.json({ ok: false, error: 'Invalid field: maxRetries must be an integer' }, { status: 400, headers: corsHeaders() })
      }
      if (timeoutMs !== undefined && (typeof timeoutMs !== 'number' || !Number.isInteger(timeoutMs))) {
        return Response.json({ ok: false, error: 'Invalid field: timeoutMs must be an integer' }, { status: 400, headers: corsHeaders() })
      }
      if (rpcMethod !== undefined && typeof rpcMethod !== 'string') {
        return Response.json({ ok: false, error: 'Invalid field: rpcMethod must be a string' }, { status: 400, headers: corsHeaders() })
      }

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const sub = await stub.getSubscription(subscriptionId)
        if (sub) {
          const result = await stub.updateSubscription(subscriptionId, {
            maxRetries: maxRetries as number | undefined,
            timeoutMs: timeoutMs as number | undefined,
            rpcMethod: rpcMethod as string | undefined,
          })
          return Response.json(result, { headers: corsHeaders() })
        }
      }

      return Response.json({ ok: false, error: 'Subscription not found' }, { status: 404, headers: corsHeaders() })
    }

    // DELETE /subscriptions/:id - try all shards
    const deleteMatch = path.match(/^\/([A-Z0-9]+)$/)
    if (deleteMatch && request.method === 'DELETE') {
      const subscriptionId = deleteMatch[1]!

      for (const { stub } of getAllSubscriptionStubs(env)) {
        const sub = await stub.getSubscription(subscriptionId)
        if (sub) {
          const result = await stub.deleteSubscription(subscriptionId)
          return Response.json(result, { headers: corsHeaders() })
        }
      }

      return Response.json({ ok: false, error: 'Subscription not found' }, { status: 404, headers: corsHeaders() })
    }

    // Not found
    return Response.json(
      { ok: false, error: 'Unknown subscription endpoint', path },
      { status: 404, headers: corsHeaders() }
    )
  } catch (e) {
    console.error('Subscription route error:', e)
    return Response.json(
      { ok: false, error: e instanceof Error ? e.message : 'Unknown error' },
      { status: 500, headers: corsHeaders() }
    )
  }
}
