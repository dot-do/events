/**
 * Quota checking middleware for ingest pipeline
 *
 * Enforces per-namespace resource quotas:
 * - max_events_per_day: Daily event ingestion limit
 *
 * Quotas are checked before processing and usage is tracked after successful ingestion.
 */

import type { IngestContext, MiddlewareResult } from './types'
import { corsHeaders } from '../../utils'
import { createLogger, logError } from '../../logger'

const log = createLogger({ component: 'QuotaMiddleware' })

// ============================================================================
// Quota Checking Middleware
// ============================================================================

/**
 * Middleware that checks event quota before allowing ingestion.
 * Returns 429 Too Many Requests if quota would be exceeded.
 */
export async function quotaMiddleware(
  context: IngestContext
): Promise<MiddlewareResult> {
  const { env, tenant, batch } = context

  // Skip quota check if no CATALOG binding (graceful degradation)
  if (!env.CATALOG) {
    log.warn('CATALOG binding not available, skipping quota check')
    return { continue: true }
  }

  // Skip if no batch parsed yet
  if (!batch) {
    return { continue: true }
  }

  const eventCount = batch.events.length
  const namespace = tenant.namespace

  try {
    // Get the CatalogDO for this namespace
    const catalogId = env.CATALOG.idFromName('catalog')
    const catalog = env.CATALOG.get(catalogId)

    // Call checkEventQuota via RPC
    const result = await (catalog as unknown as {
      checkEventQuota(namespace: string, eventCount: number): Promise<{ allowed: boolean; reason?: string }>
    }).checkEventQuota(namespace, eventCount)

    if (!result.allowed) {
      log.warn('Event quota exceeded', {
        namespace,
        eventCount,
        reason: result.reason,
      })

      return {
        continue: false,
        response: Response.json(
          {
            error: 'Quota exceeded',
            code: 'QUOTA_EXCEEDED',
            reason: result.reason,
          },
          {
            status: 429,
            headers: {
              ...corsHeaders(),
              'Retry-After': '3600', // Suggest retry in 1 hour
            },
          }
        ),
      }
    }

    // Store for later tracking
    context.quotaCheckPassed = true

    return { continue: true }
  } catch (err) {
    // Log error but allow request to continue (fail-open for availability)
    logError(log, 'Error checking quota', err)
    return { continue: true }
  }
}

// ============================================================================
// Quota Tracking
// ============================================================================

/**
 * Track event usage after successful ingestion.
 * This should be called in waitUntil() after the response is sent.
 */
export async function trackEventUsage(context: IngestContext): Promise<void> {
  const { env, tenant, batch } = context

  // Skip if no CATALOG binding or quota check didn't pass
  if (!env.CATALOG || !batch || !context.quotaCheckPassed) {
    return
  }

  const eventCount = batch.events.length
  const namespace = tenant.namespace

  try {
    const catalogId = env.CATALOG.idFromName('catalog')
    const catalog = env.CATALOG.get(catalogId)

    // Call incrementEventCount via RPC
    await (catalog as unknown as {
      incrementEventCount(namespace: string, count: number): Promise<void>
    }).incrementEventCount(namespace, eventCount)

    log.debug('Event usage tracked', {
      namespace,
      eventCount,
    })
  } catch (err) {
    // Log error but don't fail the request
    logError(log, 'Error tracking event usage', err)
  }
}

// ============================================================================
// Extended Context Type
// ============================================================================

// Extend IngestContext to include quota-related fields
declare module './types' {
  interface IngestContext {
    /** Whether quota check passed (used for tracking) */
    quotaCheckPassed?: boolean
  }
}
