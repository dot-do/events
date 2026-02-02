/**
 * Deduplication middleware for ingest
 *
 * Provides idempotent ingestion via batchId:
 * - Checks if batchId already exists in R2
 * - Returns success without re-writing if deduplicated
 * - Uses namespace-isolated dedup keys
 */

import { InvalidR2PathError } from '../../utils'
import { buildNamespacedR2Path } from '../tenant'
import { InvalidPathError, toErrorResponse } from '../../../core/src/errors'
import { corsHeaders } from '../../utils'
import type { IngestContext, IngestMiddleware, MiddlewareResult } from './types'

// Re-export buildNamespacedR2Path for use in dedup key building
export { buildNamespacedR2Path } from '../tenant'

// ============================================================================
// Deduplication Middleware
// ============================================================================

/**
 * Check for duplicate batch and return early if already ingested
 */
export const dedupMiddleware: IngestMiddleware = async (
  context: IngestContext
): Promise<MiddlewareResult> => {
  const { batchId, tenant, env, batch } = context

  // No batchId means no deduplication needed
  if (!batchId) {
    return { continue: true }
  }

  // Build namespace-isolated dedup key
  let dedupKey: string
  try {
    // Namespace-isolated dedup key: ns/<namespace>/dedup/<batchId>
    dedupKey = buildNamespacedR2Path(tenant, 'dedup', batchId)
  } catch (err) {
    if (err instanceof InvalidR2PathError) {
      return {
        continue: false,
        response: toErrorResponse(
          new InvalidPathError(`Invalid batchId: ${err.message}`, { path: err.path }),
          { headers: corsHeaders() }
        ),
      }
    }
    throw err
  }

  // Check if already ingested
  const existing = await env.EVENTS_BUCKET.head(dedupKey)
  if (existing) {
    // Already ingested - return success without re-writing (idempotent)
    context.deduplicated = true
    return {
      continue: false,
      response: Response.json(
        {
          ok: true,
          received: batch!.events.length,
          deduplicated: true,
          namespace: tenant.namespace,
        },
        { headers: corsHeaders() }
      ),
    }
  }

  return { continue: true }
}

/**
 * Write dedup marker to R2 after successful ingestion
 * Called in the background via ctx.waitUntil
 */
export async function writeDedupMarker(
  context: IngestContext
): Promise<void> {
  const { batchId, tenant, env, batch } = context

  if (!batchId) return

  // Use namespace-isolated dedup key
  const dedupKey = buildNamespacedR2Path(tenant, 'dedup', batchId)

  await env.EVENTS_BUCKET.put(
    dedupKey,
    JSON.stringify({
      batchId,
      namespace: tenant.namespace,
      eventCount: batch!.events.length,
      ingestedAt: new Date().toISOString(),
    }),
    {
      httpMetadata: { contentType: 'application/json' },
      customMetadata: {
        batchId,
        namespace: tenant.namespace,
        eventCount: String(batch!.events.length),
        // Note: R2 does not support TTL natively. Configure R2 lifecycle rules
        // on the bucket to auto-delete objects under the dedup/ prefix
        // after a reasonable retention period (e.g., 24-48 hours).
        ttlNote: 'Configure R2 lifecycle rule to expire dedup/ prefix objects',
      },
    }
  )
}
