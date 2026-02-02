/**
 * Deduplication middleware for ingest
 *
 * Provides idempotent ingestion via batchId:
 * - Uses atomic conditional put to prevent race conditions
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

  // Atomic dedup check using conditional put
  // This prevents race conditions where multiple requests with the same batchId
  // could both pass a check-then-write sequence
  const dedupBody = JSON.stringify({
    batchId,
    namespace: tenant.namespace,
    eventCount: batch!.events.length,
    ingestedAt: new Date().toISOString(),
  })

  try {
    const result = await env.EVENTS_BUCKET.put(dedupKey, dedupBody, {
      onlyIf: { uploadedBefore: new Date(0) }, // Only write if doesn't exist
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
    })

    if (result === null) {
      // Dedup marker already exists - this is a duplicate
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

    // Successfully wrote dedup marker, mark it so writeDedupMarker skips
    context.dedupMarkerWritten = true
    return { continue: true }
  } catch (e) {
    // Handle precondition failed (412) - means marker already exists
    if (e instanceof Error && e.message.includes('412')) {
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
    throw e
  }
}

/**
 * Write dedup marker to R2 after successful ingestion
 * Called in the background via ctx.waitUntil
 *
 * Note: With atomic dedup, this is now a no-op since the marker is written
 * during the middleware check. Kept for backwards compatibility.
 */
export async function writeDedupMarker(
  context: IngestContext
): Promise<void> {
  const { batchId } = context

  // No batchId means no deduplication needed
  if (!batchId) return

  // Marker already written atomically in dedupMiddleware
  if (context.dedupMarkerWritten) return

  // This should not be reached with the atomic dedup implementation,
  // but kept as a safety net for any edge cases
}
