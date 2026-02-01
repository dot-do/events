/**
 * Rate limiting middleware for events.do /ingest endpoint
 *
 * Uses a Durable Object with SQLite for sliding window rate limiting.
 * Limits both request count and event count per time window.
 *
 * Identification priority:
 * 1. API key from Authorization: Bearer header
 * 2. Client IP address (from CF-Connecting-IP or X-Forwarded-For)
 */

import type { RateLimiterDO } from './rate-limiter-do'
import { corsHeaders } from '../utils'

/** Environment variables for rate limiting */
export interface RateLimitEnv {
  RATE_LIMITER: DurableObjectNamespace<RateLimiterDO>
  /** Max requests per minute (default: 1000) */
  RATE_LIMIT_REQUESTS_PER_MINUTE?: string
  /** Max events per minute (default: 100000) */
  RATE_LIMIT_EVENTS_PER_MINUTE?: string
}

/** Result of rate limit check */
export interface RateLimitResult {
  allowed: boolean
  requestsRemaining: number
  eventsRemaining: number
  resetAt: number
  retryAfterSeconds?: number
}

/**
 * Get the rate limit key from the request.
 * Prefers API key (from Authorization header), falls back to IP address.
 */
export function getRateLimitKey(request: Request): string {
  // Try API key first
  const auth = request.headers.get('Authorization')
  if (auth?.startsWith('Bearer ')) {
    const token = auth.slice(7)
    // Use a hash prefix to identify API key-based limits
    return `key:${token}`
  }

  // Fall back to IP address
  const cfIp = request.headers.get('CF-Connecting-IP')
  if (cfIp) {
    return `ip:${cfIp}`
  }

  const xff = request.headers.get('X-Forwarded-For')
  if (xff) {
    // Take the first IP from X-Forwarded-For (client IP)
    const clientIp = xff.split(',')[0]?.trim()
    if (clientIp) {
      return `ip:${clientIp}`
    }
  }

  // Last resort: use a default key (should not happen in production)
  return 'ip:unknown'
}

/**
 * Check rate limit for an ingest request.
 *
 * @param request - The incoming request
 * @param env - Environment with RATE_LIMITER binding
 * @param eventCount - Number of events in the batch
 * @returns Response if rate limited (429), null if allowed
 */
export async function checkRateLimit(
  request: Request,
  env: RateLimitEnv,
  eventCount: number,
): Promise<Response | null> {
  // Skip rate limiting if RATE_LIMITER is not configured
  if (!env.RATE_LIMITER) {
    return null
  }

  const key = getRateLimitKey(request)
  const requestsPerMinute = parseInt(env.RATE_LIMIT_REQUESTS_PER_MINUTE || '1000', 10)
  const eventsPerMinute = parseInt(env.RATE_LIMIT_EVENTS_PER_MINUTE || '100000', 10)

  try {
    // Get the rate limiter DO for this key
    const rateLimiterId = env.RATE_LIMITER.idFromName(key)
    const rateLimiter = env.RATE_LIMITER.get(rateLimiterId)

    // Check and increment the rate limit
    const result = await rateLimiter.checkAndIncrement(
      requestsPerMinute,
      eventsPerMinute,
      eventCount,
    )

    if (!result.allowed) {
      const retryAfter = result.retryAfterSeconds || 60

      return Response.json(
        {
          error: 'Too Many Requests',
          message: 'Rate limit exceeded. Please slow down your request rate.',
          limits: {
            requestsPerMinute,
            eventsPerMinute,
          },
          current: {
            requestsRemaining: result.requestsRemaining,
            eventsRemaining: result.eventsRemaining,
          },
          resetAt: new Date(result.resetAt).toISOString(),
          retryAfterSeconds: retryAfter,
        },
        {
          status: 429,
          headers: {
            ...corsHeaders(),
            'Retry-After': String(retryAfter),
            'X-RateLimit-Limit-Requests': String(requestsPerMinute),
            'X-RateLimit-Limit-Events': String(eventsPerMinute),
            'X-RateLimit-Remaining-Requests': String(Math.max(0, result.requestsRemaining)),
            'X-RateLimit-Remaining-Events': String(Math.max(0, result.eventsRemaining)),
            'X-RateLimit-Reset': String(result.resetAt),
          },
        },
      )
    }

    // Request allowed - return null (continue processing)
    return null
  } catch (err) {
    // Log the error but don't block the request on rate limiter failures
    console.error('[rate-limit] Error checking rate limit:', err)
    return null
  }
}

/**
 * Add rate limit headers to a successful response.
 * Call this after checkRateLimit returns null to include rate limit info.
 */
export function addRateLimitHeaders(
  headers: Headers,
  env: RateLimitEnv,
  result?: RateLimitResult,
): void {
  if (!result) return

  const requestsPerMinute = parseInt(env.RATE_LIMIT_REQUESTS_PER_MINUTE || '1000', 10)
  const eventsPerMinute = parseInt(env.RATE_LIMIT_EVENTS_PER_MINUTE || '100000', 10)

  headers.set('X-RateLimit-Limit-Requests', String(requestsPerMinute))
  headers.set('X-RateLimit-Limit-Events', String(eventsPerMinute))
  headers.set('X-RateLimit-Remaining-Requests', String(Math.max(0, result.requestsRemaining)))
  headers.set('X-RateLimit-Remaining-Events', String(Math.max(0, result.eventsRemaining)))
  headers.set('X-RateLimit-Reset', String(result.resetAt))
}
