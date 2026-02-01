/**
 * Shared utilities for the events.do worker
 */

/**
 * CORS headers for public endpoints (webhooks, health, ingest)
 */
export function corsHeaders(): HeadersInit {
  return {
    'Access-Control-Allow-Origin': '*',
  }
}

/** Default allowed origins pattern for authenticated endpoints (*.do domains) */
const DEFAULT_ALLOWED_ORIGIN_PATTERN = /^https?:\/\/([a-z0-9-]+\.)*do$/

/**
 * Check if an origin is allowed for authenticated endpoints.
 * Reads ALLOWED_ORIGINS env var (comma-separated) or defaults to *.do domains.
 */
export function getAllowedOrigin(requestOrigin: string | null, env?: { ALLOWED_ORIGINS?: string }): string | null {
  if (!requestOrigin) return null

  // Check explicit allowed origins from env
  if (env?.ALLOWED_ORIGINS) {
    const allowed = env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
    if (allowed.includes(requestOrigin)) return requestOrigin
    // Also support wildcard patterns like *.do
    for (const pattern of allowed) {
      if (pattern.startsWith('*.')) {
        const suffix = pattern.slice(1) // e.g. ".do"
        try {
          const originHost = new URL(requestOrigin).hostname
          if (originHost.endsWith(suffix) || originHost === suffix.slice(1)) return requestOrigin
        } catch {
          // Invalid origin URL
        }
      }
    }
    return null
  }

  // Default: allow *.do domains
  if (DEFAULT_ALLOWED_ORIGIN_PATTERN.test(requestOrigin)) return requestOrigin
  return null
}

/**
 * CORS headers for authenticated endpoints - restrict to known .do origins.
 * Returns the request's Origin if it matches allowed origins, otherwise 'null'.
 */
export function authCorsHeaders(request: Request, env?: { ALLOWED_ORIGINS?: string }): HeadersInit {
  const requestOrigin = request.headers.get('Origin')
  const origin = getAllowedOrigin(requestOrigin, env) ?? 'null'
  return {
    'Access-Control-Allow-Origin': origin,
    'Vary': 'Origin',
  }
}
