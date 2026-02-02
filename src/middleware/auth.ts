/**
 * Unified authentication middleware for events.do
 *
 * Consolidates all auth patterns into reusable functions:
 * - requireBearerToken: checks Authorization: Bearer against AUTH_TOKEN env var
 * - requireOAuth: requires authenticated user via AUTH RPC binding (admin-only)
 * - optionalOAuth: returns user if authenticated, null otherwise
 */

import type { AuthBinding } from 'oauth.do/rpc'
import type { AuthUser, AuthRequest } from 'oauth.do/itty'
import { corsHeaders, authCorsHeaders } from '../utils'

export type { AuthUser, AuthRequest }

/**
 * Timing-safe string comparison to prevent timing attacks.
 * Uses HMAC-based comparison which is constant-time by design.
 *
 * The approach: Generate a random key, compute HMAC of both strings,
 * and compare the resulting digests. HMAC comparison is inherently
 * timing-safe because the crypto.subtle.verify function is constant-time.
 *
 * Returns true if the strings are equal, false otherwise.
 * Handles edge cases like null/undefined and different lengths safely.
 */
export async function timingSafeEqual(a: string | null | undefined, b: string | null | undefined): Promise<boolean> {
  // Handle null/undefined cases - always return false to indicate auth failure
  // We still perform a dummy comparison to maintain constant time behavior
  const aVal = a ?? ''
  const bVal = b ?? ''

  // If either was null/undefined, we'll do the comparison but return false
  const wasNullish = a == null || b == null

  const encoder = new TextEncoder()
  const aBytes = encoder.encode(aVal)
  const bBytes = encoder.encode(bVal)

  // Generate a random key for HMAC - this ensures timing cannot leak through key reuse
  const keyData = crypto.getRandomValues(new Uint8Array(32))
  const key = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  )

  // Compute HMAC of both values
  const [aHmac, bHmac] = await Promise.all([
    crypto.subtle.sign('HMAC', key, aBytes),
    crypto.subtle.sign('HMAC', key, bBytes),
  ])

  // Compare the HMACs byte-by-byte in constant time
  // Using XOR accumulator - if any byte differs, result will be non-zero
  const aView = new Uint8Array(aHmac)
  const bView = new Uint8Array(bHmac)

  let diff = 0
  for (let i = 0; i < aView.length; i++) {
    diff |= aView[i] ^ bView[i]
  }

  // Return false if either original value was nullish, or if HMACs differ
  return !wasNullish && diff === 0
}

/** Minimal env shape needed by auth middleware */
export interface AuthEnv {
  AUTH_TOKEN?: string
  AUTH: AuthBinding
  ALLOWED_ORIGINS?: string
}

/** Result of a successful OAuth authentication */
export interface OAuthResult {
  user: AuthUser
  isAuth: true
}

/**
 * Check Authorization: Bearer token against AUTH_TOKEN env var.
 *
 * Returns null on success (auth passed), or a 401 Response on failure.
 * If AUTH_TOKEN is not configured, all requests are allowed through.
 *
 * Uses timing-safe comparison to prevent timing attacks.
 */
export async function requireBearerToken(request: Request, env: AuthEnv): Promise<Response | null> {
  if (!env.AUTH_TOKEN) {
    return null // No token configured - allow all
  }

  const auth = request.headers.get('Authorization')
  const expectedAuth = `Bearer ${env.AUTH_TOKEN}`

  // Use timing-safe comparison to prevent timing attacks
  const isValid = await timingSafeEqual(auth, expectedAuth)
  if (!isValid) {
    return Response.json(
      { error: 'Unauthorized' },
      { status: 401, headers: corsHeaders() },
    )
  }

  return null // Auth passed
}

/**
 * Require authenticated admin user via AUTH RPC binding.
 *
 * Returns the user info on success, or a Response (401/403) on failure.
 * For browser requests (Accept: text/html), returns a 302 redirect to /login.
 */
export async function requireOAuth(
  request: Request,
  env: AuthEnv,
  url: URL,
): Promise<OAuthResult | Response> {
  const authHeader = request.headers.get('Authorization')
  const cookie = request.headers.get('Cookie')
  const result = await env.AUTH.authenticate(authHeader, cookie)

  if (!result.ok) {
    const accept = request.headers.get('Accept') || ''
    if (accept.includes('text/html')) {
      const loginUrl = `${url.origin}/login?redirect_uri=${encodeURIComponent(url.pathname + url.search)}`
      return Response.redirect(loginUrl, 302)
    }
    return Response.json(
      { error: result.error || 'Authentication required' },
      { status: result.status || 401 },
    )
  }

  if (!result.user?.roles?.includes('admin')) {
    return Response.json(
      {
        error: 'Admin access required',
        authenticated: true,
        message: 'You are logged in but do not have admin privileges',
      },
      { status: 403 },
    )
  }

  return { user: result.user, isAuth: true }
}

/**
 * Optionally authenticate user via AUTH RPC binding.
 *
 * Returns the user info if authenticated, or null if not.
 * Never returns an error response - unauthenticated requests pass through with null.
 */
export async function optionalOAuth(
  request: Request,
  env: AuthEnv,
): Promise<OAuthResult | null> {
  const authHeader = request.headers.get('Authorization')
  const cookie = request.headers.get('Cookie')

  try {
    const result = await env.AUTH.authenticate(authHeader, cookie)
    if (result.ok && result.user) {
      return { user: result.user, isAuth: true }
    }
  } catch {
    // Auth service unavailable - treat as unauthenticated
  }

  return null
}

/**
 * Helper to check if a requireOAuth result is a Response (auth failure).
 */
export function isAuthFailure(result: OAuthResult | Response): result is Response {
  return result instanceof Response
}
