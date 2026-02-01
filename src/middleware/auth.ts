/**
 * Unified authentication middleware for events.do
 *
 * Consolidates all auth patterns into reusable functions:
 * - requireBearerToken: checks Authorization: Bearer against AUTH_TOKEN env var
 * - requireOAuth: requires authenticated user via AUTH RPC binding (admin-only)
 * - optionalOAuth: returns user if authenticated, null otherwise
 */

import type { AuthBinding } from 'oauth.do/rpc'
import type { AuthUser, AuthRequest } from 'oauth.do/rpc/itty'
import { corsHeaders, authCorsHeaders } from '../utils'

export type { AuthUser, AuthRequest }

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
 */
export function requireBearerToken(request: Request, env: AuthEnv): Response | null {
  if (!env.AUTH_TOKEN) {
    return null // No token configured - allow all
  }

  const auth = request.headers.get('Authorization')
  if (auth !== `Bearer ${env.AUTH_TOKEN}`) {
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
        user: result.user,
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
