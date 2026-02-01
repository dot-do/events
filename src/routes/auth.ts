/**
 * Auth route handlers - /login, /callback, /logout, /me
 */

import type { Env, AuthRequest } from '../env'
import { corsHeaders, authCorsHeaders } from '../utils'
import { optionalAuth } from 'oauth.do/rpc/itty'

// ============================================================================
// Open Redirect Prevention
// ============================================================================

/**
 * Allowed origins for redirects (same-origin by default)
 * Add additional trusted origins here if needed
 */
const ALLOWED_REDIRECT_ORIGINS: string[] = [
  // Same-origin is always allowed (validated dynamically)
]

/**
 * Validates and sanitizes a redirect URI to prevent open redirect attacks.
 * Only allows:
 * 1. Relative paths starting with / (same-origin)
 * 2. Absolute URLs matching the current origin
 * 3. Absolute URLs matching explicitly allowed origins
 *
 * @param redirectUri - The redirect URI to validate
 * @param currentOrigin - The origin of the current request
 * @param defaultPath - Default path to use if validation fails
 * @returns Safe redirect URL string
 */
function validateRedirectUri(redirectUri: string, currentOrigin: string, defaultPath: string = '/events'): string {
  // Handle empty or null values
  if (!redirectUri || typeof redirectUri !== 'string') {
    return `${currentOrigin}${defaultPath}`
  }

  // Trim whitespace
  redirectUri = redirectUri.trim()

  // Block javascript:, data:, and other dangerous protocols
  const lowerUri = redirectUri.toLowerCase()
  if (lowerUri.startsWith('javascript:') ||
      lowerUri.startsWith('data:') ||
      lowerUri.startsWith('vbscript:') ||
      lowerUri.startsWith('file:')) {
    return `${currentOrigin}${defaultPath}`
  }

  // Block protocol-relative URLs (//evil.com)
  if (redirectUri.startsWith('//')) {
    return `${currentOrigin}${defaultPath}`
  }

  // Allow relative paths starting with /
  // But prevent //path which could be protocol-relative in some contexts
  if (redirectUri.startsWith('/') && !redirectUri.startsWith('//')) {
    // Ensure no protocol injection via encoded characters
    if (redirectUri.includes('%') || redirectUri.includes('\\')) {
      // Decode and re-validate to prevent encoded protocol attacks
      try {
        const decoded = decodeURIComponent(redirectUri)
        if (decoded.includes(':') || decoded.startsWith('//')) {
          return `${currentOrigin}${defaultPath}`
        }
      } catch {
        // Invalid encoding, reject
        return `${currentOrigin}${defaultPath}`
      }
    }
    return `${currentOrigin}${redirectUri}`
  }

  // For absolute URLs, validate the origin
  try {
    const redirectUrl = new URL(redirectUri)

    // Check if it matches the current origin
    if (redirectUrl.origin === currentOrigin) {
      return redirectUri
    }

    // Check against allowed origins list
    if (ALLOWED_REDIRECT_ORIGINS.includes(redirectUrl.origin)) {
      return redirectUri
    }

    // Origin not allowed - use default
    return `${currentOrigin}${defaultPath}`
  } catch {
    // Invalid URL - use default
    return `${currentOrigin}${defaultPath}`
  }
}

export async function handleAuth(
  request: Request,
  env: Env,
  url: URL,
): Promise<Response | null> {
  // Login - proxy to oauth.do/login with returnTo
  if (url.pathname === '/login') {
    let returnTo = url.searchParams.get('redirect_uri') || url.searchParams.get('returnTo') || '/events'

    // Prevent redirect loops to /login
    if (returnTo === '/login' || returnTo.startsWith('/login?')) {
      returnTo = '/events'
    }

    // Validate redirect URI to prevent open redirects
    const safeReturnTo = validateRedirectUri(returnTo, url.origin, '/events')

    const oauthUrl = new URL('/login', 'https://oauth.do')
    oauthUrl.searchParams.set('returnTo', safeReturnTo)

    // Proxy to oauth.do via service binding
    return env.OAUTH.fetch(new Request(oauthUrl.toString(), {
      headers: {
        'Accept': request.headers.get('Accept') || '*/*',
      },
      redirect: 'manual',
    }))
  }

  // Callback - exchange code for token via oauth.do/exchange
  if (url.pathname === '/callback') {
    return handleCallback(request, env, url)
  }

  // Logout - clear auth cookie and redirect
  if (url.pathname === '/logout') {
    const redirectUri = url.searchParams.get('redirect_uri') || '/'
    // Validate redirect URI to prevent open redirects
    const redirectTo = validateRedirectUri(redirectUri, url.origin, '/')
    return new Response(null, {
      status: 302,
      headers: {
        'Location': redirectTo,
        'Set-Cookie': 'auth=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax',
      },
    })
  }

  // Me - return current user info via AUTH RPC
  if (url.pathname === '/me') {
    const authReq = request as AuthRequest
    await optionalAuth()(authReq)

    if (!authReq.auth?.isAuth) {
      return Response.json({ error: 'Not authenticated' }, { status: 401, headers: authCorsHeaders(request, env) })
    }

    return Response.json(authReq.auth.user, { headers: authCorsHeaders(request, env) })
  }

  return null
}

async function handleCallback(request: Request, env: Env, url: URL): Promise<Response> {
  const code = url.searchParams.get('code')
  const returnTo = url.searchParams.get('returnTo') || '/events'
  const error = url.searchParams.get('error')
  const debug = url.searchParams.get('debug') === 'true'

  console.log('[callback] Received callback', { code: code?.slice(0, 8) + '...', returnTo, error })

  if (error) {
    return Response.json({ error, error_description: url.searchParams.get('error_description') }, { status: 400, headers: corsHeaders() })
  }

  if (!code) {
    return Response.json({ error: 'invalid_request', error_description: 'Missing code' }, { status: 400, headers: corsHeaders() })
  }

  // Exchange code for token via oauth.do service binding
  console.log('[callback] Exchanging code with oauth.do')
  const response = await env.OAUTH.fetch(new Request('https://oauth.do/exchange', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code }),
  }))

  console.log('[callback] Exchange response status:', response.status)

  if (!response.ok) {
    const err = await response.json() as { error: string; error_description?: string }
    console.error('[callback] oauth.do/exchange error:', err)
    return Response.json(err, { status: response.status, headers: corsHeaders() })
  }

  const data = await response.json() as {
    access_token?: string
    token?: string  // oauth.do may return 'token' instead of 'access_token'
    refresh_token?: string
    error?: string
    error_description?: string
  }

  console.log('[callback] Exchange data:', { hasAccessToken: !!data.access_token, hasToken: !!data.token, error: data.error })

  if (data.error) {
    console.error('[callback] oauth.do/exchange error:', data.error, data.error_description)
    return Response.json({ error: data.error, error_description: data.error_description }, { status: 400, headers: corsHeaders() })
  }

  // oauth.do returns 'token', standard OAuth returns 'access_token' - accept both
  const accessToken = data.access_token || data.token
  if (!accessToken) {
    console.error('[callback] No access_token from oauth.do/exchange')
    return Response.json({ error: 'invalid_response', error_description: 'No access token received' }, { status: 500, headers: corsHeaders() })
  }

  // Set auth cookie and redirect
  const maxAge = 3600 * 24 * 7 // 7 days
  const cookie = `auth=${accessToken}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${maxAge}`
  console.log('[callback] Setting cookie and redirecting to:', returnTo)
  console.log('[callback] Token length:', accessToken.length)

  // Validate redirect URI to prevent open redirects
  const redirectTo = validateRedirectUri(returnTo, url.origin, '/events')

  // Debug mode - return JSON instead of redirect
  if (debug) {
    return Response.json({
      success: true,
      tokenLength: accessToken.length,
      redirectTo,
      cookieSet: true,
    }, {
      headers: { 'Set-Cookie': cookie }
    })
  }

  return new Response(null, {
    status: 302,
    headers: {
      'Location': redirectTo,
      'Set-Cookie': cookie,
    },
  })
}
