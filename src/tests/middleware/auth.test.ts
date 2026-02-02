/**
 * Authentication Middleware Tests
 *
 * Unit tests for src/middleware/auth.ts
 * Tests requireBearerToken, requireOAuth, optionalOAuth, and timingSafeEqual
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  timingSafeEqual,
  requireBearerToken,
  requireOAuth,
  optionalOAuth,
  isAuthFailure,
  type AuthEnv,
  type OAuthResult,
} from '../../middleware/auth'

// ============================================================================
// Mock Types
// ============================================================================

interface MockAuthResult {
  ok: boolean
  status?: number
  error?: string
  user?: {
    email: string
    roles: string[]
    id?: string
  }
}

interface MockAuthBinding {
  authenticate: ReturnType<typeof vi.fn<[string | null, string | null], Promise<MockAuthResult>>>
}

// ============================================================================
// Helper Functions
// ============================================================================

function createMockEnv(overrides: Partial<AuthEnv> = {}): AuthEnv {
  return {
    AUTH_TOKEN: undefined,
    AUTH: {
      authenticate: vi.fn().mockResolvedValue({ ok: false, status: 401, error: 'Unauthorized' }),
    } as unknown as AuthEnv['AUTH'],
    ALLOWED_ORIGINS: undefined,
    ...overrides,
  }
}

function createRequest(
  options: {
    authorization?: string
    cookie?: string
    accept?: string
  } = {}
): Request {
  const headers = new Headers()
  if (options.authorization) {
    headers.set('Authorization', options.authorization)
  }
  if (options.cookie) {
    headers.set('Cookie', options.cookie)
  }
  if (options.accept) {
    headers.set('Accept', options.accept)
  }
  return new Request('https://events.do/test', { headers })
}

// ============================================================================
// timingSafeEqual Tests
// ============================================================================

describe('timingSafeEqual', () => {
  describe('happy paths', () => {
    it('returns true for identical strings', async () => {
      const result = await timingSafeEqual('hello', 'hello')
      expect(result).toBe(true)
    })

    it('returns true for empty strings', async () => {
      const result = await timingSafeEqual('', '')
      expect(result).toBe(true)
    })

    it('returns true for long identical strings', async () => {
      const long = 'a'.repeat(1000)
      const result = await timingSafeEqual(long, long)
      expect(result).toBe(true)
    })

    it('returns true for strings with special characters', async () => {
      const special = 'Bearer abc123!@#$%^&*()'
      const result = await timingSafeEqual(special, special)
      expect(result).toBe(true)
    })
  })

  describe('error cases', () => {
    it('returns false for different strings', async () => {
      const result = await timingSafeEqual('hello', 'world')
      expect(result).toBe(false)
    })

    it('returns false for strings of different lengths', async () => {
      const result = await timingSafeEqual('short', 'much longer string')
      expect(result).toBe(false)
    })

    it('returns false when first string is null', async () => {
      const result = await timingSafeEqual(null, 'hello')
      expect(result).toBe(false)
    })

    it('returns false when second string is null', async () => {
      const result = await timingSafeEqual('hello', null)
      expect(result).toBe(false)
    })

    it('returns false when first string is undefined', async () => {
      const result = await timingSafeEqual(undefined, 'hello')
      expect(result).toBe(false)
    })

    it('returns false when second string is undefined', async () => {
      const result = await timingSafeEqual('hello', undefined)
      expect(result).toBe(false)
    })

    it('returns false when both strings are null', async () => {
      const result = await timingSafeEqual(null, null)
      expect(result).toBe(false)
    })

    it('returns false for case-sensitive differences', async () => {
      const result = await timingSafeEqual('Hello', 'hello')
      expect(result).toBe(false)
    })

    it('returns false for strings differing by one character', async () => {
      const result = await timingSafeEqual('Bearer token123', 'Bearer token124')
      expect(result).toBe(false)
    })
  })
})

// ============================================================================
// requireBearerToken Tests
// ============================================================================

describe('requireBearerToken', () => {
  describe('when AUTH_TOKEN is not configured', () => {
    it('allows all requests through (returns null)', async () => {
      const env = createMockEnv({ AUTH_TOKEN: undefined })
      const request = createRequest()

      const result = await requireBearerToken(request, env)

      expect(result).toBeNull()
    })

    it('allows requests without Authorization header', async () => {
      const env = createMockEnv({ AUTH_TOKEN: undefined })
      const request = createRequest({ authorization: undefined })

      const result = await requireBearerToken(request, env)

      expect(result).toBeNull()
    })
  })

  describe('when AUTH_TOKEN is configured', () => {
    const AUTH_TOKEN = 'secret-token-123'

    it('allows requests with valid Bearer token', async () => {
      const env = createMockEnv({ AUTH_TOKEN })
      const request = createRequest({ authorization: `Bearer ${AUTH_TOKEN}` })

      const result = await requireBearerToken(request, env)

      expect(result).toBeNull()
    })

    it('rejects requests without Authorization header', async () => {
      const env = createMockEnv({ AUTH_TOKEN })
      const request = createRequest()

      const result = await requireBearerToken(request, env)

      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
      const json = await result?.json()
      expect(json.error).toBe('Unauthorized')
    })

    it('rejects requests with invalid token', async () => {
      const env = createMockEnv({ AUTH_TOKEN })
      const request = createRequest({ authorization: 'Bearer wrong-token' })

      const result = await requireBearerToken(request, env)

      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('rejects requests with malformed Authorization header', async () => {
      const env = createMockEnv({ AUTH_TOKEN })
      const request = createRequest({ authorization: 'Basic user:pass' })

      const result = await requireBearerToken(request, env)

      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('rejects requests with empty Bearer token', async () => {
      const env = createMockEnv({ AUTH_TOKEN })
      const request = createRequest({ authorization: 'Bearer ' })

      const result = await requireBearerToken(request, env)

      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('includes CORS headers in error response', async () => {
      const env = createMockEnv({ AUTH_TOKEN })
      const request = createRequest()

      const result = await requireBearerToken(request, env)

      expect(result?.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })
  })
})

// ============================================================================
// requireOAuth Tests
// ============================================================================

describe('requireOAuth', () => {
  describe('happy paths', () => {
    it('returns user info for authenticated admin', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'admin@example.com', roles: ['admin'] },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ authorization: 'Bearer valid-token' })
      const url = new URL('https://events.do/dashboard')

      const result = await requireOAuth(request, env, url)

      expect(result).toEqual({
        user: { email: 'admin@example.com', roles: ['admin'] },
        isAuth: true,
      })
    })

    it('passes authorization header and cookie to AUTH.authenticate', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'admin@example.com', roles: ['admin'] },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({
        authorization: 'Bearer token123',
        cookie: 'session=abc',
      })
      const url = new URL('https://events.do/dashboard')

      await requireOAuth(request, env, url)

      expect(mockAuth.authenticate).toHaveBeenCalledWith('Bearer token123', 'session=abc')
    })

    it('authenticates with cookie only', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'admin@example.com', roles: ['admin'] },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ cookie: 'session=valid-session' })
      const url = new URL('https://events.do/dashboard')

      const result = await requireOAuth(request, env, url)

      expect((result as OAuthResult).isAuth).toBe(true)
    })
  })

  describe('authentication failures', () => {
    it('returns 401 response when authentication fails', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: false,
          status: 401,
          error: 'Invalid credentials',
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest()
      const url = new URL('https://events.do/dashboard')

      const result = await requireOAuth(request, env, url)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
      const json = await (result as Response).json()
      expect(json.error).toBe('Invalid credentials')
    })

    it('returns default error message when none provided', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: false,
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest()
      const url = new URL('https://events.do/dashboard')

      const result = await requireOAuth(request, env, url)

      expect(result).toBeInstanceOf(Response)
      const json = await (result as Response).json()
      expect(json.error).toBe('Authentication required')
    })

    it('redirects to login for browser HTML requests', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: false,
          status: 401,
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ accept: 'text/html,application/xhtml+xml' })
      const url = new URL('https://events.do/dashboard?page=1')

      const result = await requireOAuth(request, env, url)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(302)
      const location = (result as Response).headers.get('Location')
      expect(location).toContain('/login')
      expect(location).toContain(encodeURIComponent('/dashboard?page=1'))
    })
  })

  describe('authorization failures', () => {
    it('returns 403 for non-admin users', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'user@example.com', roles: ['user'] },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ authorization: 'Bearer token' })
      const url = new URL('https://events.do/dashboard')

      const result = await requireOAuth(request, env, url)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(403)
      const json = await (result as Response).json()
      expect(json.error).toBe('Admin access required')
      expect(json.authenticated).toBe(true)
    })

    it('returns 403 for users with empty roles', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'user@example.com', roles: [] },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ authorization: 'Bearer token' })
      const url = new URL('https://events.do/dashboard')

      const result = await requireOAuth(request, env, url)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(403)
    })

    it('returns 403 for users with no roles property', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'user@example.com' },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ authorization: 'Bearer token' })
      const url = new URL('https://events.do/dashboard')

      const result = await requireOAuth(request, env, url)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(403)
    })
  })
})

// ============================================================================
// optionalOAuth Tests
// ============================================================================

describe('optionalOAuth', () => {
  describe('authenticated users', () => {
    it('returns user info when authenticated', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'user@example.com', roles: ['user'] },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ authorization: 'Bearer token' })

      const result = await optionalOAuth(request, env)

      expect(result).toEqual({
        user: { email: 'user@example.com', roles: ['user'] },
        isAuth: true,
      })
    })

    it('returns user even for non-admin roles', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: { email: 'viewer@example.com', roles: ['viewer'] },
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ cookie: 'session=valid' })

      const result = await optionalOAuth(request, env)

      expect(result?.isAuth).toBe(true)
      expect(result?.user.email).toBe('viewer@example.com')
    })
  })

  describe('unauthenticated users', () => {
    it('returns null when authentication fails', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: false,
          status: 401,
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest()

      const result = await optionalOAuth(request, env)

      expect(result).toBeNull()
    })

    it('returns null when user is missing', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockResolvedValue({
          ok: true,
          user: undefined,
        }),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest()

      const result = await optionalOAuth(request, env)

      expect(result).toBeNull()
    })

    it('returns null when AUTH service throws', async () => {
      const mockAuth = {
        authenticate: vi.fn().mockRejectedValue(new Error('AUTH service unavailable')),
      }
      const env = createMockEnv({ AUTH: mockAuth as unknown as AuthEnv['AUTH'] })
      const request = createRequest({ authorization: 'Bearer token' })

      const result = await optionalOAuth(request, env)

      expect(result).toBeNull()
    })
  })
})

// ============================================================================
// isAuthFailure Tests
// ============================================================================

describe('isAuthFailure', () => {
  it('returns true for Response objects', () => {
    const response = new Response('error', { status: 401 })

    expect(isAuthFailure(response)).toBe(true)
  })

  it('returns false for OAuthResult objects', () => {
    const result: OAuthResult = {
      user: { email: 'test@example.com', roles: ['admin'] } as any,
      isAuth: true,
    }

    expect(isAuthFailure(result)).toBe(false)
  })

  it('works correctly in type narrowing', () => {
    const response: OAuthResult | Response = new Response('error', { status: 403 })

    if (isAuthFailure(response)) {
      // TypeScript should recognize this as Response
      expect(response.status).toBe(403)
    }
  })
})
