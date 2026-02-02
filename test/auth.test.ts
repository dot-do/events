/**
 * events-lum: Auth Middleware Tests
 *
 * Tests for timing-safe bearer token comparison to prevent timing attacks.
 * The timingSafeEqual function uses crypto.subtle.timingSafeEqual internally.
 */

import { describe, it, expect } from 'vitest'
import { timingSafeEqual, requireBearerToken } from '../src/middleware/auth'

describe('timingSafeEqual', () => {
  describe('equal strings', () => {
    it('returns true for identical strings', async () => {
      expect(await timingSafeEqual('secret-token', 'secret-token')).toBe(true)
    })

    it('returns true for empty strings', async () => {
      expect(await timingSafeEqual('', '')).toBe(true)
    })

    it('returns true for long identical strings', async () => {
      const longString = 'a'.repeat(10000)
      expect(await timingSafeEqual(longString, longString)).toBe(true)
    })

    it('returns true for strings with special characters', async () => {
      const special = '!@#$%^&*()_+-=[]{}|;:\'",.<>?/`~'
      expect(await timingSafeEqual(special, special)).toBe(true)
    })

    it('returns true for strings with unicode characters', async () => {
      const unicode = 'Hello\u0000World\u00A0\u2019test'
      expect(await timingSafeEqual(unicode, unicode)).toBe(true)
    })

    it('returns true for strings with emojis', async () => {
      const emoji = 'test-token'
      expect(await timingSafeEqual(emoji, emoji)).toBe(true)
    })
  })

  describe('different strings', () => {
    it('returns false for different strings of same length', async () => {
      expect(await timingSafeEqual('secret-token', 'secret-tokes')).toBe(false)
    })

    it('returns false for strings differing only in first character', async () => {
      expect(await timingSafeEqual('asecret', 'bsecret')).toBe(false)
    })

    it('returns false for strings differing only in last character', async () => {
      expect(await timingSafeEqual('secreta', 'secretb')).toBe(false)
    })

    it('returns false for strings differing only in middle', async () => {
      expect(await timingSafeEqual('sec-ret', 'sec_ret')).toBe(false)
    })

    it('returns false for case differences', async () => {
      expect(await timingSafeEqual('Secret', 'secret')).toBe(false)
    })

    it('returns false for different lengths (shorter first)', async () => {
      expect(await timingSafeEqual('short', 'longer-string')).toBe(false)
    })

    it('returns false for different lengths (longer first)', async () => {
      expect(await timingSafeEqual('longer-string', 'short')).toBe(false)
    })

    it('returns false when one is empty', async () => {
      expect(await timingSafeEqual('', 'non-empty')).toBe(false)
      expect(await timingSafeEqual('non-empty', '')).toBe(false)
    })
  })

  describe('null/undefined handling', () => {
    it('returns false when first argument is null', async () => {
      expect(await timingSafeEqual(null, 'secret')).toBe(false)
    })

    it('returns false when second argument is null', async () => {
      expect(await timingSafeEqual('secret', null)).toBe(false)
    })

    it('returns false when both arguments are null', async () => {
      expect(await timingSafeEqual(null, null)).toBe(false)
    })

    it('returns false when first argument is undefined', async () => {
      expect(await timingSafeEqual(undefined, 'secret')).toBe(false)
    })

    it('returns false when second argument is undefined', async () => {
      expect(await timingSafeEqual('secret', undefined)).toBe(false)
    })

    it('returns false when both arguments are undefined', async () => {
      expect(await timingSafeEqual(undefined, undefined)).toBe(false)
    })

    it('returns false when mixing null and undefined', async () => {
      expect(await timingSafeEqual(null, undefined)).toBe(false)
      expect(await timingSafeEqual(undefined, null)).toBe(false)
    })
  })

  describe('edge cases', () => {
    it('handles strings with null bytes', async () => {
      expect(await timingSafeEqual('test\x00string', 'test\x00string')).toBe(true)
      expect(await timingSafeEqual('test\x00string', 'teststring')).toBe(false)
    })

    it('handles whitespace-only strings', async () => {
      expect(await timingSafeEqual('   ', '   ')).toBe(true)
      expect(await timingSafeEqual('   ', '  ')).toBe(false)
    })

    it('handles newlines', async () => {
      expect(await timingSafeEqual('line1\nline2', 'line1\nline2')).toBe(true)
      expect(await timingSafeEqual('line1\nline2', 'line1\rline2')).toBe(false)
    })
  })
})

describe('requireBearerToken', () => {
  // Helper to create a mock request
  function createRequest(authHeader?: string): Request {
    const headers = new Headers()
    if (authHeader !== undefined) {
      headers.set('Authorization', authHeader)
    }
    return new Request('https://example.com/test', { headers })
  }

  // Helper to create a mock env
  function createEnv(authToken?: string): { AUTH_TOKEN?: string; AUTH: unknown; ALLOWED_ORIGINS?: string } {
    return {
      AUTH_TOKEN: authToken,
      AUTH: {} as unknown,
    }
  }

  describe('no AUTH_TOKEN configured', () => {
    it('allows all requests when AUTH_TOKEN is undefined', async () => {
      const request = createRequest()
      const env = createEnv(undefined)
      const result = await requireBearerToken(request, env)
      expect(result).toBeNull()
    })

    it('allows all requests when AUTH_TOKEN is empty string', async () => {
      const request = createRequest()
      const env = createEnv('')
      // Empty string is falsy, so it should allow all
      const result = await requireBearerToken(request, env)
      expect(result).toBeNull()
    })
  })

  describe('valid authentication', () => {
    it('returns null for valid bearer token', async () => {
      const request = createRequest('Bearer my-secret-token')
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeNull()
    })

    it('accepts tokens with special characters', async () => {
      const token = 'my-token_123.ABC!@#'
      const request = createRequest(`Bearer ${token}`)
      const env = createEnv(token)
      const result = await requireBearerToken(request, env)
      expect(result).toBeNull()
    })

    it('accepts long tokens', async () => {
      const token = 'a'.repeat(1000)
      const request = createRequest(`Bearer ${token}`)
      const env = createEnv(token)
      const result = await requireBearerToken(request, env)
      expect(result).toBeNull()
    })
  })

  describe('invalid authentication', () => {
    it('returns 401 for missing Authorization header', async () => {
      const request = createRequest()
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
      const body = await result?.json() as { error: string }
      expect(body.error).toBe('Unauthorized')
    })

    it('returns 401 for wrong token', async () => {
      const request = createRequest('Bearer wrong-token')
      const env = createEnv('correct-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('returns 401 for missing Bearer prefix', async () => {
      const request = createRequest('my-secret-token')
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('returns 401 for Basic auth instead of Bearer', async () => {
      const request = createRequest('Basic dXNlcjpwYXNz')
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('returns 401 for token with extra spaces', async () => {
      const request = createRequest('Bearer  my-secret-token')
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('returns 401 for lowercase bearer prefix', async () => {
      const request = createRequest('bearer my-secret-token')
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })

    it('returns 401 for empty bearer token', async () => {
      const request = createRequest('Bearer ')
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result).toBeInstanceOf(Response)
      expect(result?.status).toBe(401)
    })
  })

  describe('timing attack mitigation', () => {
    // Note: These tests verify the function behavior, not actual timing characteristics.
    // True timing attack prevention can only be verified through statistical timing analysis.

    it('rejects tokens that differ only in the first character', async () => {
      const request = createRequest('Bearer Xsecret-token')
      const env = createEnv('secret-token')
      const result = await requireBearerToken(request, env)
      expect(result?.status).toBe(401)
    })

    it('rejects tokens that differ only in the last character', async () => {
      const request = createRequest('Bearer secret-tokeX')
      const env = createEnv('secret-token')
      const result = await requireBearerToken(request, env)
      expect(result?.status).toBe(401)
    })

    it('rejects tokens of different lengths', async () => {
      const request = createRequest('Bearer short')
      const env = createEnv('much-longer-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result?.status).toBe(401)
    })

    it('handles null authorization header securely', async () => {
      // This specifically tests the null handling path in timingSafeEqual
      const request = createRequest()
      const env = createEnv('my-secret-token')
      const result = await requireBearerToken(request, env)
      expect(result?.status).toBe(401)
    })
  })
})
