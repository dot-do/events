/**
 * Tenant Middleware Tests
 *
 * Unit tests for src/middleware/tenant.ts
 * Tests namespace-scoped API keys, tenant context extraction, and isolation helpers
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  parseNamespaceKey,
  generateNamespaceKey,
  maskApiKey,
  validateNamespace,
  extractTenantContext,
  requireTenant,
  requireAdmin,
  buildNamespacedR2Path,
  validateNamespacePath,
  addNamespaceCondition,
  getNamespacedListPrefix,
  getNamespacedShardKey,
  type TenantContext,
  type TenantEnv,
  type TenantRequest,
} from '../../middleware/tenant'

// ============================================================================
// Helper Functions
// ============================================================================

function createMockEnv(overrides: Partial<TenantEnv> = {}): TenantEnv {
  return {
    AUTH_TOKEN: undefined,
    NAMESPACE_API_KEYS: undefined,
    DEFAULT_NAMESPACE: undefined,
    ...overrides,
  }
}

function createRequest(authorization?: string): Request {
  const headers = new Headers()
  if (authorization) {
    headers.set('Authorization', authorization)
  }
  return new Request('https://events.do/ingest', { method: 'POST', headers })
}

function createTenantContext(overrides: Partial<TenantContext> = {}): TenantContext {
  return {
    namespace: 'test-namespace',
    isAdmin: false,
    keyId: 'ns_t****ng',
    ...overrides,
  }
}

// ============================================================================
// parseNamespaceKey Tests
// ============================================================================

describe('parseNamespaceKey', () => {
  describe('valid keys', () => {
    it('parses simple namespace key', () => {
      const result = parseNamespaceKey('ns_acme_abc123')

      expect(result).toEqual({ namespace: 'acme', token: 'abc123' })
    })

    it('parses namespace with hyphens', () => {
      const result = parseNamespaceKey('ns_my-project_token123')

      expect(result).toEqual({ namespace: 'my-project', token: 'token123' })
    })

    it('parses namespace with underscores', () => {
      const result = parseNamespaceKey('ns_my_project_token456')

      expect(result).toEqual({ namespace: 'my_project', token: 'token456' })
    })

    it('parses namespace with numbers', () => {
      const result = parseNamespaceKey('ns_team123_xyz789')

      expect(result).toEqual({ namespace: 'team123', token: 'xyz789' })
    })

    it('parses long token values', () => {
      const longToken = 'abcdefghijklmnopqrstuvwxyz0123456789'
      const result = parseNamespaceKey(`ns_prod_${longToken}`)

      expect(result).toEqual({ namespace: 'prod', token: longToken })
    })
  })

  describe('invalid keys', () => {
    it('returns null for non-prefixed keys', () => {
      const result = parseNamespaceKey('simple-token')

      expect(result).toBeNull()
    })

    it('returns null for empty string', () => {
      const result = parseNamespaceKey('')

      expect(result).toBeNull()
    })

    it('returns null for invalid prefix', () => {
      const result = parseNamespaceKey('namespace_acme_token')

      expect(result).toBeNull()
    })

    it('returns null for uppercase namespace', () => {
      const result = parseNamespaceKey('ns_ACME_token')

      expect(result).toBeNull()
    })

    it('returns null for missing token', () => {
      const result = parseNamespaceKey('ns_acme_')

      expect(result).toBeNull()
    })

    it('returns null for missing namespace', () => {
      const result = parseNamespaceKey('ns__token')

      expect(result).toBeNull()
    })

    it('returns null for special characters in namespace', () => {
      const result = parseNamespaceKey('ns_acme.corp_token')

      expect(result).toBeNull()
    })
  })
})

// ============================================================================
// generateNamespaceKey Tests
// ============================================================================

describe('generateNamespaceKey', () => {
  describe('valid generation', () => {
    it('generates key with correct format', () => {
      const key = generateNamespaceKey('acme', 'token123')

      expect(key).toBe('ns_acme_token123')
    })

    it('generates key with hyphenated namespace', () => {
      const key = generateNamespaceKey('my-project', 'xyz')

      expect(key).toBe('ns_my-project_xyz')
    })

    it('generates key with underscored namespace', () => {
      const key = generateNamespaceKey('my_project', 'abc')

      expect(key).toBe('ns_my_project_abc')
    })
  })

  describe('validation errors', () => {
    it('throws for uppercase namespace', () => {
      expect(() => generateNamespaceKey('ACME', 'token')).toThrow(
        'Namespace must contain only lowercase alphanumeric'
      )
    })

    it('throws for special characters in namespace', () => {
      expect(() => generateNamespaceKey('acme.corp', 'token')).toThrow(
        'Namespace must contain only lowercase alphanumeric'
      )
    })

    it('throws for empty namespace', () => {
      // Empty string doesn't match the regex [a-z0-9_-]+, so regex validation fails first
      expect(() => generateNamespaceKey('', 'token')).toThrow(
        'Namespace must contain only lowercase alphanumeric'
      )
    })

    it('throws for namespace over 64 characters', () => {
      const longNamespace = 'a'.repeat(65)
      expect(() => generateNamespaceKey(longNamespace, 'token')).toThrow(
        'Namespace must be between 1 and 64 characters'
      )
    })

    it('allows namespace of exactly 64 characters', () => {
      const namespace = 'a'.repeat(64)
      const key = generateNamespaceKey(namespace, 'token')

      expect(key).toBe(`ns_${namespace}_token`)
    })
  })
})

// ============================================================================
// maskApiKey Tests
// ============================================================================

describe('maskApiKey', () => {
  it('masks middle of key', () => {
    const masked = maskApiKey('ns_acme_verylongtoken123')

    expect(masked).toBe('ns_a****n123')
  })

  it('masks short keys completely', () => {
    const masked = maskApiKey('abc')

    expect(masked).toBe('****')
  })

  it('masks keys of exactly 8 characters', () => {
    const masked = maskApiKey('abcdefgh')

    expect(masked).toBe('****')
  })

  it('handles keys of 9 characters', () => {
    const masked = maskApiKey('abcdefghi')

    expect(masked).toBe('abcd****fghi')
  })
})

// ============================================================================
// validateNamespace Tests
// ============================================================================

describe('validateNamespace', () => {
  describe('valid namespaces', () => {
    it('accepts lowercase alphanumeric', () => {
      expect(validateNamespace('myproject123')).toBeNull()
    })

    it('accepts hyphens', () => {
      expect(validateNamespace('my-project')).toBeNull()
    })

    it('accepts underscores', () => {
      expect(validateNamespace('my_project')).toBeNull()
    })

    it('accepts single character', () => {
      expect(validateNamespace('a')).toBeNull()
    })

    it('accepts 64 character namespace', () => {
      expect(validateNamespace('a'.repeat(64))).toBeNull()
    })
  })

  describe('invalid namespaces', () => {
    it('rejects empty string', () => {
      expect(validateNamespace('')).toBe('Namespace is required')
    })

    it('rejects uppercase', () => {
      expect(validateNamespace('MyProject')).toContain('lowercase')
    })

    it('rejects special characters', () => {
      expect(validateNamespace('my.project')).toContain('lowercase alphanumeric')
    })

    it('rejects too long namespace', () => {
      expect(validateNamespace('a'.repeat(65))).toContain('between 1 and 64')
    })

    it('rejects spaces', () => {
      expect(validateNamespace('my project')).toContain('lowercase alphanumeric')
    })
  })

  describe('reserved namespaces', () => {
    const reserved = ['admin', 'system', 'internal', 'default', 'public', 'global']

    it.each(reserved)('rejects reserved namespace: %s', (name) => {
      expect(validateNamespace(name)).toContain('reserved')
    })
  })
})

// ============================================================================
// extractTenantContext Tests
// ============================================================================

describe('extractTenantContext', () => {
  describe('no authorization', () => {
    it('returns 401 when no authorization header', async () => {
      const env = createMockEnv()
      const request = createRequest()

      const result = await extractTenantContext(request, env)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
    })

    it('includes AUTH_REQUIRED code', async () => {
      const env = createMockEnv()
      const request = createRequest()

      const result = await extractTenantContext(request, env)
      const json = await (result as Response).json()

      expect(json.code).toBe('AUTH_REQUIRED')
    })
  })

  describe('invalid authorization format', () => {
    it('returns 401 for Basic auth', async () => {
      const env = createMockEnv()
      const request = createRequest('Basic dXNlcjpwYXNz')

      const result = await extractTenantContext(request, env)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
    })

    it('includes INVALID_AUTH_FORMAT code', async () => {
      const env = createMockEnv()
      const request = createRequest('Basic abc')

      const result = await extractTenantContext(request, env)
      const json = await (result as Response).json()

      expect(json.code).toBe('INVALID_AUTH_FORMAT')
    })
  })

  describe('namespace-scoped API keys', () => {
    it('extracts tenant from valid namespace key', async () => {
      const env = createMockEnv({
        NAMESPACE_API_KEYS: JSON.stringify({ 'ns_acme_token123': 'acme' }),
      })
      const request = createRequest('Bearer ns_acme_token123')

      const result = await extractTenantContext(request, env)

      expect(result).not.toBeInstanceOf(Response)
      expect((result as TenantContext).namespace).toBe('acme')
      expect((result as TenantContext).isAdmin).toBe(false)
    })

    it('returns 401 for unregistered namespace key', async () => {
      const env = createMockEnv({
        NAMESPACE_API_KEYS: JSON.stringify({ 'ns_acme_token123': 'acme' }),
      })
      const request = createRequest('Bearer ns_acme_wrong-token')

      const result = await extractTenantContext(request, env)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
    })

    it('returns 400 for reserved namespace', async () => {
      const env = createMockEnv({
        NAMESPACE_API_KEYS: JSON.stringify({ 'ns_admin_token123': 'admin' }),
      })
      const request = createRequest('Bearer ns_admin_token123')

      const result = await extractTenantContext(request, env)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(400)
    })
  })

  describe('legacy AUTH_TOKEN', () => {
    it('authenticates with legacy AUTH_TOKEN', async () => {
      const env = createMockEnv({
        AUTH_TOKEN: 'legacy-secret-token',
      })
      const request = createRequest('Bearer legacy-secret-token')

      const result = await extractTenantContext(request, env)

      expect(result).not.toBeInstanceOf(Response)
      expect((result as TenantContext).isAdmin).toBe(true)
      expect((result as TenantContext).namespace).toBe('default')
    })

    it('uses DEFAULT_NAMESPACE for legacy token', async () => {
      const env = createMockEnv({
        AUTH_TOKEN: 'legacy-token',
        DEFAULT_NAMESPACE: 'my-default',
      })
      const request = createRequest('Bearer legacy-token')

      const result = await extractTenantContext(request, env)

      expect((result as TenantContext).namespace).toBe('my-default')
    })

    it('returns 401 for invalid legacy token', async () => {
      const env = createMockEnv({
        AUTH_TOKEN: 'correct-token',
      })
      const request = createRequest('Bearer wrong-token')

      const result = await extractTenantContext(request, env)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
    })
  })

  describe('error handling', () => {
    it('handles malformed NAMESPACE_API_KEYS JSON', async () => {
      const env = createMockEnv({
        NAMESPACE_API_KEYS: 'not-json',
      })
      const request = createRequest('Bearer ns_acme_token')

      const result = await extractTenantContext(request, env)

      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
    })
  })
})

// ============================================================================
// requireTenant Tests
// ============================================================================

describe('requireTenant', () => {
  it('returns tenant context for valid key', async () => {
    const env = createMockEnv({
      NAMESPACE_API_KEYS: JSON.stringify({ 'ns_team_abc': 'team' }),
    })
    const request = createRequest('Bearer ns_team_abc')

    const result = await requireTenant(request, env)

    expect((result as TenantContext).namespace).toBe('team')
  })

  it('attaches tenant to request object', async () => {
    const env = createMockEnv({
      NAMESPACE_API_KEYS: JSON.stringify({ 'ns_team_abc': 'team' }),
    })
    const request = createRequest('Bearer ns_team_abc') as TenantRequest

    await requireTenant(request, env)

    expect(request.tenant?.namespace).toBe('team')
  })

  it('returns Response for invalid authentication', async () => {
    const env = createMockEnv()
    const request = createRequest()

    const result = await requireTenant(request, env)

    expect(result).toBeInstanceOf(Response)
  })
})

// ============================================================================
// requireAdmin Tests
// ============================================================================

describe('requireAdmin', () => {
  it('returns tenant context for admin', async () => {
    const env = createMockEnv({
      AUTH_TOKEN: 'admin-token',
    })
    const request = createRequest('Bearer admin-token')

    const result = await requireAdmin(request, env)

    expect(result).not.toBeInstanceOf(Response)
    expect((result as TenantContext).isAdmin).toBe(true)
  })

  it('returns 403 for non-admin tenant', async () => {
    const env = createMockEnv({
      NAMESPACE_API_KEYS: JSON.stringify({ 'ns_team_abc': 'team' }),
    })
    const request = createRequest('Bearer ns_team_abc')

    const result = await requireAdmin(request, env)

    expect(result).toBeInstanceOf(Response)
    expect((result as Response).status).toBe(403)
  })

  it('includes ADMIN_REQUIRED code for non-admin', async () => {
    const env = createMockEnv({
      NAMESPACE_API_KEYS: JSON.stringify({ 'ns_team_abc': 'team' }),
    })
    const request = createRequest('Bearer ns_team_abc')

    const result = await requireAdmin(request, env)
    const json = await (result as Response).json()

    expect(json.code).toBe('ADMIN_REQUIRED')
  })

  it('returns 401 for missing auth', async () => {
    const env = createMockEnv()
    const request = createRequest()

    const result = await requireAdmin(request, env)

    expect(result).toBeInstanceOf(Response)
    expect((result as Response).status).toBe(401)
  })
})

// ============================================================================
// buildNamespacedR2Path Tests
// ============================================================================

describe('buildNamespacedR2Path', () => {
  describe('regular tenant paths', () => {
    it('prefixes path with namespace', () => {
      const tenant = createTenantContext({ namespace: 'acme' })

      const path = buildNamespacedR2Path(tenant, 'events', '2024', '01')

      expect(path).toBe('ns/acme/events/2024/01')
    })

    it('handles single segment', () => {
      const tenant = createTenantContext({ namespace: 'team' })

      const path = buildNamespacedR2Path(tenant, 'dedup', 'batch123')

      expect(path).toBe('ns/team/dedup/batch123')
    })
  })

  describe('admin paths', () => {
    it('uses legacy path for admin with default namespace', () => {
      const tenant = createTenantContext({ namespace: 'default', isAdmin: true })

      const path = buildNamespacedR2Path(tenant, 'events', '2024')

      expect(path).toBe('events/2024')
    })

    it('allows admin to access explicit ns-prefixed paths', () => {
      const tenant = createTenantContext({ namespace: 'admin-ns', isAdmin: true })

      const path = buildNamespacedR2Path(tenant, 'ns/other-tenant/events', '2024')

      expect(path).toBe('ns/other-tenant/events/2024')
    })

    it('namespaces admin paths for non-default namespace', () => {
      const tenant = createTenantContext({ namespace: 'acme', isAdmin: true })

      const path = buildNamespacedR2Path(tenant, 'events', '2024')

      expect(path).toBe('ns/acme/events/2024')
    })
  })
})

// ============================================================================
// validateNamespacePath Tests
// ============================================================================

describe('validateNamespacePath', () => {
  describe('admin access', () => {
    it('allows admin to access any path', () => {
      const tenant = createTenantContext({ isAdmin: true })

      expect(validateNamespacePath(tenant, 'ns/other/events/file.json')).toBe(true)
      expect(validateNamespacePath(tenant, 'events/file.json')).toBe(true)
      expect(validateNamespacePath(tenant, 'any/path/here')).toBe(true)
    })
  })

  describe('tenant access', () => {
    it('allows tenant to access their namespace', () => {
      const tenant = createTenantContext({ namespace: 'acme', isAdmin: false })

      expect(validateNamespacePath(tenant, 'ns/acme/events/file.json')).toBe(true)
    })

    it('denies tenant access to other namespaces', () => {
      const tenant = createTenantContext({ namespace: 'acme', isAdmin: false })

      expect(validateNamespacePath(tenant, 'ns/other/events/file.json')).toBe(false)
    })

    it('denies tenant access to legacy paths', () => {
      const tenant = createTenantContext({ namespace: 'acme', isAdmin: false })

      expect(validateNamespacePath(tenant, 'events/file.json')).toBe(false)
    })
  })
})

// ============================================================================
// addNamespaceCondition Tests
// ============================================================================

describe('addNamespaceCondition', () => {
  describe('admin queries', () => {
    it('returns existing conditions unchanged for admin', () => {
      const tenant = createTenantContext({ isAdmin: true })
      const conditions = ['type = "event"', 'ts > "2024-01-01"']

      const result = addNamespaceCondition(tenant, conditions)

      expect(result).toEqual(conditions)
    })
  })

  describe('tenant queries', () => {
    it('adds namespace condition for tenant', () => {
      const tenant = createTenantContext({ namespace: 'acme', isAdmin: false })
      const conditions = ['type = "event"']

      const result = addNamespaceCondition(tenant, conditions)

      expect(result).toContain('type = "event"')
      expect(result).toContain("namespace = 'acme'")
    })

    it('escapes single quotes in namespace', () => {
      const tenant = createTenantContext({ namespace: "o'reilly", isAdmin: false })
      const conditions: string[] = []

      const result = addNamespaceCondition(tenant, conditions)

      expect(result).toContain("namespace = 'o''reilly'")
    })
  })
})

// ============================================================================
// getNamespacedListPrefix Tests
// ============================================================================

describe('getNamespacedListPrefix', () => {
  it('returns base prefix for admin', () => {
    const tenant = createTenantContext({ isAdmin: true })

    const prefix = getNamespacedListPrefix(tenant, 'events/')

    expect(prefix).toBe('events/')
  })

  it('prefixes with namespace for tenant', () => {
    const tenant = createTenantContext({ namespace: 'team', isAdmin: false })

    const prefix = getNamespacedListPrefix(tenant, 'events/')

    expect(prefix).toBe('ns/team/events/')
  })
})

// ============================================================================
// getNamespacedShardKey Tests
// ============================================================================

describe('getNamespacedShardKey', () => {
  it('returns base key for admin', () => {
    const tenant = createTenantContext({ isAdmin: true })

    const key = getNamespacedShardKey(tenant, 'collection')

    expect(key).toBe('collection')
  })

  it('prefixes with namespace for tenant', () => {
    const tenant = createTenantContext({ namespace: 'acme', isAdmin: false })

    const key = getNamespacedShardKey(tenant, 'collection')

    expect(key).toBe('acme:collection')
  })
})
