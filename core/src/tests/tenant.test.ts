/**
 * Tests for multi-tenant isolation middleware
 */

import { describe, it, expect } from 'vitest'
import {
  parseNamespaceKey,
  generateNamespaceKey,
  maskApiKey,
  validateNamespace,
  extractTenantContext,
  buildNamespacedR2Path,
  validateNamespacePath,
  addNamespaceCondition,
  getNamespacedListPrefix,
  getNamespacedShardKey,
  type TenantContext,
  type TenantEnv,
} from '../../../src/middleware/tenant'

describe('Multi-Tenant Isolation', () => {
  describe('parseNamespaceKey', () => {
    it('parses valid namespace-scoped API keys', () => {
      const result = parseNamespaceKey('ns_acme_abc123def456')
      expect(result).toEqual({ namespace: 'acme', token: 'abc123def456' })
    })

    it('parses namespace with underscores and hyphens', () => {
      const result = parseNamespaceKey('ns_my-tenant_123_token456')
      expect(result).toEqual({ namespace: 'my-tenant_123', token: 'token456' })
    })

    it('returns null for legacy keys without prefix', () => {
      const result = parseNamespaceKey('abc123def456')
      expect(result).toBeNull()
    })

    it('returns null for invalid format', () => {
      expect(parseNamespaceKey('ns_')).toBeNull()
      expect(parseNamespaceKey('ns_acme')).toBeNull()
      expect(parseNamespaceKey('ns__token')).toBeNull()
    })
  })

  describe('generateNamespaceKey', () => {
    it('generates valid namespace-scoped API keys', () => {
      const key = generateNamespaceKey('acme', 'mytoken123')
      expect(key).toBe('ns_acme_mytoken123')
    })

    it('throws for invalid namespace characters', () => {
      expect(() => generateNamespaceKey('ACME', 'token')).toThrow('lowercase')
      expect(() => generateNamespaceKey('my.namespace', 'token')).toThrow('lowercase')
      expect(() => generateNamespaceKey('my namespace', 'token')).toThrow('lowercase')
    })

    it('throws for namespace too long', () => {
      const longNamespace = 'a'.repeat(65)
      expect(() => generateNamespaceKey(longNamespace, 'token')).toThrow('64 characters')
    })
  })

  describe('maskApiKey', () => {
    it('masks long API keys', () => {
      const masked = maskApiKey('ns_acme_abcdefghijklmnop')
      expect(masked).toBe('ns_a****mnop')
    })

    it('returns all asterisks for short keys', () => {
      const masked = maskApiKey('short')
      expect(masked).toBe('****')
    })
  })

  describe('validateNamespace', () => {
    it('accepts valid namespaces', () => {
      expect(validateNamespace('acme')).toBeNull()
      expect(validateNamespace('my-company')).toBeNull()
      expect(validateNamespace('tenant_123')).toBeNull()
    })

    it('rejects reserved namespaces', () => {
      expect(validateNamespace('admin')).toMatch(/reserved/)
      expect(validateNamespace('system')).toMatch(/reserved/)
      expect(validateNamespace('default')).toMatch(/reserved/)
    })

    it('rejects invalid characters', () => {
      expect(validateNamespace('UPPERCASE')).toMatch(/lowercase/)
      expect(validateNamespace('has.dot')).toMatch(/lowercase/)
      expect(validateNamespace('has space')).toMatch(/lowercase/)
    })

    it('rejects empty or too long namespaces', () => {
      expect(validateNamespace('')).toMatch(/required/)
      expect(validateNamespace('a'.repeat(65))).toMatch(/64 characters/)
    })
  })

  describe('extractTenantContext', () => {
    const createRequest = (authorization?: string): Request => {
      const headers: Record<string, string> = {}
      if (authorization) {
        headers['Authorization'] = authorization
      }
      return new Request('https://events.do/ingest', { headers })
    }

    const createEnv = (options: Partial<TenantEnv> = {}): TenantEnv => ({
      AUTH_TOKEN: options.AUTH_TOKEN,
      NAMESPACE_API_KEYS: options.NAMESPACE_API_KEYS,
      DEFAULT_NAMESPACE: options.DEFAULT_NAMESPACE,
    })

    it('rejects requests without authorization', () => {
      const request = createRequest()
      const env = createEnv({ AUTH_TOKEN: 'secret' })
      const result = extractTenantContext(request, env)
      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
    })

    it('accepts valid namespace-scoped API keys', () => {
      const request = createRequest('Bearer ns_acme_validtoken')
      const env = createEnv({
        NAMESPACE_API_KEYS: JSON.stringify({ 'ns_acme_validtoken': 'acme' }),
      })
      const result = extractTenantContext(request, env)
      expect(result).not.toBeInstanceOf(Response)
      expect((result as TenantContext).namespace).toBe('acme')
      expect((result as TenantContext).isAdmin).toBe(false)
    })

    it('accepts legacy AUTH_TOKEN as admin', () => {
      const request = createRequest('Bearer mysecrettoken')
      const env = createEnv({
        AUTH_TOKEN: 'mysecrettoken',
        DEFAULT_NAMESPACE: 'production',
      })
      const result = extractTenantContext(request, env)
      expect(result).not.toBeInstanceOf(Response)
      expect((result as TenantContext).namespace).toBe('production')
      expect((result as TenantContext).isAdmin).toBe(true)
    })

    it('rejects invalid API keys', () => {
      const request = createRequest('Bearer invalidtoken')
      const env = createEnv({ AUTH_TOKEN: 'correcttoken' })
      const result = extractTenantContext(request, env)
      expect(result).toBeInstanceOf(Response)
      expect((result as Response).status).toBe(401)
    })
  })

  describe('buildNamespacedR2Path', () => {
    const adminTenant: TenantContext = {
      namespace: 'default',
      isAdmin: true,
      keyId: 'admin',
    }

    const regularTenant: TenantContext = {
      namespace: 'acme',
      isAdmin: false,
      keyId: 'tenant',
    }

    it('prefixes paths for regular tenants', () => {
      const path = buildNamespacedR2Path(regularTenant, 'events', '2024', '01', 'file.parquet')
      expect(path).toBe('ns/acme/events/2024/01/file.parquet')
    })

    it('uses legacy paths for admin with default namespace (backwards compatibility)', () => {
      // Admin tenants with default namespace use legacy paths for backwards compatibility
      // This allows existing data to remain accessible without migration
      const path = buildNamespacedR2Path(adminTenant, 'events', '2024', '01', 'file.parquet')
      expect(path).toBe('events/2024/01/file.parquet')
    })

    it('allows admin to access ns-prefixed paths directly', () => {
      const path = buildNamespacedR2Path(adminTenant, 'ns/acme/events', '2024', 'file.parquet')
      expect(path).toBe('ns/acme/events/2024/file.parquet')
    })
  })

  describe('validateNamespacePath', () => {
    const adminTenant: TenantContext = {
      namespace: 'default',
      isAdmin: true,
      keyId: 'admin',
    }

    const regularTenant: TenantContext = {
      namespace: 'acme',
      isAdmin: false,
      keyId: 'tenant',
    }

    it('allows admin to access any path', () => {
      expect(validateNamespacePath(adminTenant, 'ns/acme/events/file.parquet')).toBe(true)
      expect(validateNamespacePath(adminTenant, 'ns/other/events/file.parquet')).toBe(true)
      expect(validateNamespacePath(adminTenant, 'events/file.parquet')).toBe(true)
    })

    it('restricts regular tenants to their namespace', () => {
      expect(validateNamespacePath(regularTenant, 'ns/acme/events/file.parquet')).toBe(true)
      expect(validateNamespacePath(regularTenant, 'ns/other/events/file.parquet')).toBe(false)
      expect(validateNamespacePath(regularTenant, 'events/file.parquet')).toBe(false)
    })
  })

  describe('addNamespaceCondition', () => {
    const adminTenant: TenantContext = {
      namespace: 'default',
      isAdmin: true,
      keyId: 'admin',
    }

    const regularTenant: TenantContext = {
      namespace: 'acme',
      isAdmin: false,
      keyId: 'tenant',
    }

    it('does not add condition for admin', () => {
      const conditions = ['type = "test"']
      const result = addNamespaceCondition(adminTenant, conditions)
      expect(result).toEqual(['type = "test"'])
    })

    it('adds namespace condition for regular tenants', () => {
      const conditions = ['type = "test"']
      const result = addNamespaceCondition(regularTenant, conditions)
      expect(result).toContain("namespace = 'acme'")
    })

    it('escapes single quotes in namespace', () => {
      const tenant: TenantContext = {
        namespace: "o'reilly",
        isAdmin: false,
        keyId: 'tenant',
      }
      const result = addNamespaceCondition(tenant, [])
      expect(result[0]).toBe("namespace = 'o''reilly'")
    })
  })

  describe('getNamespacedListPrefix', () => {
    const adminTenant: TenantContext = {
      namespace: 'default',
      isAdmin: true,
      keyId: 'admin',
    }

    const regularTenant: TenantContext = {
      namespace: 'acme',
      isAdmin: false,
      keyId: 'tenant',
    }

    it('returns base prefix for admin', () => {
      expect(getNamespacedListPrefix(adminTenant, 'events/')).toBe('events/')
    })

    it('returns namespaced prefix for regular tenant', () => {
      expect(getNamespacedListPrefix(regularTenant, 'events/')).toBe('ns/acme/events/')
    })
  })

  describe('getNamespacedShardKey', () => {
    const adminTenant: TenantContext = {
      namespace: 'default',
      isAdmin: true,
      keyId: 'admin',
    }

    const regularTenant: TenantContext = {
      namespace: 'acme',
      isAdmin: false,
      keyId: 'tenant',
    }

    it('returns base shard key for admin', () => {
      expect(getNamespacedShardKey(adminTenant, 'collection')).toBe('collection')
    })

    it('returns namespaced shard key for regular tenant', () => {
      expect(getNamespacedShardKey(regularTenant, 'collection')).toBe('acme:collection')
    })
  })
})
