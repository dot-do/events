/**
 * Multi-tenant isolation middleware for events.do
 *
 * Provides namespace-scoped access control:
 * - Namespace-scoped API keys with format: ns_<namespace>_<token>
 * - Tenant context extraction and validation
 * - Namespace isolation for R2 paths and queries
 * - Prevents cross-namespace data access
 */

import { corsHeaders } from '../utils'
import { logger } from '../logger'
import { timingSafeEqual } from './auth'

// ============================================================================
// Types
// ============================================================================

/**
 * Tenant context attached to requests after authentication
 */
export interface TenantContext {
  /** The namespace this request is scoped to */
  namespace: string
  /** Whether this is an admin with cross-namespace access */
  isAdmin: boolean
  /** Original API key (masked for logging) */
  keyId: string
}

/**
 * Request with tenant context attached
 */
export interface TenantRequest extends Request {
  tenant?: TenantContext
}

/**
 * Minimal env shape needed by tenant middleware
 */
export interface TenantEnv {
  /** Master AUTH_TOKEN for backwards compatibility (admin access to all namespaces) */
  AUTH_TOKEN?: string
  /** Namespace-scoped API keys stored as JSON: { "ns_acme_xxx": "acme", "ns_beta_yyy": "beta" } */
  NAMESPACE_API_KEYS?: string
  /** Default namespace for legacy keys without namespace prefix */
  DEFAULT_NAMESPACE?: string
}

// ============================================================================
// API Key Format
// ============================================================================

/**
 * Namespace-scoped API key format: ns_<namespace>_<token>
 *
 * Examples:
 * - ns_acme_abc123def456 -> namespace: "acme"
 * - ns_production_xyz789 -> namespace: "production"
 * - abc123 (legacy) -> namespace: from DEFAULT_NAMESPACE or "default"
 */
const NAMESPACE_KEY_REGEX = /^ns_([a-z0-9_-]+)_([a-zA-Z0-9_-]+)$/

// ============================================================================
// Type Validators
// ============================================================================

/**
 * Type guard for namespace API keys map: Record<string, string>
 */
function isNamespaceApiKeysMap(value: unknown): value is Record<string, string> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false
  }
  const v = value as Record<string, unknown>
  for (const val of Object.values(v)) {
    if (typeof val !== 'string') {
      return false
    }
  }
  return true
}

/**
 * Parse a namespace-scoped API key
 * Returns null if the key doesn't match the namespace format
 */
export function parseNamespaceKey(key: string): { namespace: string; token: string } | null {
  const match = key.match(NAMESPACE_KEY_REGEX)
  if (!match) return null
  const namespace = match[1]
  const token = match[2]
  if (!namespace || !token) return null
  return { namespace, token }
}

/**
 * Generate a namespace-scoped API key
 */
export function generateNamespaceKey(namespace: string, token: string): string {
  // Validate namespace format
  if (!/^[a-z0-9_-]+$/.test(namespace)) {
    throw new Error('Namespace must contain only lowercase alphanumeric characters, underscores, and hyphens')
  }
  if (namespace.length < 1 || namespace.length > 64) {
    throw new Error('Namespace must be between 1 and 64 characters')
  }
  return `ns_${namespace}_${token}`
}

/**
 * Mask an API key for safe logging
 */
export function maskApiKey(key: string): string {
  if (key.length <= 8) return '****'
  return key.slice(0, 4) + '****' + key.slice(-4)
}

// ============================================================================
// Validation
// ============================================================================

/**
 * Validate that a namespace string is properly formatted
 */
export function validateNamespace(namespace: string): string | null {
  if (!namespace || typeof namespace !== 'string') {
    return 'Namespace is required'
  }
  if (namespace.length < 1 || namespace.length > 64) {
    return 'Namespace must be between 1 and 64 characters'
  }
  if (!/^[a-z0-9_-]+$/.test(namespace)) {
    return 'Namespace must contain only lowercase alphanumeric characters, underscores, and hyphens'
  }
  // Prevent reserved namespaces
  const reserved = ['admin', 'system', 'internal', 'default', 'public', 'global']
  if (reserved.includes(namespace)) {
    return `Namespace "${namespace}" is reserved`
  }
  return null
}

// ============================================================================
// Tenant Middleware
// ============================================================================

/**
 * Extract tenant context from request
 *
 * Authentication priority:
 * 1. Namespace-scoped API key (ns_<namespace>_<token>)
 * 2. Legacy AUTH_TOKEN (admin access to all namespaces)
 * 3. OAuth user with admin role (admin access to all namespaces)
 */
export async function extractTenantContext(
  request: Request,
  env: TenantEnv
): Promise<TenantContext | Response> {
  const authHeader = request.headers.get('Authorization')

  // No authorization header
  if (!authHeader) {
    return Response.json(
      { error: 'Authorization required', code: 'AUTH_REQUIRED' },
      { status: 401, headers: corsHeaders() }
    )
  }

  // Must be Bearer token
  if (!authHeader.startsWith('Bearer ')) {
    return Response.json(
      { error: 'Invalid authorization format. Use: Bearer <token>', code: 'INVALID_AUTH_FORMAT' },
      { status: 401, headers: corsHeaders() }
    )
  }

  const token = authHeader.slice(7) // Remove "Bearer " prefix

  // Check for namespace-scoped API key format
  const parsed = parseNamespaceKey(token)
  if (parsed) {
    // Validate namespace-scoped key against stored keys
    const validationResult = validateNamespaceApiKey(token, parsed.namespace, env)
    if (validationResult instanceof Response) {
      return validationResult
    }
    return validationResult
  }

  // Check for legacy AUTH_TOKEN (grants admin access to all namespaces)
  // Use timing-safe comparison to prevent timing attacks
  if (env.AUTH_TOKEN && await timingSafeEqual(token, env.AUTH_TOKEN)) {
    return {
      namespace: env.DEFAULT_NAMESPACE || 'default',
      isAdmin: true,
      keyId: maskApiKey(token),
    }
  }

  // Invalid token
  return Response.json(
    { error: 'Invalid API key', code: 'INVALID_API_KEY' },
    { status: 401, headers: corsHeaders() }
  )
}

/**
 * Validate a namespace-scoped API key
 */
function validateNamespaceApiKey(
  fullKey: string,
  namespace: string,
  env: TenantEnv
): TenantContext | Response {
  // Validate namespace format
  const namespaceError = validateNamespace(namespace)
  if (namespaceError) {
    return Response.json(
      { error: namespaceError, code: 'INVALID_NAMESPACE' },
      { status: 400, headers: corsHeaders() }
    )
  }

  // Check against stored namespace API keys
  if (env.NAMESPACE_API_KEYS) {
    try {
      const parsed: unknown = JSON.parse(env.NAMESPACE_API_KEYS)
      if (!isNamespaceApiKeysMap(parsed)) {
        logger.child({ component: 'Tenant' }).error('NAMESPACE_API_KEYS failed validation: expected Record<string, string>')
      } else {
        const expectedNamespace = parsed[fullKey]

        if (expectedNamespace === namespace) {
          return {
            namespace,
            isAdmin: false,
            keyId: maskApiKey(fullKey),
          }
        }
      }
    } catch (err) {
      logger.child({ component: 'Tenant' }).error('Failed to parse NAMESPACE_API_KEYS', {
        error: err instanceof Error ? err.message : String(err),
      })
    }
  }

  return Response.json(
    { error: 'Invalid API key for namespace', code: 'INVALID_NAMESPACE_KEY' },
    { status: 401, headers: corsHeaders() }
  )
}

/**
 * Middleware to require tenant context on a request
 * Attaches tenant context to request.tenant
 */
export async function requireTenant(
  request: Request,
  env: TenantEnv
): Promise<TenantContext | Response> {
  const result = await extractTenantContext(request, env)
  if (result instanceof Response) {
    return result
  }
  // Attach to request for downstream handlers
  ;(request as TenantRequest).tenant = result
  return result
}

/**
 * Middleware to require admin access (cross-namespace)
 */
export async function requireAdmin(
  request: Request,
  env: TenantEnv
): Promise<TenantContext | Response> {
  const result = await extractTenantContext(request, env)
  if (result instanceof Response) {
    return result
  }
  if (!result.isAdmin) {
    return Response.json(
      { error: 'Admin access required for cross-namespace operations', code: 'ADMIN_REQUIRED' },
      { status: 403, headers: corsHeaders() }
    )
  }
  ;(request as TenantRequest).tenant = result
  return result
}

// ============================================================================
// Namespace Isolation Helpers
// ============================================================================

/**
 * Build a namespace-isolated R2 path
 * Prefixes all paths with the tenant namespace to ensure isolation
 *
 * @param tenant - Tenant context
 * @param basePath - The base path (e.g., "events", "dedup")
 * @param segments - Additional path segments
 * @returns Namespace-prefixed path (e.g., "ns/acme/events/2024/01/...")
 */
export function buildNamespacedR2Path(
  tenant: TenantContext,
  basePath: string,
  ...segments: string[]
): string {
  // Admin with default namespace uses legacy paths (backwards compatibility)
  // This allows existing admin data to remain accessible without migration
  if (tenant.isAdmin && tenant.namespace === 'default') {
    return [basePath, ...segments].join('/')
  }

  // Admin can access explicit ns-prefixed paths directly
  if (tenant.isAdmin && basePath.startsWith('ns/')) {
    return [basePath, ...segments].join('/')
  }

  // All tenant data is isolated under ns/<namespace>/
  return ['ns', tenant.namespace, basePath, ...segments].join('/')
}

/**
 * Validate that a path belongs to the tenant's namespace
 * Used when reading data to ensure cross-namespace isolation
 *
 * @param tenant - Tenant context
 * @param path - The R2 path to validate
 * @returns true if the path is accessible by this tenant
 */
export function validateNamespacePath(
  tenant: TenantContext,
  path: string
): boolean {
  // Admin can access any path
  if (tenant.isAdmin) {
    return true
  }

  // Tenant can only access their namespace
  const expectedPrefix = `ns/${tenant.namespace}/`
  return path.startsWith(expectedPrefix)
}

/**
 * Build namespace-isolated query conditions
 * Adds namespace filtering to SQL queries to prevent cross-namespace data access
 *
 * @param tenant - Tenant context
 * @param existingConditions - Existing WHERE conditions
 * @returns Updated conditions with namespace isolation
 */
export function addNamespaceCondition(
  tenant: TenantContext,
  existingConditions: string[]
): string[] {
  // Admin can query all namespaces
  if (tenant.isAdmin) {
    return existingConditions
  }

  // Add namespace isolation condition
  // Escape single quotes in namespace to prevent SQL injection
  const escapedNamespace = tenant.namespace.replace(/'/g, "''")
  return [...existingConditions, `namespace = '${escapedNamespace}'`]
}

/**
 * Build namespace-isolated R2 list prefix
 * Ensures R2 list operations only see tenant's data
 *
 * @param tenant - Tenant context
 * @param basePrefix - The base prefix (e.g., "events/")
 * @returns Namespace-prefixed prefix (e.g., "ns/acme/events/")
 */
export function getNamespacedListPrefix(
  tenant: TenantContext,
  basePrefix: string
): string {
  // Admin can list without namespace prefix
  if (tenant.isAdmin) {
    return basePrefix
  }

  return `ns/${tenant.namespace}/${basePrefix}`
}

/**
 * Get the shard key with namespace prefix for DOs
 * Ensures DO instances are isolated per namespace
 *
 * @param tenant - Tenant context
 * @param baseKey - The base shard key (e.g., "collection")
 * @returns Namespace-prefixed key (e.g., "acme:collection")
 */
export function getNamespacedShardKey(
  tenant: TenantContext,
  baseKey: string
): string {
  // Admin uses base key for backwards compatibility
  if (tenant.isAdmin) {
    return baseKey
  }

  return `${tenant.namespace}:${baseKey}`
}
