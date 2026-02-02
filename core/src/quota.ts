/**
 * Per-Namespace Resource Quotas
 *
 * Provides quota management for multi-tenant namespaces:
 * - max_events_per_day: Daily event ingestion limit
 * - max_storage_bytes: Total storage limit across all tables
 * - max_subscriptions: Maximum number of subscriptions
 * - max_schemas: Maximum number of registered schemas
 *
 * Quotas are stored in the CatalogDO alongside namespace configuration.
 * Usage tracking is done with daily counters that auto-reset.
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Resource quota configuration for a namespace
 */
export interface NamespaceQuota {
  /** Maximum events allowed per day (0 = unlimited) */
  maxEventsPerDay: number
  /** Maximum storage in bytes across all tables (0 = unlimited) */
  maxStorageBytes: number
  /** Maximum number of subscriptions (0 = unlimited) */
  maxSubscriptions: number
  /** Maximum number of schemas (0 = unlimited) */
  maxSchemas: number
}

/**
 * Current resource usage for a namespace
 */
export interface NamespaceUsage {
  /** Events ingested today */
  eventsToday: number
  /** Date string for the current day's counter (YYYY-MM-DD) */
  eventsDate: string
  /** Total storage bytes across all tables */
  storageBytes: number
  /** Current number of active subscriptions */
  subscriptions: number
  /** Current number of registered schemas */
  schemas: number
}

/**
 * Quota check result
 */
export interface QuotaCheckResult {
  allowed: boolean
  reason?: string
  usage?: NamespaceUsage
  quota?: NamespaceQuota
}

/**
 * Default quota values (generous defaults, 0 = unlimited)
 */
export const DEFAULT_QUOTA: NamespaceQuota = {
  maxEventsPerDay: 0,      // Unlimited
  maxStorageBytes: 0,      // Unlimited
  maxSubscriptions: 0,     // Unlimited
  maxSchemas: 0,           // Unlimited
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Get the current date in YYYY-MM-DD format (UTC)
 */
export function getCurrentDateString(): string {
  return new Date().toISOString().slice(0, 10)
}

/**
 * Check if a quota limit has been exceeded
 * A limit of 0 means unlimited
 */
export function isQuotaExceeded(current: number, limit: number): boolean {
  return limit > 0 && current >= limit
}

/**
 * Check if adding a count would exceed the quota
 * A limit of 0 means unlimited
 */
export function wouldExceedQuota(current: number, toAdd: number, limit: number): boolean {
  return limit > 0 && (current + toAdd) > limit
}

/**
 * Create a default NamespaceUsage object
 */
export function createDefaultUsage(): NamespaceUsage {
  return {
    eventsToday: 0,
    eventsDate: getCurrentDateString(),
    storageBytes: 0,
    subscriptions: 0,
    schemas: 0,
  }
}

/**
 * SQL schema for quota tables
 */
export const QUOTA_SCHEMA_SQL = `
  -- Quota configuration per namespace
  CREATE TABLE IF NOT EXISTS namespace_quotas (
    namespace TEXT PRIMARY KEY,
    max_events_per_day INTEGER NOT NULL DEFAULT 0,
    max_storage_bytes INTEGER NOT NULL DEFAULT 0,
    max_subscriptions INTEGER NOT NULL DEFAULT 0,
    max_schemas INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  );

  -- Usage tracking per namespace
  CREATE TABLE IF NOT EXISTS namespace_usage (
    namespace TEXT PRIMARY KEY,
    events_today INTEGER NOT NULL DEFAULT 0,
    events_date TEXT NOT NULL,
    storage_bytes INTEGER NOT NULL DEFAULT 0,
    subscriptions INTEGER NOT NULL DEFAULT 0,
    schemas INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  );
`
