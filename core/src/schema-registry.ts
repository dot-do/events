/**
 * SchemaRegistryDO - Event Schema Registry and Validation
 *
 * A Durable Object that manages JSON schemas for event types.
 * Supports validation of incoming events against registered schemas.
 *
 * Key features:
 * - Register JSON schemas for event types
 * - Validate events against registered schemas
 * - Optional validation per namespace/event-type
 * - Schema versioning support
 *
 * ## Sharding Strategy
 *
 * SchemaRegistryDO is sharded by namespace. Each namespace gets its own
 * DO instance via `env.SCHEMA_REGISTRY.idFromName(namespace)`. This ensures:
 * - Schema operations for one namespace don't contend with another
 * - Each namespace's schemas are stored in isolated SQLite databases
 * - Natural horizontal scaling as namespaces are added
 *
 * Cross-namespace schema lookups (e.g., fallback to 'default' namespace)
 * require the caller to make separate RPC calls to different DO instances.
 * The validateEvent method handles this internally by returning whether a
 * schema was found, allowing callers to implement fallback logic.
 */

import { DurableObject } from 'cloudflare:workers'
import {
  getString,
  getNumber,
  getBoolean,
  getOptionalString,
  getOptionalNumber,
  getOptionalJson,
  type SqlRow,
} from './sql-mapper.js'
import { ulid } from './ulid.js'
import {
  getCachedSafeRegex,
  safeRegexTest,
  validateSchemaPattern,
  MAX_PATTERN_LENGTH,
  MAX_INPUT_LENGTH,
} from './safe-regex.js'
import { matchPattern } from './pattern-matcher.js'

// ============================================================================
// Types
// ============================================================================

/**
 * JSON Schema (subset of JSON Schema Draft-07)
 * We support a subset of JSON Schema that can be validated efficiently at runtime
 */
export interface JsonSchema {
  type?: 'object' | 'array' | 'string' | 'number' | 'integer' | 'boolean' | 'null'
  properties?: Record<string, JsonSchema>
  required?: string[]
  additionalProperties?: boolean | JsonSchema
  items?: JsonSchema
  enum?: unknown[]
  const?: unknown
  minLength?: number
  maxLength?: number
  minimum?: number
  maximum?: number
  pattern?: string
  format?: string
  minItems?: number
  maxItems?: number
  description?: string
  $ref?: string
  oneOf?: JsonSchema[]
  anyOf?: JsonSchema[]
  allOf?: JsonSchema[]
}

/**
 * Schema registration record
 */
export interface SchemaRegistration {
  id: string
  /** Event type pattern (e.g., "webhook.github.*", "collection.users.*") */
  eventType: string
  /** Namespace this schema applies to (e.g., "default", "myapp") */
  namespace: string
  /** JSON Schema definition */
  schema: JsonSchema
  /** Schema version (auto-incremented) */
  version: number
  /** Whether validation is enabled for this schema */
  validationEnabled: boolean
  /** Whether to reject invalid events (true) or just log warnings (false) */
  strictMode: boolean
  /** Optional description */
  description?: string
  createdAt: number
  updatedAt: number
}

/**
 * Schema validation result
 */
export interface ValidationResult {
  valid: boolean
  errors: ValidationError[]
}

/**
 * Individual validation error
 */
export interface ValidationError {
  path: string
  message: string
  keyword: string
  params?: Record<string, unknown>
}

/**
 * Schema configuration for a namespace
 */
export interface NamespaceConfig {
  namespace: string
  /** Enable validation for all event types in this namespace */
  validationEnabled: boolean
  /** Reject invalid events (true) or log warnings (false) */
  strictMode: boolean
  createdAt: number
  updatedAt: number
}

// Env type - users should extend this for their specific bindings
// eslint-disable-next-line @typescript-eslint/no-empty-interface
interface Env {}

// ============================================================================
// Schema Validation Functions
// ============================================================================

/**
 * Validate a value against a JSON Schema
 */
export function validateAgainstSchema(
  value: unknown,
  schema: JsonSchema,
  path = ''
): ValidationError[] {
  const errors: ValidationError[] = []

  // Handle type validation
  if (schema.type !== undefined) {
    const actualType = getJsonType(value)

    // Handle integer as a special case of number
    if (schema.type === 'integer') {
      if (actualType !== 'number' || !Number.isInteger(value)) {
        errors.push({
          path,
          message: `Expected integer, got ${actualType}`,
          keyword: 'type',
          params: { expected: 'integer', actual: actualType },
        })
        return errors // Type mismatch, skip further validation
      }
    } else if (schema.type !== actualType) {
      errors.push({
        path,
        message: `Expected ${schema.type}, got ${actualType}`,
        keyword: 'type',
        params: { expected: schema.type, actual: actualType },
      })
      return errors // Type mismatch, skip further validation
    }
  }

  // Handle const
  if (schema.const !== undefined) {
    if (!deepEqual(value, schema.const)) {
      errors.push({
        path,
        message: `Expected constant value ${JSON.stringify(schema.const)}`,
        keyword: 'const',
        params: { expected: schema.const },
      })
    }
  }

  // Handle enum
  if (schema.enum !== undefined) {
    if (!schema.enum.some((e) => deepEqual(value, e))) {
      errors.push({
        path,
        message: `Value must be one of: ${schema.enum.map((e) => JSON.stringify(e)).join(', ')}`,
        keyword: 'enum',
        params: { allowed: schema.enum },
      })
    }
  }

  // Handle string validations
  if (typeof value === 'string') {
    if (schema.minLength !== undefined && value.length < schema.minLength) {
      errors.push({
        path,
        message: `String must be at least ${schema.minLength} characters`,
        keyword: 'minLength',
        params: { limit: schema.minLength, actual: value.length },
      })
    }
    if (schema.maxLength !== undefined && value.length > schema.maxLength) {
      errors.push({
        path,
        message: `String must be at most ${schema.maxLength} characters`,
        keyword: 'maxLength',
        params: { limit: schema.maxLength, actual: value.length },
      })
    }
    if (schema.pattern !== undefined) {
      // Use safe regex to prevent ReDoS attacks
      const regex = getCachedSafeRegex(schema.pattern)
      if (!regex) {
        // Pattern is unsafe or invalid - treat as validation error
        errors.push({
          path,
          message: `Invalid or unsafe pattern: ${schema.pattern.slice(0, 50)}${schema.pattern.length > 50 ? '...' : ''}`,
          keyword: 'pattern',
          params: { pattern: schema.pattern, error: 'pattern_unsafe' },
        })
      } else {
        // Execute regex with safety measures
        const result = safeRegexTest(regex, value, { maxInputLength: MAX_INPUT_LENGTH })
        if (result.error) {
          errors.push({
            path,
            message: `Pattern validation error: ${result.error}`,
            keyword: 'pattern',
            params: { pattern: schema.pattern, error: result.error },
          })
        } else if (!result.matched) {
          errors.push({
            path,
            message: `String must match pattern: ${schema.pattern}`,
            keyword: 'pattern',
            params: { pattern: schema.pattern },
          })
        }
      }
    }
    if (schema.format !== undefined) {
      const formatError = validateFormat(value, schema.format, path)
      if (formatError) {
        errors.push(formatError)
      }
    }
  }

  // Handle number validations
  if (typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) {
      errors.push({
        path,
        message: `Number must be >= ${schema.minimum}`,
        keyword: 'minimum',
        params: { limit: schema.minimum, actual: value },
      })
    }
    if (schema.maximum !== undefined && value > schema.maximum) {
      errors.push({
        path,
        message: `Number must be <= ${schema.maximum}`,
        keyword: 'maximum',
        params: { limit: schema.maximum, actual: value },
      })
    }
  }

  // Handle array validations
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) {
      errors.push({
        path,
        message: `Array must have at least ${schema.minItems} items`,
        keyword: 'minItems',
        params: { limit: schema.minItems, actual: value.length },
      })
    }
    if (schema.maxItems !== undefined && value.length > schema.maxItems) {
      errors.push({
        path,
        message: `Array must have at most ${schema.maxItems} items`,
        keyword: 'maxItems',
        params: { limit: schema.maxItems, actual: value.length },
      })
    }
    if (schema.items !== undefined) {
      for (let i = 0; i < value.length; i++) {
        errors.push(...validateAgainstSchema(value[i], schema.items, `${path}[${i}]`))
      }
    }
  }

  // Handle object validations
  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>

    // Check required properties
    if (schema.required !== undefined) {
      for (const prop of schema.required) {
        if (!(prop in obj)) {
          errors.push({
            path: path ? `${path}.${prop}` : prop,
            message: `Missing required property: ${prop}`,
            keyword: 'required',
            params: { property: prop },
          })
        }
      }
    }

    // Validate properties
    if (schema.properties !== undefined) {
      for (const [prop, propSchema] of Object.entries(schema.properties)) {
        if (prop in obj) {
          errors.push(
            ...validateAgainstSchema(
              obj[prop],
              propSchema,
              path ? `${path}.${prop}` : prop
            )
          )
        }
      }
    }

    // Check additionalProperties
    if (schema.additionalProperties === false && schema.properties) {
      const allowedProps = new Set(Object.keys(schema.properties))
      for (const prop of Object.keys(obj)) {
        if (!allowedProps.has(prop)) {
          errors.push({
            path: path ? `${path}.${prop}` : prop,
            message: `Additional property not allowed: ${prop}`,
            keyword: 'additionalProperties',
            params: { property: prop },
          })
        }
      }
    } else if (
      typeof schema.additionalProperties === 'object' &&
      schema.properties
    ) {
      const definedProps = new Set(Object.keys(schema.properties))
      for (const [prop, propValue] of Object.entries(obj)) {
        if (!definedProps.has(prop)) {
          errors.push(
            ...validateAgainstSchema(
              propValue,
              schema.additionalProperties,
              path ? `${path}.${prop}` : prop
            )
          )
        }
      }
    }
  }

  // Handle oneOf
  if (schema.oneOf !== undefined) {
    const matches = schema.oneOf.filter(
      (s) => validateAgainstSchema(value, s, path).length === 0
    )
    if (matches.length !== 1) {
      errors.push({
        path,
        message:
          matches.length === 0
            ? 'Value must match exactly one schema (matched none)'
            : `Value must match exactly one schema (matched ${matches.length})`,
        keyword: 'oneOf',
        params: { matched: matches.length },
      })
    }
  }

  // Handle anyOf
  if (schema.anyOf !== undefined) {
    const matches = schema.anyOf.some(
      (s) => validateAgainstSchema(value, s, path).length === 0
    )
    if (!matches) {
      errors.push({
        path,
        message: 'Value must match at least one schema',
        keyword: 'anyOf',
      })
    }
  }

  // Handle allOf
  if (schema.allOf !== undefined) {
    for (const s of schema.allOf) {
      errors.push(...validateAgainstSchema(value, s, path))
    }
  }

  return errors
}

/**
 * Get JSON Schema type of a value
 */
function getJsonType(
  value: unknown
): 'object' | 'array' | 'string' | 'number' | 'boolean' | 'null' {
  if (value === null) return 'null'
  if (Array.isArray(value)) return 'array'
  const t = typeof value
  if (t === 'object') return 'object'
  if (t === 'string') return 'string'
  if (t === 'number') return 'number'
  if (t === 'boolean') return 'boolean'
  return 'null'
}

/**
 * Deep equality check
 */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (typeof a !== typeof b) return false
  if (a === null || b === null) return a === b
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    return a.every((item, i) => deepEqual(item, b[i]))
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const aObj = a as Record<string, unknown>
    const bObj = b as Record<string, unknown>
    const aKeys = Object.keys(aObj)
    const bKeys = Object.keys(bObj)
    if (aKeys.length !== bKeys.length) return false
    return aKeys.every((key) => deepEqual(aObj[key], bObj[key]))
  }
  return false
}

/**
 * Validate string format
 */
function validateFormat(
  value: string,
  format: string,
  path: string
): ValidationError | null {
  switch (format) {
    case 'date-time':
      if (isNaN(Date.parse(value))) {
        return {
          path,
          message: 'Invalid date-time format (expected ISO 8601)',
          keyword: 'format',
          params: { format: 'date-time' },
        }
      }
      break
    case 'date':
      if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        return {
          path,
          message: 'Invalid date format (expected YYYY-MM-DD)',
          keyword: 'format',
          params: { format: 'date' },
        }
      }
      break
    case 'time':
      if (!/^\d{2}:\d{2}:\d{2}/.test(value)) {
        return {
          path,
          message: 'Invalid time format (expected HH:MM:SS)',
          keyword: 'format',
          params: { format: 'time' },
        }
      }
      break
    case 'email':
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) {
        return {
          path,
          message: 'Invalid email format',
          keyword: 'format',
          params: { format: 'email' },
        }
      }
      break
    case 'uri':
      try {
        new URL(value)
      } catch {
        return {
          path,
          message: 'Invalid URI format',
          keyword: 'format',
          params: { format: 'uri' },
        }
      }
      break
    case 'uuid':
      if (
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
          value
        )
      ) {
        return {
          path,
          message: 'Invalid UUID format',
          keyword: 'format',
          params: { format: 'uuid' },
        }
      }
      break
    // Add more formats as needed
  }
  return null
}

// ============================================================================
// SchemaRegistryDO
// ============================================================================

export class SchemaRegistryDO extends DurableObject<Env> {
  private sql: SqlStorage

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.sql = ctx.storage.sql
    this.initSchema()
  }

  /**
   * Initialize SQLite schema
   */
  private initSchema(): void {
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS schemas (
        id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        namespace TEXT NOT NULL DEFAULT 'default',
        schema TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        validation_enabled INTEGER NOT NULL DEFAULT 1,
        strict_mode INTEGER NOT NULL DEFAULT 0,
        description TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        UNIQUE(namespace, event_type)
      );

      CREATE INDEX IF NOT EXISTS idx_schemas_namespace
        ON schemas(namespace);

      CREATE INDEX IF NOT EXISTS idx_schemas_event_type
        ON schemas(event_type);

      CREATE TABLE IF NOT EXISTS namespace_config (
        namespace TEXT PRIMARY KEY,
        validation_enabled INTEGER NOT NULL DEFAULT 1,
        strict_mode INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS schema_versions (
        id TEXT PRIMARY KEY,
        schema_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        schema TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        FOREIGN KEY (schema_id) REFERENCES schemas(id),
        UNIQUE(schema_id, version)
      );

      CREATE INDEX IF NOT EXISTS idx_schema_versions_schema
        ON schema_versions(schema_id);
    `)
  }

  // ---------------------------------------------------------------------------
  // Schema Registration
  // ---------------------------------------------------------------------------

  /**
   * Validate all patterns in a JSON schema for safety (ReDoS prevention)
   * Returns an error message if any pattern is unsafe, null otherwise
   */
  private validateSchemaPatterns(schema: JsonSchema, path = ''): string | null {
    // Check pattern at current level
    if (schema.pattern !== undefined) {
      const result = validateSchemaPattern(schema.pattern)
      if (!result.valid) {
        return `Unsafe pattern at ${path || 'root'}: ${result.error}`
      }
    }

    // Check nested properties
    if (schema.properties) {
      for (const [prop, propSchema] of Object.entries(schema.properties)) {
        const propPath = path ? `${path}.properties.${prop}` : `properties.${prop}`
        const error = this.validateSchemaPatterns(propSchema, propPath)
        if (error) return error
      }
    }

    // Check additionalProperties if it's a schema
    if (typeof schema.additionalProperties === 'object') {
      const error = this.validateSchemaPatterns(
        schema.additionalProperties,
        path ? `${path}.additionalProperties` : 'additionalProperties'
      )
      if (error) return error
    }

    // Check items
    if (schema.items) {
      const error = this.validateSchemaPatterns(
        schema.items,
        path ? `${path}.items` : 'items'
      )
      if (error) return error
    }

    // Check oneOf/anyOf/allOf
    for (const keyword of ['oneOf', 'anyOf', 'allOf'] as const) {
      const schemas = schema[keyword]
      if (schemas) {
        for (let i = 0; i < schemas.length; i++) {
          const error = this.validateSchemaPatterns(
            schemas[i],
            path ? `${path}.${keyword}[${i}]` : `${keyword}[${i}]`
          )
          if (error) return error
        }
      }
    }

    return null
  }

  /**
   * Register or update a schema for an event type
   */
  async registerSchema(params: {
    eventType: string
    namespace?: string
    schema: JsonSchema
    validationEnabled?: boolean
    strictMode?: boolean
    description?: string
  }): Promise<{ ok: true; schemaId: string; version: number } | { ok: false; error: string }> {
    const namespace = params.namespace ?? 'default'
    const now = Date.now()

    try {
      // Validate all patterns in the schema for safety (ReDoS prevention)
      const patternError = this.validateSchemaPatterns(params.schema)
      if (patternError) {
        return { ok: false, error: patternError }
      }
      // Check for existing schema
      const existing = this.sql
        .exec(
          `SELECT id, version FROM schemas WHERE namespace = ? AND event_type = ?`,
          namespace,
          params.eventType
        )
        .one()

      if (existing) {
        // Update existing schema
        const id = getString(existing as SqlRow, 'id')
        const currentVersion = getNumber(existing as SqlRow, 'version')
        const newVersion = currentVersion + 1

        // Archive current version
        this.sql.exec(
          `INSERT INTO schema_versions (id, schema_id, version, schema, created_at)
           SELECT ?, id, version, schema, ?
           FROM schemas WHERE id = ?`,
          ulid(),
          now,
          id
        )

        // Update schema
        this.sql.exec(
          `UPDATE schemas SET
             schema = ?,
             version = ?,
             validation_enabled = ?,
             strict_mode = ?,
             description = ?,
             updated_at = ?
           WHERE id = ?`,
          JSON.stringify(params.schema),
          newVersion,
          params.validationEnabled !== false ? 1 : 0,
          params.strictMode === true ? 1 : 0,
          params.description ?? null,
          now,
          id
        )

        return { ok: true, schemaId: id, version: newVersion }
      } else {
        // Create new schema
        const id = ulid()

        this.sql.exec(
          `INSERT INTO schemas
           (id, event_type, namespace, schema, version, validation_enabled, strict_mode, description, created_at, updated_at)
           VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`,
          id,
          params.eventType,
          namespace,
          JSON.stringify(params.schema),
          params.validationEnabled !== false ? 1 : 0,
          params.strictMode === true ? 1 : 0,
          params.description ?? null,
          now,
          now
        )

        return { ok: true, schemaId: id, version: 1 }
      }
    } catch (e) {
      const error = e instanceof Error ? e.message : 'Unknown error'
      return { ok: false, error }
    }
  }

  /**
   * Delete a schema registration
   */
  async deleteSchema(params: {
    eventType: string
    namespace?: string
  }): Promise<{ ok: boolean; deleted: boolean }> {
    const namespace = params.namespace ?? 'default'

    // Get schema ID
    const existing = this.sql
      .exec(
        `SELECT id FROM schemas WHERE namespace = ? AND event_type = ?`,
        namespace,
        params.eventType
      )
      .one()

    if (!existing) {
      return { ok: true, deleted: false }
    }

    const id = getString(existing as SqlRow, 'id')

    // Delete versions
    this.sql.exec(`DELETE FROM schema_versions WHERE schema_id = ?`, id)

    // Delete schema
    this.sql.exec(`DELETE FROM schemas WHERE id = ?`, id)

    return { ok: true, deleted: true }
  }

  /**
   * Get a schema by event type
   */
  async getSchema(params: {
    eventType: string
    namespace?: string
  }): Promise<SchemaRegistration | null> {
    const namespace = params.namespace ?? 'default'

    const row = this.sql
      .exec(
        `SELECT * FROM schemas WHERE namespace = ? AND event_type = ?`,
        namespace,
        params.eventType
      )
      .one()

    return row ? this.rowToSchema(row as SqlRow) : null
  }

  /**
   * List all schemas, optionally filtered by namespace
   */
  async listSchemas(options?: {
    namespace?: string
    limit?: number
    offset?: number
  }): Promise<SchemaRegistration[]> {
    let query = 'SELECT * FROM schemas WHERE 1=1'
    const params: unknown[] = []

    if (options?.namespace) {
      query += ' AND namespace = ?'
      params.push(options.namespace)
    }

    query += ' ORDER BY namespace, event_type'

    if (options?.limit) {
      query += ' LIMIT ?'
      params.push(options.limit)
    }
    if (options?.offset) {
      query += ' OFFSET ?'
      params.push(options.offset)
    }

    return this.sql
      .exec(query, ...params)
      .toArray()
      .map((row) => this.rowToSchema(row as SqlRow))
  }

  /**
   * Get schema version history
   */
  async getSchemaHistory(params: {
    eventType: string
    namespace?: string
  }): Promise<{ version: number; schema: JsonSchema; createdAt: number }[]> {
    const namespace = params.namespace ?? 'default'

    // Get current schema
    const current = this.sql
      .exec(
        `SELECT id FROM schemas WHERE namespace = ? AND event_type = ?`,
        namespace,
        params.eventType
      )
      .one()

    if (!current) {
      return []
    }

    const schemaId = getString(current as SqlRow, 'id')

    const rows = this.sql
      .exec(
        `SELECT version, schema, created_at FROM schema_versions
         WHERE schema_id = ?
         ORDER BY version DESC`,
        schemaId
      )
      .toArray()

    return rows.map((row) => ({
      version: getNumber(row as SqlRow, 'version'),
      schema: JSON.parse(getString(row as SqlRow, 'schema')) as JsonSchema,
      createdAt: getNumber(row as SqlRow, 'created_at'),
    }))
  }

  // ---------------------------------------------------------------------------
  // Namespace Configuration
  // ---------------------------------------------------------------------------

  /**
   * Configure validation settings for a namespace
   */
  async configureNamespace(params: {
    namespace: string
    validationEnabled?: boolean
    strictMode?: boolean
  }): Promise<{ ok: boolean }> {
    const now = Date.now()

    const existing = this.sql
      .exec(`SELECT namespace FROM namespace_config WHERE namespace = ?`, params.namespace)
      .one()

    if (existing) {
      const updates: string[] = ['updated_at = ?']
      const values: unknown[] = [now]

      if (params.validationEnabled !== undefined) {
        updates.push('validation_enabled = ?')
        values.push(params.validationEnabled ? 1 : 0)
      }
      if (params.strictMode !== undefined) {
        updates.push('strict_mode = ?')
        values.push(params.strictMode ? 1 : 0)
      }

      values.push(params.namespace)
      this.sql.exec(
        `UPDATE namespace_config SET ${updates.join(', ')} WHERE namespace = ?`,
        ...values
      )
    } else {
      this.sql.exec(
        `INSERT INTO namespace_config (namespace, validation_enabled, strict_mode, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?)`,
        params.namespace,
        params.validationEnabled !== false ? 1 : 0,
        params.strictMode === true ? 1 : 0,
        now,
        now
      )
    }

    return { ok: true }
  }

  /**
   * Get namespace configuration
   */
  async getNamespaceConfig(namespace: string): Promise<NamespaceConfig | null> {
    const row = this.sql
      .exec(`SELECT * FROM namespace_config WHERE namespace = ?`, namespace)
      .one()

    if (!row) {
      return null
    }

    return {
      namespace: getString(row as SqlRow, 'namespace'),
      validationEnabled: getBoolean(row as SqlRow, 'validation_enabled'),
      strictMode: getBoolean(row as SqlRow, 'strict_mode'),
      createdAt: getNumber(row as SqlRow, 'created_at'),
      updatedAt: getNumber(row as SqlRow, 'updated_at'),
    }
  }

  // ---------------------------------------------------------------------------
  // Event Validation
  // ---------------------------------------------------------------------------

  /**
   * Validate an event against registered schemas
   *
   * @param event - The event to validate
   * @param namespace - The namespace to check schemas for (default: 'default')
   * @returns Validation result with errors if invalid
   */
  async validateEvent(
    event: { type: string; [key: string]: unknown },
    namespace = 'default'
  ): Promise<ValidationResult & { schemaFound: boolean; strictMode: boolean }> {
    // Check namespace config first
    const nsConfig = await this.getNamespaceConfig(namespace)
    if (nsConfig && !nsConfig.validationEnabled) {
      return { valid: true, errors: [], schemaFound: false, strictMode: false }
    }

    // Find matching schema
    // First try exact match, then pattern matches
    const schema = await this.findMatchingSchema(event.type, namespace)

    if (!schema) {
      // No schema found - valid by default (no schema = no validation)
      return { valid: true, errors: [], schemaFound: false, strictMode: false }
    }

    if (!schema.validationEnabled) {
      return { valid: true, errors: [], schemaFound: true, strictMode: schema.strictMode }
    }

    // Validate event against schema
    const errors = validateAgainstSchema(event, schema.schema)

    return {
      valid: errors.length === 0,
      errors,
      schemaFound: true,
      strictMode: schema.strictMode,
    }
  }

  /**
   * Validate multiple events in batch
   */
  async validateEvents(
    events: { type: string; [key: string]: unknown }[],
    namespace = 'default'
  ): Promise<{
    valid: boolean
    results: (ValidationResult & { index: number; eventType: string })[]
  }> {
    // Check namespace config first
    const nsConfig = await this.getNamespaceConfig(namespace)
    if (nsConfig && !nsConfig.validationEnabled) {
      return { valid: true, results: [] }
    }

    // Cache schemas by event type for efficiency
    const schemaCache = new Map<string, SchemaRegistration | null>()
    const results: (ValidationResult & { index: number; eventType: string })[] = []
    let allValid = true

    for (let i = 0; i < events.length; i++) {
      const event = events[i]!

      // Get or cache schema
      let schema = schemaCache.get(event.type)
      if (schema === undefined) {
        schema = await this.findMatchingSchema(event.type, namespace)
        schemaCache.set(event.type, schema)
      }

      // Skip if no schema or validation disabled
      if (!schema || !schema.validationEnabled) {
        continue
      }

      const errors = validateAgainstSchema(event, schema.schema)

      if (errors.length > 0) {
        allValid = allValid && !schema.strictMode
        results.push({
          index: i,
          eventType: event.type,
          valid: false,
          errors,
        })
      }
    }

    return { valid: allValid, results }
  }

  /**
   * Find a matching schema for an event type within this DO's namespace
   * Supports exact matches and wildcard patterns
   *
   * IMPORTANT: This method only searches within the current DO instance's
   * namespace. Since SchemaRegistryDO is sharded by namespace (each namespace
   * gets its own DO instance via idFromName), cross-namespace fallback must
   * be handled by the caller.
   *
   * For example, to implement fallback to 'default' namespace:
   * ```typescript
   * // Try specific namespace first
   * const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
   * const registry = env.SCHEMA_REGISTRY.get(registryId)
   * const result = await registry.validateEvent(event, namespace)
   *
   * // If no schema found and not default namespace, try default
   * if (!result.schemaFound && namespace !== 'default') {
   *   const defaultId = env.SCHEMA_REGISTRY.idFromName('default')
   *   const defaultRegistry = env.SCHEMA_REGISTRY.get(defaultId)
   *   result = await defaultRegistry.validateEvent(event, 'default')
   * }
   * ```
   */
  private async findMatchingSchema(
    eventType: string,
    namespace: string
  ): Promise<SchemaRegistration | null> {
    // Try exact match first
    const exact = await this.getSchema({ eventType, namespace })
    if (exact) {
      return exact
    }

    // Try wildcard patterns
    // Get all schemas for this namespace and check patterns
    const schemas = await this.listSchemas({ namespace })

    for (const schema of schemas) {
      if (matchPattern(schema.eventType, eventType)) {
        return schema
      }
    }

    // Note: Cross-namespace fallback removed. Each namespace has its own DO
    // instance (sharded via idFromName), so searching 'default' namespace here
    // would just search this same DO's database again. Callers must implement
    // fallback by making separate RPC calls to the 'default' namespace DO.

    return null
  }

  // Note: Pattern matching now uses the shared matchPattern() from pattern-matcher.ts
  // This provides consistent glob-style matching with * (single segment) and ** (multiple segments)

  /**
   * Convert database row to SchemaRegistration object
   */
  private rowToSchema(row: SqlRow): SchemaRegistration {
    return {
      id: getString(row, 'id'),
      eventType: getString(row, 'event_type'),
      namespace: getString(row, 'namespace'),
      schema: JSON.parse(getString(row, 'schema')) as JsonSchema,
      version: getNumber(row, 'version'),
      validationEnabled: getBoolean(row, 'validation_enabled'),
      strictMode: getBoolean(row, 'strict_mode'),
      description: getOptionalString(row, 'description') ?? undefined,
      createdAt: getNumber(row, 'created_at'),
      updatedAt: getNumber(row, 'updated_at'),
    }
  }
}

// Export type for wrangler config
export type SchemaRegistryDOType = typeof SchemaRegistryDO
