/**
 * Typed SQL Row Mapper Utilities
 *
 * Provides type-safe access to SQLite row columns, replacing unsafe `as` casts
 * with runtime validation and clear error messages.
 */

// ============================================================================
// Types
// ============================================================================

/**
 * SQL row type - record with unknown column values
 */
export type SqlRow = Record<string, unknown>

/**
 * Error thrown when a column value doesn't match the expected type
 */
export class SqlTypeError extends Error {
  constructor(
    public readonly key: string,
    public readonly expectedType: string,
    public readonly actualValue: unknown
  ) {
    const actualType = actualValue === null ? 'null' : typeof actualValue
    super(`Column '${key}' expected ${expectedType}, got ${actualType}: ${JSON.stringify(actualValue)}`)
    this.name = 'SqlTypeError'
  }
}

/**
 * Error thrown when parsed JSON fails runtime validation
 */
export class JsonValidationError extends Error {
  constructor(
    public readonly key: string,
    public readonly validationError: string,
    public readonly actualValue: unknown
  ) {
    super(`Column '${key}' JSON validation failed: ${validationError}. Value: ${JSON.stringify(actualValue)}`)
    this.name = 'JsonValidationError'
  }
}

/**
 * Type guard function signature for validating parsed JSON
 */
export type JsonValidator<T> = (value: unknown) => value is T

/**
 * JSON validation result type - either success with typed value or failure with error message
 */
export type JsonValidationResult<T> =
  | { success: true; data: T }
  | { success: false; error: string }

// ============================================================================
// Required Value Accessors
// ============================================================================

/**
 * Get a string value from a row column.
 * Throws SqlTypeError if the value is not a string.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The string value
 * @throws SqlTypeError if value is not a string
 */
export function getString(row: SqlRow, key: string): string {
  const value = row[key]
  if (typeof value !== 'string') {
    throw new SqlTypeError(key, 'string', value)
  }
  return value
}

/**
 * Get a number value from a row column.
 * Throws SqlTypeError if the value is not a number.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The number value
 * @throws SqlTypeError if value is not a number
 */
export function getNumber(row: SqlRow, key: string): number {
  const value = row[key]
  if (typeof value !== 'number') {
    throw new SqlTypeError(key, 'number', value)
  }
  return value
}

/**
 * Get a boolean value from a row column.
 * Handles SQLite's integer representation (0 = false, non-0 = true).
 * Throws SqlTypeError if the value is not a boolean or number.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The boolean value
 * @throws SqlTypeError if value is not a boolean or number
 */
export function getBoolean(row: SqlRow, key: string): boolean {
  const value = row[key]
  if (typeof value === 'boolean') {
    return value
  }
  // SQLite stores booleans as integers (0 or 1)
  if (typeof value === 'number') {
    return value !== 0
  }
  throw new SqlTypeError(key, 'boolean', value)
}

// ============================================================================
// Optional Value Accessors
// ============================================================================

/**
 * Get an optional string value from a row column.
 * Returns null if the value is null or undefined.
 * Throws SqlTypeError if the value is not null, undefined, or string.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The string value or null
 * @throws SqlTypeError if value is not null, undefined, or string
 */
export function getOptionalString(row: SqlRow, key: string): string | null {
  const value = row[key]
  if (value === null || value === undefined) {
    return null
  }
  if (typeof value !== 'string') {
    throw new SqlTypeError(key, 'string | null', value)
  }
  return value
}

/**
 * Get an optional number value from a row column.
 * Returns null if the value is null or undefined.
 * Throws SqlTypeError if the value is not null, undefined, or number.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The number value or null
 * @throws SqlTypeError if value is not null, undefined, or number
 */
export function getOptionalNumber(row: SqlRow, key: string): number | null {
  const value = row[key]
  if (value === null || value === undefined) {
    return null
  }
  if (typeof value !== 'number') {
    throw new SqlTypeError(key, 'number | null', value)
  }
  return value
}

/**
 * Get an optional boolean value from a row column.
 * Returns null if the value is null or undefined.
 * Handles SQLite's integer representation (0 = false, non-0 = true).
 * Throws SqlTypeError if the value is not null, undefined, boolean, or number.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The boolean value or null
 * @throws SqlTypeError if value is not null, undefined, boolean, or number
 */
export function getOptionalBoolean(row: SqlRow, key: string): boolean | null {
  const value = row[key]
  if (value === null || value === undefined) {
    return null
  }
  if (typeof value === 'boolean') {
    return value
  }
  // SQLite stores booleans as integers (0 or 1)
  if (typeof value === 'number') {
    return value !== 0
  }
  throw new SqlTypeError(key, 'boolean | null', value)
}

// ============================================================================
// JSON Value Accessor
// ============================================================================

/**
 * Get a JSON-parsed value from a string column.
 * Parses the string value as JSON and returns the typed result.
 *
 * @deprecated This function does NOT verify that the parsed JSON matches type T.
 * Use `getValidatedJson(row, key, validator)` for type-safe parsing.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The parsed JSON value (unvalidated - use with caution)
 * @throws SqlTypeError if value is not a string
 * @throws SyntaxError if JSON parsing fails
 */
export function getJson<T>(row: SqlRow, key: string): T {
  const value = row[key]
  if (typeof value !== 'string') {
    throw new SqlTypeError(key, 'string (JSON)', value)
  }
  // Note: This cast is unsafe. Prefer getValidatedJson for new code.
  const parsed: unknown = JSON.parse(value)
  return parsed as T
}

/**
 * Get an optional JSON-parsed value from a string column.
 * Returns null if the value is null or undefined.
 * Parses the string value as JSON and returns the typed result.
 *
 * @deprecated This function does NOT verify that the parsed JSON matches type T.
 * Use `getValidatedOptionalJson(row, key, validator)` for type-safe parsing.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The parsed JSON value or null (unvalidated - use with caution)
 * @throws SqlTypeError if value is not null, undefined, or string
 * @throws SyntaxError if JSON parsing fails
 */
export function getOptionalJson<T>(row: SqlRow, key: string): T | null {
  const value = row[key]
  if (value === null || value === undefined) {
    return null
  }
  if (typeof value !== 'string') {
    throw new SqlTypeError(key, 'string (JSON) | null', value)
  }
  // Note: This cast is unsafe. Prefer getValidatedOptionalJson for new code.
  const parsed: unknown = JSON.parse(value)
  return parsed as T
}

// ============================================================================
// Validated JSON Value Accessors
// ============================================================================

/**
 * Get a JSON-parsed and validated value from a string column.
 * Parses the string value as JSON and validates it using the provided validator.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @param validator - Type guard function to validate the parsed JSON
 * @returns The validated parsed JSON value
 * @throws SqlTypeError if value is not a string
 * @throws SyntaxError if JSON parsing fails
 * @throws JsonValidationError if the parsed value fails validation
 *
 * @example
 * const isUser = (v: unknown): v is User =>
 *   isObject(v) && typeof v.name === 'string' && typeof v.age === 'number'
 * const user = getValidatedJson(row, 'user_data', isUser)
 */
export function getValidatedJson<T>(row: SqlRow, key: string, validator: JsonValidator<T>): T {
  const value = row[key]
  if (typeof value !== 'string') {
    throw new SqlTypeError(key, 'string (JSON)', value)
  }

  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch (e) {
    if (e instanceof SyntaxError) {
      throw new SyntaxError(`Column '${key}' contains invalid JSON: ${e.message}`)
    }
    throw e
  }

  if (!validator(parsed)) {
    throw new JsonValidationError(key, 'value does not match expected type', parsed)
  }

  return parsed
}

/**
 * Get an optional JSON-parsed and validated value from a string column.
 * Returns null if the value is null or undefined.
 * Parses the string value as JSON and validates it using the provided validator.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @param validator - Type guard function to validate the parsed JSON
 * @returns The validated parsed JSON value or null
 * @throws SqlTypeError if value is not null, undefined, or string
 * @throws SyntaxError if JSON parsing fails
 * @throws JsonValidationError if the parsed value fails validation
 *
 * @example
 * const isUser = (v: unknown): v is User =>
 *   isObject(v) && typeof v.name === 'string'
 * const user = getValidatedOptionalJson(row, 'user_data', isUser)
 */
export function getValidatedOptionalJson<T>(
  row: SqlRow,
  key: string,
  validator: JsonValidator<T>
): T | null {
  const value = row[key]
  if (value === null || value === undefined) {
    return null
  }
  if (typeof value !== 'string') {
    throw new SqlTypeError(key, 'string (JSON) | null', value)
  }

  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch (e) {
    if (e instanceof SyntaxError) {
      throw new SyntaxError(`Column '${key}' contains invalid JSON: ${e.message}`)
    }
    throw e
  }

  if (!validator(parsed)) {
    throw new JsonValidationError(key, 'value does not match expected type', parsed)
  }

  return parsed
}

// ============================================================================
// Type Guard Utilities
// ============================================================================

/**
 * Check if value is a non-null object (not an array)
 */
export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/**
 * Check if value is an array
 */
export function isArray(value: unknown): value is unknown[] {
  return Array.isArray(value)
}

/**
 * Check if value is an array where all elements pass the validator
 */
export function isArrayOf<T>(value: unknown, itemValidator: JsonValidator<T>): value is T[] {
  return Array.isArray(value) && value.every(itemValidator)
}

/**
 * Check if value is a string
 */
export function isString(value: unknown): value is string {
  return typeof value === 'string'
}

/**
 * Check if value is a number (and not NaN)
 */
export function isNumber(value: unknown): value is number {
  return typeof value === 'number' && !Number.isNaN(value)
}

/**
 * Check if value is a boolean
 */
export function isBoolean(value: unknown): value is boolean {
  return typeof value === 'boolean'
}

/**
 * Check if value is null
 */
export function isNull(value: unknown): value is null {
  return value === null
}

/**
 * Create a validator for an object with specific required fields
 *
 * @example
 * const isUser = createObjectValidator({
 *   name: isString,
 *   age: isNumber,
 * })
 */
export function createObjectValidator<T extends Record<string, unknown>>(
  fieldValidators: { [K in keyof T]: JsonValidator<T[K]> }
): JsonValidator<T> {
  return (value: unknown): value is T => {
    if (!isObject(value)) {
      return false
    }
    for (const [field, validator] of Object.entries(fieldValidators)) {
      if (!(validator as JsonValidator<unknown>)(value[field])) {
        return false
      }
    }
    return true
  }
}

/**
 * Create a validator that allows a value or null
 */
export function nullable<T>(validator: JsonValidator<T>): JsonValidator<T | null> {
  return (value: unknown): value is T | null => {
    return value === null || validator(value)
  }
}

/**
 * Create a validator that allows a value or undefined
 */
export function optional<T>(validator: JsonValidator<T>): JsonValidator<T | undefined> {
  return (value: unknown): value is T | undefined => {
    return value === undefined || validator(value)
  }
}

// ============================================================================
// Typed SQL Execution Helper
// ============================================================================

/**
 * Result wrapper for typed SQL execution
 */
export interface TypedExecResult<T extends SqlRow> {
  /** All rows as a typed array */
  rows: T[]
  /** Get first row or null if empty */
  one: () => T | null
  /** Alias for rows (compatibility with SqlStorage.exec().toArray()) */
  toArray: () => T[]
}

/**
 * Execute a SQL query with typed results, eliminating `as SqlRow` casts.
 *
 * @param sql - The SqlStorage instance
 * @param query - The SQL query string
 * @param params - Query parameters
 * @returns Typed result wrapper with rows, one(), and toArray() methods
 *
 * @example
 * // Before:
 * const rows = sql.exec(`SELECT * FROM users`).toArray()
 * return rows.map(r => getString(r as SqlRow, 'name'))
 *
 * // After:
 * const { rows } = typedExec(sql, `SELECT * FROM users`)
 * return rows.map(r => getString(r, 'name'))
 *
 * @example
 * // With typed interface:
 * interface UserRow extends SqlRow {
 *   name: string
 *   age: number
 * }
 * const { one } = typedExec<UserRow>(sql, `SELECT * FROM users WHERE id = ?`, id)
 * const user = one()
 */
export function typedExec<T extends SqlRow = SqlRow>(
  sql: SqlStorage,
  query: string,
  ...params: unknown[]
): TypedExecResult<T> {
  const result = sql.exec(query, ...params)
  const rows = result.toArray() as T[]
  return {
    rows,
    one: () => (rows.length > 0 ? rows[0]! : null),
    toArray: () => rows,
  }
}
