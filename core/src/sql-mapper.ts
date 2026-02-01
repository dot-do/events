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
 * Throws SqlTypeError if the value is not a string, or SyntaxError if parsing fails.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The parsed JSON value
 * @throws SqlTypeError if value is not a string
 * @throws SyntaxError if JSON parsing fails
 */
export function getJson<T>(row: SqlRow, key: string): T {
  const value = row[key]
  if (typeof value !== 'string') {
    throw new SqlTypeError(key, 'string (JSON)', value)
  }
  return JSON.parse(value) as T
}

/**
 * Get an optional JSON-parsed value from a string column.
 * Returns null if the value is null or undefined.
 * Parses the string value as JSON and returns the typed result.
 * Throws SqlTypeError if the value is not null, undefined, or string.
 *
 * @param row - The SQL row object
 * @param key - The column name
 * @returns The parsed JSON value or null
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
  return JSON.parse(value) as T
}
