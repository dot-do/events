/**
 * SQL Mapper Tests
 *
 * Tests for the typed SQL row mapper utilities that provide
 * type-safe access to SQLite row columns.
 */

import { describe, it, expect } from 'vitest'
import {
  SqlTypeError,
  JsonValidationError,
  getString,
  getNumber,
  getBoolean,
  getOptionalString,
  getOptionalNumber,
  getOptionalBoolean,
  getJson,
  getOptionalJson,
  getValidatedJson,
  getValidatedOptionalJson,
  isObject,
  isArray,
  isArrayOf,
  isString,
  isNumber,
  isBoolean,
  isNull,
  createObjectValidator,
  nullable,
  optional,
  type SqlRow,
  type JsonValidator,
} from '../sql-mapper.js'

describe('SqlTypeError', () => {
  it('creates error with correct message for null value', () => {
    const error = new SqlTypeError('column1', 'string', null)

    expect(error.name).toBe('SqlTypeError')
    expect(error.key).toBe('column1')
    expect(error.expectedType).toBe('string')
    expect(error.actualValue).toBe(null)
    expect(error.message).toBe("Column 'column1' expected string, got null: null")
  })

  it('creates error with correct message for wrong type', () => {
    const error = new SqlTypeError('age', 'number', 'twenty-five')

    expect(error.message).toBe("Column 'age' expected number, got string: \"twenty-five\"")
  })

  it('creates error with correct message for undefined', () => {
    const error = new SqlTypeError('field', 'boolean', undefined)

    expect(error.message).toBe("Column 'field' expected boolean, got undefined: undefined")
  })

  it('creates error with correct message for object', () => {
    const error = new SqlTypeError('data', 'string', { foo: 'bar' })

    expect(error.message).toBe("Column 'data' expected string, got object: {\"foo\":\"bar\"}")
  })

  it('creates error with correct message for array', () => {
    const error = new SqlTypeError('list', 'number', [1, 2, 3])

    expect(error.message).toBe("Column 'list' expected number, got object: [1,2,3]")
  })
})

describe('getString', () => {
  it('returns string value for valid string column', () => {
    const row: SqlRow = { name: 'Alice', age: 30 }

    expect(getString(row, 'name')).toBe('Alice')
  })

  it('returns empty string for empty string value', () => {
    const row: SqlRow = { name: '' }

    expect(getString(row, 'name')).toBe('')
  })

  it('throws SqlTypeError for number value', () => {
    const row: SqlRow = { name: 123 }

    expect(() => getString(row, 'name')).toThrow(SqlTypeError)
    expect(() => getString(row, 'name')).toThrow("Column 'name' expected string")
  })

  it('throws SqlTypeError for null value', () => {
    const row: SqlRow = { name: null }

    expect(() => getString(row, 'name')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for undefined/missing column', () => {
    const row: SqlRow = { other: 'value' }

    expect(() => getString(row, 'name')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for boolean value', () => {
    const row: SqlRow = { name: true }

    expect(() => getString(row, 'name')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for object value', () => {
    const row: SqlRow = { name: { first: 'Alice' } }

    expect(() => getString(row, 'name')).toThrow(SqlTypeError)
  })
})

describe('getNumber', () => {
  it('returns number value for valid number column', () => {
    const row: SqlRow = { age: 30 }

    expect(getNumber(row, 'age')).toBe(30)
  })

  it('returns 0 for zero value', () => {
    const row: SqlRow = { count: 0 }

    expect(getNumber(row, 'count')).toBe(0)
  })

  it('returns negative numbers', () => {
    const row: SqlRow = { temperature: -10.5 }

    expect(getNumber(row, 'temperature')).toBe(-10.5)
  })

  it('returns floating point numbers', () => {
    const row: SqlRow = { price: 19.99 }

    expect(getNumber(row, 'price')).toBe(19.99)
  })

  it('throws SqlTypeError for string value', () => {
    const row: SqlRow = { age: '30' }

    expect(() => getNumber(row, 'age')).toThrow(SqlTypeError)
    expect(() => getNumber(row, 'age')).toThrow("Column 'age' expected number")
  })

  it('throws SqlTypeError for null value', () => {
    const row: SqlRow = { age: null }

    expect(() => getNumber(row, 'age')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for undefined/missing column', () => {
    const row: SqlRow = {}

    expect(() => getNumber(row, 'age')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for boolean value', () => {
    const row: SqlRow = { count: true }

    expect(() => getNumber(row, 'count')).toThrow(SqlTypeError)
  })

  it('handles special number values', () => {
    const row: SqlRow = { inf: Infinity, negInf: -Infinity, nan: NaN }

    expect(getNumber(row, 'inf')).toBe(Infinity)
    expect(getNumber(row, 'negInf')).toBe(-Infinity)
    expect(getNumber(row, 'nan')).toBeNaN()
  })
})

describe('getBoolean', () => {
  it('returns true for boolean true', () => {
    const row: SqlRow = { active: true }

    expect(getBoolean(row, 'active')).toBe(true)
  })

  it('returns false for boolean false', () => {
    const row: SqlRow = { active: false }

    expect(getBoolean(row, 'active')).toBe(false)
  })

  it('returns true for non-zero number (SQLite integer storage)', () => {
    const row: SqlRow = { active: 1 }

    expect(getBoolean(row, 'active')).toBe(true)
  })

  it('returns false for zero (SQLite integer storage)', () => {
    const row: SqlRow = { active: 0 }

    expect(getBoolean(row, 'active')).toBe(false)
  })

  it('returns true for any non-zero number', () => {
    const row1: SqlRow = { flag: 42 }
    const row2: SqlRow = { flag: -1 }
    const row3: SqlRow = { flag: 0.5 }

    expect(getBoolean(row1, 'flag')).toBe(true)
    expect(getBoolean(row2, 'flag')).toBe(true)
    expect(getBoolean(row3, 'flag')).toBe(true)
  })

  it('throws SqlTypeError for string value', () => {
    const row: SqlRow = { active: 'true' }

    expect(() => getBoolean(row, 'active')).toThrow(SqlTypeError)
    expect(() => getBoolean(row, 'active')).toThrow("Column 'active' expected boolean")
  })

  it('throws SqlTypeError for null value', () => {
    const row: SqlRow = { active: null }

    expect(() => getBoolean(row, 'active')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for undefined/missing column', () => {
    const row: SqlRow = {}

    expect(() => getBoolean(row, 'active')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for object value', () => {
    const row: SqlRow = { active: {} }

    expect(() => getBoolean(row, 'active')).toThrow(SqlTypeError)
  })
})

describe('getOptionalString', () => {
  it('returns string value for valid string column', () => {
    const row: SqlRow = { name: 'Alice' }

    expect(getOptionalString(row, 'name')).toBe('Alice')
  })

  it('returns null for null value', () => {
    const row: SqlRow = { name: null }

    expect(getOptionalString(row, 'name')).toBeNull()
  })

  it('returns null for undefined/missing column', () => {
    const row: SqlRow = {}

    expect(getOptionalString(row, 'name')).toBeNull()
  })

  it('returns empty string for empty string value', () => {
    const row: SqlRow = { name: '' }

    expect(getOptionalString(row, 'name')).toBe('')
  })

  it('throws SqlTypeError for number value', () => {
    const row: SqlRow = { name: 123 }

    expect(() => getOptionalString(row, 'name')).toThrow(SqlTypeError)
    expect(() => getOptionalString(row, 'name')).toThrow("string | null")
  })

  it('throws SqlTypeError for boolean value', () => {
    const row: SqlRow = { name: true }

    expect(() => getOptionalString(row, 'name')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for object value', () => {
    const row: SqlRow = { name: { value: 'test' } }

    expect(() => getOptionalString(row, 'name')).toThrow(SqlTypeError)
  })
})

describe('getOptionalNumber', () => {
  it('returns number value for valid number column', () => {
    const row: SqlRow = { age: 25 }

    expect(getOptionalNumber(row, 'age')).toBe(25)
  })

  it('returns null for null value', () => {
    const row: SqlRow = { age: null }

    expect(getOptionalNumber(row, 'age')).toBeNull()
  })

  it('returns null for undefined/missing column', () => {
    const row: SqlRow = {}

    expect(getOptionalNumber(row, 'age')).toBeNull()
  })

  it('returns 0 for zero value', () => {
    const row: SqlRow = { count: 0 }

    expect(getOptionalNumber(row, 'count')).toBe(0)
  })

  it('throws SqlTypeError for string value', () => {
    const row: SqlRow = { age: '25' }

    expect(() => getOptionalNumber(row, 'age')).toThrow(SqlTypeError)
    expect(() => getOptionalNumber(row, 'age')).toThrow("number | null")
  })

  it('throws SqlTypeError for boolean value', () => {
    const row: SqlRow = { age: true }

    expect(() => getOptionalNumber(row, 'age')).toThrow(SqlTypeError)
  })
})

describe('getOptionalBoolean', () => {
  it('returns true for boolean true', () => {
    const row: SqlRow = { active: true }

    expect(getOptionalBoolean(row, 'active')).toBe(true)
  })

  it('returns false for boolean false', () => {
    const row: SqlRow = { active: false }

    expect(getOptionalBoolean(row, 'active')).toBe(false)
  })

  it('returns null for null value', () => {
    const row: SqlRow = { active: null }

    expect(getOptionalBoolean(row, 'active')).toBeNull()
  })

  it('returns null for undefined/missing column', () => {
    const row: SqlRow = {}

    expect(getOptionalBoolean(row, 'active')).toBeNull()
  })

  it('returns true for non-zero number (SQLite storage)', () => {
    const row: SqlRow = { active: 1 }

    expect(getOptionalBoolean(row, 'active')).toBe(true)
  })

  it('returns false for zero (SQLite storage)', () => {
    const row: SqlRow = { active: 0 }

    expect(getOptionalBoolean(row, 'active')).toBe(false)
  })

  it('throws SqlTypeError for string value', () => {
    const row: SqlRow = { active: 'yes' }

    expect(() => getOptionalBoolean(row, 'active')).toThrow(SqlTypeError)
    expect(() => getOptionalBoolean(row, 'active')).toThrow("boolean | null")
  })

  it('throws SqlTypeError for object value', () => {
    const row: SqlRow = { active: {} }

    expect(() => getOptionalBoolean(row, 'active')).toThrow(SqlTypeError)
  })
})

describe('getJson', () => {
  it('parses JSON object from string column', () => {
    const row: SqlRow = { data: '{"name":"Alice","age":30}' }

    const result = getJson<{ name: string; age: number }>(row, 'data')

    expect(result).toEqual({ name: 'Alice', age: 30 })
  })

  it('parses JSON array from string column', () => {
    const row: SqlRow = { items: '[1, 2, 3, 4, 5]' }

    const result = getJson<number[]>(row, 'items')

    expect(result).toEqual([1, 2, 3, 4, 5])
  })

  it('parses JSON string from string column', () => {
    const row: SqlRow = { value: '"hello world"' }

    const result = getJson<string>(row, 'value')

    expect(result).toBe('hello world')
  })

  it('parses JSON number from string column', () => {
    const row: SqlRow = { value: '42' }

    const result = getJson<number>(row, 'value')

    expect(result).toBe(42)
  })

  it('parses JSON boolean from string column', () => {
    const row: SqlRow = { flag: 'true' }

    const result = getJson<boolean>(row, 'flag')

    expect(result).toBe(true)
  })

  it('parses JSON null from string column', () => {
    const row: SqlRow = { value: 'null' }

    const result = getJson<null>(row, 'value')

    expect(result).toBeNull()
  })

  it('parses nested JSON structures', () => {
    const row: SqlRow = {
      complex: '{"users":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}],"total":2}',
    }

    interface Result {
      users: Array<{ id: number; name: string }>
      total: number
    }

    const result = getJson<Result>(row, 'complex')

    expect(result.users).toHaveLength(2)
    expect(result.users[0].name).toBe('Alice')
    expect(result.total).toBe(2)
  })

  it('throws SqlTypeError for non-string value', () => {
    const row: SqlRow = { data: { name: 'Alice' } } // Object, not string

    expect(() => getJson(row, 'data')).toThrow(SqlTypeError)
    expect(() => getJson(row, 'data')).toThrow("string (JSON)")
  })

  it('throws SqlTypeError for null value', () => {
    const row: SqlRow = { data: null }

    expect(() => getJson(row, 'data')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for number value', () => {
    const row: SqlRow = { data: 123 }

    expect(() => getJson(row, 'data')).toThrow(SqlTypeError)
  })

  it('throws SyntaxError for invalid JSON string', () => {
    const row: SqlRow = { data: 'not valid json' }

    expect(() => getJson(row, 'data')).toThrow(SyntaxError)
  })

  it('throws SyntaxError for malformed JSON', () => {
    const row: SqlRow = { data: '{"name": "Alice"' } // Missing closing brace

    expect(() => getJson(row, 'data')).toThrow(SyntaxError)
  })
})

describe('getOptionalJson', () => {
  it('parses JSON object from string column', () => {
    const row: SqlRow = { data: '{"id":1}' }

    const result = getOptionalJson<{ id: number }>(row, 'data')

    expect(result).toEqual({ id: 1 })
  })

  it('returns null for null value', () => {
    const row: SqlRow = { data: null }

    const result = getOptionalJson<{ id: number }>(row, 'data')

    expect(result).toBeNull()
  })

  it('returns null for undefined/missing column', () => {
    const row: SqlRow = {}

    const result = getOptionalJson<{ id: number }>(row, 'data')

    expect(result).toBeNull()
  })

  it('parses JSON array from string column', () => {
    const row: SqlRow = { tags: '["a", "b", "c"]' }

    const result = getOptionalJson<string[]>(row, 'tags')

    expect(result).toEqual(['a', 'b', 'c'])
  })

  it('throws SqlTypeError for non-string value', () => {
    const row: SqlRow = { data: 123 }

    expect(() => getOptionalJson(row, 'data')).toThrow(SqlTypeError)
    expect(() => getOptionalJson(row, 'data')).toThrow("string (JSON) | null")
  })

  it('throws SqlTypeError for boolean value', () => {
    const row: SqlRow = { data: true }

    expect(() => getOptionalJson(row, 'data')).toThrow(SqlTypeError)
  })

  it('throws SqlTypeError for object value (not string)', () => {
    const row: SqlRow = { data: { key: 'value' } }

    expect(() => getOptionalJson(row, 'data')).toThrow(SqlTypeError)
  })

  it('throws SyntaxError for invalid JSON string', () => {
    const row: SqlRow = { data: '{invalid}' }

    expect(() => getOptionalJson(row, 'data')).toThrow(SyntaxError)
  })
})

describe('integration scenarios', () => {
  it('handles typical database row with mixed types', () => {
    const row: SqlRow = {
      id: 1,
      name: 'Alice',
      email: 'alice@example.com',
      age: 30,
      is_active: 1, // SQLite boolean as integer
      metadata: '{"role":"admin","permissions":["read","write"]}',
      deleted_at: null,
      notes: null,
    }

    expect(getNumber(row, 'id')).toBe(1)
    expect(getString(row, 'name')).toBe('Alice')
    expect(getString(row, 'email')).toBe('alice@example.com')
    expect(getNumber(row, 'age')).toBe(30)
    expect(getBoolean(row, 'is_active')).toBe(true)
    expect(getJson<{ role: string }>(row, 'metadata').role).toBe('admin')
    expect(getOptionalString(row, 'deleted_at')).toBeNull()
    expect(getOptionalString(row, 'notes')).toBeNull()
  })

  it('handles CDC event row', () => {
    const row: SqlRow = {
      id: 'evt-123',
      type: 'collection.created',
      collection: 'users',
      doc_id: 'user-456',
      ts: 1704067200000,
      bookmark: 'bk-789',
      doc: '{"name":"Alice","email":"alice@test.com"}',
      prev: null,
    }

    expect(getString(row, 'id')).toBe('evt-123')
    expect(getString(row, 'type')).toBe('collection.created')
    expect(getString(row, 'collection')).toBe('users')
    expect(getString(row, 'doc_id')).toBe('user-456')
    expect(getNumber(row, 'ts')).toBe(1704067200000)
    expect(getString(row, 'bookmark')).toBe('bk-789')

    const doc = getJson<{ name: string; email: string }>(row, 'doc')
    expect(doc.name).toBe('Alice')

    expect(getOptionalJson<Record<string, unknown>>(row, 'prev')).toBeNull()
  })

  it('handles row with all optional fields being null', () => {
    const row: SqlRow = {
      id: 'test-1',
      optional_str: null,
      optional_num: null,
      optional_bool: null,
      optional_json: null,
    }

    expect(getString(row, 'id')).toBe('test-1')
    expect(getOptionalString(row, 'optional_str')).toBeNull()
    expect(getOptionalNumber(row, 'optional_num')).toBeNull()
    expect(getOptionalBoolean(row, 'optional_bool')).toBeNull()
    expect(getOptionalJson(row, 'optional_json')).toBeNull()
  })

  it('handles row with all optional fields being undefined', () => {
    const row: SqlRow = {
      id: 'test-1',
      // All other fields are undefined (not present)
    }

    expect(getString(row, 'id')).toBe('test-1')
    expect(getOptionalString(row, 'missing_str')).toBeNull()
    expect(getOptionalNumber(row, 'missing_num')).toBeNull()
    expect(getOptionalBoolean(row, 'missing_bool')).toBeNull()
    expect(getOptionalJson(row, 'missing_json')).toBeNull()
  })
})

// ============================================================================
// JsonValidationError Tests
// ============================================================================

describe('JsonValidationError', () => {
  it('creates error with correct message', () => {
    const error = new JsonValidationError('data', 'expected object with name field', { id: 1 })

    expect(error.name).toBe('JsonValidationError')
    expect(error.key).toBe('data')
    expect(error.validationError).toBe('expected object with name field')
    expect(error.actualValue).toEqual({ id: 1 })
    expect(error.message).toContain("Column 'data' JSON validation failed")
    expect(error.message).toContain('expected object with name field')
  })

  it('serializes complex values in error message', () => {
    const error = new JsonValidationError('field', 'type mismatch', { nested: { array: [1, 2, 3] } })

    expect(error.message).toContain('{"nested":{"array":[1,2,3]}}')
  })
})

// ============================================================================
// Type Guard Utility Tests
// ============================================================================

describe('isObject', () => {
  it('returns true for plain objects', () => {
    expect(isObject({})).toBe(true)
    expect(isObject({ name: 'Alice' })).toBe(true)
    expect(isObject({ nested: { deep: true } })).toBe(true)
  })

  it('returns false for arrays', () => {
    expect(isObject([])).toBe(false)
    expect(isObject([1, 2, 3])).toBe(false)
  })

  it('returns false for null', () => {
    expect(isObject(null)).toBe(false)
  })

  it('returns false for primitives', () => {
    expect(isObject('string')).toBe(false)
    expect(isObject(123)).toBe(false)
    expect(isObject(true)).toBe(false)
    expect(isObject(undefined)).toBe(false)
  })
})

describe('isArray', () => {
  it('returns true for arrays', () => {
    expect(isArray([])).toBe(true)
    expect(isArray([1, 2, 3])).toBe(true)
    expect(isArray(['a', 'b'])).toBe(true)
  })

  it('returns false for objects', () => {
    expect(isArray({})).toBe(false)
    expect(isArray({ length: 3 })).toBe(false)
  })

  it('returns false for primitives', () => {
    expect(isArray('string')).toBe(false)
    expect(isArray(123)).toBe(false)
    expect(isArray(null)).toBe(false)
  })
})

describe('isArrayOf', () => {
  it('returns true for array of matching items', () => {
    expect(isArrayOf([1, 2, 3], isNumber)).toBe(true)
    expect(isArrayOf(['a', 'b'], isString)).toBe(true)
    expect(isArrayOf([true, false], isBoolean)).toBe(true)
  })

  it('returns true for empty array', () => {
    expect(isArrayOf([], isNumber)).toBe(true)
    expect(isArrayOf([], isString)).toBe(true)
  })

  it('returns false for mixed array', () => {
    expect(isArrayOf([1, 'two', 3], isNumber)).toBe(false)
    expect(isArrayOf(['a', 2, 'c'], isString)).toBe(false)
  })

  it('returns false for non-array', () => {
    expect(isArrayOf({}, isNumber)).toBe(false)
    expect(isArrayOf('string', isString)).toBe(false)
  })
})

describe('isString', () => {
  it('returns true for strings', () => {
    expect(isString('')).toBe(true)
    expect(isString('hello')).toBe(true)
  })

  it('returns false for non-strings', () => {
    expect(isString(123)).toBe(false)
    expect(isString(null)).toBe(false)
    expect(isString(undefined)).toBe(false)
  })
})

describe('isNumber', () => {
  it('returns true for numbers', () => {
    expect(isNumber(0)).toBe(true)
    expect(isNumber(42)).toBe(true)
    expect(isNumber(-10.5)).toBe(true)
    expect(isNumber(Infinity)).toBe(true)
  })

  it('returns false for NaN', () => {
    expect(isNumber(NaN)).toBe(false)
  })

  it('returns false for non-numbers', () => {
    expect(isNumber('42')).toBe(false)
    expect(isNumber(null)).toBe(false)
  })
})

describe('isBoolean', () => {
  it('returns true for booleans', () => {
    expect(isBoolean(true)).toBe(true)
    expect(isBoolean(false)).toBe(true)
  })

  it('returns false for non-booleans', () => {
    expect(isBoolean(1)).toBe(false)
    expect(isBoolean(0)).toBe(false)
    expect(isBoolean('true')).toBe(false)
  })
})

describe('isNull', () => {
  it('returns true for null', () => {
    expect(isNull(null)).toBe(true)
  })

  it('returns false for non-null', () => {
    expect(isNull(undefined)).toBe(false)
    expect(isNull(0)).toBe(false)
    expect(isNull('')).toBe(false)
  })
})

describe('createObjectValidator', () => {
  interface User {
    name: string
    age: number
  }

  const isUser = createObjectValidator<User>({
    name: isString,
    age: isNumber,
  })

  it('validates matching objects', () => {
    expect(isUser({ name: 'Alice', age: 30 })).toBe(true)
  })

  it('validates objects with extra fields', () => {
    expect(isUser({ name: 'Alice', age: 30, email: 'alice@test.com' })).toBe(true)
  })

  it('rejects objects missing required fields', () => {
    expect(isUser({ name: 'Alice' })).toBe(false)
    expect(isUser({ age: 30 })).toBe(false)
    expect(isUser({})).toBe(false)
  })

  it('rejects objects with wrong field types', () => {
    expect(isUser({ name: 'Alice', age: '30' })).toBe(false)
    expect(isUser({ name: 123, age: 30 })).toBe(false)
  })

  it('rejects non-objects', () => {
    expect(isUser(null)).toBe(false)
    expect(isUser([])).toBe(false)
    expect(isUser('string')).toBe(false)
  })
})

describe('nullable', () => {
  const nullableString = nullable(isString)

  it('accepts null', () => {
    expect(nullableString(null)).toBe(true)
  })

  it('accepts valid type', () => {
    expect(nullableString('hello')).toBe(true)
  })

  it('rejects other types', () => {
    expect(nullableString(undefined)).toBe(false)
    expect(nullableString(123)).toBe(false)
  })
})

describe('optional', () => {
  const optionalNumber = optional(isNumber)

  it('accepts undefined', () => {
    expect(optionalNumber(undefined)).toBe(true)
  })

  it('accepts valid type', () => {
    expect(optionalNumber(42)).toBe(true)
  })

  it('rejects other types', () => {
    expect(optionalNumber(null)).toBe(false)
    expect(optionalNumber('42')).toBe(false)
  })
})

// ============================================================================
// Validated JSON Accessor Tests
// ============================================================================

describe('getValidatedJson', () => {
  interface User {
    name: string
    age: number
  }

  const isUser: JsonValidator<User> = (v): v is User =>
    isObject(v) && typeof v.name === 'string' && typeof v.age === 'number'

  it('parses and validates matching JSON', () => {
    const row: SqlRow = { data: '{"name":"Alice","age":30}' }

    const result = getValidatedJson(row, 'data', isUser)

    expect(result).toEqual({ name: 'Alice', age: 30 })
  })

  it('throws JsonValidationError for non-matching structure', () => {
    const row: SqlRow = { data: '{"name":"Alice"}' } // missing age

    expect(() => getValidatedJson(row, 'data', isUser)).toThrow(JsonValidationError)
    expect(() => getValidatedJson(row, 'data', isUser)).toThrow('JSON validation failed')
  })

  it('throws JsonValidationError for wrong field types', () => {
    const row: SqlRow = { data: '{"name":"Alice","age":"thirty"}' }

    expect(() => getValidatedJson(row, 'data', isUser)).toThrow(JsonValidationError)
  })

  it('throws SqlTypeError for non-string column', () => {
    const row: SqlRow = { data: { name: 'Alice', age: 30 } }

    expect(() => getValidatedJson(row, 'data', isUser)).toThrow(SqlTypeError)
  })

  it('throws SyntaxError for invalid JSON with context', () => {
    const row: SqlRow = { data: '{invalid json}' }

    expect(() => getValidatedJson(row, 'data', isUser)).toThrow(SyntaxError)
    expect(() => getValidatedJson(row, 'data', isUser)).toThrow("Column 'data' contains invalid JSON")
  })

  it('validates primitive JSON values', () => {
    const row: SqlRow = { num: '42', str: '"hello"', bool: 'true' }

    expect(getValidatedJson(row, 'num', isNumber)).toBe(42)
    expect(getValidatedJson(row, 'str', isString)).toBe('hello')
    expect(getValidatedJson(row, 'bool', isBoolean)).toBe(true)
  })

  it('validates arrays with isArrayOf', () => {
    const row: SqlRow = { numbers: '[1, 2, 3, 4]' }
    const isNumberArray: JsonValidator<number[]> = (v): v is number[] => isArrayOf(v, isNumber)

    expect(getValidatedJson(row, 'numbers', isNumberArray)).toEqual([1, 2, 3, 4])
  })

  it('throws for arrays when expecting object', () => {
    const row: SqlRow = { data: '[1, 2, 3]' }

    expect(() => getValidatedJson(row, 'data', isUser)).toThrow(JsonValidationError)
  })

  it('works with createObjectValidator', () => {
    interface Config {
      host: string
      port: number
    }

    const isConfig = createObjectValidator<Config>({
      host: isString,
      port: isNumber,
    })

    const row: SqlRow = { config: '{"host":"localhost","port":8080}' }

    expect(getValidatedJson(row, 'config', isConfig)).toEqual({ host: 'localhost', port: 8080 })
  })
})

describe('getValidatedOptionalJson', () => {
  interface User {
    name: string
  }

  const isUser: JsonValidator<User> = (v): v is User => isObject(v) && typeof v.name === 'string'

  it('returns null for null value', () => {
    const row: SqlRow = { data: null }

    expect(getValidatedOptionalJson(row, 'data', isUser)).toBeNull()
  })

  it('returns null for undefined/missing column', () => {
    const row: SqlRow = {}

    expect(getValidatedOptionalJson(row, 'data', isUser)).toBeNull()
  })

  it('parses and validates matching JSON', () => {
    const row: SqlRow = { data: '{"name":"Alice"}' }

    expect(getValidatedOptionalJson(row, 'data', isUser)).toEqual({ name: 'Alice' })
  })

  it('throws JsonValidationError for non-matching structure', () => {
    const row: SqlRow = { data: '{"id":1}' } // missing name

    expect(() => getValidatedOptionalJson(row, 'data', isUser)).toThrow(JsonValidationError)
  })

  it('throws SqlTypeError for non-string value', () => {
    const row: SqlRow = { data: 123 }

    expect(() => getValidatedOptionalJson(row, 'data', isUser)).toThrow(SqlTypeError)
    expect(() => getValidatedOptionalJson(row, 'data', isUser)).toThrow('string (JSON) | null')
  })

  it('throws SyntaxError for invalid JSON with context', () => {
    const row: SqlRow = { data: '{bad' }

    expect(() => getValidatedOptionalJson(row, 'data', isUser)).toThrow(SyntaxError)
    expect(() => getValidatedOptionalJson(row, 'data', isUser)).toThrow("Column 'data' contains invalid JSON")
  })
})

// ============================================================================
// Validation Edge Cases
// ============================================================================

describe('validation edge cases', () => {
  it('handles deeply nested objects', () => {
    interface DeepObject {
      level1: {
        level2: {
          level3: {
            value: string
          }
        }
      }
    }

    const isDeepObject: JsonValidator<DeepObject> = (v): v is DeepObject =>
      isObject(v) &&
      isObject(v.level1) &&
      isObject((v.level1 as Record<string, unknown>).level2) &&
      isObject(((v.level1 as Record<string, unknown>).level2 as Record<string, unknown>).level3) &&
      typeof ((((v.level1 as Record<string, unknown>).level2 as Record<string, unknown>).level3 as Record<string, unknown>).value) === 'string'

    const row: SqlRow = { deep: '{"level1":{"level2":{"level3":{"value":"found"}}}}' }

    const result = getValidatedJson(row, 'deep', isDeepObject)
    expect(result.level1.level2.level3.value).toBe('found')
  })

  it('handles empty objects', () => {
    const isEmptyObj: JsonValidator<object> = (v): v is object => isObject(v)
    const row: SqlRow = { data: '{}' }

    expect(getValidatedJson(row, 'data', isEmptyObj)).toEqual({})
  })

  it('handles empty arrays', () => {
    const isEmptyArray: JsonValidator<unknown[]> = (v): v is unknown[] => isArray(v)
    const row: SqlRow = { data: '[]' }

    expect(getValidatedJson(row, 'data', isEmptyArray)).toEqual([])
  })

  it('handles JSON with unicode characters', () => {
    interface Message {
      text: string
    }

    const isMessage: JsonValidator<Message> = (v): v is Message =>
      isObject(v) && typeof v.text === 'string'

    const row: SqlRow = { data: '{"text":"Hello \\u4e16\\u754c \\ud83d\\ude00"}' }

    const result = getValidatedJson(row, 'data', isMessage)
    expect(result.text).toContain('\u4e16\u754c')
  })

  it('handles JSON with escaped characters', () => {
    interface Data {
      path: string
    }

    const isData: JsonValidator<Data> = (v): v is Data => isObject(v) && typeof v.path === 'string'

    const row: SqlRow = { data: '{"path":"C:\\\\Users\\\\test\\\\file.txt"}' }

    const result = getValidatedJson(row, 'data', isData)
    expect(result.path).toBe('C:\\Users\\test\\file.txt')
  })

  it('handles JSON with special number values as strings', () => {
    // Note: JSON doesn't support Infinity or NaN natively
    const row: SqlRow = { data: '{"large":1e308,"small":1e-308}' }

    interface Numbers {
      large: number
      small: number
    }

    const isNumbers: JsonValidator<Numbers> = (v): v is Numbers =>
      isObject(v) && typeof v.large === 'number' && typeof v.small === 'number'

    const result = getValidatedJson(row, 'data', isNumbers)
    expect(result.large).toBe(1e308)
    expect(result.small).toBe(1e-308)
  })

  it('rejects JSON primitive when object expected', () => {
    const isObj: JsonValidator<object> = (v): v is object => isObject(v)
    const row: SqlRow = { data: '"just a string"' }

    expect(() => getValidatedJson(row, 'data', isObj)).toThrow(JsonValidationError)
  })

  it('rejects null JSON value when object expected', () => {
    const isObj: JsonValidator<object> = (v): v is object => isObject(v)
    const row: SqlRow = { data: 'null' }

    expect(() => getValidatedJson(row, 'data', isObj)).toThrow(JsonValidationError)
  })

  it('handles whitespace in JSON', () => {
    const isObj: JsonValidator<{ a: number }> = (v): v is { a: number } =>
      isObject(v) && typeof v.a === 'number'

    const row: SqlRow = { data: '  {  "a"  :  1  }  ' }

    expect(getValidatedJson(row, 'data', isObj)).toEqual({ a: 1 })
  })
})

describe('integration with validated JSON', () => {
  it('handles CDC event row with validation', () => {
    interface CDCDoc {
      name: string
      email: string
    }

    const isCDCDoc: JsonValidator<CDCDoc> = (v): v is CDCDoc =>
      isObject(v) && typeof v.name === 'string' && typeof v.email === 'string'

    const row: SqlRow = {
      id: 'evt-123',
      type: 'collection.created',
      doc: '{"name":"Alice","email":"alice@test.com"}',
      prev: null,
    }

    const doc = getValidatedJson(row, 'doc', isCDCDoc)
    expect(doc.name).toBe('Alice')
    expect(doc.email).toBe('alice@test.com')

    const prev = getValidatedOptionalJson(row, 'prev', isCDCDoc)
    expect(prev).toBeNull()
  })

  it('catches malformed CDC document early', () => {
    interface CDCDoc {
      name: string
      email: string
    }

    const isCDCDoc: JsonValidator<CDCDoc> = (v): v is CDCDoc =>
      isObject(v) && typeof v.name === 'string' && typeof v.email === 'string'

    // Corrupted data - email is a number
    const row: SqlRow = {
      id: 'evt-123',
      doc: '{"name":"Alice","email":12345}',
    }

    expect(() => getValidatedJson(row, 'doc', isCDCDoc)).toThrow(JsonValidationError)
  })

  it('works with complex metadata validation', () => {
    interface Metadata {
      role: string
      permissions: string[]
    }

    const isMetadata: JsonValidator<Metadata> = (v): v is Metadata =>
      isObject(v) &&
      typeof v.role === 'string' &&
      isArrayOf(v.permissions, isString)

    const row: SqlRow = {
      metadata: '{"role":"admin","permissions":["read","write","delete"]}',
    }

    const meta = getValidatedJson(row, 'metadata', isMetadata)
    expect(meta.role).toBe('admin')
    expect(meta.permissions).toEqual(['read', 'write', 'delete'])
  })
})
