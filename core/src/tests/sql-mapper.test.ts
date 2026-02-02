/**
 * SQL Mapper Tests
 *
 * Tests for the typed SQL row mapper utilities that provide
 * type-safe access to SQLite row columns.
 */

import { describe, it, expect } from 'vitest'
import {
  SqlTypeError,
  getString,
  getNumber,
  getBoolean,
  getOptionalString,
  getOptionalNumber,
  getOptionalBoolean,
  getJson,
  getOptionalJson,
  type SqlRow,
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
      type: 'collection.insert',
      collection: 'users',
      doc_id: 'user-456',
      ts: 1704067200000,
      bookmark: 'bk-789',
      doc: '{"name":"Alice","email":"alice@test.com"}',
      prev: null,
    }

    expect(getString(row, 'id')).toBe('evt-123')
    expect(getString(row, 'type')).toBe('collection.insert')
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
