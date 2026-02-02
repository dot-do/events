/**
 * Schema Registry Tests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  validateAgainstSchema,
  type JsonSchema,
  type ValidationError,
} from '../schema-registry'

describe('validateAgainstSchema', () => {
  describe('type validation', () => {
    it('validates string type', () => {
      const schema: JsonSchema = { type: 'string' }
      expect(validateAgainstSchema('hello', schema)).toEqual([])
      expect(validateAgainstSchema(123, schema)).toHaveLength(1)
      expect(validateAgainstSchema(123, schema)[0]?.keyword).toBe('type')
    })

    it('validates number type', () => {
      const schema: JsonSchema = { type: 'number' }
      expect(validateAgainstSchema(123, schema)).toEqual([])
      expect(validateAgainstSchema(3.14, schema)).toEqual([])
      expect(validateAgainstSchema('hello', schema)).toHaveLength(1)
    })

    it('validates integer type', () => {
      const schema: JsonSchema = { type: 'integer' }
      expect(validateAgainstSchema(123, schema)).toEqual([])
      expect(validateAgainstSchema(3.14, schema)).toHaveLength(1)
      expect(validateAgainstSchema('hello', schema)).toHaveLength(1)
    })

    it('validates boolean type', () => {
      const schema: JsonSchema = { type: 'boolean' }
      expect(validateAgainstSchema(true, schema)).toEqual([])
      expect(validateAgainstSchema(false, schema)).toEqual([])
      expect(validateAgainstSchema('true', schema)).toHaveLength(1)
    })

    it('validates null type', () => {
      const schema: JsonSchema = { type: 'null' }
      expect(validateAgainstSchema(null, schema)).toEqual([])
      // Note: undefined is treated as null by getJsonType, so we skip that test
      expect(validateAgainstSchema('null', schema)).toHaveLength(1)
      expect(validateAgainstSchema(0, schema)).toHaveLength(1)
    })

    it('validates array type', () => {
      const schema: JsonSchema = { type: 'array' }
      expect(validateAgainstSchema([], schema)).toEqual([])
      expect(validateAgainstSchema([1, 2, 3], schema)).toEqual([])
      expect(validateAgainstSchema('hello', schema)).toHaveLength(1)
    })

    it('validates object type', () => {
      const schema: JsonSchema = { type: 'object' }
      expect(validateAgainstSchema({}, schema)).toEqual([])
      expect(validateAgainstSchema({ a: 1 }, schema)).toEqual([])
      expect(validateAgainstSchema('hello', schema)).toHaveLength(1)
      expect(validateAgainstSchema([], schema)).toHaveLength(1)
    })
  })

  describe('string validations', () => {
    it('validates minLength', () => {
      const schema: JsonSchema = { type: 'string', minLength: 3 }
      expect(validateAgainstSchema('hello', schema)).toEqual([])
      expect(validateAgainstSchema('ab', schema)).toHaveLength(1)
      expect(validateAgainstSchema('ab', schema)[0]?.keyword).toBe('minLength')
    })

    it('validates maxLength', () => {
      const schema: JsonSchema = { type: 'string', maxLength: 5 }
      expect(validateAgainstSchema('hello', schema)).toEqual([])
      expect(validateAgainstSchema('toolong', schema)).toHaveLength(1)
      expect(validateAgainstSchema('toolong', schema)[0]?.keyword).toBe('maxLength')
    })

    it('validates pattern', () => {
      const schema: JsonSchema = { type: 'string', pattern: '^[a-z]+$' }
      expect(validateAgainstSchema('hello', schema)).toEqual([])
      expect(validateAgainstSchema('Hello', schema)).toHaveLength(1)
      expect(validateAgainstSchema('123', schema)).toHaveLength(1)
    })

    it('validates format: email', () => {
      const schema: JsonSchema = { type: 'string', format: 'email' }
      expect(validateAgainstSchema('test@example.com', schema)).toEqual([])
      expect(validateAgainstSchema('invalid', schema)).toHaveLength(1)
    })

    it('validates format: date-time', () => {
      const schema: JsonSchema = { type: 'string', format: 'date-time' }
      expect(validateAgainstSchema('2024-01-15T10:30:00Z', schema)).toEqual([])
      expect(validateAgainstSchema('not-a-date', schema)).toHaveLength(1)
    })

    it('validates format: uri', () => {
      const schema: JsonSchema = { type: 'string', format: 'uri' }
      expect(validateAgainstSchema('https://example.com', schema)).toEqual([])
      expect(validateAgainstSchema('not-a-url', schema)).toHaveLength(1)
    })

    it('validates format: uuid', () => {
      const schema: JsonSchema = { type: 'string', format: 'uuid' }
      expect(validateAgainstSchema('550e8400-e29b-41d4-a716-446655440000', schema)).toEqual([])
      expect(validateAgainstSchema('not-a-uuid', schema)).toHaveLength(1)
    })
  })

  describe('number validations', () => {
    it('validates minimum', () => {
      const schema: JsonSchema = { type: 'number', minimum: 5 }
      expect(validateAgainstSchema(10, schema)).toEqual([])
      expect(validateAgainstSchema(5, schema)).toEqual([])
      expect(validateAgainstSchema(4, schema)).toHaveLength(1)
    })

    it('validates maximum', () => {
      const schema: JsonSchema = { type: 'number', maximum: 10 }
      expect(validateAgainstSchema(5, schema)).toEqual([])
      expect(validateAgainstSchema(10, schema)).toEqual([])
      expect(validateAgainstSchema(11, schema)).toHaveLength(1)
    })
  })

  describe('array validations', () => {
    it('validates minItems', () => {
      const schema: JsonSchema = { type: 'array', minItems: 2 }
      expect(validateAgainstSchema([1, 2, 3], schema)).toEqual([])
      expect(validateAgainstSchema([1], schema)).toHaveLength(1)
    })

    it('validates maxItems', () => {
      const schema: JsonSchema = { type: 'array', maxItems: 3 }
      expect(validateAgainstSchema([1, 2], schema)).toEqual([])
      expect(validateAgainstSchema([1, 2, 3, 4], schema)).toHaveLength(1)
    })

    it('validates items schema', () => {
      const schema: JsonSchema = {
        type: 'array',
        items: { type: 'number' },
      }
      expect(validateAgainstSchema([1, 2, 3], schema)).toEqual([])
      expect(validateAgainstSchema([1, 'two', 3], schema)).toHaveLength(1)
      expect(validateAgainstSchema([1, 'two', 3], schema)[0]?.path).toBe('[1]')
    })
  })

  describe('object validations', () => {
    it('validates required properties', () => {
      const schema: JsonSchema = {
        type: 'object',
        required: ['name', 'email'],
      }
      expect(validateAgainstSchema({ name: 'John', email: 'john@example.com' }, schema)).toEqual([])
      expect(validateAgainstSchema({ name: 'John' }, schema)).toHaveLength(1)
      expect(validateAgainstSchema({ name: 'John' }, schema)[0]?.keyword).toBe('required')
    })

    it('validates property schemas', () => {
      const schema: JsonSchema = {
        type: 'object',
        properties: {
          name: { type: 'string' },
          age: { type: 'number' },
        },
      }
      expect(validateAgainstSchema({ name: 'John', age: 30 }, schema)).toEqual([])
      expect(validateAgainstSchema({ name: 'John', age: 'thirty' }, schema)).toHaveLength(1)
      expect(validateAgainstSchema({ name: 'John', age: 'thirty' }, schema)[0]?.path).toBe('age')
    })

    it('validates additionalProperties: false', () => {
      const schema: JsonSchema = {
        type: 'object',
        properties: {
          name: { type: 'string' },
        },
        additionalProperties: false,
      }
      expect(validateAgainstSchema({ name: 'John' }, schema)).toEqual([])
      expect(validateAgainstSchema({ name: 'John', extra: 'field' }, schema)).toHaveLength(1)
      expect(validateAgainstSchema({ name: 'John', extra: 'field' }, schema)[0]?.keyword).toBe('additionalProperties')
    })

    it('validates additionalProperties with schema', () => {
      const schema: JsonSchema = {
        type: 'object',
        properties: {
          name: { type: 'string' },
        },
        additionalProperties: { type: 'number' },
      }
      expect(validateAgainstSchema({ name: 'John', score: 100 }, schema)).toEqual([])
      expect(validateAgainstSchema({ name: 'John', score: 'high' }, schema)).toHaveLength(1)
    })
  })

  describe('enum and const', () => {
    it('validates enum', () => {
      const schema: JsonSchema = { enum: ['red', 'green', 'blue'] }
      expect(validateAgainstSchema('red', schema)).toEqual([])
      expect(validateAgainstSchema('yellow', schema)).toHaveLength(1)
      expect(validateAgainstSchema('yellow', schema)[0]?.keyword).toBe('enum')
    })

    it('validates const', () => {
      const schema: JsonSchema = { const: 'specific-value' }
      expect(validateAgainstSchema('specific-value', schema)).toEqual([])
      expect(validateAgainstSchema('other-value', schema)).toHaveLength(1)
    })
  })

  describe('composition (oneOf, anyOf, allOf)', () => {
    it('validates oneOf', () => {
      const schema: JsonSchema = {
        oneOf: [{ type: 'string' }, { type: 'number' }],
      }
      expect(validateAgainstSchema('hello', schema)).toEqual([])
      expect(validateAgainstSchema(123, schema)).toEqual([])
      expect(validateAgainstSchema(true, schema)).toHaveLength(1)
    })

    it('validates anyOf', () => {
      const schema: JsonSchema = {
        anyOf: [{ type: 'string', minLength: 5 }, { type: 'number', minimum: 10 }],
      }
      expect(validateAgainstSchema('hello', schema)).toEqual([])
      expect(validateAgainstSchema(15, schema)).toEqual([])
      expect(validateAgainstSchema('hi', schema)).toHaveLength(1)
    })

    it('validates allOf', () => {
      const schema: JsonSchema = {
        allOf: [{ type: 'object', required: ['name'] }, { type: 'object', required: ['email'] }],
      }
      expect(validateAgainstSchema({ name: 'John', email: 'john@example.com' }, schema)).toEqual([])
      expect(validateAgainstSchema({ name: 'John' }, schema)).toHaveLength(1)
    })
  })

  describe('complex event schema', () => {
    it('validates a typical event schema', () => {
      const eventSchema: JsonSchema = {
        type: 'object',
        required: ['type', 'ts', 'data'],
        properties: {
          type: { type: 'string', pattern: '^[a-z]+\\.[a-z]+$' },
          ts: { type: 'string', format: 'date-time' },
          data: {
            type: 'object',
            properties: {
              userId: { type: 'string' },
              action: { enum: ['create', 'update', 'delete'] },
            },
            required: ['userId', 'action'],
          },
        },
      }

      const validEvent = {
        type: 'user.created',
        ts: '2024-01-15T10:30:00Z',
        data: {
          userId: 'user-123',
          action: 'create',
        },
      }

      expect(validateAgainstSchema(validEvent, eventSchema)).toEqual([])

      // Missing required field
      const missingType = { ts: '2024-01-15T10:30:00Z', data: { userId: 'user-123', action: 'create' } }
      expect(validateAgainstSchema(missingType, eventSchema)).toHaveLength(1)

      // Invalid type pattern
      const invalidType = { ...validEvent, type: 'invalid' }
      expect(validateAgainstSchema(invalidType, eventSchema)).toHaveLength(1)

      // Invalid enum value
      const invalidAction = { ...validEvent, data: { ...validEvent.data, action: 'invalid' } }
      expect(validateAgainstSchema(invalidAction, eventSchema)).toHaveLength(1)
    })

    it('validates webhook event schema', () => {
      const webhookSchema: JsonSchema = {
        type: 'object',
        required: ['type', 'ts', 'provider', 'payload'],
        properties: {
          type: { type: 'string', pattern: '^webhook\\.' },
          ts: { type: 'string', format: 'date-time' },
          provider: { enum: ['github', 'stripe', 'slack'] },
          payload: { type: 'object' },
          signature: { type: 'string' },
        },
      }

      const validWebhook = {
        type: 'webhook.received',
        ts: '2024-01-15T10:30:00Z',
        provider: 'github',
        payload: { event: 'push' },
      }

      expect(validateAgainstSchema(validWebhook, webhookSchema)).toEqual([])

      // Invalid provider
      const invalidProvider = { ...validWebhook, provider: 'unknown' }
      expect(validateAgainstSchema(invalidProvider, webhookSchema)).toHaveLength(1)
    })
  })

  describe('error path tracking', () => {
    it('tracks nested paths correctly', () => {
      const schema: JsonSchema = {
        type: 'object',
        properties: {
          user: {
            type: 'object',
            properties: {
              profile: {
                type: 'object',
                properties: {
                  email: { type: 'string', format: 'email' },
                },
              },
            },
          },
        },
      }

      const invalid = {
        user: {
          profile: {
            email: 'not-an-email',
          },
        },
      }

      const errors = validateAgainstSchema(invalid, schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.path).toBe('user.profile.email')
    })

    it('tracks array item paths correctly', () => {
      const schema: JsonSchema = {
        type: 'object',
        properties: {
          items: {
            type: 'array',
            items: {
              type: 'object',
              required: ['id'],
            },
          },
        },
      }

      const invalid = {
        items: [{ id: 1 }, {}, { id: 3 }],
      }

      const errors = validateAgainstSchema(invalid, schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.path).toBe('items[1].id')
    })
  })

  describe('ReDoS prevention', () => {
    it('rejects patterns with nested quantifiers', () => {
      const schema: JsonSchema = {
        type: 'string',
        pattern: '(a+)+',  // Classic ReDoS pattern
      }

      const errors = validateAgainstSchema('aaaa', schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.keyword).toBe('pattern')
      expect(errors[0]?.params?.error).toBe('pattern_unsafe')
    })

    it('rejects patterns with overlapping alternations', () => {
      const schema: JsonSchema = {
        type: 'string',
        pattern: '(a|aa)+',  // ReDoS via overlapping alternation
      }

      const errors = validateAgainstSchema('aaaa', schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.params?.error).toBe('pattern_unsafe')
    })

    it('rejects patterns exceeding max length', () => {
      const schema: JsonSchema = {
        type: 'string',
        pattern: 'a'.repeat(300),  // Exceeds 256 char limit
      }

      const errors = validateAgainstSchema('a', schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.keyword).toBe('pattern')
    })

    it('accepts safe patterns and validates correctly', () => {
      const schema: JsonSchema = {
        type: 'string',
        pattern: '^[a-z]+$',  // Safe pattern
      }

      // Should pass
      expect(validateAgainstSchema('hello', schema)).toEqual([])

      // Should fail (not matching)
      const errors = validateAgainstSchema('Hello123', schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.keyword).toBe('pattern')
      expect(errors[0]?.params?.error).toBeUndefined()  // Not a safety error
    })

    it('handles safe complex patterns', () => {
      const schema: JsonSchema = {
        type: 'string',
        // Safe email-like pattern
        pattern: '^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$',
      }

      expect(validateAgainstSchema('test@example.com', schema)).toEqual([])
      expect(validateAgainstSchema('invalid', schema)).toHaveLength(1)
    })

    it('rejects excessively long input', () => {
      const schema: JsonSchema = {
        type: 'string',
        pattern: '^[a-z]+$',
      }

      // Input exceeding MAX_INPUT_LENGTH (10000)
      const longInput = 'a'.repeat(15000)
      const errors = validateAgainstSchema(longInput, schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.keyword).toBe('pattern')
      expect(errors[0]?.params?.error).toContain('maximum length')
    })

    it('handles patterns with reasonable quantifiers', () => {
      const schema: JsonSchema = {
        type: 'string',
        pattern: '^\\d{1,10}$',  // Safe bounded quantifier
      }

      expect(validateAgainstSchema('12345', schema)).toEqual([])
      expect(validateAgainstSchema('12345678901', schema)).toHaveLength(1)  // Too long
    })

    it('rejects patterns with excessive quantifiers', () => {
      const schema: JsonSchema = {
        type: 'string',
        pattern: 'a{1000}',  // Exceeds MAX_QUANTIFIER (100)
      }

      const errors = validateAgainstSchema('a', schema)
      expect(errors).toHaveLength(1)
      expect(errors[0]?.params?.error).toBe('pattern_unsafe')
    })

    it('protects against known evil regex patterns', () => {
      // Collection of known ReDoS patterns
      const evilPatterns = [
        '(a+)+$',           // Exponential
        '([a-zA-Z]+)*',     // Exponential
        '(a|aa)+',          // Overlapping
        '(.*)+',            // Catastrophic
        '(\\s*,)+',         // Polynomial
      ]

      for (const pattern of evilPatterns) {
        const schema: JsonSchema = { type: 'string', pattern }
        const errors = validateAgainstSchema('test', schema)
        expect(errors.length).toBeGreaterThan(0)
        expect(errors[0]?.params?.error).toBe('pattern_unsafe')
      }
    })
  })
})

/**
 * SchemaRegistryDO Sharding Strategy Documentation
 *
 * The SchemaRegistryDO is designed with namespace-based sharding:
 *
 * 1. Each namespace gets its own DO instance via idFromName(namespace)
 * 2. Schema operations are isolated per namespace
 * 3. Cross-namespace queries require separate RPC calls
 *
 * Usage pattern:
 * ```typescript
 * // Get registry for specific namespace
 * const registryId = env.SCHEMA_REGISTRY.idFromName(namespace)
 * const registry = env.SCHEMA_REGISTRY.get(registryId)
 *
 * // Validate with optional fallback to default namespace
 * const result = await registry.validateEvent(event, namespace)
 * if (!result.schemaFound && namespace !== 'default') {
 *   const defaultId = env.SCHEMA_REGISTRY.idFromName('default')
 *   const defaultRegistry = env.SCHEMA_REGISTRY.get(defaultId)
 *   const fallbackResult = await defaultRegistry.validateEvent(event, 'default')
 *   // Use fallbackResult
 * }
 * ```
 *
 * This sharding approach ensures:
 * - No single DO becomes a bottleneck
 * - Namespaces are isolated (multi-tenant friendly)
 * - SQLite storage scales well within each shard
 * - Pattern: O(namespaces) DOs, each with O(schemas_per_namespace) rows
 */
describe('SchemaRegistryDO sharding (design documentation)', () => {
  it('documents that SchemaRegistryDO is sharded by namespace', () => {
    // This is a documentation test - it describes the sharding design
    // Actual integration tests require a DO runtime environment

    // The key insight is that idFromName(namespace) creates isolated DOs:
    // - idFromName('acme') -> one DO instance
    // - idFromName('beta') -> different DO instance
    // - idFromName('default') -> fallback DO instance

    // Each DO has its own SQLite database, so:
    // - Schemas registered in 'acme' are not visible in 'beta'
    // - Cross-namespace lookup must be done at the caller level
    // - This provides natural tenant isolation

    expect(true).toBe(true) // Design documentation test
  })

  it('documents cross-namespace fallback pattern', () => {
    // When validating events, the recommended pattern is:
    // 1. Try the specific namespace's registry first
    // 2. If no schema found and namespace !== 'default', try 'default'
    //
    // This allows global schemas in 'default' while namespace-specific
    // schemas override them.

    // Example pseudo-code for the caller:
    /*
      async function validateWithFallback(
        event: Event,
        namespace: string,
        env: Env
      ): Promise<ValidationResult> {
        // Try specific namespace
        const registry = env.SCHEMA_REGISTRY.get(
          env.SCHEMA_REGISTRY.idFromName(namespace)
        )
        const result = await registry.validateEvent(event, namespace)

        // If no schema found and not default, try default namespace
        if (!result.schemaFound && namespace !== 'default') {
          const defaultRegistry = env.SCHEMA_REGISTRY.get(
            env.SCHEMA_REGISTRY.idFromName('default')
          )
          return defaultRegistry.validateEvent(event, 'default')
        }

        return result
      }
    */

    expect(true).toBe(true) // Design documentation test
  })
})
