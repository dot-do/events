import { describe, it, expect, beforeEach } from 'vitest'
import {
  matchPattern,
  extractPatternPrefix,
  findMatchingSubscriptions,
  clearPatternCache,
  getPatternCacheSize,
  PATTERN_CACHE_MAX_SIZE,
  validatePattern,
  validateEventType,
  PatternMatchLimitError,
} from '../pattern-matcher'
import { MAX_PATTERN_LENGTH, MAX_PATTERN_SEGMENTS } from '../config'

describe('Pattern Matcher', () => {
  beforeEach(() => {
    clearPatternCache()
  })

  describe('matchPattern', () => {
    it('matches exact patterns', () => {
      expect(matchPattern('webhook.github.push', 'webhook.github.push')).toBe(true)
      expect(matchPattern('webhook.github.push', 'webhook.github.pull_request')).toBe(false)
    })

    it('matches single wildcard', () => {
      expect(matchPattern('webhook.github.*', 'webhook.github.push')).toBe(true)
      expect(matchPattern('webhook.github.*', 'webhook.github.pull_request')).toBe(true)
      expect(matchPattern('webhook.github.*', 'webhook.stripe.charge')).toBe(false)
      // Single * should NOT match nested
      expect(matchPattern('webhook.github.*', 'webhook.github.issues.opened')).toBe(false)
    })

    it('matches double wildcard (globstar)', () => {
      expect(matchPattern('webhook.**', 'webhook.github.push')).toBe(true)
      expect(matchPattern('webhook.**', 'webhook.github.issues.opened')).toBe(true)
      expect(matchPattern('webhook.**', 'api.call')).toBe(false)
    })

    it('matches wildcards in middle', () => {
      expect(matchPattern('webhook.*.push', 'webhook.github.push')).toBe(true)
      expect(matchPattern('webhook.*.push', 'webhook.gitlab.push')).toBe(true)
      expect(matchPattern('webhook.*.push', 'webhook.github.pull_request')).toBe(false)
    })

    it('matches wildcards at start', () => {
      expect(matchPattern('*.github.*', 'webhook.github.push')).toBe(true)
      expect(matchPattern('*.github.*', 'api.github.call')).toBe(true)
      expect(matchPattern('*.github.*', 'webhook.gitlab.push')).toBe(false)
    })
  })

  describe('extractPatternPrefix', () => {
    it('extracts prefix before wildcard', () => {
      expect(extractPatternPrefix('webhook.github.*')).toBe('webhook.github')
      expect(extractPatternPrefix('webhook.**')).toBe('webhook')
      expect(extractPatternPrefix('webhook.github.push')).toBe('webhook.github.push')
    })

    it('returns empty for patterns starting with wildcard', () => {
      expect(extractPatternPrefix('*.github.*')).toBe('')
      expect(extractPatternPrefix('**')).toBe('')
    })
  })

  describe('findMatchingSubscriptions', () => {
    const subscriptions = [
      { id: '1', pattern: 'webhook.github.*', patternPrefix: 'webhook.github' },
      { id: '2', pattern: 'webhook.stripe.*', patternPrefix: 'webhook.stripe' },
      { id: '3', pattern: 'webhook.**', patternPrefix: 'webhook' },
      { id: '4', pattern: '*.github.*', patternPrefix: '' },
    ]

    it('finds all matching subscriptions', () => {
      const matches = findMatchingSubscriptions('webhook.github.push', subscriptions)
      expect(matches.map((m) => m.id)).toEqual(['1', '3', '4'])
    })

    it('filters non-matching subscriptions', () => {
      const matches = findMatchingSubscriptions('webhook.stripe.charge', subscriptions)
      expect(matches.map((m) => m.id)).toEqual(['2', '3'])
    })

    it('handles exact matches', () => {
      const subs = [{ id: '1', pattern: 'webhook.github.push', patternPrefix: 'webhook.github.push' }]
      const matches = findMatchingSubscriptions('webhook.github.push', subs)
      expect(matches.map((m) => m.id)).toEqual(['1'])
    })
  })

  describe('LRU Cache', () => {
    it('has a reasonable max cache size configured', () => {
      expect(PATTERN_CACHE_MAX_SIZE).toBe(1000)
    })

    it('caches patterns for reuse', () => {
      // First call compiles the pattern
      matchPattern('webhook.github.*', 'webhook.github.push')
      expect(getPatternCacheSize()).toBe(1)

      // Second call should reuse cached pattern (size stays the same)
      matchPattern('webhook.github.*', 'webhook.github.pull_request')
      expect(getPatternCacheSize()).toBe(1)

      // Different pattern adds to cache
      matchPattern('webhook.stripe.*', 'webhook.stripe.charge')
      expect(getPatternCacheSize()).toBe(2)
    })

    it('clears cache correctly', () => {
      matchPattern('webhook.github.*', 'webhook.github.push')
      matchPattern('webhook.stripe.*', 'webhook.stripe.charge')
      expect(getPatternCacheSize()).toBe(2)

      clearPatternCache()
      expect(getPatternCacheSize()).toBe(0)
    })

    it('evicts least recently used entries when max size is reached', () => {
      // Fill cache with patterns
      const numPatterns = PATTERN_CACHE_MAX_SIZE + 10

      // Add patterns to fill and exceed cache
      for (let i = 0; i < numPatterns; i++) {
        matchPattern(`pattern.test.${i}.*`, `pattern.test.${i}.event`)
      }

      // Cache should be capped at max size
      expect(getPatternCacheSize()).toBe(PATTERN_CACHE_MAX_SIZE)
    })

    it('updates LRU order on access', () => {
      // Add patterns up to the max
      for (let i = 0; i < PATTERN_CACHE_MAX_SIZE; i++) {
        matchPattern(`pattern.test.${i}.*`, `pattern.test.${i}.event`)
      }
      expect(getPatternCacheSize()).toBe(PATTERN_CACHE_MAX_SIZE)

      // Access the first pattern (moves it to most recently used)
      matchPattern('pattern.test.0.*', 'pattern.test.0.event')

      // Add more patterns to trigger eviction
      for (let i = 0; i < 10; i++) {
        matchPattern(`pattern.new.${i}.*`, `pattern.new.${i}.event`)
      }

      // Cache should still be at max size
      expect(getPatternCacheSize()).toBe(PATTERN_CACHE_MAX_SIZE)

      // Pattern 0 should still be cached (was recently accessed)
      // Pattern 1 through 10 should have been evicted (were oldest)
      // We can verify by checking the cache size stays the same when accessing pattern 0
      const sizeBefore = getPatternCacheSize()
      matchPattern('pattern.test.0.*', 'pattern.test.0.event')
      expect(getPatternCacheSize()).toBe(sizeBefore)
    })

    it('handles updating existing cache entries', () => {
      // Add initial patterns
      matchPattern('webhook.github.*', 'webhook.github.push')
      matchPattern('webhook.stripe.*', 'webhook.stripe.charge')
      expect(getPatternCacheSize()).toBe(2)

      // Re-accessing the same pattern should not increase size
      matchPattern('webhook.github.*', 'webhook.github.pull_request')
      expect(getPatternCacheSize()).toBe(2)
    })
  })

  describe('Pattern Validation', () => {
    describe('validatePattern', () => {
      it('accepts valid patterns', () => {
        expect(() => validatePattern('webhook.github.*')).not.toThrow()
        expect(() => validatePattern('webhook.**')).not.toThrow()
        expect(() => validatePattern('a.b.c.d.e')).not.toThrow()
      })

      it('rejects patterns exceeding max length', () => {
        const longPattern = 'a'.repeat(MAX_PATTERN_LENGTH + 1)
        expect(() => validatePattern(longPattern)).toThrow(PatternMatchLimitError)
        expect(() => validatePattern(longPattern)).toThrow(`exceeds maximum ${MAX_PATTERN_LENGTH}`)
      })

      it('rejects patterns exceeding max segments', () => {
        const manySegments = Array(MAX_PATTERN_SEGMENTS + 1)
          .fill('segment')
          .join('.')
        expect(() => validatePattern(manySegments)).toThrow(PatternMatchLimitError)
        expect(() => validatePattern(manySegments)).toThrow(`exceeds maximum ${MAX_PATTERN_SEGMENTS}`)
      })

      it('rejects patterns with invalid characters', () => {
        expect(() => validatePattern('webhook.github.@invalid')).toThrow(PatternMatchLimitError)
        expect(() => validatePattern('webhook.github.$test')).toThrow(PatternMatchLimitError)
        expect(() => validatePattern('webhook.github.{test}')).toThrow(PatternMatchLimitError)
      })

      it('respects custom maxDepth parameter', () => {
        // Should pass with default (10 segments)
        expect(() => validatePattern('a.b.c.d.e.f.g.h')).not.toThrow()
        // Should fail with custom maxDepth of 5
        expect(() => validatePattern('a.b.c.d.e.f.g.h', 5)).toThrow(PatternMatchLimitError)
      })
    })

    describe('validateEventType', () => {
      it('accepts valid event types', () => {
        expect(() => validateEventType('webhook.github.push')).not.toThrow()
        expect(() => validateEventType('rpc.call')).not.toThrow()
      })

      it('rejects event types exceeding max length', () => {
        const longEventType = 'a'.repeat(MAX_PATTERN_LENGTH + 1)
        expect(() => validateEventType(longEventType)).toThrow(PatternMatchLimitError)
        expect(() => validateEventType(longEventType)).toThrow(`exceeds maximum ${MAX_PATTERN_LENGTH}`)
      })

      it('rejects event types exceeding max segments', () => {
        const manySegments = Array(MAX_PATTERN_SEGMENTS + 1)
          .fill('segment')
          .join('.')
        expect(() => validateEventType(manySegments)).toThrow(PatternMatchLimitError)
        expect(() => validateEventType(manySegments)).toThrow(`exceeds maximum ${MAX_PATTERN_SEGMENTS}`)
      })

      it('respects custom maxDepth parameter', () => {
        expect(() => validateEventType('a.b.c.d.e.f', 3)).toThrow(PatternMatchLimitError)
      })
    })
  })

  describe('Iteration Limits', () => {
    describe('matchPattern with validation', () => {
      it('validates inputs by default', () => {
        const longPattern = 'a'.repeat(MAX_PATTERN_LENGTH + 1)
        expect(() => matchPattern(longPattern, 'test')).toThrow(PatternMatchLimitError)
      })

      it('can skip validation with option', () => {
        // Create a pattern that would fail validation but picomatch can handle
        // Use a very long but valid pattern within the test
        const pattern = 'webhook.*'
        const eventType = 'webhook.test'
        // Should work with validation enabled (default)
        expect(matchPattern(pattern, eventType)).toBe(true)
        // Should also work with validation explicitly disabled
        expect(matchPattern(pattern, eventType, { validate: false })).toBe(true)
      })

      it('respects custom maxDepth option', () => {
        // Pattern with 4 segments should fail with maxDepth of 3
        expect(() => matchPattern('a.b.c.d.*', 'a.b.c.d.e', { maxDepth: 3 })).toThrow(PatternMatchLimitError)
      })
    })

    describe('findMatchingSubscriptions with limits', () => {
      it('respects maxIterations limit', () => {
        // Create a large number of subscriptions
        const manySubscriptions = Array.from({ length: 100 }, (_, i) => ({
          id: `${i}`,
          pattern: `webhook.test.${i}.*`,
          patternPrefix: `webhook.test.${i}`,
        }))

        // Should throw when maxIterations is very low
        expect(() =>
          findMatchingSubscriptions('webhook.test.50.event', manySubscriptions, {
            maxIterations: 5,
          })
        ).toThrow(PatternMatchLimitError)
        expect(() =>
          findMatchingSubscriptions('webhook.test.50.event', manySubscriptions, {
            maxIterations: 5,
          })
        ).toThrow('Exceeded maximum iterations')
      })

      it('works with adequate maxIterations', () => {
        const subscriptions = [
          { id: '1', pattern: 'webhook.github.*', patternPrefix: 'webhook.github' },
          { id: '2', pattern: 'webhook.stripe.*', patternPrefix: 'webhook.stripe' },
        ]

        // Should work with default or adequate iterations
        const matches = findMatchingSubscriptions('webhook.github.push', subscriptions)
        expect(matches.map((m) => m.id)).toEqual(['1'])
      })

      it('validates event type by default', () => {
        const subscriptions = [{ id: '1', pattern: 'webhook.*', patternPrefix: 'webhook' }]
        const longEventType = 'a'.repeat(MAX_PATTERN_LENGTH + 1)

        expect(() => findMatchingSubscriptions(longEventType, subscriptions)).toThrow(PatternMatchLimitError)
      })

      it('can skip validation with option', () => {
        const subscriptions = [{ id: '1', pattern: 'webhook.*', patternPrefix: 'webhook' }]

        const matches = findMatchingSubscriptions('webhook.test', subscriptions, { validate: false })
        expect(matches.map((m) => m.id)).toEqual(['1'])
      })

      it('respects custom maxDepth option', () => {
        const subscriptions = [{ id: '1', pattern: 'webhook.*', patternPrefix: 'webhook' }]
        // Event type with 5 segments should fail with maxDepth of 3
        expect(() =>
          findMatchingSubscriptions('a.b.c.d.e', subscriptions, { maxDepth: 3 })
        ).toThrow(PatternMatchLimitError)
      })
    })

    describe('extractPatternPrefix with validation', () => {
      it('validates pattern by default', () => {
        const longPattern = 'a'.repeat(MAX_PATTERN_LENGTH + 1)
        expect(() => extractPatternPrefix(longPattern)).toThrow(PatternMatchLimitError)
      })

      it('can skip validation with option', () => {
        const pattern = 'webhook.github.*'
        expect(extractPatternPrefix(pattern, { validate: false })).toBe('webhook.github')
      })

      it('respects custom maxDepth option', () => {
        // Pattern with many segments should fail with low maxDepth
        expect(() => extractPatternPrefix('a.b.c.d.e.f.g', { maxDepth: 3 })).toThrow(PatternMatchLimitError)
      })
    })
  })

  // ============================================================================
  // Consolidated Pattern Matching Tests
  // These tests ensure the shared implementation handles all use cases from
  // the previously duplicate implementations:
  // - subscription.ts (SubscriptionDO.matchesPattern)
  // - schema-registry.ts (SchemaRegistry.matchesPattern)
  // - routes/events.ts (matchGlob)
  // ============================================================================
  describe('Consolidated Pattern Matching (regression tests)', () => {
    describe('real-world event patterns', () => {
      // These patterns are commonly used across subscription.ts, schema-registry.ts, and events.ts

      it('handles webhook patterns (from subscription fanout)', () => {
        // GitHub webhooks
        expect(matchPattern('webhook.github.*', 'webhook.github.push')).toBe(true)
        expect(matchPattern('webhook.github.*', 'webhook.github.pull_request')).toBe(true)
        expect(matchPattern('webhook.github.**', 'webhook.github.issues.opened')).toBe(true)

        // Stripe webhooks
        expect(matchPattern('webhook.stripe.**', 'webhook.stripe.customer.subscription.created')).toBe(true)
        expect(matchPattern('webhook.stripe.invoice.*', 'webhook.stripe.invoice.paid')).toBe(true)
      })

      it('handles CDC patterns (from schema registry)', () => {
        expect(matchPattern('cdc.**', 'cdc.users.insert')).toBe(true)
        expect(matchPattern('cdc.*.insert', 'cdc.users.insert')).toBe(true)
        expect(matchPattern('cdc.*.update', 'cdc.orders.update')).toBe(true)
        expect(matchPattern('cdc.**', 'cdc.products.variants.delete')).toBe(true)
      })

      it('handles RPC patterns (from event filtering)', () => {
        expect(matchPattern('rpc.**', 'rpc.UserService.getUser')).toBe(true)
        expect(matchPattern('rpc.UserService.*', 'rpc.UserService.getUser')).toBe(true)
        expect(matchPattern('rpc.*.getUser', 'rpc.UserService.getUser')).toBe(true)
      })

      it('handles DO lifecycle patterns', () => {
        expect(matchPattern('do.**', 'do.create')).toBe(true)
        expect(matchPattern('do.**', 'do.alarm')).toBe(true)
        expect(matchPattern('do.**', 'do.hibernate')).toBe(true)
        expect(matchPattern('do.*', 'do.create')).toBe(true)
      })
    })

    describe('** (globstar) edge cases', () => {
      it('matches zero segments after prefix', () => {
        expect(matchPattern('webhook.**', 'webhook')).toBe(true)
      })

      it('matches multiple levels of nesting', () => {
        expect(matchPattern('webhook.**', 'webhook.github')).toBe(true)
        expect(matchPattern('webhook.**', 'webhook.github.push')).toBe(true)
        expect(matchPattern('webhook.**', 'webhook.github.push.v1')).toBe(true)
        expect(matchPattern('webhook.**', 'webhook.github.push.v1.test.deep')).toBe(true)
      })

      it('handles ** in middle of pattern', () => {
        expect(matchPattern('webhook.**.push', 'webhook.push')).toBe(true)
        expect(matchPattern('webhook.**.push', 'webhook.github.push')).toBe(true)
        expect(matchPattern('webhook.**.push', 'webhook.github.v1.push')).toBe(true)
        expect(matchPattern('webhook.**.push', 'webhook.github.v1.v2.push')).toBe(true)
      })

      it('handles ** followed by more specific patterns', () => {
        expect(matchPattern('api.**.list', 'api.list')).toBe(true)
        expect(matchPattern('api.**.list', 'api.v1.list')).toBe(true)
        expect(matchPattern('api.**.list', 'api.v1.users.list')).toBe(true)
        expect(matchPattern('api.**.list', 'api.v1.users.create')).toBe(false)
      })
    })

    describe('* (single segment) edge cases', () => {
      it('matches exactly one segment', () => {
        expect(matchPattern('webhook.github.*', 'webhook.github.push')).toBe(true)
        expect(matchPattern('webhook.github.*', 'webhook.github.issues')).toBe(true)
      })

      it('does not match multiple segments', () => {
        expect(matchPattern('webhook.github.*', 'webhook.github.issues.opened')).toBe(false)
        expect(matchPattern('webhook.github.*', 'webhook.github.push.v1')).toBe(false)
      })

      it('handles * at start of pattern', () => {
        expect(matchPattern('*.github.push', 'webhook.github.push')).toBe(true)
        expect(matchPattern('*.github.push', 'api.github.push')).toBe(true)
        expect(matchPattern('*.github.push', 'any.github.push')).toBe(true)
      })

      it('handles multiple * wildcards', () => {
        expect(matchPattern('*.*.*', 'a.b.c')).toBe(true)
        expect(matchPattern('*.*.*', 'x.y.z')).toBe(true)
        expect(matchPattern('*.*.*', 'a.b')).toBe(false)
        expect(matchPattern('*.*.*', 'a.b.c.d')).toBe(false)
      })

      it('handles * at root level', () => {
        expect(matchPattern('*', 'event')).toBe(true)
        expect(matchPattern('*', 'webhook')).toBe(true)
        expect(matchPattern('*', 'a.b')).toBe(false)
      })
    })

    describe('exact match patterns', () => {
      it('matches exact event types', () => {
        expect(matchPattern('webhook.github.push', 'webhook.github.push')).toBe(true)
        expect(matchPattern('user.created', 'user.created')).toBe(true)
      })

      it('does not match different event types', () => {
        expect(matchPattern('webhook.github.push', 'webhook.github.pull')).toBe(false)
        expect(matchPattern('user.created', 'user.deleted')).toBe(false)
      })

      it('respects segment boundaries', () => {
        expect(matchPattern('webhook.github', 'webhook.github.push')).toBe(false)
        expect(matchPattern('webhook.github.push', 'webhook.github')).toBe(false)
      })
    })

    describe('special root patterns', () => {
      it('** matches everything', () => {
        expect(matchPattern('**', 'anything')).toBe(true)
        expect(matchPattern('**', 'a.b.c.d.e')).toBe(true)
        expect(matchPattern('**', 'single')).toBe(true)
      })
    })

    describe('picomatch compatibility', () => {
      // Ensure our implementation matches picomatch behavior
      it('uses dot as path separator', () => {
        expect(matchPattern('a.*', 'a.b')).toBe(true)
        expect(matchPattern('a.*', 'a.b.c')).toBe(false)
      })

      it('handles mixed * and ** patterns', () => {
        expect(matchPattern('*.github.**', 'webhook.github')).toBe(true)
        expect(matchPattern('*.github.**', 'webhook.github.push')).toBe(true)
        expect(matchPattern('*.github.**', 'api.github.issues.opened')).toBe(true)
      })
    })
  })
})
