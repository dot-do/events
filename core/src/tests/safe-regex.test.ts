/**
 * Safe Regex Tests - ReDoS Prevention
 *
 * Tests the safe-regex module to ensure it properly prevents
 * Regular Expression Denial of Service (ReDoS) attacks.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import {
  analyzeRegexSafety,
  createSafeRegex,
  safeRegexTest,
  safeRegexMatch,
  validateSchemaPattern,
  getCachedSafeRegex,
  clearRegexCache,
  MAX_PATTERN_LENGTH,
  MAX_INPUT_LENGTH,
  MAX_QUANTIFIER,
} from '../safe-regex'

describe('safe-regex', () => {
  beforeEach(() => {
    clearRegexCache()
  })

  describe('analyzeRegexSafety', () => {
    describe('valid patterns', () => {
      it('accepts simple literal patterns', () => {
        expect(analyzeRegexSafety('hello')).toEqual({ safe: true, pattern: 'hello' })
        expect(analyzeRegexSafety('user-123')).toEqual({ safe: true, pattern: 'user-123' })
      })

      it('accepts common safe patterns', () => {
        // Email-like pattern (simplified)
        expect(analyzeRegexSafety('^[a-z]+@[a-z]+\\.[a-z]+$').safe).toBe(true)
        // UUID pattern
        expect(analyzeRegexSafety('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$').safe).toBe(true)
        // Date pattern
        expect(analyzeRegexSafety('^\\d{4}-\\d{2}-\\d{2}$').safe).toBe(true)
        // Simple word pattern
        expect(analyzeRegexSafety('^[a-zA-Z]+$').safe).toBe(true)
      })

      it('accepts patterns with reasonable quantifiers', () => {
        expect(analyzeRegexSafety('a{1,10}').safe).toBe(true)
        expect(analyzeRegexSafety('\\d{1,50}').safe).toBe(true)
        expect(analyzeRegexSafety(`a{1,${MAX_QUANTIFIER}}`).safe).toBe(true)
      })

      it('accepts patterns with alternation without quantifiers', () => {
        expect(analyzeRegexSafety('(foo|bar|baz)').safe).toBe(true)
        expect(analyzeRegexSafety('^(create|update|delete)$').safe).toBe(true)
      })

      it('accepts patterns with limited nesting', () => {
        expect(analyzeRegexSafety('((a)(b))').safe).toBe(true)
        expect(analyzeRegexSafety('(a(b(c)))').safe).toBe(true)
      })
    })

    describe('dangerous patterns - ReDoS prevention', () => {
      it('rejects nested quantifiers (classic ReDoS)', () => {
        // These patterns can cause exponential backtracking
        expect(analyzeRegexSafety('(a+)+').safe).toBe(false)
        expect(analyzeRegexSafety('(a*)*').safe).toBe(false)
        expect(analyzeRegexSafety('(a+)*').safe).toBe(false)
        expect(analyzeRegexSafety('(.*)+').safe).toBe(false)
        expect(analyzeRegexSafety('([a-z]+)+').safe).toBe(false)
      })

      it('rejects patterns with quantified groups containing quantifiers', () => {
        expect(analyzeRegexSafety('(a+b+)+').safe).toBe(false)
        expect(analyzeRegexSafety('(\\d+\\.\\d+)+').safe).toBe(false)
      })

      it('rejects patterns with overlapping alternations with quantifiers', () => {
        // These can cause ReDoS when matching fails
        expect(analyzeRegexSafety('(a|a)+').safe).toBe(false)
        expect(analyzeRegexSafety('(aa|a)+').safe).toBe(false)
        expect(analyzeRegexSafety('(a|aa)*').safe).toBe(false)
      })

      it('rejects consecutive quantifiers', () => {
        expect(analyzeRegexSafety('a++').safe).toBe(false)
        expect(analyzeRegexSafety('a*+').safe).toBe(false)
        expect(analyzeRegexSafety('a?*').safe).toBe(false)
      })

      it('rejects excessively large quantifiers', () => {
        expect(analyzeRegexSafety(`a{${MAX_QUANTIFIER + 1}}`).safe).toBe(false)
        expect(analyzeRegexSafety('a{1000}').safe).toBe(false)
        expect(analyzeRegexSafety('a{1,1000}').safe).toBe(false)
      })

      it('rejects patterns exceeding max length', () => {
        const longPattern = 'a'.repeat(MAX_PATTERN_LENGTH + 1)
        expect(analyzeRegexSafety(longPattern).safe).toBe(false)
        expect(analyzeRegexSafety(longPattern).reason).toContain('maximum length')
      })

      it('rejects deeply nested patterns', () => {
        // More than 5 levels of nesting
        expect(analyzeRegexSafety('((((((a))))))').safe).toBe(false)
        expect(analyzeRegexSafety('((((((a))))))').reason).toContain('nesting depth')
      })

      it('rejects unbalanced parentheses', () => {
        expect(analyzeRegexSafety('(a').safe).toBe(false)
        expect(analyzeRegexSafety('a)').safe).toBe(false)
        expect(analyzeRegexSafety('((a)').safe).toBe(false)
      })

      it('rejects empty patterns', () => {
        expect(analyzeRegexSafety('').safe).toBe(false)
        expect(analyzeRegexSafety('').reason).toContain('empty')
      })

      it('rejects invalid regex syntax', () => {
        expect(analyzeRegexSafety('[').safe).toBe(false)
        expect(analyzeRegexSafety('\\').safe).toBe(false)
        expect(analyzeRegexSafety('*').safe).toBe(false)
      })
    })

    describe('known ReDoS attack patterns', () => {
      it('rejects evil regex patterns', () => {
        // From OWASP ReDoS examples
        expect(analyzeRegexSafety('(a+)+$').safe).toBe(false)
        expect(analyzeRegexSafety('([a-zA-Z]+)*').safe).toBe(false)
        expect(analyzeRegexSafety('(a|aa)+').safe).toBe(false)
        expect(analyzeRegexSafety('(a|a?)+').safe).toBe(false)
        // Note: (.*a){x} is actually safe because {x} is not a valid quantifier
        // and wouldn't cause ReDoS. Testing real dangerous patterns instead:
        expect(analyzeRegexSafety('(.*a)+').safe).toBe(false)
      })

      it('rejects polynomial ReDoS patterns', () => {
        // These cause polynomial (O(n^2) or worse) backtracking
        expect(analyzeRegexSafety('(\\s*,)+').safe).toBe(false)
        expect(analyzeRegexSafety('([\\s\\S]*)+').safe).toBe(false)
      })
    })
  })

  describe('createSafeRegex', () => {
    it('creates regex for safe patterns', () => {
      const regex = createSafeRegex('^hello$')
      expect(regex).toBeInstanceOf(RegExp)
      expect(regex!.test('hello')).toBe(true)
      expect(regex!.test('world')).toBe(false)
    })

    it('returns null for unsafe patterns', () => {
      expect(createSafeRegex('(a+)+')).toBeNull()
      expect(createSafeRegex('(.*)*')).toBeNull()
    })

    it('supports flags', () => {
      const regex = createSafeRegex('^hello$', 'i')
      expect(regex).toBeInstanceOf(RegExp)
      expect(regex!.test('HELLO')).toBe(true)
    })
  })

  describe('safeRegexTest', () => {
    it('executes safe regex matches', () => {
      const regex = /^hello$/
      expect(safeRegexTest(regex, 'hello').matched).toBe(true)
      expect(safeRegexTest(regex, 'world').matched).toBe(false)
    })

    it('rejects input exceeding max length', () => {
      const regex = /test/
      const longInput = 'a'.repeat(MAX_INPUT_LENGTH + 1)
      const result = safeRegexTest(regex, longInput)
      expect(result.matched).toBe(false)
      expect(result.error).toContain('maximum length')
    })

    it('truncates input when option is set', () => {
      const regex = /^a+$/
      const longInput = 'a'.repeat(MAX_INPUT_LENGTH + 100)
      const result = safeRegexTest(regex, longInput, { truncateInput: true })
      expect(result.matched).toBe(true)
    })

    it('respects custom max input length', () => {
      const regex = /test/
      const input = 'a'.repeat(100)
      const result = safeRegexTest(regex, input, { maxInputLength: 50 })
      expect(result.matched).toBe(false)
      expect(result.error).toContain('maximum length')
    })
  })

  describe('safeRegexMatch', () => {
    it('validates and executes in one call', () => {
      expect(safeRegexMatch('^hello$', 'hello').matched).toBe(true)
      expect(safeRegexMatch('^hello$', 'world').matched).toBe(false)
    })

    it('rejects unsafe patterns', () => {
      const result = safeRegexMatch('(a+)+', 'aaa')
      expect(result.matched).toBe(false)
      expect(result.error).toContain('exponential')
    })

    it('rejects overly long input', () => {
      const result = safeRegexMatch('test', 'a'.repeat(MAX_INPUT_LENGTH + 1))
      expect(result.matched).toBe(false)
      expect(result.error).toContain('maximum length')
    })
  })

  describe('validateSchemaPattern', () => {
    it('validates safe patterns for schema use', () => {
      expect(validateSchemaPattern('^[a-z]+$').valid).toBe(true)
      expect(validateSchemaPattern('^\\d{4}-\\d{2}-\\d{2}$').valid).toBe(true)
    })

    it('rejects unsafe patterns', () => {
      const result = validateSchemaPattern('(a+)+')
      expect(result.valid).toBe(false)
      expect(result.error).toBeDefined()
    })
  })

  describe('getCachedSafeRegex', () => {
    it('caches safe regex instances', () => {
      const regex1 = getCachedSafeRegex('^hello$')
      const regex2 = getCachedSafeRegex('^hello$')
      expect(regex1).toBe(regex2) // Same instance from cache
    })

    it('caches with different flags separately', () => {
      const regex1 = getCachedSafeRegex('^hello$', 'i')
      const regex2 = getCachedSafeRegex('^hello$', 'g')
      expect(regex1).not.toBe(regex2)
    })

    it('returns null for unsafe patterns', () => {
      expect(getCachedSafeRegex('(a+)+')).toBeNull()
    })

    it('clears cache properly', () => {
      const regex1 = getCachedSafeRegex('^test$')
      clearRegexCache()
      const regex2 = getCachedSafeRegex('^test$')
      expect(regex1).not.toBe(regex2) // Different instance after cache clear
    })
  })

  describe('real-world ReDoS scenarios', () => {
    it('protects against classic email ReDoS', () => {
      // This email pattern is known to be vulnerable
      const badEmailPattern = '^([a-zA-Z0-9]+)+@'
      expect(analyzeRegexSafety(badEmailPattern).safe).toBe(false)

      // Safe alternative
      const safeEmailPattern = '^[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+$'
      expect(analyzeRegexSafety(safeEmailPattern).safe).toBe(true)
    })

    it('protects against URL path ReDoS', () => {
      // Vulnerable pattern
      const badPathPattern = '(/[a-z]+)+$'
      expect(analyzeRegexSafety(badPathPattern).safe).toBe(false)
    })

    it('accepts JSON Schema typical patterns', () => {
      // Patterns commonly used in JSON Schema
      expect(analyzeRegexSafety('^[a-z]+\\.[a-z]+$').safe).toBe(true) // Event type
      expect(analyzeRegexSafety('^webhook\\.').safe).toBe(true) // Prefix match
      expect(analyzeRegexSafety('^[A-Z][a-z]+$').safe).toBe(true) // Capitalized word
      expect(analyzeRegexSafety('^\\d{3}-\\d{2}-\\d{4}$').safe).toBe(true) // SSN format
    })
  })

  describe('performance verification', () => {
    it('does not hang on malicious input with previously vulnerable pattern', () => {
      // This test verifies our protection by ensuring the analysis completes quickly
      const maliciousPattern = '(a+)+'
      const start = performance.now()
      const result = analyzeRegexSafety(maliciousPattern)
      const elapsed = performance.now() - start

      expect(result.safe).toBe(false)
      expect(elapsed).toBeLessThan(100) // Analysis should be fast
    })

    it('safe regex test completes quickly even with adversarial input', () => {
      // Use a safe pattern
      const safePattern = '^[a-z]+$'
      const regex = createSafeRegex(safePattern)
      expect(regex).not.toBeNull()

      // Input designed to cause backtracking (but safe pattern won't)
      const input = 'a'.repeat(1000) + '!'

      const start = performance.now()
      const result = safeRegexTest(regex!, input)
      const elapsed = performance.now() - start

      expect(result.matched).toBe(false)
      expect(elapsed).toBeLessThan(100)
    })
  })
})
