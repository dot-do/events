import { describe, it, expect, beforeEach } from 'vitest'
import { matchPattern, extractPatternPrefix, findMatchingSubscriptions, clearPatternCache } from '../pattern-matcher'

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
})
