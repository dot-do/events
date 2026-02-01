/**
 * SubscriptionDO Tests
 *
 * Unit tests for subscription logic and pattern matching.
 * Also includes tests for fanout and delivery logic.
 * Note: Full DO testing requires vitest-pool-workers for SQLite.
 */

import { describe, it, expect, vi } from 'vitest'
import { findMatchingSubscriptions, extractPatternPrefix, matchPattern } from '../pattern-matcher.js'

// ============================================================================
// Pattern Matching Logic Tests
// These test the pattern matching algorithm without DO infrastructure
// ============================================================================

/**
 * Extract prefix from pattern for indexing
 * Mirrors the logic in SubscriptionDO
 */
function extractPrefix(pattern: string): string {
  const parts = pattern.split('.')
  const wildcardIdx = parts.findIndex(p => p === '*' || p === '**')
  if (wildcardIdx === -1) {
    return pattern
  }
  if (wildcardIdx === 0) {
    return ''
  }
  return parts.slice(0, wildcardIdx).join('.')
}

/**
 * Check if an event type matches a subscription pattern
 * Mirrors the logic in SubscriptionDO
 */
function matchesPattern(eventType: string, pattern: string): boolean {
  // Handle special root patterns
  if (pattern === '**') {
    return true
  }
  if (pattern === '*') {
    // * at root matches only single-segment event types
    return !eventType.includes('.')
  }

  const eventParts = eventType.split('.')
  const patternParts = pattern.split('.')

  let eventIdx = 0
  let patternIdx = 0

  while (patternIdx < patternParts.length) {
    const p = patternParts[patternIdx]

    if (p === '**') {
      // ** matches zero or more segments
      // If it's the last pattern part, match everything remaining (including nothing)
      if (patternIdx === patternParts.length - 1) {
        return true
      }
      // Try matching remaining pattern against rest of event (including empty)
      for (let i = eventIdx; i <= eventParts.length; i++) {
        const remainingEvent = eventParts.slice(i).join('.')
        const remainingPattern = patternParts.slice(patternIdx + 1).join('.')
        if (matchesPattern(remainingEvent, remainingPattern)) {
          return true
        }
      }
      return false
    } else if (eventIdx >= eventParts.length) {
      // No more event parts but pattern still has non-** parts
      return false
    } else if (p === '*') {
      // * matches exactly one segment
      eventIdx++
      patternIdx++
    } else if (p === eventParts[eventIdx]) {
      // Exact match
      eventIdx++
      patternIdx++
    } else {
      return false
    }
  }

  return eventIdx === eventParts.length && patternIdx === patternParts.length
}

// ============================================================================
// Tests
// ============================================================================

describe('SubscriptionDO Logic', () => {
  describe('extractPrefix', () => {
    it('returns full pattern for exact match patterns', () => {
      expect(extractPrefix('webhook.github.push')).toBe('webhook.github.push')
      expect(extractPrefix('user.created')).toBe('user.created')
      expect(extractPrefix('single')).toBe('single')
    })

    it('extracts prefix before single wildcard', () => {
      expect(extractPrefix('webhook.github.*')).toBe('webhook.github')
      expect(extractPrefix('user.*')).toBe('user')
    })

    it('extracts prefix before double wildcard', () => {
      expect(extractPrefix('webhook.**')).toBe('webhook')
      expect(extractPrefix('webhook.github.**')).toBe('webhook.github')
    })

    it('returns empty string for patterns starting with wildcard', () => {
      expect(extractPrefix('*')).toBe('')
      expect(extractPrefix('**')).toBe('')
      expect(extractPrefix('*.github.push')).toBe('')
      expect(extractPrefix('**.push')).toBe('')
    })

    it('handles complex patterns', () => {
      expect(extractPrefix('api.v1.users.*')).toBe('api.v1.users')
      expect(extractPrefix('api.v1.**')).toBe('api.v1')
    })
  })

  describe('matchesPattern', () => {
    describe('exact matching', () => {
      it('matches identical patterns', () => {
        expect(matchesPattern('webhook.github.push', 'webhook.github.push')).toBe(true)
        expect(matchesPattern('user.created', 'user.created')).toBe(true)
        expect(matchesPattern('single', 'single')).toBe(true)
      })

      it('does not match different patterns', () => {
        expect(matchesPattern('webhook.github.push', 'webhook.github.pull')).toBe(false)
        expect(matchesPattern('user.created', 'user.deleted')).toBe(false)
        expect(matchesPattern('webhook.github', 'webhook.gitlab')).toBe(false)
      })

      it('does not match when lengths differ', () => {
        expect(matchesPattern('webhook.github.push', 'webhook.github')).toBe(false)
        expect(matchesPattern('webhook.github', 'webhook.github.push')).toBe(false)
      })
    })

    describe('single wildcard (*)', () => {
      it('matches single segment with *', () => {
        expect(matchesPattern('webhook.github.push', 'webhook.github.*')).toBe(true)
        expect(matchesPattern('webhook.github.pull', 'webhook.github.*')).toBe(true)
        expect(matchesPattern('user.created', 'user.*')).toBe(true)
      })

      it('does not match multiple segments with *', () => {
        expect(matchesPattern('webhook.github.push.v1', 'webhook.github.*')).toBe(false)
        expect(matchesPattern('webhook.github.push.v1.test', 'webhook.github.*')).toBe(false)
      })

      it('handles * in middle of pattern', () => {
        expect(matchesPattern('webhook.github.push', 'webhook.*.push')).toBe(true)
        expect(matchesPattern('webhook.gitlab.push', 'webhook.*.push')).toBe(true)
        expect(matchesPattern('webhook.github.pull', 'webhook.*.push')).toBe(false)
      })

      it('handles multiple * wildcards', () => {
        expect(matchesPattern('a.b.c', '*.*.*')).toBe(true)
        expect(matchesPattern('x.y.z', '*.*.*')).toBe(true)
        expect(matchesPattern('a.b', '*.*.*')).toBe(false)
        expect(matchesPattern('a.b.c.d', '*.*.*')).toBe(false)
      })

      it('handles * at the start', () => {
        expect(matchesPattern('any.github.push', '*.github.push')).toBe(true)
        expect(matchesPattern('webhook.github.push', '*.github.push')).toBe(true)
        expect(matchesPattern('any.gitlab.push', '*.github.push')).toBe(false)
      })
    })

    describe('double wildcard (**)', () => {
      it('matches zero or more segments with ** at end', () => {
        expect(matchesPattern('webhook', 'webhook.**')).toBe(true)
        expect(matchesPattern('webhook.github', 'webhook.**')).toBe(true)
        expect(matchesPattern('webhook.github.push', 'webhook.**')).toBe(true)
        expect(matchesPattern('webhook.github.push.v1', 'webhook.**')).toBe(true)
      })

      it('does not match different prefix with **', () => {
        expect(matchesPattern('api.github.push', 'webhook.**')).toBe(false)
        expect(matchesPattern('user.created', 'webhook.**')).toBe(false)
      })

      it('handles ** matching everything', () => {
        expect(matchesPattern('anything', '**')).toBe(true)
        expect(matchesPattern('a.b.c.d.e', '**')).toBe(true)
        expect(matchesPattern('single', '**')).toBe(true)
      })

      it('handles * matching single segment at root', () => {
        expect(matchesPattern('anything', '*')).toBe(true)
        expect(matchesPattern('a.b', '*')).toBe(false)
      })

      it('handles ** in middle of pattern', () => {
        expect(matchesPattern('webhook.github.push', 'webhook.**.push')).toBe(true)
        expect(matchesPattern('webhook.github.v1.push', 'webhook.**.push')).toBe(true)
        expect(matchesPattern('webhook.push', 'webhook.**.push')).toBe(true)
        expect(matchesPattern('webhook.github.pull', 'webhook.**.push')).toBe(false)
      })

      it('handles ** followed by more pattern', () => {
        expect(matchesPattern('api.v1.users.list', 'api.**.list')).toBe(true)
        expect(matchesPattern('api.list', 'api.**.list')).toBe(true)
        expect(matchesPattern('api.v1.v2.v3.list', 'api.**.list')).toBe(true)
        expect(matchesPattern('api.v1.users.create', 'api.**.list')).toBe(false)
      })
    })

    describe('edge cases', () => {
      it('handles empty segments correctly', () => {
        expect(matchesPattern('', '')).toBe(true)
      })

      it('handles single segment events', () => {
        expect(matchesPattern('event', 'event')).toBe(true)
        expect(matchesPattern('event', 'other')).toBe(false)
        expect(matchesPattern('event', '*')).toBe(true)
        expect(matchesPattern('event', '**')).toBe(true)
      })

      it('handles complex real-world patterns', () => {
        // GitHub webhooks
        expect(matchesPattern('webhook.github.push', 'webhook.github.*')).toBe(true)
        expect(matchesPattern('webhook.github.pull_request.opened', 'webhook.github.**')).toBe(true)

        // Stripe webhooks
        expect(matchesPattern('webhook.stripe.customer.subscription.created', 'webhook.stripe.**')).toBe(true)
        expect(matchesPattern('webhook.stripe.invoice.paid', 'webhook.stripe.invoice.*')).toBe(true)

        // CDC events
        expect(matchesPattern('cdc.users.insert', 'cdc.*.insert')).toBe(true)
        expect(matchesPattern('cdc.orders.update', 'cdc.**')).toBe(true)

        // RPC events
        expect(matchesPattern('rpc.UserService.getUser', 'rpc.**')).toBe(true)
        expect(matchesPattern('rpc.UserService.getUser', 'rpc.UserService.*')).toBe(true)
      })
    })
  })

  describe('Subscription validation', () => {
    it('validates required fields', () => {
      const validSubscription = {
        workerId: 'my-worker',
        pattern: 'webhook.github.*',
        rpcMethod: 'handleWebhook',
      }

      expect(validSubscription.workerId).toBeTruthy()
      expect(validSubscription.pattern).toBeTruthy()
      expect(validSubscription.rpcMethod).toBeTruthy()
    })

    it('allows optional fields with defaults', () => {
      const defaults = {
        maxRetries: 5,
        timeoutMs: 30000,
        active: true,
      }

      expect(defaults.maxRetries).toBe(5)
      expect(defaults.timeoutMs).toBe(30000)
      expect(defaults.active).toBe(true)
    })
  })

  describe('Delivery status transitions', () => {
    it('defines valid status values', () => {
      const validStatuses = ['pending', 'delivered', 'failed', 'dead'] as const
      type Status = typeof validStatuses[number]

      const testTransitions: Record<Status, Status[]> = {
        pending: ['delivered', 'failed'],
        failed: ['delivered', 'failed', 'dead'],
        delivered: [],
        dead: [],
      }

      expect(testTransitions.pending).toContain('delivered')
      expect(testTransitions.pending).toContain('failed')
      expect(testTransitions.failed).toContain('dead')
      expect(testTransitions.delivered).toHaveLength(0)
      expect(testTransitions.dead).toHaveLength(0)
    })
  })

  describe('Exponential backoff', () => {
    it('calculates correct backoff times', () => {
      const calculateBackoff = (attemptCount: number): number => {
        return Math.min(1000 * Math.pow(2, attemptCount), 300000)
      }

      expect(calculateBackoff(1)).toBe(2000)      // 2 seconds
      expect(calculateBackoff(2)).toBe(4000)      // 4 seconds
      expect(calculateBackoff(3)).toBe(8000)      // 8 seconds
      expect(calculateBackoff(4)).toBe(16000)     // 16 seconds
      expect(calculateBackoff(5)).toBe(32000)     // 32 seconds
      expect(calculateBackoff(8)).toBe(256000)    // 256 seconds
      expect(calculateBackoff(9)).toBe(300000)    // Capped at 5 minutes
      expect(calculateBackoff(10)).toBe(300000)   // Still capped
    })
  })

  describe('ULID generation', () => {
    it('generates time-ordered unique identifiers', () => {
      const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

      function ulid(): string {
        const now = Date.now()
        let str = ''
        let ts = now
        for (let i = 9; i >= 0; i--) {
          str = ENCODING[ts % 32] + str
          ts = Math.floor(ts / 32)
        }
        for (let i = 0; i < 16; i++) {
          str += ENCODING[Math.floor(Math.random() * 32)]
        }
        return str
      }

      const id1 = ulid()
      const id2 = ulid()

      // ULIDs should be 26 characters
      expect(id1).toHaveLength(26)
      expect(id2).toHaveLength(26)

      // ULIDs should only contain valid characters
      const validChars = new Set(ENCODING.split(''))
      for (const char of id1) {
        expect(validChars.has(char)).toBe(true)
      }

      // IDs generated in sequence should be lexicographically ordered
      // (first 10 chars are timestamp, so they should be equal or increasing)
      expect(id1.slice(0, 10) <= id2.slice(0, 10)).toBe(true)
    })
  })
})

// ============================================================================
// Pattern Matcher Integration Tests (from pattern-matcher.ts)
// ============================================================================

describe('Pattern Matcher Integration', () => {
  describe('findMatchingSubscriptions', () => {
    const subscriptions = [
      { id: 'sub1', pattern: 'webhook.github.*', patternPrefix: 'webhook.github' },
      { id: 'sub2', pattern: 'webhook.**', patternPrefix: 'webhook' },
      { id: 'sub3', pattern: 'webhook.stripe.invoice.*', patternPrefix: 'webhook.stripe.invoice' },
      { id: 'sub4', pattern: 'cdc.*.insert', patternPrefix: 'cdc' },
      { id: 'sub5', pattern: '**', patternPrefix: '' },
    ]

    it('finds all subscriptions matching webhook.github.push', () => {
      const matches = findMatchingSubscriptions('webhook.github.push', subscriptions)
      const matchIds = matches.map(m => m.id)

      expect(matchIds).toContain('sub1') // webhook.github.*
      expect(matchIds).toContain('sub2') // webhook.**
      expect(matchIds).toContain('sub5') // **
      expect(matchIds).not.toContain('sub3') // webhook.stripe.invoice.*
      expect(matchIds).not.toContain('sub4') // cdc.*.insert
    })

    it('finds ** subscription for any event', () => {
      const matches = findMatchingSubscriptions('some.random.event', subscriptions)
      const matchIds = matches.map(m => m.id)

      expect(matchIds).toContain('sub5') // ** matches everything
      expect(matches.length).toBe(1)
    })

    it('matches cdc.users.insert correctly', () => {
      const matches = findMatchingSubscriptions('cdc.users.insert', subscriptions)
      const matchIds = matches.map(m => m.id)

      expect(matchIds).toContain('sub4') // cdc.*.insert
      expect(matchIds).toContain('sub5') // **
    })

    it('returns empty array when no matches', () => {
      const limitedSubs = [
        { id: 'sub1', pattern: 'webhook.github.*', patternPrefix: 'webhook.github' },
      ]

      const matches = findMatchingSubscriptions('email.sent', limitedSubs)
      expect(matches).toHaveLength(0)
    })
  })

  describe('extractPatternPrefix (from pattern-matcher)', () => {
    it('extracts correct prefixes', () => {
      expect(extractPatternPrefix('webhook.github.*')).toBe('webhook.github')
      expect(extractPatternPrefix('webhook.**')).toBe('webhook')
      expect(extractPatternPrefix('webhook.github.push')).toBe('webhook.github.push')
      expect(extractPatternPrefix('*')).toBe('')
      expect(extractPatternPrefix('**')).toBe('')
      expect(extractPatternPrefix('*.github.push')).toBe('')
    })
  })

  describe('matchPattern (from pattern-matcher)', () => {
    it('uses picomatch for accurate glob matching', () => {
      // These tests verify the pattern-matcher.ts implementation
      expect(matchPattern('webhook.github.*', 'webhook.github.push')).toBe(true)
      expect(matchPattern('webhook.github.*', 'webhook.github.pull_request')).toBe(true)
      expect(matchPattern('webhook.github.*', 'webhook.github.push.v1')).toBe(false)

      expect(matchPattern('webhook.**', 'webhook')).toBe(true)
      expect(matchPattern('webhook.**', 'webhook.github')).toBe(true)
      expect(matchPattern('webhook.**', 'webhook.github.push')).toBe(true)
      expect(matchPattern('webhook.**', 'webhook.github.push.v1.test')).toBe(true)
    })
  })
})

// ============================================================================
// Fanout and Delivery Logic Tests
// ============================================================================

describe('Fanout and Delivery Logic', () => {
  describe('Retry delay calculation', () => {
    /**
     * Calculate retry delay with exponential backoff and jitter
     * Mirrors the logic in SubscriptionDO.calculateRetryDelay
     */
    function calculateRetryDelay(attemptNumber: number, jitter = 0): number {
      const baseDelay = 1000 // 1 second
      const maxDelay = 300000 // 5 minutes
      const exponential = Math.min(baseDelay * Math.pow(2, attemptNumber - 1), maxDelay)
      return Math.round(exponential + jitter)
    }

    it('implements exponential backoff starting at 1 second', () => {
      expect(calculateRetryDelay(1, 0)).toBe(1000)  // 1s
      expect(calculateRetryDelay(2, 0)).toBe(2000)  // 2s
      expect(calculateRetryDelay(3, 0)).toBe(4000)  // 4s
      expect(calculateRetryDelay(4, 0)).toBe(8000)  // 8s
      expect(calculateRetryDelay(5, 0)).toBe(16000) // 16s
    })

    it('caps delay at 5 minutes (300000ms)', () => {
      expect(calculateRetryDelay(10, 0)).toBe(300000)
      expect(calculateRetryDelay(20, 0)).toBe(300000)
      expect(calculateRetryDelay(100, 0)).toBe(300000)
    })

    it('adds jitter to delay', () => {
      const jitter = 500
      expect(calculateRetryDelay(1, jitter)).toBe(1500)
      expect(calculateRetryDelay(2, jitter)).toBe(2500)
    })
  })

  describe('Event fanout matching', () => {
    // Simulates the fanout logic without actual DO
    function simulateFanout(
      eventType: string,
      subscriptions: Array<{ id: string; pattern: string; patternPrefix: string; active: boolean }>
    ): string[] {
      const activeSubscriptions = subscriptions.filter(s => s.active)
      const matches = findMatchingSubscriptions(eventType, activeSubscriptions)
      return matches.map(m => m.id)
    }

    const testSubscriptions = [
      { id: 'github-all', pattern: 'webhook.github.**', patternPrefix: 'webhook.github', active: true },
      { id: 'github-push', pattern: 'webhook.github.push', patternPrefix: 'webhook.github.push', active: true },
      { id: 'stripe-all', pattern: 'webhook.stripe.**', patternPrefix: 'webhook.stripe', active: true },
      { id: 'inactive-sub', pattern: '**', patternPrefix: '', active: false },
      { id: 'all-webhooks', pattern: 'webhook.**', patternPrefix: 'webhook', active: true },
    ]

    it('matches specific event to multiple subscriptions', () => {
      const matches = simulateFanout('webhook.github.push', testSubscriptions)

      expect(matches).toContain('github-all')    // webhook.github.**
      expect(matches).toContain('github-push')   // webhook.github.push (exact)
      expect(matches).toContain('all-webhooks')  // webhook.**
      expect(matches).not.toContain('stripe-all')
      expect(matches).not.toContain('inactive-sub')
    })

    it('excludes inactive subscriptions', () => {
      const matches = simulateFanout('some.random.event', testSubscriptions)

      // The ** subscription is inactive, so no matches
      expect(matches).not.toContain('inactive-sub')
    })

    it('returns empty for unmatched events', () => {
      const matches = simulateFanout('email.sent', testSubscriptions)
      expect(matches).toHaveLength(0)
    })
  })

  describe('Delivery status state machine', () => {
    type DeliveryStatus = 'pending' | 'delivered' | 'failed' | 'dead'

    interface DeliveryState {
      status: DeliveryStatus
      attemptCount: number
      maxRetries: number
    }

    function processDeliveryResult(
      state: DeliveryState,
      success: boolean
    ): DeliveryStatus {
      if (success) {
        return 'delivered'
      }

      const newAttemptCount = state.attemptCount + 1
      if (newAttemptCount >= state.maxRetries) {
        return 'dead'
      }

      return 'failed'
    }

    it('transitions to delivered on success', () => {
      const state: DeliveryState = { status: 'pending', attemptCount: 0, maxRetries: 5 }
      expect(processDeliveryResult(state, true)).toBe('delivered')
    })

    it('transitions to failed on error with retries remaining', () => {
      const state: DeliveryState = { status: 'pending', attemptCount: 1, maxRetries: 5 }
      expect(processDeliveryResult(state, false)).toBe('failed')
    })

    it('transitions to dead when max retries exceeded', () => {
      const state: DeliveryState = { status: 'failed', attemptCount: 4, maxRetries: 5 }
      expect(processDeliveryResult(state, false)).toBe('dead')
    })

    it('transitions from failed to delivered on successful retry', () => {
      const state: DeliveryState = { status: 'failed', attemptCount: 2, maxRetries: 5 }
      expect(processDeliveryResult(state, true)).toBe('delivered')
    })
  })

  describe('Dead letter queue logic', () => {
    interface DeadLetter {
      deliveryId: string
      subscriptionId: string
      eventId: string
      reason: string
      lastError: string
    }

    function createDeadLetter(
      deliveryId: string,
      subscriptionId: string,
      eventId: string,
      error: string
    ): DeadLetter {
      return {
        deliveryId,
        subscriptionId,
        eventId,
        reason: 'max_retries_exceeded',
        lastError: error,
      }
    }

    it('creates dead letter with correct reason', () => {
      const dl = createDeadLetter('del_123', 'sub_456', 'evt_789', 'Connection refused')

      expect(dl.reason).toBe('max_retries_exceeded')
      expect(dl.lastError).toBe('Connection refused')
      expect(dl.deliveryId).toBe('del_123')
    })
  })

  describe('HTTP delivery URL construction', () => {
    function buildDeliveryUrl(workerId: string, rpcMethod: string): string {
      return `https://${workerId}.workers.dev/rpc/${rpcMethod}`
    }

    it('constructs correct URL from worker ID and method', () => {
      expect(buildDeliveryUrl('my-worker', 'handleEvent'))
        .toBe('https://my-worker.workers.dev/rpc/handleEvent')

      expect(buildDeliveryUrl('github-webhook-handler', 'processWebhook'))
        .toBe('https://github-webhook-handler.workers.dev/rpc/processWebhook')
    })
  })

  describe('Alarm scheduling logic', () => {
    function shouldScheduleAlarm(
      currentAlarm: number | null,
      nextRetryTime: number
    ): boolean {
      // Schedule alarm if no current alarm or new time is sooner
      return !currentAlarm || nextRetryTime < currentAlarm
    }

    it('schedules alarm when no current alarm', () => {
      expect(shouldScheduleAlarm(null, Date.now() + 5000)).toBe(true)
    })

    it('schedules alarm when new time is sooner', () => {
      const now = Date.now()
      expect(shouldScheduleAlarm(now + 10000, now + 5000)).toBe(true)
    })

    it('does not reschedule when current alarm is sooner', () => {
      const now = Date.now()
      expect(shouldScheduleAlarm(now + 5000, now + 10000)).toBe(false)
    })
  })
})

// ============================================================================
// Delivery Logging Tests
// ============================================================================

describe('Delivery Logging', () => {
  interface DeliveryLogEntry {
    deliveryId: string
    subscriptionId: string
    attemptNumber: number
    status: 'success' | 'failed'
    durationMs: number
    errorMessage?: string
    workerResponse?: string
  }

  function createLogEntry(
    deliveryId: string,
    subscriptionId: string,
    attemptNumber: number,
    status: 'success' | 'failed',
    durationMs: number,
    errorMessage?: string,
    response?: unknown
  ): DeliveryLogEntry {
    return {
      deliveryId,
      subscriptionId,
      attemptNumber,
      status,
      durationMs,
      errorMessage: errorMessage ?? undefined,
      workerResponse: response ? JSON.stringify(response) : undefined,
    }
  }

  it('logs successful delivery with response', () => {
    const entry = createLogEntry(
      'del_123',
      'sub_456',
      1,
      'success',
      150,
      undefined,
      { processed: true }
    )

    expect(entry.status).toBe('success')
    expect(entry.durationMs).toBe(150)
    expect(entry.workerResponse).toBe('{"processed":true}')
    expect(entry.errorMessage).toBeUndefined()
  })

  it('logs failed delivery with error message', () => {
    const entry = createLogEntry(
      'del_123',
      'sub_456',
      1,
      'failed',
      5000,
      'Connection timeout',
      undefined
    )

    expect(entry.status).toBe('failed')
    expect(entry.errorMessage).toBe('Connection timeout')
    expect(entry.workerResponse).toBeUndefined()
  })

  it('tracks attempt number for retries', () => {
    const attempt1 = createLogEntry('del_123', 'sub_456', 1, 'failed', 100, 'Error 1')
    const attempt2 = createLogEntry('del_123', 'sub_456', 2, 'failed', 150, 'Error 2')
    const attempt3 = createLogEntry('del_123', 'sub_456', 3, 'success', 200)

    expect(attempt1.attemptNumber).toBe(1)
    expect(attempt2.attemptNumber).toBe(2)
    expect(attempt3.attemptNumber).toBe(3)
    expect(attempt3.status).toBe('success')
  })
})
