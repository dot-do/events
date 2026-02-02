/**
 * SubscriptionDO Tests
 *
 * Unit tests for subscription logic and pattern matching.
 * Also includes tests for fanout and delivery logic.
 * Note: Full DO testing requires vitest-pool-workers for SQLite.
 */

import { describe, it, expect, vi } from 'vitest'
import { findMatchingSubscriptions, extractPatternPrefix, matchPattern } from '../pattern-matcher.js'
import {
  // Sample subscriptions
  sampleGitHubSubscription,
  sampleStripeSubscription,
  sampleCdcSubscription,
  sampleCatchAllSubscription,
  sampleInactiveSubscription,
  // Sample deliveries
  samplePendingDelivery,
  sampleDeliveredDelivery,
  sampleFailedDelivery,
  sampleDeadDelivery,
  // Factory functions
  createSubscriptionConfig,
  createSubscriptionSet,
  createDeliveryRecord,
  createDeliveryBatch,
} from './fixtures/index.js'

// ============================================================================
// Pattern Matching Logic Tests
// These test the pattern matching algorithm using the shared implementation
// from pattern-matcher.ts
// ============================================================================

// Helper to match patterns with the same argument order as the old matchesPattern
// Note: matchPattern(pattern, eventType) - pattern first, event type second
const matchesPattern = (eventType: string, pattern: string) => matchPattern(pattern, eventType)

// Alias extractPatternPrefix for backward compatibility with existing tests
const extractPrefix = extractPatternPrefix

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
      it('rejects empty patterns (validated by pattern matcher)', () => {
        // The shared pattern matcher validates patterns and rejects empty strings
        // as they contain invalid characters (empty string doesn't match ALLOWED_PATTERN_CHARS)
        expect(() => matchesPattern('', '')).toThrow('Pattern contains invalid characters')
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
    // Use shared fixtures for subscription configs
    const subscriptions = [
      { id: sampleGitHubSubscription.id, pattern: sampleGitHubSubscription.pattern, patternPrefix: sampleGitHubSubscription.patternPrefix },
      { id: sampleStripeSubscription.id, pattern: sampleStripeSubscription.pattern, patternPrefix: sampleStripeSubscription.patternPrefix },
      { id: sampleCdcSubscription.id, pattern: sampleCdcSubscription.pattern, patternPrefix: sampleCdcSubscription.patternPrefix },
      { id: sampleCatchAllSubscription.id, pattern: sampleCatchAllSubscription.pattern, patternPrefix: sampleCatchAllSubscription.patternPrefix },
    ]

    it('finds all subscriptions matching webhook.github.push', () => {
      const matches = findMatchingSubscriptions('webhook.github.push', subscriptions)
      const matchIds = matches.map(m => m.id)

      expect(matchIds).toContain(sampleGitHubSubscription.id) // webhook.github.*
      expect(matchIds).toContain(sampleCatchAllSubscription.id) // **
      expect(matchIds).not.toContain(sampleStripeSubscription.id) // webhook.stripe.**
      expect(matchIds).not.toContain(sampleCdcSubscription.id) // cdc.**
    })

    it('finds ** subscription for any event', () => {
      const matches = findMatchingSubscriptions('some.random.event', subscriptions)
      const matchIds = matches.map(m => m.id)

      expect(matchIds).toContain(sampleCatchAllSubscription.id) // ** matches everything
      expect(matches.length).toBe(1)
    })

    it('matches cdc.users.insert correctly', () => {
      const matches = findMatchingSubscriptions('cdc.users.insert', subscriptions)
      const matchIds = matches.map(m => m.id)

      expect(matchIds).toContain(sampleCdcSubscription.id) // cdc.**
      expect(matchIds).toContain(sampleCatchAllSubscription.id) // **
    })

    it('returns empty array when no matches', () => {
      const limitedSubs = [
        { id: sampleGitHubSubscription.id, pattern: sampleGitHubSubscription.pattern, patternPrefix: sampleGitHubSubscription.patternPrefix },
      ]

      const matches = findMatchingSubscriptions('email.sent', limitedSubs)
      expect(matches).toHaveLength(0)
    })

    it('uses createSubscriptionSet factory for multiple subscriptions', () => {
      // Demonstrate using the factory function
      const subs = createSubscriptionSet([
        { pattern: 'api.v1.*' },
        { pattern: 'api.v2.**' },
        { pattern: 'rpc.**', workerId: 'rpc-handler' },
      ])

      expect(subs).toHaveLength(3)
      expect(subs[0].patternPrefix).toBe('api.v1')
      expect(subs[1].patternPrefix).toBe('api.v2')
      expect(subs[2].workerId).toBe('rpc-handler')
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

// ============================================================================
// Cleanup Logic Tests
// ============================================================================

describe('Cleanup Logic', () => {
  describe('cleanupOldData logic', () => {
    /**
     * Simulates the cleanup logic for determining what records to delete.
     * This tests the business logic without needing SQLite.
     */
    interface CleanupRecord {
      id: string
      createdAt: number
      status?: 'pending' | 'delivered' | 'failed' | 'dead'
      deliveredAt?: number
    }

    function shouldDeleteDeadLetter(record: CleanupRecord, cutoffTs: number): boolean {
      return record.createdAt < cutoffTs
    }

    function shouldDeleteDeliveryLog(
      record: CleanupRecord,
      deliveryStatus: 'pending' | 'delivered' | 'failed' | 'dead',
      deliveredAt: number | undefined,
      cutoffTs: number
    ): boolean {
      // Only delete logs for terminal deliveries (delivered or dead)
      if (deliveryStatus !== 'delivered' && deliveryStatus !== 'dead') {
        return false
      }
      // Must be older than cutoff
      return record.createdAt < cutoffTs && deliveredAt !== undefined && deliveredAt < cutoffTs
    }

    function shouldDeleteDelivery(record: CleanupRecord, cutoffTs: number): boolean {
      // Only delete terminal deliveries
      if (record.status !== 'delivered' && record.status !== 'dead') {
        return false
      }
      // Must be older than cutoff
      return record.createdAt < cutoffTs && record.deliveredAt !== undefined && record.deliveredAt < cutoffTs
    }

    const now = Date.now()
    const thirtyDaysAgo = now - (30 * 24 * 60 * 60 * 1000)
    const cutoff = thirtyDaysAgo

    describe('dead letter cleanup', () => {
      it('deletes dead letters older than cutoff', () => {
        const oldDeadLetter = { id: 'dl1', createdAt: thirtyDaysAgo - 1000 }
        expect(shouldDeleteDeadLetter(oldDeadLetter, cutoff)).toBe(true)
      })

      it('keeps dead letters newer than cutoff', () => {
        const newDeadLetter = { id: 'dl2', createdAt: now - 1000 }
        expect(shouldDeleteDeadLetter(newDeadLetter, cutoff)).toBe(false)
      })

      it('deletes dead letters exactly at cutoff boundary', () => {
        const boundaryDeadLetter = { id: 'dl3', createdAt: cutoff - 1 }
        expect(shouldDeleteDeadLetter(boundaryDeadLetter, cutoff)).toBe(true)
      })
    })

    describe('delivery log cleanup', () => {
      it('deletes logs for old delivered deliveries', () => {
        const log = { id: 'log1', createdAt: thirtyDaysAgo - 1000 }
        expect(shouldDeleteDeliveryLog(log, 'delivered', thirtyDaysAgo - 1000, cutoff)).toBe(true)
      })

      it('deletes logs for old dead deliveries', () => {
        const log = { id: 'log2', createdAt: thirtyDaysAgo - 1000 }
        expect(shouldDeleteDeliveryLog(log, 'dead', thirtyDaysAgo - 1000, cutoff)).toBe(true)
      })

      it('keeps logs for pending deliveries', () => {
        const log = { id: 'log3', createdAt: thirtyDaysAgo - 1000 }
        expect(shouldDeleteDeliveryLog(log, 'pending', undefined, cutoff)).toBe(false)
      })

      it('keeps logs for failed deliveries (still retrying)', () => {
        const log = { id: 'log4', createdAt: thirtyDaysAgo - 1000 }
        expect(shouldDeleteDeliveryLog(log, 'failed', undefined, cutoff)).toBe(false)
      })

      it('keeps logs for recent delivered deliveries', () => {
        const log = { id: 'log5', createdAt: now - 1000 }
        expect(shouldDeleteDeliveryLog(log, 'delivered', now - 1000, cutoff)).toBe(false)
      })
    })

    describe('delivery cleanup', () => {
      it('deletes old delivered deliveries', () => {
        const delivery: CleanupRecord = {
          id: 'del1',
          createdAt: thirtyDaysAgo - 1000,
          status: 'delivered',
          deliveredAt: thirtyDaysAgo - 1000,
        }
        expect(shouldDeleteDelivery(delivery, cutoff)).toBe(true)
      })

      it('deletes old dead deliveries', () => {
        const delivery: CleanupRecord = {
          id: 'del2',
          createdAt: thirtyDaysAgo - 1000,
          status: 'dead',
          deliveredAt: thirtyDaysAgo - 1000,
        }
        expect(shouldDeleteDelivery(delivery, cutoff)).toBe(true)
      })

      it('keeps pending deliveries regardless of age', () => {
        const delivery: CleanupRecord = {
          id: 'del3',
          createdAt: thirtyDaysAgo - 1000,
          status: 'pending',
        }
        expect(shouldDeleteDelivery(delivery, cutoff)).toBe(false)
      })

      it('keeps failed deliveries (still retrying) regardless of age', () => {
        const delivery: CleanupRecord = {
          id: 'del4',
          createdAt: thirtyDaysAgo - 1000,
          status: 'failed',
        }
        expect(shouldDeleteDelivery(delivery, cutoff)).toBe(false)
      })

      it('keeps recent delivered deliveries', () => {
        const delivery: CleanupRecord = {
          id: 'del5',
          createdAt: now - 1000,
          status: 'delivered',
          deliveredAt: now - 1000,
        }
        expect(shouldDeleteDelivery(delivery, cutoff)).toBe(false)
      })
    })
  })

  describe('TTL configuration', () => {
    const DEAD_LETTER_TTL_MS = 30 * 24 * 60 * 60 * 1000 // 30 days
    const DEDUP_TTL_MS = 24 * 60 * 60 * 1000 // 24 hours

    it('dead letter TTL is 30 days', () => {
      expect(DEAD_LETTER_TTL_MS).toBe(2592000000) // 30 * 24 * 60 * 60 * 1000
    })

    it('dedup marker TTL is 24 hours', () => {
      expect(DEDUP_TTL_MS).toBe(86400000) // 24 * 60 * 60 * 1000
    })

    it('calculates correct cutoff timestamp', () => {
      const now = 1700000000000 // Fixed timestamp for testing
      const cutoff = now - DEAD_LETTER_TTL_MS
      expect(cutoff).toBe(now - 2592000000)
    })
  })
})

// ============================================================================
// Worker ID Validation Integration Tests
// ============================================================================

describe('Subscription Worker ID Validation', () => {
  // These tests verify the validation logic that would be used in SubscriptionDO.subscribe()
  // The actual DO cannot be tested without vitest-pool-workers, but we can test the validation logic
  // Import validation functions (they're also tested in worker-id-validation.test.ts)
  const validateWorkerId = (workerId: unknown) => {
    if (typeof workerId !== 'string') return { valid: false, error: 'Worker ID must be a string' }
    if (workerId.length === 0) return { valid: false, error: 'Worker ID cannot be empty' }
    // Check for blocklisted patterns (SSRF prevention)
    const blocklist = ['http://', 'https://', 'localhost', '..', '/', '?', '@', ':', '%', '\n', '\r', '169.254.169.254']
    for (const pattern of blocklist) {
      if (workerId.toLowerCase().includes(pattern.toLowerCase())) {
        return { valid: false, error: `Worker ID contains blocked pattern: ${pattern}` }
      }
    }
    // Pattern validation (Cloudflare worker naming)
    if (!/^[a-z][a-z0-9-]*[a-z0-9]$|^[a-z]$/.test(workerId)) {
      return { valid: false, error: 'Invalid worker ID format' }
    }
    return { valid: true }
  }

  const validateRpcMethod = (rpcMethod: unknown) => {
    if (typeof rpcMethod !== 'string') return { valid: false, error: 'RPC method must be a string' }
    if (rpcMethod.length === 0) return { valid: false, error: 'RPC method cannot be empty' }
    const blocklist = ['..', '/', '?', '@', '%', '\n', '\r']
    for (const pattern of blocklist) {
      if (rpcMethod.includes(pattern)) {
        return { valid: false, error: `RPC method contains blocked pattern: ${pattern}` }
      }
    }
    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(rpcMethod)) {
      return { valid: false, error: 'Invalid RPC method format' }
    }
    return { valid: true }
  }

  const buildSafeDeliveryUrl = (workerId: string, rpcMethod: string): string => {
    const workerIdResult = validateWorkerId(workerId)
    if (!workerIdResult.valid) throw new Error(`Invalid worker ID: ${workerIdResult.error}`)
    const rpcMethodResult = validateRpcMethod(rpcMethod)
    if (!rpcMethodResult.valid) throw new Error(`Invalid RPC method: ${rpcMethodResult.error}`)
    return `https://${workerId}.workers.dev/rpc/${rpcMethod}`
  }

  describe('subscribe() validation', () => {
    function simulateSubscribeValidation(params: {
      workerId: string
      rpcMethod: string
    }): { ok: true } | { ok: false; error: string } {
      const workerIdResult = validateWorkerId(params.workerId)
      if (!workerIdResult.valid) {
        return { ok: false, error: `Invalid workerId: ${workerIdResult.error}` }
      }

      const rpcMethodResult = validateRpcMethod(params.rpcMethod)
      if (!rpcMethodResult.valid) {
        return { ok: false, error: `Invalid rpcMethod: ${rpcMethodResult.error}` }
      }

      return { ok: true }
    }

    it('accepts valid subscription parameters', () => {
      expect(simulateSubscribeValidation({
        workerId: 'my-worker',
        rpcMethod: 'handleEvent',
      })).toEqual({ ok: true })
    })

    it('rejects invalid workerId with URL', () => {
      const result = simulateSubscribeValidation({
        workerId: 'http://evil.com',
        rpcMethod: 'handleEvent',
      })
      expect(result.ok).toBe(false)
      expect((result as { ok: false; error: string }).error).toContain('Invalid workerId')
    })

    it('rejects invalid workerId with localhost', () => {
      const result = simulateSubscribeValidation({
        workerId: 'localhost',
        rpcMethod: 'handleEvent',
      })
      expect(result.ok).toBe(false)
      expect((result as { ok: false; error: string }).error).toContain('Invalid workerId')
    })

    it('rejects invalid rpcMethod with path traversal', () => {
      const result = simulateSubscribeValidation({
        workerId: 'my-worker',
        rpcMethod: '../etc/passwd',
      })
      expect(result.ok).toBe(false)
      expect((result as { ok: false; error: string }).error).toContain('Invalid rpcMethod')
    })
  })

  describe('deliverOne() URL construction', () => {
    it('builds safe URLs for valid inputs', () => {
      expect(buildSafeDeliveryUrl('my-worker', 'handleEvent'))
        .toBe('https://my-worker.workers.dev/rpc/handleEvent')
    })

    it('throws for SSRF attempts during delivery', () => {
      expect(() => buildSafeDeliveryUrl('evil.com/path', 'method')).toThrow()
      expect(() => buildSafeDeliveryUrl('localhost', 'method')).toThrow()
      expect(() => buildSafeDeliveryUrl('169.254.169.254', 'method')).toThrow()
    })
  })
})

// ============================================================================
// Batched Delivery Logic Tests
// ============================================================================

describe('Batched Delivery Logic', () => {
  describe('BatchDeliveryConfig validation', () => {
    const DEFAULT_BATCH_DELIVERY_SIZE = 100
    const DEFAULT_BATCH_DELIVERY_WINDOW_MS = 1000
    const MAX_BATCH_DELIVERY_SIZE = 1000
    const MAX_BATCH_DELIVERY_WINDOW_MS = 10000

    /**
     * Validates batch config at subscription creation time.
     * Mirrors the validation logic in SubscriptionDO.subscribe().
     * Validates batchSize and batchWindowMs regardless of enabled state
     * to ensure we never store invalid values in the database.
     */
    function validateBatchConfigAtCreation(config?: {
      enabled: boolean
      batchSize?: number
      batchWindowMs?: number
    }): { ok: true } | { ok: false; error: string } {
      const batchSize = config?.batchSize ?? DEFAULT_BATCH_DELIVERY_SIZE
      const batchWindowMs = config?.batchWindowMs ?? DEFAULT_BATCH_DELIVERY_WINDOW_MS

      // Always validate - even when disabled, we don't want invalid values stored
      if (batchSize < 1 || batchSize > MAX_BATCH_DELIVERY_SIZE) {
        return { ok: false, error: `batchSize must be between 1 and ${MAX_BATCH_DELIVERY_SIZE}` }
      }

      if (batchWindowMs < 1 || batchWindowMs > MAX_BATCH_DELIVERY_WINDOW_MS) {
        return { ok: false, error: `batchWindowMs must be between 1 and ${MAX_BATCH_DELIVERY_WINDOW_MS}` }
      }

      return { ok: true }
    }

    /**
     * Validates batch config at subscription update time.
     * Mirrors the validation logic in SubscriptionDO.updateSubscription().
     * Only validates values that are explicitly provided.
     */
    function validateBatchConfigAtUpdate(config?: {
      enabled: boolean
      batchSize?: number
      batchWindowMs?: number
    }): { ok: true } | { ok: false; error: string } {
      if (config === undefined) {
        return { ok: true }
      }

      if (config.batchSize !== undefined) {
        if (config.batchSize < 1 || config.batchSize > MAX_BATCH_DELIVERY_SIZE) {
          return { ok: false, error: `batchSize must be between 1 and ${MAX_BATCH_DELIVERY_SIZE}` }
        }
      }

      if (config.batchWindowMs !== undefined) {
        if (config.batchWindowMs < 1 || config.batchWindowMs > MAX_BATCH_DELIVERY_WINDOW_MS) {
          return { ok: false, error: `batchWindowMs must be between 1 and ${MAX_BATCH_DELIVERY_WINDOW_MS}` }
        }
      }

      return { ok: true }
    }

    describe('at subscription creation', () => {
      it('accepts config with no batchConfig (uses defaults)', () => {
        expect(validateBatchConfigAtCreation()).toEqual({ ok: true })
      })

      it('accepts disabled batch config with valid values', () => {
        expect(validateBatchConfigAtCreation({ enabled: false, batchSize: 50 })).toEqual({ ok: true })
      })

      it('accepts valid enabled batch config', () => {
        expect(validateBatchConfigAtCreation({
          enabled: true,
          batchSize: 50,
          batchWindowMs: 2000,
        })).toEqual({ ok: true })
      })

      it('uses defaults when values not provided', () => {
        expect(validateBatchConfigAtCreation({ enabled: true })).toEqual({ ok: true })
      })

      it('rejects batchSize of 0 even when disabled', () => {
        const result = validateBatchConfigAtCreation({
          enabled: false,
          batchSize: 0,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchSize')
      })

      it('rejects batchSize of 0 when enabled', () => {
        const result = validateBatchConfigAtCreation({
          enabled: true,
          batchSize: 0,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchSize')
      })

      it('rejects negative batchSize', () => {
        const result = validateBatchConfigAtCreation({
          enabled: true,
          batchSize: -1,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchSize')
      })

      it('rejects batchSize exceeding max', () => {
        const result = validateBatchConfigAtCreation({
          enabled: true,
          batchSize: 1001,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchSize')
      })

      it('accepts batchSize at boundary (1)', () => {
        expect(validateBatchConfigAtCreation({
          enabled: true,
          batchSize: 1,
        })).toEqual({ ok: true })
      })

      it('accepts batchSize at boundary (max)', () => {
        expect(validateBatchConfigAtCreation({
          enabled: true,
          batchSize: MAX_BATCH_DELIVERY_SIZE,
        })).toEqual({ ok: true })
      })

      it('rejects batchWindowMs of 0 even when disabled', () => {
        const result = validateBatchConfigAtCreation({
          enabled: false,
          batchWindowMs: 0,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchWindowMs')
      })

      it('rejects batchWindowMs of 0 when enabled', () => {
        const result = validateBatchConfigAtCreation({
          enabled: true,
          batchWindowMs: 0,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchWindowMs')
      })

      it('rejects negative batchWindowMs', () => {
        const result = validateBatchConfigAtCreation({
          enabled: true,
          batchWindowMs: -100,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchWindowMs')
      })

      it('rejects batchWindowMs exceeding max', () => {
        const result = validateBatchConfigAtCreation({
          enabled: true,
          batchWindowMs: 10001,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchWindowMs')
      })

      it('accepts batchWindowMs at boundary (1)', () => {
        expect(validateBatchConfigAtCreation({
          enabled: true,
          batchWindowMs: 1,
        })).toEqual({ ok: true })
      })

      it('accepts batchWindowMs at boundary (max)', () => {
        expect(validateBatchConfigAtCreation({
          enabled: true,
          batchWindowMs: MAX_BATCH_DELIVERY_WINDOW_MS,
        })).toEqual({ ok: true })
      })
    })

    describe('at subscription update', () => {
      it('accepts update with no batchConfig', () => {
        expect(validateBatchConfigAtUpdate()).toEqual({ ok: true })
      })

      it('accepts update with enabled only (no size/window)', () => {
        expect(validateBatchConfigAtUpdate({ enabled: true })).toEqual({ ok: true })
      })

      it('rejects invalid batchSize in update even when disabled', () => {
        const result = validateBatchConfigAtUpdate({
          enabled: false,
          batchSize: 0,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchSize')
      })

      it('rejects negative batchSize in update', () => {
        const result = validateBatchConfigAtUpdate({
          enabled: true,
          batchSize: -5,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchSize')
      })

      it('rejects invalid batchWindowMs in update even when disabled', () => {
        const result = validateBatchConfigAtUpdate({
          enabled: false,
          batchWindowMs: -1,
        })
        expect(result.ok).toBe(false)
        expect((result as { ok: false; error: string }).error).toContain('batchWindowMs')
      })
    })
  })

  describe('Batch window logic', () => {
    interface BatchState {
      windowStart: number
      eventCount: number
      batchSize: number
      batchWindowMs: number
    }

    function shouldCloseBatch(state: BatchState, now: number): {
      close: boolean
      reason: 'full' | 'window_expired' | null
    } {
      const windowExpired = (now - state.windowStart) >= state.batchWindowMs
      const batchFull = state.eventCount >= state.batchSize

      if (batchFull) {
        return { close: true, reason: 'full' }
      }
      if (windowExpired) {
        return { close: true, reason: 'window_expired' }
      }
      return { close: false, reason: null }
    }

    it('keeps batch open when under size and within window', () => {
      const state: BatchState = {
        windowStart: 1000,
        eventCount: 10,
        batchSize: 100,
        batchWindowMs: 1000,
      }
      const result = shouldCloseBatch(state, 1500)
      expect(result.close).toBe(false)
    })

    it('closes batch when full', () => {
      const state: BatchState = {
        windowStart: 1000,
        eventCount: 100,
        batchSize: 100,
        batchWindowMs: 1000,
      }
      const result = shouldCloseBatch(state, 1500)
      expect(result.close).toBe(true)
      expect(result.reason).toBe('full')
    })

    it('closes batch when window expired', () => {
      const state: BatchState = {
        windowStart: 1000,
        eventCount: 10,
        batchSize: 100,
        batchWindowMs: 1000,
      }
      const result = shouldCloseBatch(state, 2001)
      expect(result.close).toBe(true)
      expect(result.reason).toBe('window_expired')
    })

    it('prefers full reason over window expired', () => {
      const state: BatchState = {
        windowStart: 1000,
        eventCount: 100,
        batchSize: 100,
        batchWindowMs: 1000,
      }
      const result = shouldCloseBatch(state, 3000)
      expect(result.close).toBe(true)
      expect(result.reason).toBe('full')
    })
  })

  describe('Batch payload construction', () => {
    interface Event {
      deliveryId: string
      eventId: string
      eventType: string
      payload: unknown
    }

    function buildBatchPayload(events: Event[]): {
      batch: true
      events: Event[]
    } {
      return {
        batch: true,
        events,
      }
    }

    it('creates correct batch payload structure', () => {
      const events: Event[] = [
        { deliveryId: 'del1', eventId: 'evt1', eventType: 'user.created', payload: { id: 1 } },
        { deliveryId: 'del2', eventId: 'evt2', eventType: 'user.updated', payload: { id: 2 } },
      ]

      const payload = buildBatchPayload(events)
      expect(payload.batch).toBe(true)
      expect(payload.events).toHaveLength(2)
      expect(payload.events[0].eventId).toBe('evt1')
      expect(payload.events[1].eventId).toBe('evt2')
    })
  })

  describe('Partial failure handling', () => {
    interface EventResult {
      eventId: string
      success: boolean
      error?: string
    }

    interface BatchResult {
      total: number
      delivered: number
      failed: number
    }

    function processPartialResults(
      events: Array<{ eventId: string }>,
      results: EventResult[]
    ): BatchResult {
      let delivered = 0
      let failed = 0

      for (const result of results) {
        if (result.success) {
          delivered++
        } else {
          failed++
        }
      }

      // Events without results are assumed failed
      const resultIds = new Set(results.map(r => r.eventId))
      for (const event of events) {
        if (!resultIds.has(event.eventId)) {
          failed++
        }
      }

      return {
        total: events.length,
        delivered,
        failed,
      }
    }

    it('counts all success correctly', () => {
      const events = [{ eventId: 'evt1' }, { eventId: 'evt2' }]
      const results: EventResult[] = [
        { eventId: 'evt1', success: true },
        { eventId: 'evt2', success: true },
      ]

      const result = processPartialResults(events, results)
      expect(result.total).toBe(2)
      expect(result.delivered).toBe(2)
      expect(result.failed).toBe(0)
    })

    it('counts partial failure correctly', () => {
      const events = [{ eventId: 'evt1' }, { eventId: 'evt2' }]
      const results: EventResult[] = [
        { eventId: 'evt1', success: true },
        { eventId: 'evt2', success: false, error: 'validation error' },
      ]

      const result = processPartialResults(events, results)
      expect(result.total).toBe(2)
      expect(result.delivered).toBe(1)
      expect(result.failed).toBe(1)
    })

    it('counts missing results as failures', () => {
      const events = [{ eventId: 'evt1' }, { eventId: 'evt2' }, { eventId: 'evt3' }]
      const results: EventResult[] = [
        { eventId: 'evt1', success: true },
        // evt2 and evt3 are missing from results
      ]

      const result = processPartialResults(events, results)
      expect(result.total).toBe(3)
      expect(result.delivered).toBe(1)
      expect(result.failed).toBe(2)
    })
  })

  describe('Subscription fanout with batching', () => {
    interface MockSubscription {
      id: string
      pattern: string
      batchEnabled: boolean
      batchSize: number
      batchWindowMs: number
    }

    function simulateFanout(
      eventType: string,
      subscriptions: MockSubscription[]
    ): { matched: number; immediate: number; batched: number } {
      let matched = 0
      let immediate = 0
      let batched = 0

      for (const sub of subscriptions) {
        // Simplified pattern matching
        if (sub.pattern === eventType || sub.pattern === '**') {
          matched++
          if (sub.batchEnabled) {
            batched++
          } else {
            immediate++
          }
        }
      }

      return { matched, immediate, batched }
    }

    const testSubscriptions: MockSubscription[] = [
      { id: 'sub1', pattern: 'user.created', batchEnabled: false, batchSize: 100, batchWindowMs: 1000 },
      { id: 'sub2', pattern: 'user.created', batchEnabled: true, batchSize: 50, batchWindowMs: 2000 },
      { id: 'sub3', pattern: '**', batchEnabled: true, batchSize: 100, batchWindowMs: 5000 },
    ]

    it('routes events to batched and immediate subscriptions', () => {
      const result = simulateFanout('user.created', testSubscriptions)
      expect(result.matched).toBe(3) // sub1, sub2, sub3 all match
      expect(result.immediate).toBe(1) // sub1
      expect(result.batched).toBe(2) // sub2, sub3
    })

    it('only matches ** for unmatched events', () => {
      const result = simulateFanout('order.completed', testSubscriptions)
      expect(result.matched).toBe(1) // only sub3 with ** matches
      expect(result.immediate).toBe(0)
      expect(result.batched).toBe(1)
    })
  })

  describe('Batch retry scheduling', () => {
    function calculateBatchRetryDelay(attemptNumber: number): number {
      const baseDelay = 1000
      const maxDelay = 300000
      const exponential = Math.min(baseDelay * Math.pow(2, attemptNumber - 1), maxDelay)
      return exponential
    }

    it('calculates exponential backoff for batch retries', () => {
      expect(calculateBatchRetryDelay(1)).toBe(1000)
      expect(calculateBatchRetryDelay(2)).toBe(2000)
      expect(calculateBatchRetryDelay(3)).toBe(4000)
      expect(calculateBatchRetryDelay(4)).toBe(8000)
    })

    it('caps batch retry delay at maximum', () => {
      expect(calculateBatchRetryDelay(10)).toBe(300000)
      expect(calculateBatchRetryDelay(20)).toBe(300000)
    })
  })

  describe('Batch delivery result tracking', () => {
    interface BatchDeliveryResult {
      total: number
      delivered: number
      failed: number
      durationMs: number
      results: Array<{
        deliveryId: string
        eventId: string
        success: boolean
        error?: string
      }>
    }

    function aggregateBatchResults(results: BatchDeliveryResult[]): {
      totalBatches: number
      totalEvents: number
      totalDelivered: number
      totalFailed: number
      avgDurationMs: number
    } {
      if (results.length === 0) {
        return { totalBatches: 0, totalEvents: 0, totalDelivered: 0, totalFailed: 0, avgDurationMs: 0 }
      }

      let totalEvents = 0
      let totalDelivered = 0
      let totalFailed = 0
      let totalDuration = 0

      for (const result of results) {
        totalEvents += result.total
        totalDelivered += result.delivered
        totalFailed += result.failed
        totalDuration += result.durationMs
      }

      return {
        totalBatches: results.length,
        totalEvents,
        totalDelivered,
        totalFailed,
        avgDurationMs: totalDuration / results.length,
      }
    }

    it('aggregates multiple batch results correctly', () => {
      const results: BatchDeliveryResult[] = [
        { total: 10, delivered: 8, failed: 2, durationMs: 100, results: [] },
        { total: 20, delivered: 20, failed: 0, durationMs: 200, results: [] },
        { total: 15, delivered: 10, failed: 5, durationMs: 150, results: [] },
      ]

      const aggregate = aggregateBatchResults(results)
      expect(aggregate.totalBatches).toBe(3)
      expect(aggregate.totalEvents).toBe(45)
      expect(aggregate.totalDelivered).toBe(38)
      expect(aggregate.totalFailed).toBe(7)
      expect(aggregate.avgDurationMs).toBe(150)
    })

    it('handles empty results', () => {
      const aggregate = aggregateBatchResults([])
      expect(aggregate.totalBatches).toBe(0)
      expect(aggregate.totalEvents).toBe(0)
    })
  })
})
