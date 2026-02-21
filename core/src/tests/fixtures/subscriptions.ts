/**
 * Shared Subscription Test Fixtures
 *
 * Provides sample subscription configurations and delivery records for testing:
 * - Subscription configurations
 * - Delivery records
 * - Dead letter records
 * - Factory functions for creating test data
 */

import type { SubscriptionId, DeliveryId, EventId } from '../../types.js'

// ============================================================================
// Subscription Configuration Types
// ============================================================================

/** Subscription configuration for testing */
export interface TestSubscriptionConfig {
  /** Unique subscription identifier */
  id: string
  /** Worker ID to deliver events to */
  workerId: string
  /** Event pattern to match (e.g., "webhook.github.*", "cdc.**") */
  pattern: string
  /** Pre-computed pattern prefix for efficient matching */
  patternPrefix: string
  /** RPC method to call on the worker */
  rpcMethod: string
  /** Maximum retry attempts (default: 5) */
  maxRetries?: number
  /** Timeout for delivery in milliseconds (default: 30000) */
  timeoutMs?: number
  /** Whether the subscription is active */
  active?: boolean
  /** Batch delivery configuration */
  batchDelivery?: {
    enabled: boolean
    batchSize?: number
    batchWindowMs?: number
  }
  /** Metadata for the subscription */
  metadata?: Record<string, unknown>
  /** ISO timestamp of creation */
  createdAt?: string
  /** ISO timestamp of last update */
  updatedAt?: string
}

/** Delivery status values */
export type DeliveryStatus = 'pending' | 'delivered' | 'failed' | 'dead'

/** Delivery record for testing */
export interface TestDeliveryRecord {
  /** Unique delivery identifier */
  id: string
  /** Associated subscription ID */
  subscriptionId: string
  /** Event ID being delivered */
  eventId: string
  /** Event type for reference */
  eventType: string
  /** Current delivery status */
  status: DeliveryStatus
  /** Number of delivery attempts */
  attemptCount: number
  /** Next retry timestamp (ISO format) */
  nextRetryAt?: string
  /** ISO timestamp of creation */
  createdAt: string
  /** ISO timestamp of last update */
  updatedAt: string
  /** ISO timestamp of successful delivery */
  deliveredAt?: string
  /** Last error message */
  lastError?: string
  /** Last HTTP status code from delivery attempt */
  lastHttpStatus?: number
}

/** Dead letter record for testing */
export interface TestDeadLetterRecord {
  /** Unique dead letter identifier */
  id: string
  /** Associated delivery ID */
  deliveryId: string
  /** Associated subscription ID */
  subscriptionId: string
  /** Event ID that failed delivery */
  eventId: string
  /** Reason for dead letter (e.g., "max_retries_exceeded") */
  reason: string
  /** Last error message */
  lastError: string
  /** ISO timestamp of creation */
  createdAt: string
  /** Full event payload (stored for debugging) */
  eventPayload?: Record<string, unknown>
}

// ============================================================================
// Sample Subscription Configs
// ============================================================================

/** Sample GitHub webhook subscription */
export const sampleGitHubSubscription: TestSubscriptionConfig = {
  id: 'sub_github_123',
  workerId: 'github-handler',
  pattern: 'webhook.github.*',
  patternPrefix: 'webhook.github',
  rpcMethod: 'handleGitHubWebhook',
  maxRetries: 5,
  timeoutMs: 30000,
  active: true,
  createdAt: '2024-01-15T10:00:00.000Z',
  updatedAt: '2024-01-15T10:00:00.000Z',
}

/** Sample Stripe webhook subscription */
export const sampleStripeSubscription: TestSubscriptionConfig = {
  id: 'sub_stripe_456',
  workerId: 'stripe-handler',
  pattern: 'webhook.stripe.**',
  patternPrefix: 'webhook.stripe',
  rpcMethod: 'handleStripeWebhook',
  maxRetries: 10,
  timeoutMs: 60000,
  active: true,
  createdAt: '2024-01-15T11:00:00.000Z',
  updatedAt: '2024-01-15T11:00:00.000Z',
}

/** Sample CDC subscription for all collections */
export const sampleCdcSubscription: TestSubscriptionConfig = {
  id: 'sub_cdc_789',
  workerId: 'analytics-processor',
  pattern: 'cdc.**',
  patternPrefix: 'cdc',
  rpcMethod: 'processCdcEvent',
  maxRetries: 3,
  timeoutMs: 15000,
  active: true,
  batchDelivery: {
    enabled: true,
    batchSize: 100,
    batchWindowMs: 5000,
  },
  createdAt: '2024-01-15T12:00:00.000Z',
  updatedAt: '2024-01-15T12:00:00.000Z',
}

/** Sample subscription for specific collection inserts */
export const sampleCollectionInsertSubscription: TestSubscriptionConfig = {
  id: 'sub_insert_abc',
  workerId: 'user-service',
  pattern: 'collection.created',
  patternPrefix: 'collection.created',
  rpcMethod: 'onUserCreated',
  maxRetries: 5,
  timeoutMs: 30000,
  active: true,
  metadata: { collection: 'users' },
  createdAt: '2024-01-15T13:00:00.000Z',
  updatedAt: '2024-01-15T13:00:00.000Z',
}

/** Sample catch-all subscription */
export const sampleCatchAllSubscription: TestSubscriptionConfig = {
  id: 'sub_all_xyz',
  workerId: 'event-logger',
  pattern: '**',
  patternPrefix: '',
  rpcMethod: 'logEvent',
  maxRetries: 2,
  timeoutMs: 10000,
  active: true,
  createdAt: '2024-01-15T14:00:00.000Z',
  updatedAt: '2024-01-15T14:00:00.000Z',
}

/** Sample inactive subscription */
export const sampleInactiveSubscription: TestSubscriptionConfig = {
  id: 'sub_inactive_999',
  workerId: 'legacy-handler',
  pattern: 'legacy.*',
  patternPrefix: 'legacy',
  rpcMethod: 'handleLegacyEvent',
  maxRetries: 5,
  timeoutMs: 30000,
  active: false,
  createdAt: '2024-01-01T00:00:00.000Z',
  updatedAt: '2024-01-10T00:00:00.000Z',
}

// ============================================================================
// Sample Delivery Records
// ============================================================================

/** Sample pending delivery */
export const samplePendingDelivery: TestDeliveryRecord = {
  id: 'del_pending_001',
  subscriptionId: 'sub_github_123',
  eventId: 'evt_abc123',
  eventType: 'webhook.github.push',
  status: 'pending',
  attemptCount: 0,
  createdAt: '2024-01-15T12:00:00.000Z',
  updatedAt: '2024-01-15T12:00:00.000Z',
}

/** Sample delivered (successful) delivery */
export const sampleDeliveredDelivery: TestDeliveryRecord = {
  id: 'del_success_002',
  subscriptionId: 'sub_stripe_456',
  eventId: 'evt_def456',
  eventType: 'webhook.stripe.payment_intent.succeeded',
  status: 'delivered',
  attemptCount: 1,
  createdAt: '2024-01-15T12:00:00.000Z',
  updatedAt: '2024-01-15T12:00:05.000Z',
  deliveredAt: '2024-01-15T12:00:05.000Z',
}

/** Sample failed delivery with retries remaining */
export const sampleFailedDelivery: TestDeliveryRecord = {
  id: 'del_failed_003',
  subscriptionId: 'sub_github_123',
  eventId: 'evt_ghi789',
  eventType: 'webhook.github.pull_request',
  status: 'failed',
  attemptCount: 2,
  nextRetryAt: '2024-01-15T12:05:00.000Z',
  createdAt: '2024-01-15T12:00:00.000Z',
  updatedAt: '2024-01-15T12:02:00.000Z',
  lastError: 'Connection timeout',
  lastHttpStatus: 504,
}

/** Sample dead delivery (max retries exceeded) */
export const sampleDeadDelivery: TestDeliveryRecord = {
  id: 'del_dead_004',
  subscriptionId: 'sub_cdc_789',
  eventId: 'evt_jkl012',
  eventType: 'cdc.users.update',
  status: 'dead',
  attemptCount: 5,
  createdAt: '2024-01-15T10:00:00.000Z',
  updatedAt: '2024-01-15T12:00:00.000Z',
  lastError: 'Service unavailable',
  lastHttpStatus: 503,
}

// ============================================================================
// Sample Dead Letter Records
// ============================================================================

/** Sample dead letter record */
export const sampleDeadLetter: TestDeadLetterRecord = {
  id: 'dlq_001',
  deliveryId: 'del_dead_004',
  subscriptionId: 'sub_cdc_789',
  eventId: 'evt_jkl012',
  reason: 'max_retries_exceeded',
  lastError: 'Service unavailable',
  createdAt: '2024-01-15T12:00:00.000Z',
  eventPayload: {
    type: 'cdc.users.update',
    collection: 'users',
    docId: 'user-123',
    doc: { name: 'Updated Name' },
  },
}

/** Sample dead letter from timeout */
export const sampleDeadLetterTimeout: TestDeadLetterRecord = {
  id: 'dlq_002',
  deliveryId: 'del_timeout_005',
  subscriptionId: 'sub_github_123',
  eventId: 'evt_mno345',
  reason: 'max_retries_exceeded',
  lastError: 'Request timeout after 30000ms',
  createdAt: '2024-01-15T14:00:00.000Z',
}

// ============================================================================
// Subscription Factory Functions
// ============================================================================

/**
 * Create a subscription config with custom overrides
 */
export function createSubscriptionConfig(
  overrides: Partial<TestSubscriptionConfig> = {}
): TestSubscriptionConfig {
  const id = overrides.id ?? `sub_${Date.now()}`
  return {
    id,
    workerId: 'test-worker',
    pattern: 'test.*',
    patternPrefix: 'test',
    rpcMethod: 'handleEvent',
    maxRetries: 5,
    timeoutMs: 30000,
    active: true,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    ...overrides,
  }
}

/**
 * Create multiple subscriptions with different patterns
 */
export function createSubscriptionSet(
  patterns: Array<{ pattern: string; workerId?: string; active?: boolean }>
): TestSubscriptionConfig[] {
  return patterns.map((p, i) =>
    createSubscriptionConfig({
      id: `sub_${i}`,
      pattern: p.pattern,
      patternPrefix: extractPatternPrefix(p.pattern),
      workerId: p.workerId ?? `worker-${i}`,
      active: p.active ?? true,
    })
  )
}

/**
 * Extract pattern prefix for efficient matching
 */
function extractPatternPrefix(pattern: string): string {
  const wildcardIndex = pattern.indexOf('*')
  if (wildcardIndex === -1) return pattern
  if (wildcardIndex === 0) return ''
  // Remove trailing dot before wildcard
  const prefix = pattern.slice(0, wildcardIndex)
  return prefix.endsWith('.') ? prefix.slice(0, -1) : prefix
}

// ============================================================================
// Delivery Record Factory Functions
// ============================================================================

/**
 * Create a delivery record with custom overrides
 */
export function createDeliveryRecord(
  overrides: Partial<TestDeliveryRecord> = {}
): TestDeliveryRecord {
  const now = new Date().toISOString()
  return {
    id: `del_${Date.now()}`,
    subscriptionId: 'sub_test',
    eventId: `evt_${Date.now()}`,
    eventType: 'test.event',
    status: 'pending',
    attemptCount: 0,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  }
}

/**
 * Create a pending delivery for a specific subscription
 */
export function createPendingDelivery(
  subscriptionId: string,
  eventId: string,
  eventType: string
): TestDeliveryRecord {
  const now = new Date().toISOString()
  return {
    id: `del_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    subscriptionId,
    eventId,
    eventType,
    status: 'pending',
    attemptCount: 0,
    createdAt: now,
    updatedAt: now,
  }
}

/**
 * Create a failed delivery with retry scheduled
 */
export function createFailedDeliveryWithRetry(
  subscriptionId: string,
  eventId: string,
  eventType: string,
  attemptCount: number,
  error: string
): TestDeliveryRecord {
  const now = new Date()
  // Exponential backoff: 1s * 2^(attemptCount-1), capped at 5 minutes
  const retryDelay = Math.min(1000 * Math.pow(2, attemptCount - 1), 300000)
  const nextRetryAt = new Date(now.getTime() + retryDelay)

  return {
    id: `del_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    subscriptionId,
    eventId,
    eventType,
    status: 'failed',
    attemptCount,
    nextRetryAt: nextRetryAt.toISOString(),
    createdAt: new Date(now.getTime() - attemptCount * 60000).toISOString(),
    updatedAt: now.toISOString(),
    lastError: error,
  }
}

/**
 * Create a successful delivery
 */
export function createSuccessfulDelivery(
  subscriptionId: string,
  eventId: string,
  eventType: string,
  attemptCount: number = 1
): TestDeliveryRecord {
  const now = new Date().toISOString()
  return {
    id: `del_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    subscriptionId,
    eventId,
    eventType,
    status: 'delivered',
    attemptCount,
    createdAt: now,
    updatedAt: now,
    deliveredAt: now,
  }
}

// ============================================================================
// Dead Letter Factory Functions
// ============================================================================

/**
 * Create a dead letter record with custom overrides
 */
export function createDeadLetterRecord(
  overrides: Partial<TestDeadLetterRecord> = {}
): TestDeadLetterRecord {
  return {
    id: `dlq_${Date.now()}`,
    deliveryId: `del_${Date.now()}`,
    subscriptionId: 'sub_test',
    eventId: `evt_${Date.now()}`,
    reason: 'max_retries_exceeded',
    lastError: 'Test error',
    createdAt: new Date().toISOString(),
    ...overrides,
  }
}

/**
 * Create a dead letter from a failed delivery
 */
export function createDeadLetterFromDelivery(
  delivery: TestDeliveryRecord,
  reason: string = 'max_retries_exceeded',
  eventPayload?: Record<string, unknown>
): TestDeadLetterRecord {
  return {
    id: `dlq_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    deliveryId: delivery.id,
    subscriptionId: delivery.subscriptionId,
    eventId: delivery.eventId,
    reason,
    lastError: delivery.lastError ?? 'Unknown error',
    createdAt: new Date().toISOString(),
    eventPayload,
  }
}

// ============================================================================
// Batch Test Data Generators
// ============================================================================

/**
 * Create a batch of deliveries with various statuses
 */
export function createDeliveryBatch(
  subscriptionId: string,
  statusDistribution: { pending: number; delivered: number; failed: number; dead: number }
): TestDeliveryRecord[] {
  const deliveries: TestDeliveryRecord[] = []
  let eventCounter = 0

  // Create pending deliveries
  for (let i = 0; i < statusDistribution.pending; i++) {
    deliveries.push(
      createPendingDelivery(subscriptionId, `evt_${eventCounter++}`, 'test.event')
    )
  }

  // Create delivered deliveries
  for (let i = 0; i < statusDistribution.delivered; i++) {
    deliveries.push(
      createSuccessfulDelivery(subscriptionId, `evt_${eventCounter++}`, 'test.event')
    )
  }

  // Create failed deliveries
  for (let i = 0; i < statusDistribution.failed; i++) {
    deliveries.push(
      createFailedDeliveryWithRetry(
        subscriptionId,
        `evt_${eventCounter++}`,
        'test.event',
        i + 1,
        `Error ${i + 1}`
      )
    )
  }

  // Create dead deliveries
  for (let i = 0; i < statusDistribution.dead; i++) {
    deliveries.push(
      createDeliveryRecord({
        subscriptionId,
        eventId: `evt_${eventCounter++}`,
        eventType: 'test.event',
        status: 'dead',
        attemptCount: 5,
        lastError: `Final error ${i + 1}`,
      })
    )
  }

  return deliveries
}

/**
 * Simulate delivery lifecycle from pending to delivered/dead
 */
export function createDeliveryLifecycle(
  subscriptionId: string,
  eventId: string,
  eventType: string,
  outcome: 'success' | 'dead',
  maxAttempts: number = 5
): TestDeliveryRecord[] {
  const lifecycle: TestDeliveryRecord[] = []
  const baseTime = Date.now()
  const deliveryId = `del_${baseTime}_${Math.random().toString(36).slice(2, 8)}`

  if (outcome === 'success') {
    // Successful on first or second attempt
    const successAttempt = Math.min(2, maxAttempts)

    for (let attempt = 1; attempt <= successAttempt; attempt++) {
      if (attempt < successAttempt) {
        lifecycle.push({
          id: deliveryId,
          subscriptionId,
          eventId,
          eventType,
          status: attempt === 1 ? 'pending' : 'failed',
          attemptCount: attempt - 1,
          createdAt: new Date(baseTime).toISOString(),
          updatedAt: new Date(baseTime + attempt * 1000).toISOString(),
          lastError: attempt > 1 ? 'Temporary failure' : undefined,
        })
      } else {
        lifecycle.push({
          id: deliveryId,
          subscriptionId,
          eventId,
          eventType,
          status: 'delivered',
          attemptCount: attempt,
          createdAt: new Date(baseTime).toISOString(),
          updatedAt: new Date(baseTime + attempt * 1000).toISOString(),
          deliveredAt: new Date(baseTime + attempt * 1000).toISOString(),
        })
      }
    }
  } else {
    // Dead after max attempts
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const status: DeliveryStatus =
        attempt === 1 ? 'pending' : attempt === maxAttempts ? 'dead' : 'failed'

      lifecycle.push({
        id: deliveryId,
        subscriptionId,
        eventId,
        eventType,
        status,
        attemptCount: status === 'pending' ? 0 : attempt,
        createdAt: new Date(baseTime).toISOString(),
        updatedAt: new Date(baseTime + attempt * 60000).toISOString(),
        lastError: status !== 'pending' ? `Attempt ${attempt} failed` : undefined,
        nextRetryAt:
          status === 'failed'
            ? new Date(baseTime + (attempt + 1) * 60000).toISOString()
            : undefined,
      })
    }
  }

  return lifecycle
}
