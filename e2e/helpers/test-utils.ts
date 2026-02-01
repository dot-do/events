/**
 * E2E Test Utilities
 *
 * Common utilities for running E2E tests against production workers.
 * No mocking - all requests hit real endpoints.
 */

/**
 * Production endpoint URLs
 * Override via environment variables for different environments
 */
export const ENDPOINTS = {
  /** Main events-do worker endpoint */
  EVENTS_DO: process.env.EVENTS_DO_URL || 'https://events-do.dotdo.workers.dev',
  /** Events tail worker for real-time streaming */
  EVENTS_TAIL: process.env.EVENTS_TAIL_URL || 'https://events-tail.dotdo.workers.dev',
  /** Events compactor worker for R2 file management */
  EVENTS_COMPACTOR: process.env.EVENTS_COMPACTOR_URL || 'https://events-compactor.dotdo.workers.dev',
  /** HTTP ingest endpoint for direct pipeline ingestion */
  PIPELINE_INGEST: process.env.PIPELINE_INGEST_URL || 'https://46678f20f29748488165c19d7e43f331.ingest.cloudflare.com',
} as const

/**
 * Test secrets for webhook verification
 * These should match what's configured in the worker secrets
 */
export const TEST_SECRETS = {
  github: process.env.TEST_GITHUB_SECRET || 'test-github-secret',
  stripe: process.env.TEST_STRIPE_SECRET || 'test-stripe-secret',
  slack: process.env.TEST_SLACK_SECRET || 'test-slack-secret',
  linear: process.env.TEST_LINEAR_SECRET || 'test-linear-secret',
  svix: process.env.TEST_SVIX_SECRET || 'whsec_' + btoa('test-svix-secret-key-32bytes!!'),
  workos: process.env.TEST_WORKOS_SECRET || 'test-workos-secret',
} as const

/**
 * Generates a unique test ID for tracking events through the system
 *
 * @returns A unique identifier prefixed with 'e2e-'
 *
 * @example
 * ```ts
 * const testId = generateTestId()
 * // 'e2e-1706745123456-abc123'
 * ```
 */
export function generateTestId(): string {
  return `e2e-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

/**
 * Generates a unique correlation ID for request tracing
 *
 * @returns A UUID v4 string
 */
export function generateCorrelationId(): string {
  return crypto.randomUUID()
}

/**
 * Options for waiting operations
 */
export interface WaitOptions {
  /** Maximum time to wait in milliseconds (default: 30000) */
  timeoutMs?: number
  /** Polling interval in milliseconds (default: 1000) */
  intervalMs?: number
  /** Description for error messages */
  description?: string
}

/**
 * Waits for a condition to become true, polling at intervals
 *
 * @param condition - Async function that returns true when condition is met
 * @param options - Wait configuration options
 * @returns The truthy value returned by the condition
 * @throws Error if condition is not met within timeout
 *
 * @example
 * ```ts
 * const event = await waitFor(async () => {
 *   const response = await fetch(`${endpoint}/events/${eventId}`)
 *   if (response.ok) return await response.json()
 *   return false
 * }, { timeoutMs: 60000, description: 'event to appear' })
 * ```
 */
export async function waitFor<T>(
  condition: () => Promise<T | false | null | undefined>,
  options: WaitOptions = {}
): Promise<T> {
  const { timeoutMs = 30000, intervalMs = 1000, description = 'condition' } = options
  const startTime = Date.now()

  while (Date.now() - startTime < timeoutMs) {
    const result = await condition()
    if (result) {
      return result
    }
    await sleep(intervalMs)
  }

  throw new Error(`Timeout waiting for ${description} after ${timeoutMs}ms`)
}

/**
 * Waits for an event to appear in R2 storage
 *
 * @param endpoint - The events-do endpoint URL
 * @param predicate - Function to match the target event
 * @param options - Wait configuration options
 * @returns The matched event
 *
 * @example
 * ```ts
 * const event = await waitForEventInR2(
 *   ENDPOINTS.EVENTS_DO,
 *   (event) => event.data?.testId === testId,
 *   { timeoutMs: 60000 }
 * )
 * ```
 */
export async function waitForEventInR2(
  endpoint: string,
  predicate: (event: Record<string, unknown>) => boolean,
  options: WaitOptions = {}
): Promise<Record<string, unknown>> {
  const { timeoutMs = 30000, intervalMs = 2000, description = 'event in R2' } = options
  const startTime = Date.now()

  while (Date.now() - startTime < timeoutMs) {
    try {
      // Query recent events from the stats endpoint
      const response = await fetch(`${endpoint}/stats`)
      if (response.ok) {
        const stats = (await response.json()) as { recentEvents?: Record<string, unknown>[] }

        // Check recent events for matching predicate
        if (stats.recentEvents) {
          for (const event of stats.recentEvents) {
            if (predicate(event)) {
              return event
            }
          }
        }
      }
    } catch {
      // Ignore errors and retry
    }

    await sleep(intervalMs)
  }

  throw new Error(`Timeout waiting for ${description} after ${timeoutMs}ms`)
}

/**
 * Waits for a webhook event to be processed and stored
 *
 * @param endpoint - The events-do endpoint URL
 * @param testId - The unique test ID to match
 * @param options - Wait configuration options
 * @returns The matched webhook event
 */
export async function waitForWebhookEvent(
  endpoint: string,
  testId: string,
  options: WaitOptions = {}
): Promise<Record<string, unknown>> {
  return waitForEventInR2(
    endpoint,
    (event) => {
      const data = event.data as Record<string, unknown> | undefined
      return data?.testId === testId
    },
    { ...options, description: `webhook event with testId=${testId}` }
  )
}

/**
 * Simple sleep utility
 *
 * @param ms - Milliseconds to sleep
 * @returns Promise that resolves after the delay
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Result type for fetch with error handling
 */
export interface FetchResult<T = unknown> {
  success: boolean
  status: number
  statusText: string
  headers: Headers
  data?: T
  error?: string
}

/**
 * Performs a fetch with structured error handling
 *
 * @param url - The URL to fetch
 * @param options - Fetch options
 * @returns Structured result with success/error info
 *
 * @example
 * ```ts
 * const result = await safeFetch<{ accepted: boolean }>(
 *   `${endpoint}/webhooks`,
 *   { method: 'POST', headers, body }
 * )
 * if (result.success) {
 *   console.log('Accepted:', result.data?.accepted)
 * }
 * ```
 */
export async function safeFetch<T = unknown>(
  url: string,
  options?: RequestInit
): Promise<FetchResult<T>> {
  try {
    const response = await fetch(url, options)
    const result: FetchResult<T> = {
      success: response.ok,
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    }

    // Try to parse JSON response
    try {
      const text = await response.text()
      if (text) {
        result.data = JSON.parse(text) as T
      }
    } catch {
      // Response wasn't JSON, that's ok
    }

    if (!response.ok) {
      result.error = result.data
        ? JSON.stringify(result.data)
        : `HTTP ${response.status}: ${response.statusText}`
    }

    return result
  } catch (error) {
    return {
      success: false,
      status: 0,
      statusText: 'Network Error',
      headers: new Headers(),
      error: error instanceof Error ? error.message : 'Unknown error',
    }
  }
}

/**
 * Checks if a test should be skipped due to missing secrets
 *
 * @param secretName - Name of the secret to check
 * @param envVar - Environment variable name
 * @returns Object with skip status and reason
 *
 * @example
 * ```ts
 * const { shouldSkip, reason } = checkSecretConfigured('github', 'TEST_GITHUB_SECRET')
 * if (shouldSkip) {
 *   console.log(`Skipping: ${reason}`)
 *   return
 * }
 * ```
 */
export function checkSecretConfigured(
  secretName: string,
  envVar: string
): { shouldSkip: boolean; reason: string } {
  const value = process.env[envVar]
  if (!value || value.startsWith('test-')) {
    return {
      shouldSkip: true,
      reason: `${envVar} not configured (using default test secret for ${secretName})`,
    }
  }
  return { shouldSkip: false, reason: '' }
}

/**
 * Creates test context with common test utilities
 *
 * @returns Object with test ID, correlation ID, and helper functions
 */
export function createTestContext() {
  const testId = generateTestId()
  const correlationId = generateCorrelationId()

  return {
    testId,
    correlationId,
    /**
     * Creates metadata to include in test events for tracking
     */
    createMetadata: (extra: Record<string, unknown> = {}) => ({
      testId,
      correlationId,
      timestamp: Date.now(),
      ...extra,
    }),
    /**
     * Logs with test context prefix
     */
    log: (message: string, ...args: unknown[]) => {
      console.log(`[${testId}] ${message}`, ...args)
    },
  }
}
