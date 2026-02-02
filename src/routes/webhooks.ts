/**
 * Webhook route handlers
 *
 * Events are sent directly to EventWriterDO (not buffered at module level)
 * to prevent data loss on isolate eviction. The DO handles batching and
 * persistence with alarm-based retries.
 */

import type { Env } from '../env'
import { handleWebhook, type NormalizedWebhookEvent } from '../webhook-handler'
import { ingestWithOverflow } from '../event-writer-do'
import type { EventRecord } from '../event-writer'
import { corsHeaders } from '../utils'
import { checkRateLimit, type RateLimitEnv } from '../middleware/rate-limit'
import { logger } from '../logger'

const log = logger.child({ component: 'Webhooks' })

/** More lenient rate limits for server-to-server webhook traffic */
const WEBHOOK_RATE_LIMIT_REQUESTS_PER_MINUTE = 10000
const WEBHOOK_RATE_LIMIT_EVENTS_PER_MINUTE = 100000

const WEBHOOK_PROVIDERS = ['github', 'stripe', 'workos', 'slack', 'linear', 'svix']

/**
 * Check rate limit for webhook requests with lenient server-to-server limits.
 * Applied BEFORE signature verification to prevent DoS attacks.
 */
async function checkWebhookRateLimit(
  request: Request,
  env: Env,
): Promise<Response | null> {
  // Create a modified env with webhook-specific rate limits
  const webhookEnv: RateLimitEnv = {
    RATE_LIMITER: env.RATE_LIMITER,
    RATE_LIMIT_REQUESTS_PER_MINUTE: String(WEBHOOK_RATE_LIMIT_REQUESTS_PER_MINUTE),
    RATE_LIMIT_EVENTS_PER_MINUTE: String(WEBHOOK_RATE_LIMIT_EVENTS_PER_MINUTE),
  }
  // Webhooks are single events, so eventCount is always 1
  return checkRateLimit(request, webhookEnv, 1)
}

/**
 * Send a verified webhook event to EventWriterDO for batched Parquet writes.
 * The DO handles buffering, persistence, and alarm-based flush retries.
 */
async function sendWebhookEvent(
  response: Response,
  env: Env,
): Promise<void> {
  const result = await response.clone().json() as { event?: NormalizedWebhookEvent }
  if (!result.event) return

  const record: EventRecord = {
    ts: result.event.ts,
    type: result.event.type,
    source: result.event.source,
    provider: result.event.webhook.provider,
    eventType: result.event.webhook.eventType,
    verified: result.event.webhook.verified,
    payload: result.event.payload,
  }

  // Send directly to EventWriterDO - no module-level buffering.
  // The DO handles batching and persistence with alarm-based retries.
  const result2 = await ingestWithOverflow(env, [record], 'webhook')
  if (!result2.ok) {
    log.error('Failed to ingest event to EventWriterDO', { shard: result2.shard })
  } else {
    log.info('Ingested webhook event', { shard: result2.shard, buffered: result2.buffered })
  }
}

export async function handleWebhooks(
  request: Request,
  env: Env,
  _ctx: ExecutionContext,
  url: URL,
  isWebhooksDomain: boolean,
  startTime: number,
): Promise<Response | null> {
  // POST /webhooks?provider=xxx (events.do style)
  if (url.pathname === '/webhooks' && request.method === 'POST') {
    const provider = url.searchParams.get('provider')
    if (!provider) {
      return Response.json(
        { error: 'provider query param required' },
        { status: 400, headers: corsHeaders() }
      )
    }

    // Apply rate limiting BEFORE signature verification to prevent DoS
    const rateLimitResponse = await checkWebhookRateLimit(request, env)
    if (rateLimitResponse) {
      return rateLimitResponse
    }

    const response = await handleWebhook(request, env, provider)

    // Send webhook events to EventWriterDO for batched Parquet writes
    if (response.status === 200) {
      await sendWebhookEvent(response, env)
    }

    const cpuTime = performance.now() - startTime
    console.log(`[CPU:${cpuTime.toFixed(2)}ms] POST /webhooks?provider=${provider}`)
    return response
  }

  // webhooks.do/provider format (e.g., webhooks.do/github, webhooks.do/stripe)
  if (isWebhooksDomain && request.method === 'POST') {
    const provider = url.pathname.slice(1).split('/')[0] // Get first path segment
    if (provider && WEBHOOK_PROVIDERS.includes(provider)) {
      // Apply rate limiting BEFORE signature verification to prevent DoS
      const rateLimitResponse = await checkWebhookRateLimit(request, env)
      if (rateLimitResponse) {
        return rateLimitResponse
      }

      const response = await handleWebhook(request, env, provider)

      // Send webhook events to EventWriterDO for batched Parquet writes
      if (response.status === 200) {
        await sendWebhookEvent(response, env)
      }

      const cpuTime = performance.now() - startTime
      console.log(`[CPU:${cpuTime.toFixed(2)}ms] POST /${provider} (webhooks.do)`)
      return response
    }
  }

  return null
}
