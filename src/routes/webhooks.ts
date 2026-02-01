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

const WEBHOOK_PROVIDERS = ['github', 'stripe', 'workos', 'slack', 'linear', 'svix']

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
    console.error(`[webhook] Failed to ingest event to EventWriterDO, shard ${result2.shard}`)
  } else {
    console.log(`[webhook] Ingested to shard ${result2.shard}, buffered: ${result2.buffered}`)
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
