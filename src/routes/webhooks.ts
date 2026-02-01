/**
 * Webhook route handlers
 */

import type { Env } from '../env'
import { handleWebhook, type NormalizedWebhookEvent } from '../webhook-handler'
import { getEventBuffer, type EventRecord } from '../event-writer'
import { corsHeaders } from '../utils'

const WEBHOOK_PROVIDERS = ['github', 'stripe', 'workos', 'slack', 'linear', 'svix']

/**
 * Buffer a verified webhook event for batched Parquet writes.
 * Reads the response clone inline, then flushes in the background via ctx.waitUntil.
 */
async function bufferWebhookEvent(
  response: Response,
  env: Env,
  ctx: ExecutionContext,
): Promise<void> {
  const result = await response.clone().json() as { event?: NormalizedWebhookEvent }
  if (!result.event) return

  const buffer = getEventBuffer(env.EVENTS_BUCKET, 'events', {
    countThreshold: 50,
    timeThresholdMs: 5000,
  })

  const record: EventRecord = {
    ts: result.event.ts,
    type: result.event.type,
    source: result.event.source,
    provider: result.event.webhook.provider,
    eventType: result.event.webhook.eventType,
    verified: result.event.webhook.verified,
    payload: result.event.payload,
  }

  buffer.add(record)
  ctx.waitUntil(buffer.maybeFlush(ctx).then(r => {
    if (r) console.log(`[webhook] Flushed ${r.events} events to ${r.key}`)
  }))
}

export async function handleWebhooks(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
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

    // Buffer webhook events for batched Parquet writes
    if (response.status === 200) {
      await bufferWebhookEvent(response, env, ctx)
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

      // Buffer webhook events for batched Parquet writes
      if (response.status === 200) {
        await bufferWebhookEvent(response, env, ctx)
      }

      const cpuTime = performance.now() - startTime
      console.log(`[CPU:${cpuTime.toFixed(2)}ms] POST /${provider} (webhooks.do)`)
      return response
    }
  }

  return null
}
