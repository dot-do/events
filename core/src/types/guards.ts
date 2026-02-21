import type { BaseEvent } from './base.js'
import type { CdcEvent } from './cdc.js'
import type { ActionEvent } from './action.js'
import type { RelEvent } from './rel.js'
import type { OtelSpanEvent, OtelLogEvent } from './otel.js'
import type { SessionEvent } from './session.js'
import type { LlmObservationEvent, LlmScoreEvent } from './llm.js'
import type { WebhookEvent } from './webhook.js'
import type { PageEvent, TrackEvent, IdentifyEvent } from './analytics.js'
import type { TailEvent } from './tail.js'
import type { CustomEvent } from './custom.js'

export function isCdcEvent<E extends { type: string }>(event: E): event is E & CdcEvent {
  return event.type === 'cdc'
}

export function isActionEvent<E extends { type: string }>(event: E): event is E & ActionEvent {
  return event.type === 'action'
}

export function isRelEvent<E extends { type: string }>(event: E): event is E & RelEvent {
  return event.type === 'rel'
}

export function isOtelSpanEvent<E extends { type: string }>(event: E): event is E & OtelSpanEvent {
  return event.type === 'otel.span'
}

export function isOtelLogEvent<E extends { type: string }>(event: E): event is E & OtelLogEvent {
  return event.type === 'otel.log'
}

export function isSessionEvent<E extends { type: string }>(event: E): event is E & SessionEvent {
  return event.type === 'session'
}

export function isLlmObservationEvent<E extends { type: string }>(event: E): event is E & LlmObservationEvent {
  return event.type === 'llm.generation' || event.type === 'llm.span' || event.type === 'llm.event'
}

export function isLlmScoreEvent<E extends { type: string }>(event: E): event is E & LlmScoreEvent {
  return event.type === 'llm.score'
}

export function isWebhookEvent<E extends { type: string }>(event: E): event is E & WebhookEvent {
  return event.type === 'webhook'
}

export function isPageEvent<E extends { type: string }>(event: E): event is E & PageEvent {
  return event.type === 'page'
}

export function isTrackEvent<E extends { type: string }>(event: E): event is E & TrackEvent {
  return event.type === 'track'
}

export function isIdentifyEvent<E extends { type: string }>(event: E): event is E & IdentifyEvent {
  return event.type === 'identify'
}

export function isAnalyticsEvent<E extends { type: string }>(event: E): event is E & (PageEvent | TrackEvent | IdentifyEvent) {
  return event.type === 'page' || event.type === 'track' || event.type === 'identify'
}

export function isTailEvent<E extends { type: string }>(event: E): event is E & TailEvent {
  return event.type === 'tail'
}

export function isCustomEvent<E extends { type: string }>(event: E): event is E & CustomEvent {
  return event.type.startsWith('custom.')
}

export function isBaseEvent(value: unknown): value is BaseEvent {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  return (
    typeof v.id === 'string' &&
    typeof v.ns === 'string' &&
    typeof v.ts === 'string' &&
    typeof v.type === 'string' &&
    typeof v.event === 'string' &&
    typeof v.url === 'string' &&
    typeof v.source === 'string' &&
    typeof v.actor === 'string' &&
    typeof v.data === 'object' &&
    v.data !== null &&
    typeof v.meta === 'object' &&
    v.meta !== null
  )
}
