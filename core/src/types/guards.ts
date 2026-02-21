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
import type { ScreenEvent, GroupEvent, AliasEvent } from './segment.js'
import type { ExperimentAssignmentEvent, ExperimentMetricEvent, FlagEvaluatedEvent } from './experimentation.js'
import type { MeterEvent, StripeEvent } from './billing.js'
import type { ObjectEvent, AggregationEvent, TransactionEvent, TransformationEvent, AssociationEvent, EpcisEvent } from './epcis.js'

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

export function isScreenEvent<E extends { type: string }>(event: E): event is E & ScreenEvent {
  return event.type === 'screen'
}

export function isGroupEvent<E extends { type: string }>(event: E): event is E & GroupEvent {
  return event.type === 'group'
}

export function isAliasEvent<E extends { type: string }>(event: E): event is E & AliasEvent {
  return event.type === 'alias'
}

export function isSegmentEvent<E extends { type: string }>(event: E): event is E & (PageEvent | TrackEvent | IdentifyEvent | ScreenEvent | GroupEvent | AliasEvent) {
  return event.type === 'page' || event.type === 'track' || event.type === 'identify' || event.type === 'screen' || event.type === 'group' || event.type === 'alias'
}

export function isExperimentAssignmentEvent<E extends { type: string }>(event: E): event is E & ExperimentAssignmentEvent {
  return event.type === 'experiment.assigned'
}

export function isExperimentMetricEvent<E extends { type: string }>(event: E): event is E & ExperimentMetricEvent {
  return event.type === 'experiment.metric'
}

export function isFlagEvaluatedEvent<E extends { type: string }>(event: E): event is E & FlagEvaluatedEvent {
  return event.type === 'flag.evaluated'
}

export function isExperimentationEvent<E extends { type: string }>(event: E): event is E & (ExperimentAssignmentEvent | ExperimentMetricEvent | FlagEvaluatedEvent) {
  return event.type === 'experiment.assigned' || event.type === 'experiment.metric' || event.type === 'flag.evaluated'
}

export function isMeterEvent<E extends { type: string }>(event: E): event is E & MeterEvent {
  return event.type === 'meter'
}

export function isStripeEvent<E extends { type: string }>(event: E): event is E & StripeEvent {
  return event.type === 'stripe'
}

export function isBillingEvent<E extends { type: string }>(event: E): event is E & (MeterEvent | StripeEvent) {
  return event.type === 'meter' || event.type === 'stripe'
}

export function isEpcisEvent<E extends { type: string }>(event: E): event is E & EpcisEvent {
  return event.type.startsWith('epcis.')
}

export function isObjectEvent<E extends { type: string }>(event: E): event is E & ObjectEvent {
  return event.type === 'epcis.object'
}

export function isAggregationEvent<E extends { type: string }>(event: E): event is E & AggregationEvent {
  return event.type === 'epcis.aggregation'
}

export function isTransactionEvent<E extends { type: string }>(event: E): event is E & TransactionEvent {
  return event.type === 'epcis.transaction'
}

export function isTransformationEvent<E extends { type: string }>(event: E): event is E & TransformationEvent {
  return event.type === 'epcis.transformation'
}

export function isAssociationEvent<E extends { type: string }>(event: E): event is E & AssociationEvent {
  return event.type === 'epcis.association'
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
