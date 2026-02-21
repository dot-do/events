export type { EventType, BaseEvent, ExperimentEvent, RecordingEvent } from './base.js'

export type { CdcEventData, CdcEvent } from './cdc.js'
export type { ActionEventData, ActionEvent } from './action.js'
export type { RelEventData, RelEvent } from './rel.js'
export type { OtelSpanEventData, OtelSpanEvent, OtelLogEventData, OtelLogEvent } from './otel.js'
export type { SessionEventData, SessionEvent } from './session.js'
export type { LlmObservationType, LlmObservationEventData, LlmObservationEvent, LlmScoreEventData, LlmScoreEvent } from './llm.js'
export type { StripeEventShape, GitHubEventShape, WebhookProvider, WebhookEventData, WebhookEvent } from './webhook.js'
export type { PageEventData, PageEvent, TrackEventData, TrackEvent, IdentifyEventData, IdentifyEvent, AnalyticsEvent } from './analytics.js'
export type { TailEventData, TailEvent } from './tail.js'
export type { CustomEvent } from './custom.js'

export {
  isCdcEvent,
  isActionEvent,
  isRelEvent,
  isOtelSpanEvent,
  isOtelLogEvent,
  isSessionEvent,
  isLlmObservationEvent,
  isLlmScoreEvent,
  isWebhookEvent,
  isPageEvent,
  isTrackEvent,
  isIdentifyEvent,
  isAnalyticsEvent,
  isTailEvent,
  isCustomEvent,
  isBaseEvent,
} from './guards.js'

import type { CdcEvent } from './cdc.js'
import type { ActionEvent } from './action.js'
import type { RelEvent } from './rel.js'
import type { OtelSpanEvent, OtelLogEvent } from './otel.js'
import type { SessionEvent } from './session.js'
import type { LlmObservationEvent, LlmScoreEvent } from './llm.js'
import type { WebhookEvent } from './webhook.js'
import type { PageEvent, TrackEvent, IdentifyEvent } from './analytics.js'
import type { TailEvent } from './tail.js'
import type { ExperimentEvent, RecordingEvent } from './base.js'
import type { CustomEvent } from './custom.js'

export type Event =
  | CdcEvent
  | ActionEvent
  | RelEvent
  | OtelSpanEvent
  | OtelLogEvent
  | SessionEvent
  | LlmObservationEvent
  | LlmScoreEvent
  | WebhookEvent
  | PageEvent
  | TrackEvent
  | IdentifyEvent
  | TailEvent
  | ExperimentEvent
  | RecordingEvent
  | CustomEvent

export interface EventBatch {
  events: Event[]
}
