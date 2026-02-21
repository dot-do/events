export type { EventType, EpcisEventType, BaseEvent, RecordingEvent } from './base.js'

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
export type { MeterEventData, MeterEvent, StripeEventData, StripeEvent, BillingEvent } from './billing.js'
export type {
  BizStep,
  Disposition,
  EpcisAction,
  QuantityElement,
  BizTransaction,
  SourceDest,
  SensorReport,
  SensorElement,
  PersistentDisposition,
  ErrorDeclaration,
  EpcisCommonData,
  ObjectEventData,
  ObjectEvent,
  AggregationEventData,
  AggregationEvent,
  TransactionEventData,
  TransactionEvent,
  TransformationEventData,
  TransformationEvent,
  AssociationEventData,
  AssociationEvent,
  EpcisEvent,
} from './epcis.js'
export type {
  ExperimentAssignmentEventData,
  ExperimentAssignmentEvent,
  ExperimentMetricEventData,
  ExperimentMetricEvent,
  FlagEvaluatedEventData,
  FlagEvaluatedEvent,
  ExperimentationEvent,
} from './experimentation.js'
export type {
  SegmentContext,
  ScreenEventData,
  ScreenEvent,
  GroupEventData,
  GroupEvent,
  AliasEventData,
  AliasEvent,
  SegmentEvent,
} from './segment.js'
export type { RpcBatchEventData, RpcBatchEvent, RpcCallEventData, RpcCallEvent } from './rpc.js'
export type { McpEventData, McpEvent } from './mcp.js'
export type { EvalEventData, EvalEvent } from './eval.js'
export type { ApiRequestEventData, ApiRequestEvent } from './api.js'

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
  isScreenEvent,
  isGroupEvent,
  isAliasEvent,
  isSegmentEvent,
  isExperimentAssignmentEvent,
  isExperimentMetricEvent,
  isFlagEvaluatedEvent,
  isExperimentationEvent,
  isMeterEvent,
  isStripeEvent,
  isBillingEvent,
  isEpcisEvent,
  isObjectEvent,
  isAggregationEvent,
  isTransactionEvent,
  isTransformationEvent,
  isAssociationEvent,
  isTailEvent,
  isCustomEvent,
  isRpcBatchEvent,
  isRpcCallEvent,
  isRpcEvent,
  isMcpEvent,
  isEvalEvent,
  isApiRequestEvent,
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
import type { RecordingEvent } from './base.js'
import type { CustomEvent } from './custom.js'
import type { MeterEvent, StripeEvent } from './billing.js'
import type { ObjectEvent, AggregationEvent, TransactionEvent, TransformationEvent, AssociationEvent } from './epcis.js'
import type { ExperimentAssignmentEvent, ExperimentMetricEvent, FlagEvaluatedEvent } from './experimentation.js'
import type { ScreenEvent, GroupEvent, AliasEvent } from './segment.js'
import type { RpcBatchEvent, RpcCallEvent } from './rpc.js'
import type { McpEvent } from './mcp.js'
import type { EvalEvent } from './eval.js'
import type { ApiRequestEvent } from './api.js'

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
  | ScreenEvent
  | GroupEvent
  | AliasEvent
  | MeterEvent
  | StripeEvent
  | ObjectEvent
  | AggregationEvent
  | TransactionEvent
  | TransformationEvent
  | AssociationEvent
  | ExperimentAssignmentEvent
  | ExperimentMetricEvent
  | FlagEvaluatedEvent
  | TailEvent
  | RecordingEvent
  | CustomEvent
  | RpcBatchEvent
  | RpcCallEvent
  | McpEvent
  | EvalEvent
  | ApiRequestEvent

export interface EventBatch {
  events: Event[]
}
