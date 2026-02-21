import type { BaseEvent } from './base.js'

export interface ExperimentAssignmentEventData {
  experimentId: string
  variationId: string
  variationName?: string | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
  sessionId?: string | undefined
  featureFlag?: string | undefined
  allocation?: string | undefined
  attributes?: Record<string, unknown> | undefined
}

export interface ExperimentAssignmentEvent extends BaseEvent<'experiment.assigned', ExperimentAssignmentEventData> {}

export interface ExperimentMetricEventData {
  metricName: string
  userId?: string | undefined
  anonymousId?: string | undefined
  value?: number | undefined
  currency?: string | undefined
  experimentId?: string | undefined
  properties?: Record<string, unknown> | undefined
}

export interface ExperimentMetricEvent extends BaseEvent<'experiment.metric', ExperimentMetricEventData> {}

export interface FlagEvaluatedEventData {
  flagKey: string
  variant?: string | undefined
  value?: unknown
  reason?: 'targeting_match' | 'split' | 'disabled' | 'default' | 'cached' | 'static' | 'unknown' | 'error' | (string & {}) | undefined
  enabled?: boolean | undefined
  contextId?: string | undefined
  userId?: string | undefined
  provider?: string | undefined
  setId?: string | undefined
  version?: string | undefined
  ruleId?: string | undefined
  source?: 'experiment' | 'rule' | 'default' | (string & {}) | undefined
  namespace?: string | undefined
  environment?: string | undefined
  appName?: string | undefined
  entityId?: string | undefined
}

export interface FlagEvaluatedEvent extends BaseEvent<'flag.evaluated', FlagEvaluatedEventData> {}

export type ExperimentationEvent = ExperimentAssignmentEvent | ExperimentMetricEvent | FlagEvaluatedEvent
