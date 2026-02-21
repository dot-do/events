import type { BaseEvent } from './base.js'

export type LlmObservationType = 'llm.generation' | 'llm.span' | 'llm.event'

export interface LlmObservationEventData {
  id: string
  traceId: string
  type: 'generation' | 'span' | 'event'
  name: string
  model?: string | undefined
  level: string
  status?: string | undefined
  startTime?: string | undefined
  endTime?: string | undefined
  completionStartTime?: string | undefined
  input?: string | undefined
  output?: string | undefined
  usage?: Record<string, number> | undefined
  cost?: Record<string, number> | undefined
  totalCost?: number | undefined
  metadata?: Record<string, string> | undefined
  parentId?: string | undefined
}

export interface LlmObservationEvent extends BaseEvent<LlmObservationType, LlmObservationEventData> {}

export interface LlmScoreEventData {
  id: string
  traceId?: string | undefined
  observationId?: string | undefined
  name: string
  value: number
  source: string
  dataType: string
  comment?: string | undefined
  metadata?: Record<string, string> | undefined
}

export interface LlmScoreEvent extends BaseEvent<'llm.score', LlmScoreEventData> {}
