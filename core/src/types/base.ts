export type EventType =
  | 'cdc'
  | 'action'
  | 'rel'
  | 'otel.span'
  | 'otel.log'
  | 'session'
  | 'llm.generation'
  | 'llm.span'
  | 'llm.event'
  | 'llm.score'
  | 'webhook'
  | 'tail'
  | 'page'
  | 'track'
  | 'identify'
  | 'screen'
  | 'group'
  | 'alias'
  | 'meter'
  | 'stripe'
  | 'experiment.assigned'
  | 'experiment.metric'
  | 'flag.evaluated'
  | EpcisEventType
  | 'recording'
  | `custom.${string}`

export type EpcisEventType = 'epcis.object' | 'epcis.aggregation' | 'epcis.transaction' | 'epcis.transformation' | 'epcis.association'

export interface BaseEvent<T extends string = string, D = Record<string, unknown>> {
  id: string
  ns: string
  ts: string
  type: T
  event: string
  url: string
  source: string
  actor: string
  data: D
  meta: Record<string, unknown>
}

export interface RecordingEvent extends BaseEvent<'recording', Record<string, unknown>> {}
