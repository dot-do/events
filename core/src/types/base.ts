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
  | 'experiment'
  | 'recording'
  | `custom.${string}`

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

export interface ExperimentEvent extends BaseEvent<'experiment', Record<string, unknown>> {}

export interface RecordingEvent extends BaseEvent<'recording', Record<string, unknown>> {}
