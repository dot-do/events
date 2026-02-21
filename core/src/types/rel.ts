import type { BaseEvent } from './base.js'

export interface RelEventData {
  from: string
  predicate: string
  to: string
  reverse: string
  type?: string | undefined
  meta?: Record<string, unknown> | undefined
}

export interface RelEvent extends BaseEvent<'rel', RelEventData> {}
