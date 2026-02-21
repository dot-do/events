import type { BaseEvent } from './base.js'

export interface TailEventData {
  scriptName?: string | undefined
  outcome?: string | undefined
  eventTimestamp?: number | undefined
  logs?: Array<{ message: unknown[]; level: string; timestamp: number }> | undefined
  exceptions?: Array<{ name: string; message: string; timestamp: number }> | undefined
  event?: Record<string, unknown> | undefined
  [key: string]: unknown
}

export interface TailEvent extends BaseEvent<'tail', TailEventData> {}
