import type { BaseEvent } from './base.js'

export interface SessionEventData {
  sessionId: string
  serviceName: string
  body: string
  resourceAttributes: Record<string, string>
  logAttributes: Record<string, string>
}

export interface SessionEvent extends BaseEvent<'session', SessionEventData> {}
