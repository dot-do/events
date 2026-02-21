import type { BaseEvent } from './base.js'

export interface PageEventData {
  properties?: Record<string, unknown> | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
  sessionId?: string | undefined
  path?: string | undefined
  referrer?: string | undefined
  ua?: string | undefined
}

export interface PageEvent extends BaseEvent<'page', PageEventData> {}

export interface TrackEventData {
  properties?: Record<string, unknown> | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
  sessionId?: string | undefined
}

export interface TrackEvent extends BaseEvent<'track', TrackEventData> {}

export interface IdentifyEventData {
  traits?: Record<string, unknown> | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
  sessionId?: string | undefined
}

export interface IdentifyEvent extends BaseEvent<'identify', IdentifyEventData> {}

export type AnalyticsEvent = PageEvent | TrackEvent | IdentifyEvent
