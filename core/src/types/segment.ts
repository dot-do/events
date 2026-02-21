import type { BaseEvent } from './base.js'
import type { PageEvent, TrackEvent, IdentifyEvent } from './analytics.js'

export interface SegmentContext {
  ip?: string
  userAgent?: string
  locale?: string
  timezone?: string
  library?: { name: string; version: string }
  campaign?: { name?: string; source?: string; medium?: string; term?: string; content?: string }
  page?: { path?: string; referrer?: string; search?: string; title?: string; url?: string; host?: string }
  screen?: { width?: number; height?: number; density?: number }
  app?: { name?: string; version?: string; build?: string; namespace?: string }
  device?: { id?: string; manufacturer?: string; model?: string; name?: string; type?: string }
  network?: { bluetooth?: boolean; carrier?: string; cellular?: boolean; wifi?: boolean }
  os?: { name?: string; version?: string }
}

export interface ScreenEventData {
  name?: string | undefined
  properties?: Record<string, unknown> | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
  sessionId?: string | undefined
  category?: string | undefined
}

export interface ScreenEvent extends BaseEvent<'screen', ScreenEventData> {}

export interface GroupEventData {
  groupId?: string | undefined
  traits?: Record<string, unknown> | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
  sessionId?: string | undefined
}

export interface GroupEvent extends BaseEvent<'group', GroupEventData> {}

export interface AliasEventData {
  previousId?: string | undefined
  userId?: string | undefined
  anonymousId?: string | undefined
}

export interface AliasEvent extends BaseEvent<'alias', AliasEventData> {}

export type SegmentEvent = PageEvent | TrackEvent | IdentifyEvent | ScreenEvent | GroupEvent | AliasEvent
