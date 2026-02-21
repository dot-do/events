import type { BaseEvent } from './base.js'

export interface CdcEventData {
  type: string
  id: string
  name?: string | undefined
  visibility?: string | undefined
  content?: string | undefined
  code?: string | undefined
  [key: string]: unknown
}

export interface CdcEvent extends BaseEvent<'cdc', CdcEventData> {}
