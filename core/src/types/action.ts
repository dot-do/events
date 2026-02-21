import type { BaseEvent } from './base.js'

export interface ActionEventData {
  actionId: string
  action: string
  status: string
  input?: Record<string, unknown> | undefined
  output?: Record<string, unknown> | undefined
  options?: Record<string, unknown> | undefined
  error?: Record<string, unknown> | undefined
  parent?: string | undefined
  duration?: number | undefined
}

export interface ActionEvent extends BaseEvent<'action', ActionEventData> {}
