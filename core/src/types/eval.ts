import type { BaseEvent } from './base.js'

export interface EvalEventData {
  code: string
  codeHash: string
  codeType: 'javascript' | 'typescript' | 'wasm'
  success: boolean
  value?: unknown
  logs: Array<{ level: string; message: string; ts: string }>
  error?: { name: string; message: string; stack?: string } | undefined
  duration: number
  caller?: string | undefined
  timedOut: boolean
  fetchBlocked: boolean
}

export interface EvalEvent extends BaseEvent<'eval', EvalEventData> {}
