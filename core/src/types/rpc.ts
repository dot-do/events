import type { BaseEvent } from './base.js'

export interface RpcBatchEventData {
  transport: 'http' | 'websocket'
  callCount: number
  duration: number
  pipelineDepth?: number | undefined
  caller?: string | undefined
  target?: { class: string; id: string } | undefined
  colo?: string | undefined
  methods: string[]
  errors?: number | undefined
}

export interface RpcBatchEvent extends BaseEvent<'rpc.batch', RpcBatchEventData> {}

export interface RpcCallEventData {
  batchId?: string | undefined
  method: string
  namespace?: string | undefined
  args?: unknown
  result?: unknown
  error?: { name: string; message: string; stack?: string } | undefined
  duration: number
  index: number
  pipelined: boolean
  target?: { class: string; id: string } | undefined
}

export interface RpcCallEvent extends BaseEvent<'rpc.call', RpcCallEventData> {}
