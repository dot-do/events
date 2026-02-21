import type { BaseEvent } from './base.js'

export interface McpEventData {
  method: string
  toolName?: string | undefined
  resourceUri?: string | undefined
  promptName?: string | undefined
  arguments?: Record<string, unknown> | undefined
  result?: unknown
  isError: boolean
  duration: number
  sessionId?: string | undefined
  traceId?: string | undefined
  model?: string | undefined
  tokens?: { input?: number; output?: number } | undefined
}

export interface McpEvent extends BaseEvent<'mcp', McpEventData> {}
