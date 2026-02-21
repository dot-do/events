import type { BaseEvent } from './base.js'

export interface ApiRequestEventData {
  // Request
  method: string
  url: string
  path: string
  query?: Record<string, string> | undefined
  requestHeaders?: Record<string, string> | undefined
  requestBody?: unknown
  requestSize?: number | undefined
  requestTruncated?: boolean | undefined

  // Response
  status: number
  responseHeaders?: Record<string, string> | undefined
  responseBody?: unknown
  responseSize?: number | undefined
  responseTruncated?: boolean | undefined

  // Content
  contentType?: string | undefined

  // Timing
  latency: number
  upstreamLatency?: number | undefined
  gatewayLatency?: number | undefined

  // Identity
  clientIp?: string | undefined
  userAgent?: string | undefined
  apiKeyId?: string | undefined
  consumerId?: string | undefined

  // Routing
  routeId?: string | undefined
  serviceId?: string | undefined
  upstream?: string | undefined

  // Direction
  direction: 'incoming' | 'outgoing' | 'internal'

  // Verbosity
  verbosity?: 'full' | 'metadata' | 'minimal' | undefined

  // OTel
  traceId?: string | undefined
  spanId?: string | undefined
}

export interface ApiRequestEvent extends BaseEvent<'api.request', ApiRequestEventData> {}
