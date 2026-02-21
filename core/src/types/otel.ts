import type { BaseEvent } from './base.js'

export interface OtelSpanEventData {
  traceId: string
  spanId: string
  parentSpanId: string
  spanName: string
  spanKind: string
  serviceName: string
  resourceAttributes: Record<string, string>
  spanAttributes: Record<string, string>
  duration: number
  statusCode: string
  statusMessage: string
}

export interface OtelSpanEvent extends BaseEvent<'otel.span', OtelSpanEventData> {}

export interface OtelLogEventData {
  traceId: string
  spanId: string
  severityText: string
  severityNumber: number
  serviceName: string
  body: string
  resourceAttributes: Record<string, string>
  logAttributes: Record<string, string>
}

export interface OtelLogEvent extends BaseEvent<'otel.log', OtelLogEventData> {}
