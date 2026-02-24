export type EventType =
  | 'cdc'
  | 'action'
  | 'rel'
  | 'otel.span'
  | 'otel.log'
  | 'session'
  | 'llm.generation'
  | 'llm.span'
  | 'llm.event'
  | 'llm.score'
  | 'webhook'
  | 'tail'
  | 'page'
  | 'track'
  | 'identify'
  | 'screen'
  | 'group'
  | 'alias'
  | 'meter'
  | 'stripe'
  | 'experiment.assigned'
  | 'experiment.metric'
  | 'flag.evaluated'
  | EpcisEventType
  | 'recording'
  | 'rpc.batch'
  | 'rpc.call'
  | 'mcp'
  | 'eval'
  | 'api.request'
  | `custom.${string}`

export type EpcisEventType = 'epcis.object' | 'epcis.aggregation' | 'epcis.transaction' | 'epcis.transformation' | 'epcis.association'

/** Rich actor identity — columnar JSON in ClickHouse for easy filtering by org, user, ip, asn, etc. */
export interface Actor {
  // Authenticated identity (from id-org-ai header)
  id?: string
  name?: string
  email?: string
  orgId?: string

  // Network
  ip?: string
  asn?: number
  asOrganization?: string
  clientTcpRtt?: number

  // Geo
  city?: string
  region?: string
  regionCode?: string
  country?: string
  continent?: string
  postalCode?: string
  metroCode?: string
  latitude?: number
  longitude?: number
  timezone?: string
  isEU?: boolean

  // UA (parsed via ua-parser-js)
  ua?: string
  browser?: string
  browserVersion?: string
  os?: string
  osVersion?: string
  device?: string
  deviceVendor?: string
  deviceModel?: string
  engine?: string

  // Bot detection (isbot + CF Bot Management enterprise)
  bot?: boolean
  botScore?: number
  botVerified?: boolean
  botCorporateProxy?: boolean
  botStaticResource?: boolean
  botDetectionIds?: number[]

  // TLS fingerprint
  ja3?: string
  ja4?: string
  tlsVersion?: string

  // CF edge
  colo?: string
  httpProtocol?: string
  requestPriority?: string
  clientAcceptEncoding?: string

  [key: string]: unknown
}

export interface BaseEvent<T extends string = string, D = Record<string, unknown>> {
  id: string
  ns: string
  ts: string
  type: T
  event: string
  url: string
  source: string
  actor: Actor
  data: D
  meta: Record<string, unknown>
}

export interface RecordingEvent extends BaseEvent<'recording', Record<string, unknown>> {}
