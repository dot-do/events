import type { BaseEvent } from './base.js'

export interface StripeEventShape {
  id: string
  type: string
  api_version: string | null
  created: number
  livemode: boolean
  data: { object: Record<string, unknown> }
  [key: string]: unknown
}

export interface GitHubEventShape {
  action?: string | undefined
  sender?: { login: string; id: number; [key: string]: unknown } | undefined
  repository?: { full_name: string; [key: string]: unknown } | undefined
  [key: string]: unknown
}

export type WebhookProvider = 'github' | 'stripe' | 'workos' | 'slack' | 'linear' | 'svix'

export interface WebhookEventData {
  provider: WebhookProvider
  eventType: string
  deliveryId?: string | undefined
  verified: boolean
  payload: Record<string, unknown>
}

export interface WebhookEvent extends BaseEvent<'webhook', WebhookEventData> {}
