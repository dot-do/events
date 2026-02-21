import type { BaseEvent } from './base.js'

export interface MeterEventData {
  // CloudEvents-compatible fields (OpenMeter)
  code: string // billable metric code / CloudEvents type
  subject?: string // customer/entity being metered (OpenMeter subject)

  // Value
  value?: string | number // the metered value (string for Lago compat, number for direct)

  // Customer identity
  customerId?: string // internal customer ID
  externalCustomerId?: string // external customer ID (Lago)
  subscriptionId?: string // internal subscription ID
  externalSubscriptionId?: string // external subscription ID (Lago)

  // Properties
  properties?: Record<string, string> // additional dimensions (region, tier, model, etc.)

  // Billing context
  transactionId?: string // idempotency key
  preciseTotalAmountCents?: string // Lago: pre-calculated amount as string (Decimal precision)
  currency?: string // ISO 4217 currency code

  // CloudEvents metadata
  ceSource?: string // CloudEvents source
  ceType?: string // CloudEvents type (if different from code)
}

export interface MeterEvent extends BaseEvent<'meter', MeterEventData> {}

export interface StripeEventData {
  stripeEventId: string // evt_xxx
  eventType: string // invoice.paid, charge.succeeded, etc.
  apiVersion?: string
  livemode?: boolean
  objectId?: string // the Stripe object ID
  objectType?: string // invoice, charge, subscription, customer
  customerId?: string // cus_xxx
  amount?: number // in cents
  currency?: string
  status?: string
  subscriptionId?: string
  invoiceId?: string
  payload?: Record<string, unknown> // full Stripe event data.object
}

export interface StripeEvent extends BaseEvent<'stripe', StripeEventData> {}

export type BillingEvent = MeterEvent | StripeEvent
