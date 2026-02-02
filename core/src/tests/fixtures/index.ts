/**
 * Shared Test Fixtures
 *
 * Re-exports all fixtures for convenient importing:
 *
 * ```ts
 * import { sampleRpcCallEvent, createSubscriptionConfig } from './fixtures'
 * ```
 */

// Event fixtures
export {
  // Default values
  defaultDoIdentity,
  defaultTimestamp,
  // Sample RPC events
  sampleRpcCallEvent,
  sampleRpcCallEventFailed,
  sampleRpcCallEventNoNamespace,
  // Sample CDC events
  sampleCollectionInsertEvent,
  sampleCollectionUpdateEvent,
  sampleCollectionDeleteEvent,
  sampleCollectionUpdateEventNoPrev,
  // Sample WebSocket events
  sampleWsConnectEvent,
  sampleWsMessageEvent,
  sampleWsCloseEvent,
  sampleWsErrorEvent,
  // Sample lifecycle events
  sampleDoCreateEvent,
  sampleDoAlarmEvent,
  sampleDoHibernateEvent,
  sampleDoEvictEvent,
  // Sample custom events
  sampleCustomEvent,
  sampleCustomEventMinimal,
  // Factory functions
  createRpcCallEvent,
  createCollectionInsertEvent,
  createCollectionUpdateEvent,
  createCollectionDeleteEvent,
  createWebSocketEvent,
  createLifecycleEvent,
  createCustomEvent,
  createEventBatch,
  createCdcEventBatch,
  createRpcEventSeries,
} from './events.js'

// Subscription fixtures
export {
  // Types
  type TestSubscriptionConfig,
  type DeliveryStatus,
  type TestDeliveryRecord,
  type TestDeadLetterRecord,
  // Sample subscriptions
  sampleGitHubSubscription,
  sampleStripeSubscription,
  sampleCdcSubscription,
  sampleCollectionInsertSubscription,
  sampleCatchAllSubscription,
  sampleInactiveSubscription,
  // Sample deliveries
  samplePendingDelivery,
  sampleDeliveredDelivery,
  sampleFailedDelivery,
  sampleDeadDelivery,
  // Sample dead letters
  sampleDeadLetter,
  sampleDeadLetterTimeout,
  // Factory functions
  createSubscriptionConfig,
  createSubscriptionSet,
  createDeliveryRecord,
  createPendingDelivery,
  createFailedDeliveryWithRetry,
  createSuccessfulDelivery,
  createDeadLetterRecord,
  createDeadLetterFromDelivery,
  createDeliveryBatch,
  createDeliveryLifecycle,
} from './subscriptions.js'
