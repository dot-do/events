# TypeScript Conventions

This document outlines the TypeScript conventions used in the events.do codebase. Following these guidelines ensures consistency and maintainability across the project.

## Table of Contents

1. [Interface vs Type Alias](#interface-vs-type-alias)
2. [Naming Conventions](#naming-conventions)
3. [Export Organization](#export-organization)
4. [Type-Only Imports](#type-only-imports)
5. [Generic Usage](#generic-usage)
6. [Utility Types and Patterns](#utility-types-and-patterns)

---

## Interface vs Type Alias

### When to Use `interface`

Use `interface` for:

1. **Object shapes and contracts** - When defining the structure of objects, configuration options, or API contracts

```typescript
// Good: Object shape
interface EventEmitterOptions {
  endpoint?: string
  batchSize?: number
  flushIntervalMs?: number
  cdc?: boolean
}

// Good: Class contracts
interface IngestContext {
  request: Request
  env: Env
  ctx: ExecutionContext
  tenant: TenantContext
}
```

2. **Extending existing types** - Interfaces support declaration merging and extension

```typescript
// Good: Extending interfaces
interface Env extends WebhookEnv {
  EVENTS_BUCKET: R2Bucket
  CATALOG: DurableObjectNamespace<CatalogDO>
}

// Good: Extending Request with CF properties
interface CfRequest extends Request {
  cf?: IncomingRequestCfProperties
}
```

3. **Class implementations** - When defining contracts that classes should implement

```typescript
interface Collection<T> {
  get(id: string): T | null
  put(id: string, doc: T): void
  delete(id: string): boolean
}
```

### When to Use `type`

Use `type` for:

1. **Union types** - When a value can be one of several types

```typescript
// Good: Union of object types (discriminated union)
type DurableEvent =
  | RpcCallEvent
  | CollectionChangeEvent
  | LifecycleEvent
  | WebSocketEvent
  | ClientEvent
  | CustomEvent

// Good: Simple union
type EventType = 'insert' | 'update' | 'delete'
```

2. **Intersection types** - When combining multiple types

```typescript
// Good: Intersection for combining types
type Required<T> = T & { [P in keyof T]-?: T[P] }
```

3. **Mapped and conditional types** - For type transformations

```typescript
// Good: Mapped type
type AutoFilledFields = 'ts' | 'do'
type EmitInput = Omit<RpcCallEvent, AutoFilledFields> | Omit<CollectionChangeEvent, AutoFilledFields>

// Good: Conditional type
type NonNullable<T> = T extends null | undefined ? never : T
```

4. **Tuple types** - For fixed-length arrays with specific types

```typescript
// Good: Tuple type
type Coordinate = [number, number]
type ParseResult = [boolean, string | null]
```

5. **Function types** - When defining function signatures

```typescript
// Good: Function type
type IngestMiddleware = (context: IngestContext) => Promise<MiddlewareResult>

// Good: Callback type
type OnErrorCallback = (error: Error, events: DurableEvent[]) => void
```

6. **Branded/nominal types** - For creating distinct types from primitives

```typescript
// Good: Branded types for type safety
declare const __brand: unique symbol
type Brand<T, B extends string> = T & { [__brand]: B }

type SubscriptionId = Brand<string, 'SubscriptionId'>
type EventId = Brand<string, 'EventId'>
```

### Decision Summary

| Use Case | Use | Example |
|----------|-----|---------|
| Object shapes | `interface` | `interface Config { ... }` |
| Class contracts | `interface` | `interface Serializable { ... }` |
| Extending types | `interface` | `interface Env extends Base { ... }` |
| Union types | `type` | `type Result = Success \| Error` |
| Mapped types | `type` | `type Partial<T> = { ... }` |
| Function signatures | `type` | `type Handler = (req: Request) => Response` |
| Branded types | `type` | `type UserId = Brand<string, 'UserId'>` |
| Tuples | `type` | `type Point = [number, number]` |

---

## Naming Conventions

### No Hungarian Notation

This codebase does **not** use Hungarian notation prefixes like `I` for interfaces or `T` for types. Names should be descriptive and self-documenting.

```typescript
// Good: Clear, descriptive names
interface EventEmitterOptions { }
interface IngestContext { }
type DurableEvent = ...
type MiddlewareResult = ...

// Bad: Hungarian notation
interface IEventEmitterOptions { }  // Don't prefix with I
type TDurableEvent = ...            // Don't prefix with T
```

### Naming Patterns

| Type | Convention | Examples |
|------|------------|----------|
| Interfaces | PascalCase, noun or noun phrase | `EventBatch`, `IngestContext`, `CircuitBreakerState` |
| Type aliases | PascalCase, noun or noun phrase | `DurableEvent`, `EmitInput`, `MiddlewareResult` |
| Branded types | PascalCase with `Id` suffix | `SubscriptionId`, `EventId`, `TableId` |
| Error classes | PascalCase with `Error` suffix | `EventBufferFullError`, `CircuitBreakerOpenError` |
| Constants | SCREAMING_SNAKE_CASE | `MAX_BATCH_SIZE`, `DEFAULT_FLUSH_INTERVAL_MS` |
| Type parameters | Single uppercase letter or descriptive name | `T`, `K`, `V`, `TDoc`, `TResult` |

### Branded Type Factory Functions

For branded types, provide lowercase factory functions:

```typescript
// Type definition
type SubscriptionId = Brand<string, 'SubscriptionId'>

// Factory function (lowercase, matches type name)
function subscriptionId(id: string): SubscriptionId {
  return id as SubscriptionId
}
```

---

## Export Organization

### File Structure

Organize type exports in a clear, consistent manner:

```typescript
/**
 * Module description
 */

// 1. External imports (type-only first)
import type { Request, Response } from '@cloudflare/workers-types'
import { someFunction } from 'some-package'

// 2. Internal type imports
import type { Env } from './env'
import type { TenantContext } from './tenant'

// 3. Constants and configuration
export const MAX_BATCH_SIZE = 1000
export const DEFAULT_TIMEOUT = 5000

// 4. Type definitions (interfaces first, then type aliases)
export interface Config {
  // ...
}

export interface Options {
  // ...
}

export type Result = Success | Error

export type Handler = (ctx: Context) => Promise<Response>

// 5. Classes and functions
export class MyClass {
  // ...
}

export function myFunction() {
  // ...
}
```

### Section Comments

Use section comments to organize large type files:

```typescript
// ============================================================================
// Branded Types for IDs
// ============================================================================

type SubscriptionId = Brand<string, 'SubscriptionId'>
type EventId = Brand<string, 'EventId'>

// ============================================================================
// Event Types
// ============================================================================

interface BaseEvent {
  type: string
  ts: string
}

interface RpcCallEvent extends BaseEvent {
  // ...
}

// ============================================================================
// Validation Types
// ============================================================================

interface ValidationError {
  path?: string
  message: string
}
```

### Re-exports

Use barrel files (`index.ts`) to consolidate exports:

```typescript
// index.ts
export type { DurableEvent, EmitInput, EventBatch } from './types'
export type { IngestContext, MiddlewareResult } from './middleware/types'
export { EventEmitter } from './emitter'
export { CDCCollection } from './cdc'
```

---

## Type-Only Imports

### Always Use `import type` for Types

When importing only types, use `import type` to ensure the import is erased at runtime:

```typescript
// Good: Type-only import
import type { DurableEvent, EventBatch } from './types'
import type { Env } from './env'

// Good: Mixed import (values and types)
import { EventEmitter } from './emitter'
import type { EventEmitterOptions } from './types'

// Bad: Importing types without type keyword
import { DurableEvent, EventBatch } from './types'  // May cause issues if only types
```

### Inline Type Imports

For mixed imports, use inline `type` keyword:

```typescript
// Good: Inline type import
import { EventEmitter, type EventEmitterOptions } from './emitter'

// Good: Separate imports (also acceptable)
import { EventEmitter } from './emitter'
import type { EventEmitterOptions } from './emitter'
```

### Export Type

When re-exporting types, use `export type`:

```typescript
// Good: Re-export types
export type { Env } from './env'
export type { AuthUser, AuthRequest } from 'oauth.do/itty'

// Good: Mixed re-export
export { EventEmitter } from './emitter'
export type { EventEmitterOptions } from './emitter'
```

---

## Generic Usage

### Generic Type Parameters

Use descriptive generic names for clarity:

```typescript
// Good: Single-letter for simple, common generics
interface Collection<T> {
  get(id: string): T | null
  put(id: string, doc: T): void
}

// Good: Descriptive names for complex generics
type CDCCollection<TDoc> = {
  get(id: string): TDoc | null
  put(id: string, doc: TDoc): void
}

// Good: Multiple related generics
type MapFunction<TInput, TOutput> = (input: TInput) => TOutput
```

### Constrained Generics

Use constraints to limit generic types:

```typescript
// Good: Constraining to object types
interface Repository<T extends Record<string, unknown>> {
  save(entity: T): Promise<void>
}

// Good: Constraining to specific interface
interface EventHandler<T extends DurableEvent> {
  handle(event: T): Promise<void>
}

// Good: Default generic parameter
interface QueryOptions<T = unknown> {
  filter?: (item: T) => boolean
  limit?: number
}
```

### Generic Inference

Let TypeScript infer generics when possible:

```typescript
// Good: Inference from argument
function identity<T>(value: T): T {
  return value
}
const str = identity('hello')  // T inferred as string

// Good: Inference in collections
const users = new CDCCollection<User>(collection, events, 'users')
// Type of users.get() return is automatically User | null
```

---

## Utility Types and Patterns

### Common Utility Types

Use built-in TypeScript utility types:

```typescript
// Partial - all properties optional
type UpdateUser = Partial<User>

// Required - all properties required
type CompleteConfig = Required<Config>

// Pick - select specific properties
type UserCredentials = Pick<User, 'email' | 'passwordHash'>

// Omit - exclude specific properties
type PublicUser = Omit<User, 'passwordHash' | 'apiKey'>

// Record - key-value mapping
type EventHandlers = Record<string, (event: DurableEvent) => void>
```

### Optional Properties

Use `| undefined` explicitly when a property may be undefined:

```typescript
// Good: Explicit undefined for optional properties
interface BaseEvent {
  type: string
  ts: string
  do: {
    id: string
    name?: string | undefined  // Explicitly shows it can be undefined
    class?: string | undefined
  }
}
```

### Discriminated Unions

Use a common property (discriminant) for type narrowing:

```typescript
// Good: Discriminated union with 'type' field
interface RpcCallEvent {
  type: 'rpc.call'
  method: string
  durationMs: number
}

interface CollectionChangeEvent {
  type: 'collection.insert' | 'collection.update' | 'collection.delete'
  collection: string
  docId: string
}

type DurableEvent = RpcCallEvent | CollectionChangeEvent

// Type guard using discriminant
function isRpcCall(event: DurableEvent): event is RpcCallEvent {
  return event.type === 'rpc.call'
}
```

### Type Guards

Create type guards for runtime type checking:

```typescript
// Good: Type guard function
function isCollectionChangeEvent(event: DurableEvent): event is CollectionChangeEvent {
  return (
    event.type === 'collection.insert' ||
    event.type === 'collection.update' ||
    event.type === 'collection.delete'
  )
}

// Usage
if (isCollectionChangeEvent(event)) {
  console.log(event.collection)  // TypeScript knows event is CollectionChangeEvent
}
```

### Satisfies Operator

Use `satisfies` for type checking without widening:

```typescript
// Good: Validate object shape while keeping literal types
const config = {
  endpoint: 'https://events.do/ingest',
  batchSize: 100,
} satisfies EventEmitterOptions

// config.endpoint is typed as 'https://events.do/ingest', not string
```

---

## Additional Guidelines

### JSDoc Comments

Add JSDoc comments for public APIs:

```typescript
/**
 * Emit an event to be batched and sent to the events endpoint.
 * Events are batched based on `batchSize` and `flushIntervalMs` options.
 *
 * @param event - The event data to emit (type, collection, docId, etc.)
 * @throws {CircuitBreakerOpenError} When circuit breaker is open
 */
emit(event: EmitInput): void
```

### Avoid `any`

Prefer `unknown` over `any` when the type is truly unknown:

```typescript
// Good: Use unknown for truly unknown types
async function parseBody(request: Request): Promise<unknown> {
  return request.json()
}

// Good: Type assertion after validation
const body = await parseBody(request)
if (isEventBatch(body)) {
  // body is now EventBatch
}

// Bad: Using any
async function parseBody(request: Request): Promise<any> {
  return request.json()
}
```

### Strict Null Checks

Always handle null and undefined explicitly:

```typescript
// Good: Explicit null handling
function getUser(id: string): User | null {
  const user = this.users.get(id)
  return user ?? null
}

// Good: Optional chaining
const userName = user?.profile?.name ?? 'Anonymous'
```

---

## Summary

| Convention | This Codebase |
|------------|---------------|
| Interface prefix | No `I` prefix |
| Type prefix | No `T` prefix |
| Object shapes | Use `interface` |
| Unions/functions | Use `type` |
| Type imports | Always use `import type` |
| Generic naming | `T`, `TDoc`, `TResult` |
| Constants | `SCREAMING_SNAKE_CASE` |
| Branded type factories | `lowercase` function name |
