# Error Code Reference

This document provides a comprehensive reference for all error codes used by `@dotdo/events`. Each error includes its HTTP status, description, common causes, and resolution steps.

## Table of Contents

- [Error Response Format](#error-response-format)
- [Validation Errors (400)](#validation-errors-400)
- [Authentication Errors (401)](#authentication-errors-401)
- [Authorization Errors (403)](#authorization-errors-403)
- [Resource Errors (404/409)](#resource-errors-404409)
- [Rate Limiting (429)](#rate-limiting-429)
- [Server Errors (500)](#server-errors-500)
- [Service Errors (502/503/504)](#service-errors-502503504)
- [Troubleshooting Tips](#troubleshooting-tips)

---

## Error Response Format

All errors return a consistent JSON response format:

```json
{
  "error": "Human-readable error message",
  "code": "ERROR_CODE",
  "details": {
    "field": "optional additional context"
  }
}
```

### Extended Error Response

Some error types include additional fields:

```json
{
  "error": "Schema validation failed",
  "code": "SCHEMA_VALIDATION_ERROR",
  "validationErrors": [...],
  "summary": ["field 'type' is required", "field 'timestamp' must be a number"]
}
```

For rate limit errors:

```json
{
  "error": "Rate limit exceeded",
  "code": "RATE_LIMITED",
  "details": {
    "retryAfter": 60,
    "limit": 1000
  },
  "retryAfter": 60
}
```

The `Retry-After` HTTP header is also set for rate limit responses.

---

## Validation Errors (400)

These errors indicate problems with the request data.

### VALIDATION_ERROR

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Generic validation failure for input data |

**Common Causes:**
- Missing required fields in request body
- Invalid field types (e.g., string instead of number)
- Field values outside acceptable range

**Resolution:**
1. Check the `details` field for specific validation issues
2. Ensure all required fields are provided
3. Verify field types match the expected schema

**Example Response:**
```json
{
  "error": "Invalid event type",
  "code": "VALIDATION_ERROR",
  "details": {
    "field": "type",
    "expected": "string",
    "received": "number"
  }
}
```

---

### INVALID_JSON

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Request body contains malformed JSON |

**Common Causes:**
- Syntax errors in JSON (missing quotes, trailing commas)
- Incorrect content-type header
- Empty request body when JSON expected

**Resolution:**
1. Validate your JSON using a linter before sending
2. Ensure `Content-Type: application/json` header is set
3. Check for encoding issues (use UTF-8)

**Example Response:**
```json
{
  "error": "Invalid JSON",
  "code": "INVALID_JSON"
}
```

---

### INVALID_BATCH

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Event batch structure is invalid |

**Common Causes:**
- Missing `events` array in batch request
- Batch contains more than 1000 events
- `events` field is not an array

**Resolution:**
1. Ensure your batch has an `events` array at the root
2. Limit batch size to 1000 events maximum
3. Split larger batches into multiple requests

**Example Response:**
```json
{
  "error": "Invalid batch: must have events array with max 1000 events",
  "code": "INVALID_BATCH",
  "details": {
    "eventCount": 1500,
    "maxEvents": 1000
  }
}
```

---

### INVALID_EVENT

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | One or more events in the batch failed validation |

**Common Causes:**
- Missing required event fields (`type`, `timestamp`)
- Invalid event type format
- Malformed event data

**Resolution:**
1. Check the `details.indices` field to identify which events failed
2. Ensure all events have required fields: `type`, `timestamp`
3. Verify event type follows naming conventions

**Example Response:**
```json
{
  "error": "Events missing required 'type' field",
  "code": "INVALID_EVENT",
  "details": {
    "indices": [2, 5, 7],
    "field": "type"
  }
}
```

---

### EVENT_TOO_LARGE

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Individual event exceeds maximum size limit |

**Common Causes:**
- Event payload contains large binary data
- Deeply nested objects with redundant data
- Including full documents instead of IDs/references

**Resolution:**
1. Reduce event payload size by removing unnecessary fields
2. Store large data in R2/external storage and reference by ID
3. Compress or truncate large text fields

**Example Response:**
```json
{
  "error": "Event exceeds maximum size",
  "code": "EVENT_TOO_LARGE",
  "details": {
    "indices": [3],
    "maxSize": 65536
  }
}
```

---

### PAYLOAD_TOO_LARGE

| Property | Value |
|----------|-------|
| **HTTP Status** | 413 Payload Too Large |
| **Description** | Entire request body exceeds size limit |

**Common Causes:**
- Batch contains too much data overall
- Large base64-encoded attachments
- Compressed content that expands beyond limits

**Resolution:**
1. Split large batches into smaller requests
2. Reduce individual event sizes
3. Use streaming endpoints for large data transfers

**Example Response:**
```json
{
  "error": "Payload Too Large",
  "code": "PAYLOAD_TOO_LARGE",
  "details": {
    "maxSize": 10485760,
    "contentLength": 15728640
  }
}
```

---

### SCHEMA_VALIDATION_ERROR

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Events failed JSON schema validation |

**Common Causes:**
- Events don't match registered schema
- Missing required schema fields
- Incorrect field types per schema definition

**Resolution:**
1. Review the `validationErrors` array for specific issues
2. Compare your events against the registered schema
3. Use schema validation locally before sending

**Example Response:**
```json
{
  "error": "Schema validation failed",
  "code": "SCHEMA_VALIDATION_ERROR",
  "details": {
    "validationErrors": [
      {"path": "/data/userId", "message": "must be string"}
    ],
    "summary": ["field 'data.userId' must be string"]
  }
}
```

---

### INVALID_PATTERN

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Subscription pattern is invalid |

**Common Causes:**
- Malformed glob/regex pattern
- Unsupported pattern syntax
- Empty pattern string

**Resolution:**
1. Use valid glob patterns (e.g., `user.*`, `collection.*.*`)
2. Test patterns with sample event types before subscribing
3. Refer to pattern documentation for supported syntax

**Example Response:**
```json
{
  "error": "Invalid subscription pattern",
  "code": "INVALID_PATTERN",
  "details": {
    "pattern": "[invalid"
  }
}
```

---

### INVALID_PATH

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | R2 storage path is invalid |

**Common Causes:**
- Path contains invalid characters
- Path traversal attempts (e.g., `../`)
- Empty or null path

**Resolution:**
1. Use alphanumeric characters, hyphens, and forward slashes
2. Avoid special characters and path traversal sequences
3. Ensure path is not empty

**Example Response:**
```json
{
  "error": "Invalid storage path",
  "code": "INVALID_PATH",
  "details": {
    "path": "../../../etc/passwd"
  }
}
```

---

### INVALID_SIGNATURE

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Webhook signature verification failed |

**Common Causes:**
- Incorrect webhook secret configured
- Request body was modified after signing
- Signature header is missing or malformed

**Resolution:**
1. Verify the webhook secret matches on both ends
2. Ensure you're using the raw request body for verification
3. Check that signature headers are correctly formatted

**Example Response:**
```json
{
  "error": "Invalid signature",
  "code": "INVALID_SIGNATURE",
  "details": {
    "provider": "stripe",
    "timestamp": 1704067200
  }
}
```

---

### INVALID_TIMESTAMP

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Timestamp is outside acceptable tolerance window |

**Common Causes:**
- Clock drift between client and server
- Replayed request with old timestamp
- Timestamp in wrong format (seconds vs milliseconds)

**Resolution:**
1. Synchronize client clock with NTP
2. Use millisecond Unix timestamps
3. Ensure requests are sent promptly after creation

**Example Response:**
```json
{
  "error": "Timestamp outside tolerance window",
  "code": "INVALID_TIMESTAMP",
  "details": {
    "timestamp": 1704067200000,
    "tolerance": 300000
  }
}
```

---

### INVALID_CONFIG

| Property | Value |
|----------|-------|
| **HTTP Status** | 400 Bad Request |
| **Description** | Configuration settings are invalid |

**Common Causes:**
- Missing required configuration fields
- Invalid configuration values
- Incompatible configuration combinations

**Resolution:**
1. Check the `details.errors` array for specific issues
2. Review configuration documentation
3. Validate configuration before applying

**Example Response:**
```json
{
  "error": "Invalid configuration",
  "code": "INVALID_CONFIG",
  "details": {
    "provider": "webhook",
    "field": "url",
    "errors": ["URL must use HTTPS"]
  }
}
```

---

## Authentication Errors (401)

These errors indicate authentication problems.

### AUTH_ERROR

| Property | Value |
|----------|-------|
| **HTTP Status** | 401 Unauthorized |
| **Description** | Generic authentication failure |

**Common Causes:**
- No authentication credentials provided
- Authentication mechanism not supported
- Session expired

**Resolution:**
1. Include valid authentication credentials
2. Use supported authentication method (API key, bearer token)
3. Refresh expired tokens

**Example Response:**
```json
{
  "error": "Authentication required",
  "code": "AUTH_ERROR"
}
```

---

### UNAUTHORIZED

| Property | Value |
|----------|-------|
| **HTTP Status** | 401 Unauthorized |
| **Description** | Request is not authorized |

**Common Causes:**
- Invalid credentials
- Token validation failed
- Account suspended

**Resolution:**
1. Verify credentials are correct
2. Check token hasn't expired
3. Contact support if account may be suspended

**Example Response:**
```json
{
  "error": "Unauthorized",
  "code": "UNAUTHORIZED"
}
```

---

### INVALID_API_KEY

| Property | Value |
|----------|-------|
| **HTTP Status** | 401 Unauthorized |
| **Description** | API key is invalid or revoked |

**Common Causes:**
- Typo in API key
- API key has been revoked
- Using test key in production (or vice versa)

**Resolution:**
1. Double-check the API key for typos
2. Generate a new API key if current one is compromised
3. Ensure you're using the correct key for the environment

**Example Response:**
```json
{
  "error": "Invalid API key",
  "code": "INVALID_API_KEY"
}
```

---

### MISSING_API_KEY

| Property | Value |
|----------|-------|
| **HTTP Status** | 401 Unauthorized |
| **Description** | API key was not provided |

**Common Causes:**
- Forgot to include `Authorization` header
- Header name is incorrect
- API key is empty string

**Resolution:**
1. Add `Authorization: Bearer <api_key>` header
2. Or use `X-API-Key: <api_key>` header
3. Ensure the key value is not empty

**Example Response:**
```json
{
  "error": "API key is required",
  "code": "MISSING_API_KEY"
}
```

---

## Authorization Errors (403)

These errors indicate permission problems.

### FORBIDDEN

| Property | Value |
|----------|-------|
| **HTTP Status** | 403 Forbidden |
| **Description** | User lacks permission for the requested action |

**Common Causes:**
- Accessing resource outside your namespace
- Action requires higher permission level
- IP address not in allowlist

**Resolution:**
1. Verify you have permission for the resource
2. Request elevated permissions if needed
3. Check IP allowlist configuration

**Example Response:**
```json
{
  "error": "Forbidden",
  "code": "FORBIDDEN",
  "details": {
    "resource": "namespace/production",
    "action": "write"
  }
}
```

---

## Resource Errors (404/409)

These errors indicate problems with resources.

### NOT_FOUND

| Property | Value |
|----------|-------|
| **HTTP Status** | 404 Not Found |
| **Description** | Requested resource does not exist |

**Common Causes:**
- Resource ID is incorrect
- Resource was deleted
- Typo in resource path

**Resolution:**
1. Verify the resource ID is correct
2. Check if resource was recently deleted
3. List resources to find the correct ID

**Example Response:**
```json
{
  "error": "Not found",
  "code": "NOT_FOUND"
}
```

---

### SUBSCRIPTION_NOT_FOUND

| Property | Value |
|----------|-------|
| **HTTP Status** | 404 Not Found |
| **Description** | Requested subscription does not exist |

**Common Causes:**
- Subscription ID is incorrect
- Subscription was deleted
- Subscription belongs to different namespace

**Resolution:**
1. List subscriptions to find the correct ID
2. Create a new subscription if needed
3. Verify namespace access

**Example Response:**
```json
{
  "error": "Subscription not found: sub_abc123",
  "code": "SUBSCRIPTION_NOT_FOUND",
  "details": {
    "subscriptionId": "sub_abc123"
  }
}
```

---

### DELIVERY_NOT_FOUND

| Property | Value |
|----------|-------|
| **HTTP Status** | 404 Not Found |
| **Description** | Requested delivery record does not exist |

**Common Causes:**
- Delivery ID is incorrect
- Delivery was purged after retention period
- Delivery belongs to different namespace

**Resolution:**
1. Use correct delivery ID from webhook response
2. Check delivery retention settings
3. Query recent deliveries to find correct ID

**Example Response:**
```json
{
  "error": "Delivery not found: del_xyz789",
  "code": "DELIVERY_NOT_FOUND",
  "details": {
    "deliveryId": "del_xyz789"
  }
}
```

---

### CONFLICT

| Property | Value |
|----------|-------|
| **HTTP Status** | 409 Conflict |
| **Description** | Operation conflicts with current resource state |

**Common Causes:**
- Concurrent modification detected
- Resource already in target state
- Optimistic locking failure

**Resolution:**
1. Refetch the resource and retry
2. Use conditional requests with ETags
3. Implement retry logic with backoff

**Example Response:**
```json
{
  "error": "Conflict",
  "code": "CONFLICT",
  "details": {
    "expectedVersion": 5,
    "actualVersion": 7
  }
}
```

---

### DUPLICATE_ENTRY

| Property | Value |
|----------|-------|
| **HTTP Status** | 409 Conflict |
| **Description** | Attempting to create a resource that already exists |

**Common Causes:**
- Creating subscription with existing ID
- Duplicate idempotency key
- Unique constraint violation

**Resolution:**
1. Use a unique identifier for new resources
2. Update existing resource instead of creating
3. Check for existing resource before creation

**Example Response:**
```json
{
  "error": "Subscription already exists",
  "code": "DUPLICATE_ENTRY",
  "details": {
    "subscriptionId": "sub_abc123"
  }
}
```

---

## Rate Limiting (429)

### RATE_LIMITED

| Property | Value |
|----------|-------|
| **HTTP Status** | 429 Too Many Requests |
| **Description** | Request rate limit has been exceeded |

**Common Causes:**
- Too many requests in a short time window
- Burst of traffic exceeding quota
- Missing rate limit handling in client

**Resolution:**
1. Check `Retry-After` header and wait before retrying
2. Implement exponential backoff
3. Batch events to reduce request count
4. Contact support to increase limits if needed

**Example Response:**
```json
{
  "error": "Rate limit exceeded",
  "code": "RATE_LIMITED",
  "details": {
    "retryAfter": 60,
    "limit": 1000
  },
  "retryAfter": 60
}
```

---

## Server Errors (500)

These errors indicate server-side problems.

### INTERNAL_ERROR

| Property | Value |
|----------|-------|
| **HTTP Status** | 500 Internal Server Error |
| **Description** | Unexpected server error occurred |

**Common Causes:**
- Bug in server code
- Unhandled exception
- System resource exhaustion

**Resolution:**
1. Retry the request after a short delay
2. Check service status page for outages
3. Contact support if issue persists

**Example Response:**
```json
{
  "error": "Internal server error",
  "code": "INTERNAL_ERROR"
}
```

---

### DATABASE_ERROR

| Property | Value |
|----------|-------|
| **HTTP Status** | 500 Internal Server Error |
| **Description** | Database operation failed |

**Common Causes:**
- Database connection issues
- Query timeout
- Storage quota exceeded

**Resolution:**
1. Retry the request (this error is retryable)
2. Check for service degradation
3. Contact support if persistent

**Example Response:**
```json
{
  "error": "Database operation failed",
  "code": "DATABASE_ERROR"
}
```

---

### STORAGE_ERROR

| Property | Value |
|----------|-------|
| **HTTP Status** | 500 Internal Server Error |
| **Description** | R2/storage operation failed |

**Common Causes:**
- R2 service temporarily unavailable
- Storage quota exceeded
- Network issues to storage backend

**Resolution:**
1. Retry the request (this error is retryable)
2. Check R2 service status
3. Verify storage quota

**Example Response:**
```json
{
  "error": "Failed to write to storage",
  "code": "STORAGE_ERROR",
  "details": {
    "bucket": "events",
    "key": "2024/01/01/00/abc123.jsonl"
  }
}
```

---

### DELIVERY_FAILED

| Property | Value |
|----------|-------|
| **HTTP Status** | 500 Internal Server Error |
| **Description** | Event delivery to subscriber failed |

**Common Causes:**
- Subscriber endpoint returned error
- Network timeout to subscriber
- Maximum retry attempts exceeded

**Resolution:**
1. Check subscriber endpoint health
2. Review delivery attempt logs
3. Verify webhook URL is accessible

**Example Response:**
```json
{
  "error": "Delivery failed after 5 attempts",
  "code": "DELIVERY_FAILED",
  "details": {
    "deliveryId": "del_xyz789",
    "subscriptionId": "sub_abc123",
    "attemptNumber": 5
  }
}
```

---

### WEBHOOK_ERROR

| Property | Value |
|----------|-------|
| **HTTP Status** | 500 Internal Server Error |
| **Description** | Webhook processing failed |

**Common Causes:**
- Webhook handler threw exception
- Invalid webhook payload from provider
- Configuration mismatch

**Resolution:**
1. Check webhook handler logs
2. Verify webhook configuration
3. Validate payload against expected format

**Example Response:**
```json
{
  "error": "Webhook processing failed",
  "code": "WEBHOOK_ERROR",
  "details": {
    "provider": "stripe",
    "reason": "Unhandled event type: checkout.session.expired"
  }
}
```

---

## Service Errors (502/503/504)

These errors indicate service availability problems.

### SERVICE_UNAVAILABLE

| Property | Value |
|----------|-------|
| **HTTP Status** | 503 Service Unavailable |
| **Description** | Service is temporarily unavailable |

**Common Causes:**
- Planned maintenance
- Service overloaded
- Dependency service down

**Resolution:**
1. Retry with exponential backoff
2. Check status page for maintenance windows
3. Implement circuit breaker pattern

**Example Response:**
```json
{
  "error": "Service temporarily unavailable",
  "code": "SERVICE_UNAVAILABLE"
}
```

---

### TIMEOUT

| Property | Value |
|----------|-------|
| **HTTP Status** | 504 Gateway Timeout |
| **Description** | Operation timed out |

**Common Causes:**
- Long-running query exceeded timeout
- Network latency
- Downstream service slow

**Resolution:**
1. Retry the request (this error is retryable)
2. Reduce query complexity or batch size
3. Check for service degradation

**Example Response:**
```json
{
  "error": "Operation timed out",
  "code": "TIMEOUT",
  "details": {
    "timeoutMs": 30000
  }
}
```

---

### CIRCUIT_BREAKER_OPEN

| Property | Value |
|----------|-------|
| **HTTP Status** | 503 Service Unavailable |
| **Description** | Circuit breaker is open due to repeated failures |

**Common Causes:**
- Dependency service experiencing issues
- Too many consecutive failures
- Service degradation detected

**Resolution:**
1. Wait for circuit breaker to reset (typically 30-60 seconds)
2. Check dependency service health
3. Retry after the reset period

**Example Response:**
```json
{
  "error": "Circuit breaker open",
  "code": "CIRCUIT_BREAKER_OPEN",
  "details": {
    "service": "webhook-delivery",
    "resetAt": "2024-01-01T12:05:00Z"
  }
}
```

---

### BUFFER_FULL

| Property | Value |
|----------|-------|
| **HTTP Status** | 503 Service Unavailable |
| **Description** | Internal buffer is full, cannot accept more events |

**Common Causes:**
- Event ingestion rate too high
- Downstream processing slow
- Sudden traffic spike

**Resolution:**
1. Implement client-side buffering with backoff
2. Reduce event submission rate
3. Contact support to increase capacity

**Example Response:**
```json
{
  "error": "Buffer full, please retry later",
  "code": "BUFFER_FULL"
}
```

---

## Troubleshooting Tips

### General Debugging Steps

1. **Check the error code** - Use this reference to understand the error
2. **Review the details** - The `details` field often contains specific information
3. **Check request format** - Ensure JSON is valid and content-type is correct
4. **Verify authentication** - Confirm API key is valid and included
5. **Check rate limits** - Respect `Retry-After` headers

### Retryable Errors

The following errors are safe to retry with exponential backoff:

- `RATE_LIMITED` (respect `Retry-After` header)
- `SERVICE_UNAVAILABLE`
- `TIMEOUT`
- `DATABASE_ERROR`
- `STORAGE_ERROR`
- `CIRCUIT_BREAKER_OPEN`
- `BUFFER_FULL`

### Implementing Retry Logic

```typescript
import { isRetryableError } from '@dotdo/events'

async function sendWithRetry(events: Event[], maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await sendEvents(events)
    } catch (error) {
      if (!isRetryableError(error) || attempt === maxRetries) {
        throw error
      }

      // Exponential backoff with jitter
      const delay = Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 30000)
      await new Promise(resolve => setTimeout(resolve, delay))
    }
  }
}
```

### Getting Help

If you continue experiencing issues:

1. Check the [status page](https://status.events.do) for service health
2. Review [documentation](https://docs.events.do) for API usage
3. Search [community forums](https://community.events.do) for similar issues
4. Contact [support](mailto:support@events.do) with:
   - Error code and full response
   - Request ID (if provided in headers)
   - Timestamp of the error
   - Steps to reproduce
