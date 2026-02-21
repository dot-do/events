const MAX_BODY_BYTES = 131_072 // 128KB

export interface CapturedBody {
  body: unknown
  truncated: boolean
  originalSize?: number
}

function truncateObject(body: Record<string, unknown>, originalSize: number, maxBytes: number): CapturedBody {
  const result: Record<string, unknown> = {}
  let budget = maxBytes - 128 // reserve for __truncated + __originalSize keys
  for (const [key, val] of Object.entries(body)) {
    const valJson = JSON.stringify(val)
    if (valJson.length <= budget) {
      result[key] = val
      budget -= valJson.length + key.length + 4
    } else if (typeof val === 'string' && budget > 64) {
      result[key] = val.slice(0, budget - 32) + '\u2026[truncated]'
      budget = 0
    } else {
      break
    }
  }
  result.__truncated = true
  result.__originalSize = originalSize
  return { body: result, truncated: true, originalSize }
}

function truncateArray(body: unknown[], originalSize: number, maxBytes: number): CapturedBody {
  const result: unknown[] = []
  let budget = maxBytes - 128
  for (const item of body) {
    const itemJson = JSON.stringify(item)
    if (itemJson.length <= budget) {
      result.push(item)
      budget -= itemJson.length + 1
    } else break
  }
  return {
    body: { items: result, __truncated: true, __originalSize: originalSize, __totalItems: body.length },
    truncated: true,
    originalSize,
  }
}

export function captureBody(body: unknown, contentLength?: number | string | null): CapturedBody {
  // Fast path: Content-Length tells us it's small
  const knownSize = contentLength ? Number(contentLength) : undefined
  if (knownSize !== undefined && !Number.isNaN(knownSize) && knownSize <= MAX_BODY_BYTES) {
    return { body, truncated: false }
  }

  // Fast path: already a string (raw wire bytes)
  if (typeof body === 'string') {
    if (body.length <= MAX_BODY_BYTES) return { body, truncated: false }
    return {
      body: body.slice(0, MAX_BODY_BYTES - 32) + '\u2026[truncated]',
      truncated: true,
      originalSize: body.length,
    }
  }

  // Primitives are always small enough
  if (body === null || body === undefined || typeof body === 'number' || typeof body === 'boolean') {
    return { body, truncated: false }
  }

  // Stringify once, check, truncate if needed
  const json = JSON.stringify(body)
  if (json.length <= MAX_BODY_BYTES) return { body, truncated: false }

  if (Array.isArray(body)) return truncateArray(body, json.length, MAX_BODY_BYTES)
  if (typeof body === 'object') return truncateObject(body as Record<string, unknown>, json.length, MAX_BODY_BYTES)

  return { body, truncated: false }
}
