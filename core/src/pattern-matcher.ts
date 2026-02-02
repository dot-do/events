import picomatch from 'picomatch'
import {
  MAX_PATTERN_LENGTH,
  MAX_PATTERN_SEGMENTS,
  ALLOWED_PATTERN_CHARS,
  MAX_PATTERN_MATCH_ITERATIONS,
} from './config'

// LRU Cache configuration
const MAX_CACHE_SIZE = 1000

/**
 * Error thrown when pattern matching limits are exceeded
 */
export class PatternMatchLimitError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'PatternMatchLimitError'
  }
}

/**
 * Options for pattern matching with limits
 */
export interface PatternMatchOptions {
  /** Maximum iterations for subscription matching (default: MAX_PATTERN_MATCH_ITERATIONS) */
  maxIterations?: number
  /** Maximum pattern depth/segments (default: MAX_PATTERN_SEGMENTS) */
  maxDepth?: number
  /** Whether to validate patterns (default: true) */
  validate?: boolean
}

/**
 * Validate a pattern for safety before matching
 * @throws PatternMatchLimitError if pattern exceeds limits
 */
export function validatePattern(pattern: string, maxDepth: number = MAX_PATTERN_SEGMENTS): void {
  if (pattern.length > MAX_PATTERN_LENGTH) {
    throw new PatternMatchLimitError(`Pattern length ${pattern.length} exceeds maximum ${MAX_PATTERN_LENGTH}`)
  }

  const segments = pattern.split('.')
  if (segments.length > maxDepth) {
    throw new PatternMatchLimitError(`Pattern has ${segments.length} segments, exceeds maximum ${maxDepth}`)
  }

  if (!ALLOWED_PATTERN_CHARS.test(pattern)) {
    throw new PatternMatchLimitError(`Pattern contains invalid characters`)
  }
}

/**
 * Validate an event type for safety before matching
 * @throws PatternMatchLimitError if event type exceeds limits
 */
export function validateEventType(eventType: string, maxDepth: number = MAX_PATTERN_SEGMENTS): void {
  if (eventType.length > MAX_PATTERN_LENGTH) {
    throw new PatternMatchLimitError(`Event type length ${eventType.length} exceeds maximum ${MAX_PATTERN_LENGTH}`)
  }

  const segments = eventType.split('.')
  if (segments.length > maxDepth) {
    throw new PatternMatchLimitError(
      `Event type has ${segments.length} segments, exceeds maximum ${maxDepth}`
    )
  }
}

/**
 * LRU Cache for compiled patterns
 * Uses Map's insertion order property: entries are iterated in insertion order.
 * When accessed, entries are deleted and re-inserted to move them to the end.
 * When evicting, the first entry (oldest/least recently used) is removed.
 */
class LRUCache<K, V> {
  private cache: Map<K, V>
  private maxSize: number

  constructor(maxSize: number) {
    this.cache = new Map()
    this.maxSize = maxSize
  }

  get(key: K): V | undefined {
    const value = this.cache.get(key)
    if (value !== undefined) {
      // Move to end (most recently used) by deleting and re-inserting
      this.cache.delete(key)
      this.cache.set(key, value)
    }
    return value
  }

  set(key: K, value: V): void {
    // If key exists, delete it first (will be re-inserted at end)
    if (this.cache.has(key)) {
      this.cache.delete(key)
    } else if (this.cache.size >= this.maxSize) {
      // Evict the least recently used (first entry)
      const firstKey = this.cache.keys().next().value
      if (firstKey !== undefined) {
        this.cache.delete(firstKey)
      }
    }
    this.cache.set(key, value)
  }

  clear(): void {
    this.cache.clear()
  }

  get size(): number {
    return this.cache.size
  }
}

// Cache compiled patterns for performance with LRU eviction
const patternCache = new LRUCache<string, picomatch.Matcher>(MAX_CACHE_SIZE)

/**
 * Match an event type against a glob pattern
 *
 * Patterns:
 * - "webhook.github.*" matches "webhook.github.push", "webhook.github.pull_request"
 * - "webhook.github.**" matches "webhook.github.push", "webhook.github.issues.opened"
 * - "webhook.github.push" matches exactly "webhook.github.push"
 * - "*.github.*" matches "webhook.github.push", "api.github.call"
 *
 * @param pattern - The glob pattern to match against
 * @param eventType - The event type to match
 * @param options - Optional configuration for limits
 * @throws PatternMatchLimitError if inputs exceed configured limits
 */
export function matchPattern(
  pattern: string,
  eventType: string,
  options: PatternMatchOptions = {}
): boolean {
  const { maxDepth = MAX_PATTERN_SEGMENTS, validate = true } = options

  // Validate inputs if enabled
  if (validate) {
    validatePattern(pattern, maxDepth)
    validateEventType(eventType, maxDepth)
  }

  let matcher = patternCache.get(pattern)

  if (!matcher) {
    // Convert dots to slashes so picomatch treats them as path separators
    // This makes * match only within a segment (not across dots)
    // and ** match across multiple segments
    const pathPattern = pattern.replace(/\./g, '/')
    matcher = picomatch(pathPattern, {
      dot: true,
      nobrace: true,
      noextglob: true,
    })
    patternCache.set(pattern, matcher)
  }

  // Convert event type dots to slashes for matching
  const pathEventType = eventType.replace(/\./g, '/')
  return matcher(pathEventType)
}

/**
 * Extract prefix from pattern for database indexing
 * This allows us to do efficient lookups before applying full pattern matching
 *
 * "webhook.github.*" -> "webhook.github"
 * "webhook.github.push" -> "webhook.github.push"
 * "webhook.**" -> "webhook"
 * "*.github.*" -> "" (no useful prefix)
 *
 * @param pattern - The pattern to extract prefix from
 * @param options - Optional configuration for limits
 * @throws PatternMatchLimitError if pattern exceeds configured limits
 */
export function extractPatternPrefix(pattern: string, options: PatternMatchOptions = {}): string {
  const { maxDepth = MAX_PATTERN_SEGMENTS, validate = true } = options

  if (validate) {
    validatePattern(pattern, maxDepth)
  }

  const parts = pattern.split('.')
  const prefix: string[] = []

  for (const part of parts) {
    if (part === '*' || part === '**' || part.includes('*')) {
      break
    }
    prefix.push(part)
  }

  return prefix.join('.')
}

/**
 * Find all subscriptions that could match an event type
 * First filters by prefix (fast SQL), then applies full pattern matching
 *
 * @param eventType - The event type to match subscriptions against
 * @param subscriptions - Array of subscriptions to search through
 * @param options - Optional configuration for limits
 * @throws PatternMatchLimitError if iteration limits are exceeded
 */
export function findMatchingSubscriptions<T extends { pattern: string; patternPrefix: string }>(
  eventType: string,
  subscriptions: T[],
  options: PatternMatchOptions = {}
): T[] {
  const {
    maxIterations = MAX_PATTERN_MATCH_ITERATIONS,
    maxDepth = MAX_PATTERN_SEGMENTS,
    validate = true,
  } = options

  // Validate event type first
  if (validate) {
    validateEventType(eventType, maxDepth)
  }

  // Track iterations to prevent runaway computation
  let iterations = 0

  // Get all potentially matching prefixes
  const eventParts = eventType.split('.')
  const potentialPrefixes = new Set<string>()

  // Add all prefix lengths: "webhook.github.push" -> ["", "webhook", "webhook.github", "webhook.github.push"]
  potentialPrefixes.add('') // For patterns like "*.github.*"
  for (let i = 1; i <= eventParts.length; i++) {
    potentialPrefixes.add(eventParts.slice(0, i).join('.'))
    iterations++
    if (iterations > maxIterations) {
      throw new PatternMatchLimitError(`Exceeded maximum iterations (${maxIterations}) during prefix generation`)
    }
  }

  // Filter subscriptions that have matching prefix
  const candidates: T[] = []
  for (const sub of subscriptions) {
    iterations++
    if (iterations > maxIterations) {
      throw new PatternMatchLimitError(`Exceeded maximum iterations (${maxIterations}) during prefix filtering`)
    }
    if (potentialPrefixes.has(sub.patternPrefix)) {
      candidates.push(sub)
    }
  }

  // Apply full pattern matching
  const matches: T[] = []
  for (const sub of candidates) {
    iterations++
    if (iterations > maxIterations) {
      throw new PatternMatchLimitError(`Exceeded maximum iterations (${maxIterations}) during pattern matching`)
    }
    // Pass validate: false since we already validated eventType, and patterns should be validated on creation
    if (matchPattern(sub.pattern, eventType, { maxDepth, validate })) {
      matches.push(sub)
    }
  }

  return matches
}

/**
 * Clear the pattern cache (useful for testing)
 */
export function clearPatternCache(): void {
  patternCache.clear()
}

/**
 * Get the current size of the pattern cache (useful for testing)
 */
export function getPatternCacheSize(): number {
  return patternCache.size
}

/**
 * Maximum number of patterns that can be cached before LRU eviction kicks in
 */
export const PATTERN_CACHE_MAX_SIZE = MAX_CACHE_SIZE
