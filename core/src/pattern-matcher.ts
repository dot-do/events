import picomatch from 'picomatch'

// Cache compiled patterns for performance
const patternCache = new Map<string, picomatch.Matcher>()

/**
 * Match an event type against a glob pattern
 *
 * Patterns:
 * - "webhook.github.*" matches "webhook.github.push", "webhook.github.pull_request"
 * - "webhook.github.**" matches "webhook.github.push", "webhook.github.issues.opened"
 * - "webhook.github.push" matches exactly "webhook.github.push"
 * - "*.github.*" matches "webhook.github.push", "api.github.call"
 */
export function matchPattern(pattern: string, eventType: string): boolean {
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
 */
export function extractPatternPrefix(pattern: string): string {
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
 */
export function findMatchingSubscriptions<T extends { pattern: string; patternPrefix: string }>(
  eventType: string,
  subscriptions: T[]
): T[] {
  // Get all potentially matching prefixes
  const eventParts = eventType.split('.')
  const potentialPrefixes = new Set<string>()

  // Add all prefix lengths: "webhook.github.push" -> ["", "webhook", "webhook.github", "webhook.github.push"]
  potentialPrefixes.add('') // For patterns like "*.github.*"
  for (let i = 1; i <= eventParts.length; i++) {
    potentialPrefixes.add(eventParts.slice(0, i).join('.'))
  }

  // Filter subscriptions that have matching prefix
  const candidates = subscriptions.filter((sub) => potentialPrefixes.has(sub.patternPrefix))

  // Apply full pattern matching
  return candidates.filter((sub) => matchPattern(sub.pattern, eventType))
}

/**
 * Clear the pattern cache (useful for testing)
 */
export function clearPatternCache(): void {
  patternCache.clear()
}
