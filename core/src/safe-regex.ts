/**
 * Safe regex utilities to prevent ReDoS (Regular Expression Denial of Service) attacks
 *
 * This module provides functions for safely creating and executing regex patterns,
 * particularly for user-provided patterns in schema validation.
 */

// ============================================================================
// Constants
// ============================================================================

/** Maximum allowed pattern length */
export const MAX_PATTERN_LENGTH = 256

/** Maximum allowed input length for regex matching */
export const MAX_INPUT_LENGTH = 10000

/** Maximum allowed quantifier value (e.g., {1000} would be rejected) */
export const MAX_QUANTIFIER = 100

/** Maximum execution time for regex matching in milliseconds */
export const MAX_REGEX_EXECUTION_MS = 10

// ============================================================================
// ReDoS Detection
// ============================================================================

/**
 * Patterns that indicate potential ReDoS vulnerabilities
 *
 * These patterns detect:
 * - Nested quantifiers: (a+)+ (a*)*
 * - Overlapping alternations with quantifiers: (a|a)+
 * - Quantified groups that could overlap: (.*a)+
 */
const DANGEROUS_PATTERNS = [
  // Nested quantifiers - most common ReDoS patterns
  /(\+|\*|\?|\{[0-9,]+\})\s*\)(\+|\*|\?|\{[0-9,]+\})/,
  // Groups with quantifiers containing quantified elements
  /\([^)]*(\+|\*|\{[0-9,]+\})[^)]*\)(\+|\*|\{[0-9,]+\})/,
  // Backreference with quantifier (can cause exponential matching)
  /\\[1-9]\d*(\+|\*|\{[0-9,]+\})/,
  // Alternation with overlapping patterns
  /\([^)]*\|[^)]*\)(\+|\*|\{[0-9,]+\})/,
  // Multiple consecutive quantifiers
  /(\+|\*|\?)(\+|\*|\?)/,
]

/**
 * Characters that need escaping in regex but are commonly provided raw
 */
const SPECIAL_CHARS = /[[\]{}()*+?.,\\^$|#\s]/g

/**
 * Result of regex safety analysis
 */
export interface RegexSafetyResult {
  safe: boolean
  reason?: string
  pattern?: string
}

/**
 * Analyze a regex pattern for potential ReDoS vulnerabilities
 */
export function analyzeRegexSafety(pattern: string): RegexSafetyResult {
  // Check pattern length
  if (pattern.length > MAX_PATTERN_LENGTH) {
    return {
      safe: false,
      reason: `Pattern exceeds maximum length of ${MAX_PATTERN_LENGTH} characters`,
    }
  }

  // Check for empty pattern
  if (pattern.length === 0) {
    return {
      safe: false,
      reason: 'Pattern cannot be empty',
    }
  }

  // Check for overly large quantifiers
  const quantifierMatch = pattern.match(/\{(\d+)(?:,(\d*))?\}/g)
  if (quantifierMatch) {
    for (const q of quantifierMatch) {
      const nums = q.match(/\d+/g)
      if (nums) {
        for (const n of nums) {
          if (parseInt(n, 10) > MAX_QUANTIFIER) {
            return {
              safe: false,
              reason: `Quantifier value exceeds maximum of ${MAX_QUANTIFIER}`,
            }
          }
        }
      }
    }
  }

  // Check for dangerous patterns that could cause ReDoS
  for (const dangerous of DANGEROUS_PATTERNS) {
    if (dangerous.test(pattern)) {
      return {
        safe: false,
        reason: 'Pattern contains potentially dangerous constructs that could cause exponential backtracking',
      }
    }
  }

  // Check for excessive grouping depth
  let maxDepth = 0
  let currentDepth = 0
  for (const char of pattern) {
    if (char === '(') {
      currentDepth++
      maxDepth = Math.max(maxDepth, currentDepth)
    } else if (char === ')') {
      currentDepth--
    }
  }
  if (maxDepth > 5) {
    return {
      safe: false,
      reason: 'Pattern has excessive nesting depth',
    }
  }

  // Check for unbalanced parentheses
  if (currentDepth !== 0) {
    return {
      safe: false,
      reason: 'Pattern has unbalanced parentheses',
    }
  }

  // Check for valid regex syntax
  try {
    new RegExp(pattern)
  } catch (e) {
    return {
      safe: false,
      reason: `Invalid regex pattern: ${e instanceof Error ? e.message : 'unknown error'}`,
    }
  }

  return { safe: true, pattern }
}

// ============================================================================
// Safe Regex Execution
// ============================================================================

/**
 * Options for safe regex matching
 */
export interface SafeRegexOptions {
  /** Maximum time in milliseconds for regex execution (default: MAX_REGEX_EXECUTION_MS) */
  timeoutMs?: number
  /** Maximum input length to match against (default: MAX_INPUT_LENGTH) */
  maxInputLength?: number
  /** Whether to truncate input instead of failing if too long (default: false) */
  truncateInput?: boolean
}

/**
 * Result of safe regex execution
 */
export interface SafeRegexResult {
  matched: boolean
  timedOut?: boolean | undefined
  error?: string | undefined
  /** The actual match result if available */
  match?: RegExpMatchArray | null | undefined
}

/**
 * Create a safe regex from a pattern string
 * Returns null if the pattern is unsafe
 */
export function createSafeRegex(pattern: string, flags?: string): RegExp | null {
  const analysis = analyzeRegexSafety(pattern)
  if (!analysis.safe) {
    return null
  }
  try {
    return new RegExp(pattern, flags)
  } catch {
    return null
  }
}

/**
 * Execute a regex match with timeout protection
 *
 * Note: True regex timeout requires native support which isn't available in JS.
 * This implementation uses:
 * 1. Input length limiting
 * 2. Pattern complexity analysis (done before calling this)
 * 3. Simple time tracking (checked after execution)
 *
 * For true timeout protection in a Workers environment, consider:
 * - Using the RE2 library (not available in Workers)
 * - Moving to an edge function with native timeout
 */
export function safeRegexTest(
  regex: RegExp,
  input: string,
  options: SafeRegexOptions = {}
): SafeRegexResult {
  const {
    maxInputLength = MAX_INPUT_LENGTH,
    truncateInput = false,
  } = options

  // Check input length
  if (input.length > maxInputLength) {
    if (truncateInput) {
      input = input.slice(0, maxInputLength)
    } else {
      return {
        matched: false,
        error: `Input exceeds maximum length of ${maxInputLength} characters`,
      }
    }
  }

  const startTime = performance.now()

  try {
    const result = regex.test(input)
    const elapsed = performance.now() - startTime

    // If execution took too long, log a warning
    // Note: we can't actually interrupt the regex, but we can track slow patterns
    if (elapsed > MAX_REGEX_EXECUTION_MS) {
      console.warn(`[safe-regex] Slow regex execution: ${elapsed.toFixed(2)}ms for pattern ${regex.source.slice(0, 50)}...`)
    }

    return { matched: result }
  } catch (e) {
    return {
      matched: false,
      error: e instanceof Error ? e.message : 'Regex execution failed',
    }
  }
}

/**
 * Safe regex match that combines pattern analysis and execution safety
 */
export function safeRegexMatch(
  pattern: string,
  input: string,
  options: SafeRegexOptions = {}
): SafeRegexResult {
  // Analyze pattern safety
  const analysis = analyzeRegexSafety(pattern)
  if (!analysis.safe) {
    return {
      matched: false,
      error: analysis.reason,
    }
  }

  // Create regex
  let regex: RegExp
  try {
    regex = new RegExp(pattern)
  } catch (e) {
    return {
      matched: false,
      error: `Invalid regex: ${e instanceof Error ? e.message : 'unknown'}`,
    }
  }

  // Execute with safety measures
  return safeRegexTest(regex, input, options)
}

// ============================================================================
// Pattern Validation for Schema Registration
// ============================================================================

/**
 * Validate a pattern for use in JSON Schema validation
 * This is called when registering a schema to ensure the pattern is safe
 */
export function validateSchemaPattern(pattern: string): { valid: boolean; error?: string | undefined } {
  const analysis = analyzeRegexSafety(pattern)
  if (!analysis.safe) {
    return { valid: false, error: analysis.reason }
  }
  return { valid: true }
}

/**
 * Compiled regex cache to avoid recompiling safe patterns
 */
const regexCache = new Map<string, RegExp>()

/** Maximum cache size to prevent memory exhaustion */
const MAX_CACHE_SIZE = 1000

/**
 * Get or create a cached safe regex
 * Returns null if the pattern is unsafe
 */
export function getCachedSafeRegex(pattern: string, flags?: string): RegExp | null {
  const cacheKey = `${pattern}:${flags ?? ''}`

  // Check cache
  const cached = regexCache.get(cacheKey)
  if (cached) {
    return cached
  }

  // Validate and create
  const regex = createSafeRegex(pattern, flags)
  if (!regex) {
    return null
  }

  // Cache with size limit
  if (regexCache.size >= MAX_CACHE_SIZE) {
    // Simple eviction: delete first entry
    const firstKey = regexCache.keys().next().value
    if (firstKey) {
      regexCache.delete(firstKey)
    }
  }
  regexCache.set(cacheKey, regex)

  return regex
}

/**
 * Clear the regex cache (for testing)
 */
export function clearRegexCache(): void {
  regexCache.clear()
}
