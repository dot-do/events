/**
 * Utils Tests - R2 Path Sanitization (Security-Critical)
 *
 * Comprehensive unit tests for security-critical path sanitization utilities
 * that prevent path traversal attacks and ensure safe R2 key generation.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  InvalidR2PathError,
  sanitizePathSegment,
  sanitizeR2Path,
  buildSafeR2Path,
  getAllowedOrigin,
  withTimeout,
  TimeoutError,
  CircuitBreaker,
  CircuitBreakerOpenError,
  protectedCall,
} from '../utils'
import { simpleHash } from '../utils/hash'
import {
  successResponse,
  errorResponse,
  badRequest,
  unauthorized,
  forbidden,
  notFound,
  methodNotAllowed,
  rateLimited,
  internalError,
  notConfigured,
  serviceUnavailable,
  ErrorCodes,
} from '../utils/response'

// ============================================================================
// sanitizePathSegment() Tests
// ============================================================================

describe('sanitizePathSegment', () => {
  describe('valid segments', () => {
    it('accepts simple alphanumeric segments', () => {
      expect(sanitizePathSegment('events')).toBe('events')
      expect(sanitizePathSegment('2024')).toBe('2024')
      expect(sanitizePathSegment('abc123')).toBe('abc123')
    })

    it('accepts segments with hyphens and underscores', () => {
      expect(sanitizePathSegment('my-file')).toBe('my-file')
      expect(sanitizePathSegment('my_file')).toBe('my_file')
      expect(sanitizePathSegment('event-batch-001')).toBe('event-batch-001')
    })

    it('accepts segments with allowed special characters', () => {
      expect(sanitizePathSegment('file(1)')).toBe('file(1)')
      expect(sanitizePathSegment('data[0]')).toBe('data[0]')
      expect(sanitizePathSegment('file@2024')).toBe('file@2024')
    })

    it('accepts file extensions', () => {
      expect(sanitizePathSegment('data.parquet')).toBe('data.parquet')
      expect(sanitizePathSegment('events.jsonl')).toBe('events.jsonl')
      expect(sanitizePathSegment('backup.tar.gz')).toBe('backup.tar.gz')
    })

    it('accepts segments up to 256 characters', () => {
      const longSegment = 'a'.repeat(256)
      expect(sanitizePathSegment(longSegment)).toBe(longSegment)
    })

    it('accepts UUID-style segments', () => {
      expect(sanitizePathSegment('550e8400-e29b-41d4-a716-446655440000')).toBe(
        '550e8400-e29b-41d4-a716-446655440000'
      )
    })
  })

  describe('empty and invalid segments', () => {
    it('rejects empty string', () => {
      expect(() => sanitizePathSegment('')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('')).toThrow('Path segment cannot be empty')
    })

    it('rejects null/undefined equivalent', () => {
      // @ts-expect-error - testing invalid input
      expect(() => sanitizePathSegment(null)).toThrow(InvalidR2PathError)
      // @ts-expect-error - testing invalid input
      expect(() => sanitizePathSegment(undefined)).toThrow(InvalidR2PathError)
    })
  })

  describe('path traversal prevention', () => {
    it('rejects single dot (.)', () => {
      expect(() => sanitizePathSegment('.')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('.')).toThrow('Path traversal sequences are not allowed')
    })

    it('rejects double dot (..)', () => {
      expect(() => sanitizePathSegment('..')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('..')).toThrow('Path traversal sequences are not allowed')
    })

    it('rejects segments starting with dot', () => {
      expect(() => sanitizePathSegment('.hidden')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('.gitignore')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('.env')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('...')).toThrow(InvalidR2PathError)
    })

    it('rejects segments ending with dot', () => {
      expect(() => sanitizePathSegment('file.')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('test.')).toThrow(InvalidR2PathError)
    })

    it('rejects segments containing forward slash', () => {
      expect(() => sanitizePathSegment('path/segment')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('a/b')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('../etc')).toThrow(InvalidR2PathError)
    })

    it('rejects segments containing backslash', () => {
      expect(() => sanitizePathSegment('path\\segment')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('a\\b')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('..\\etc')).toThrow(InvalidR2PathError)
    })
  })

  describe('null byte injection prevention', () => {
    it('rejects null bytes', () => {
      expect(() => sanitizePathSegment('file\0name')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('file\0')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('\0file')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('file\0.txt')).toThrow(InvalidR2PathError)
    })

    it('rejects segments with only null byte', () => {
      expect(() => sanitizePathSegment('\0')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('\0\0')).toThrow(InvalidR2PathError)
    })
  })

  describe('control character prevention', () => {
    it('rejects newline characters', () => {
      expect(() => sanitizePathSegment('file\nname')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('file\rname')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('file\r\nname')).toThrow(InvalidR2PathError)
    })

    it('rejects tab characters', () => {
      expect(() => sanitizePathSegment('file\tname')).toThrow(InvalidR2PathError)
    })

    it('rejects other control characters', () => {
      // Bell character
      expect(() => sanitizePathSegment('file\x07name')).toThrow(InvalidR2PathError)
      // Backspace
      expect(() => sanitizePathSegment('file\x08name')).toThrow(InvalidR2PathError)
      // Form feed
      expect(() => sanitizePathSegment('file\x0cname')).toThrow(InvalidR2PathError)
      // Escape
      expect(() => sanitizePathSegment('file\x1bname')).toThrow(InvalidR2PathError)
      // DEL character
      expect(() => sanitizePathSegment('file\x7fname')).toThrow(InvalidR2PathError)
    })

    it('rejects ASCII control characters (0x00-0x1f)', () => {
      for (let i = 0; i <= 0x1f; i++) {
        const char = String.fromCharCode(i)
        expect(() => sanitizePathSegment(`file${char}name`)).toThrow(InvalidR2PathError)
      }
    })
  })

  describe('length limits', () => {
    it('accepts segment at exactly 256 characters', () => {
      const segment = 'x'.repeat(256)
      expect(sanitizePathSegment(segment)).toBe(segment)
    })

    it('rejects segment exceeding 256 characters', () => {
      const segment = 'x'.repeat(257)
      expect(() => sanitizePathSegment(segment)).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment(segment)).toThrow('exceeds maximum length')
    })
  })

  describe('InvalidR2PathError properties', () => {
    it('includes the invalid path in the error', () => {
      try {
        sanitizePathSegment('..')
      } catch (e) {
        expect(e).toBeInstanceOf(InvalidR2PathError)
        expect((e as InvalidR2PathError).path).toBe('..')
        expect((e as InvalidR2PathError).name).toBe('InvalidR2PathError')
      }
    })
  })
})

// ============================================================================
// sanitizeR2Path() Tests
// ============================================================================

describe('sanitizeR2Path', () => {
  describe('valid paths', () => {
    it('accepts simple valid paths', () => {
      expect(sanitizeR2Path('events')).toBe('events')
      expect(sanitizeR2Path('events/2024')).toBe('events/2024')
      expect(sanitizeR2Path('events/2024/01/file.parquet')).toBe('events/2024/01/file.parquet')
    })

    it('normalizes paths with leading slash', () => {
      expect(sanitizeR2Path('/events')).toBe('events')
      expect(sanitizeR2Path('/events/2024')).toBe('events/2024')
    })

    it('normalizes paths with trailing slash', () => {
      expect(sanitizeR2Path('events/')).toBe('events')
      expect(sanitizeR2Path('events/2024/')).toBe('events/2024')
    })

    it('normalizes paths with multiple consecutive slashes', () => {
      expect(sanitizeR2Path('events//2024')).toBe('events/2024')
      expect(sanitizeR2Path('events///2024///01')).toBe('events/2024/01')
    })

    it('normalizes paths with leading and trailing slashes', () => {
      expect(sanitizeR2Path('/events/2024/')).toBe('events/2024')
      expect(sanitizeR2Path('///events///')).toBe('events')
    })

    it('accepts deep nested paths', () => {
      const path = 'a/b/c/d/e/f/g/h/i/j'
      expect(sanitizeR2Path(path)).toBe(path)
    })

    it('accepts paths with file extensions', () => {
      expect(sanitizeR2Path('events/data.jsonl')).toBe('events/data.jsonl')
      expect(sanitizeR2Path('backup/archive.tar.gz')).toBe('backup/archive.tar.gz')
    })
  })

  describe('empty and invalid paths', () => {
    it('rejects empty string', () => {
      expect(() => sanitizeR2Path('')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('')).toThrow('Path cannot be empty')
    })

    it('rejects path with only slashes', () => {
      expect(() => sanitizeR2Path('/')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('//')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('///')).toThrow(InvalidR2PathError)
    })
  })

  describe('path traversal prevention', () => {
    it('rejects .. at the start', () => {
      expect(() => sanitizeR2Path('../etc/passwd')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('..\\etc\\passwd')).toThrow(InvalidR2PathError)
    })

    it('rejects .. in the middle', () => {
      expect(() => sanitizeR2Path('events/../secrets')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/2024/../../../etc')).toThrow(InvalidR2PathError)
    })

    it('rejects .. at the end', () => {
      expect(() => sanitizeR2Path('events/..')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/2024/..')).toThrow(InvalidR2PathError)
    })

    it('rejects . traversal', () => {
      expect(() => sanitizeR2Path('./events')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/./2024')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/.')).toThrow(InvalidR2PathError)
    })

    it('rejects hidden files/directories', () => {
      expect(() => sanitizeR2Path('events/.hidden')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('.env/secrets')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/.git/config')).toThrow(InvalidR2PathError)
    })

    it('rejects backslash paths', () => {
      expect(() => sanitizeR2Path('events\\2024')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events\\..\\secrets')).toThrow(InvalidR2PathError)
    })
  })

  describe('encoded traversal attempts', () => {
    // Note: These test raw strings - URL decoding happens before this function
    it('rejects literal encoded sequences if decoded', () => {
      // If someone passes already-decoded URL-encoded sequences
      expect(() => sanitizeR2Path('events/../secrets')).toThrow(InvalidR2PathError)
    })

    it('accepts percent signs in path (URL encoding should be decoded before sanitization)', () => {
      // The sanitizer works on decoded strings - %2e%2e should already be decoded to ..
      // If not decoded, %2e is just literal characters which are allowed
      expect(sanitizeR2Path('events/%2e%2e/data')).toBe('events/%2e%2e/data')
    })
  })

  describe('null byte injection prevention', () => {
    it('rejects null bytes anywhere in path', () => {
      expect(() => sanitizeR2Path('events\0/data')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/\0data')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/data\0.txt')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('\0events/data')).toThrow(InvalidR2PathError)
    })

    it('rejects null bytes combined with traversal', () => {
      expect(() => sanitizeR2Path('events/\0../secrets')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/../\0secrets')).toThrow(InvalidR2PathError)
    })
  })

  describe('control character prevention', () => {
    it('rejects newlines in path', () => {
      expect(() => sanitizeR2Path('events\n/data')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/data\r\n/file')).toThrow(InvalidR2PathError)
    })

    it('rejects tabs in path', () => {
      expect(() => sanitizeR2Path('events\t/data')).toThrow(InvalidR2PathError)
    })
  })

  describe('length limits', () => {
    it('accepts path at exactly 1024 characters', () => {
      // Create a path of exactly 1024 characters
      const segments = []
      let length = 0
      while (length < 1020) {
        const segment = 'segment'
        segments.push(segment)
        length += segment.length + 1 // +1 for slash
      }
      // Adjust final segment to hit exactly 1024
      const path = segments.join('/')
      const remainingLength = 1024 - path.length
      if (remainingLength > 0) {
        const finalSegment = 'x'.repeat(remainingLength - 1) // -1 for slash
        segments.push(finalSegment)
      }
      const finalPath = segments.join('/').slice(0, 1024)
      expect(finalPath.length).toBeLessThanOrEqual(1024)
      expect(() => sanitizeR2Path(finalPath)).not.toThrow()
    })

    it('rejects path exceeding 1024 characters', () => {
      const longPath = 'segment/'.repeat(200).slice(0, 1025)
      expect(() => sanitizeR2Path(longPath)).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path(longPath)).toThrow('exceeds maximum length')
    })
  })

  describe('complex attack patterns', () => {
    it('rejects multiple .. sequences', () => {
      expect(() => sanitizeR2Path('events/../../..')).toThrow(InvalidR2PathError)
    })

    it('rejects mixed traversal attempts', () => {
      expect(() => sanitizeR2Path('events/./../data')).toThrow(InvalidR2PathError)
    })

    it('rejects traversal at path boundaries', () => {
      expect(() => sanitizeR2Path('../')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('/..')).toThrow(InvalidR2PathError)
    })
  })
})

// ============================================================================
// buildSafeR2Path() Tests
// ============================================================================

describe('buildSafeR2Path', () => {
  describe('valid path construction', () => {
    it('builds path from prefix only', () => {
      expect(buildSafeR2Path('events')).toBe('events')
      expect(buildSafeR2Path('dedup')).toBe('dedup')
    })

    it('builds path from prefix and segments', () => {
      expect(buildSafeR2Path('events', '2024')).toBe('events/2024')
      expect(buildSafeR2Path('events', '2024', '01', '15')).toBe('events/2024/01/15')
    })

    it('builds path with filename', () => {
      expect(buildSafeR2Path('events', '2024', '01', 'data.jsonl')).toBe('events/2024/01/data.jsonl')
    })

    it('builds path with UUID segments', () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000'
      expect(buildSafeR2Path('dedup', uuid)).toBe(`dedup/${uuid}`)
    })

    it('handles prefix with nested path', () => {
      expect(buildSafeR2Path('events/raw', '2024', 'data.jsonl')).toBe('events/raw/2024/data.jsonl')
    })
  })

  describe('invalid prefix', () => {
    it('rejects empty prefix', () => {
      expect(() => buildSafeR2Path('')).toThrow(InvalidR2PathError)
    })

    it('rejects prefix with traversal', () => {
      expect(() => buildSafeR2Path('../events')).toThrow(InvalidR2PathError)
      expect(() => buildSafeR2Path('events/../secrets')).toThrow(InvalidR2PathError)
    })

    it('rejects prefix with hidden directory', () => {
      expect(() => buildSafeR2Path('.hidden')).toThrow(InvalidR2PathError)
    })
  })

  describe('invalid segments', () => {
    it('rejects segment with traversal', () => {
      expect(() => buildSafeR2Path('events', '..')).toThrow(InvalidR2PathError)
      expect(() => buildSafeR2Path('events', '2024', '..')).toThrow(InvalidR2PathError)
    })

    it('rejects segment starting with dot', () => {
      expect(() => buildSafeR2Path('events', '.hidden')).toThrow(InvalidR2PathError)
    })

    it('rejects segment with slash', () => {
      expect(() => buildSafeR2Path('events', '2024/01')).toThrow(InvalidR2PathError)
    })

    it('rejects segment with null byte', () => {
      expect(() => buildSafeR2Path('events', 'file\0name')).toThrow(InvalidR2PathError)
    })

    it('rejects empty segment', () => {
      expect(() => buildSafeR2Path('events', '')).toThrow(InvalidR2PathError)
    })
  })

  describe('multiple segments validation', () => {
    it('validates all segments before building path', () => {
      // Valid prefix but invalid later segment
      expect(() => buildSafeR2Path('events', '2024', '..', 'secrets')).toThrow(InvalidR2PathError)
    })

    it('rejects if any segment is invalid', () => {
      expect(() => buildSafeR2Path('events', 'valid1', 'valid2', '.hidden', 'valid3')).toThrow(
        InvalidR2PathError
      )
    })
  })

  describe('constructed path final validation', () => {
    it('validates the complete constructed path', () => {
      // Even if individual segments pass, final path is validated
      expect(buildSafeR2Path('events', '2024', '01')).toBe('events/2024/01')
    })
  })
})

// ============================================================================
// getAllowedOrigin() Tests
// ============================================================================

describe('getAllowedOrigin', () => {
  describe('null/empty origin handling', () => {
    it('returns null for null origin', () => {
      expect(getAllowedOrigin(null)).toBeNull()
    })

    it('returns null for empty string origin', () => {
      expect(getAllowedOrigin('')).toBeNull()
    })
  })

  describe('default pattern (*.do domains)', () => {
    it('allows .do domains', () => {
      expect(getAllowedOrigin('https://events.do')).toBe('https://events.do')
      expect(getAllowedOrigin('https://api.do')).toBe('https://api.do')
      expect(getAllowedOrigin('http://test.do')).toBe('http://test.do')
    })

    it('allows nested .do subdomains', () => {
      expect(getAllowedOrigin('https://api.events.do')).toBe('https://api.events.do')
      expect(getAllowedOrigin('https://staging.api.events.do')).toBe('https://staging.api.events.do')
    })

    it('rejects non-.do domains by default', () => {
      expect(getAllowedOrigin('https://example.com')).toBeNull()
      expect(getAllowedOrigin('https://google.com')).toBeNull()
      expect(getAllowedOrigin('https://evil.do.com')).toBeNull()
    })

    it('rejects domains that end with .do but have more after', () => {
      expect(getAllowedOrigin('https://events.domain')).toBeNull()
      expect(getAllowedOrigin('https://events.donor')).toBeNull()
    })
  })

  describe('explicit ALLOWED_ORIGINS env', () => {
    it('allows exact matches from env', () => {
      const env = { ALLOWED_ORIGINS: 'https://example.com,https://app.example.org' }
      expect(getAllowedOrigin('https://example.com', env)).toBe('https://example.com')
      expect(getAllowedOrigin('https://app.example.org', env)).toBe('https://app.example.org')
    })

    it('rejects origins not in explicit list', () => {
      const env = { ALLOWED_ORIGINS: 'https://example.com' }
      expect(getAllowedOrigin('https://other.com', env)).toBeNull()
      expect(getAllowedOrigin('https://events.do', env)).toBeNull() // Default pattern not used when env is set
    })

    it('handles whitespace in ALLOWED_ORIGINS', () => {
      const env = { ALLOWED_ORIGINS: 'https://example.com , https://other.com' }
      expect(getAllowedOrigin('https://example.com', env)).toBe('https://example.com')
      expect(getAllowedOrigin('https://other.com', env)).toBe('https://other.com')
    })
  })

  describe('wildcard patterns in ALLOWED_ORIGINS', () => {
    it('allows wildcard .do pattern', () => {
      const env = { ALLOWED_ORIGINS: '*.do' }
      expect(getAllowedOrigin('https://events.do', env)).toBe('https://events.do')
      expect(getAllowedOrigin('https://api.events.do', env)).toBe('https://api.events.do')
    })

    it('allows wildcard for other domains', () => {
      const env = { ALLOWED_ORIGINS: '*.example.com' }
      expect(getAllowedOrigin('https://app.example.com', env)).toBe('https://app.example.com')
      expect(getAllowedOrigin('https://api.example.com', env)).toBe('https://api.example.com')
    })

    it('allows exact domain with wildcard suffix', () => {
      const env = { ALLOWED_ORIGINS: '*.do' }
      expect(getAllowedOrigin('https://do', env)).toBe('https://do')
    })

    it('rejects domains not matching wildcard', () => {
      const env = { ALLOWED_ORIGINS: '*.example.com' }
      expect(getAllowedOrigin('https://example.org', env)).toBeNull()
      expect(getAllowedOrigin('https://malicious.example.com.evil.com', env)).toBeNull()
    })

    it('handles mixed explicit and wildcard patterns', () => {
      const env = { ALLOWED_ORIGINS: 'https://exact.com,*.do' }
      expect(getAllowedOrigin('https://exact.com', env)).toBe('https://exact.com')
      expect(getAllowedOrigin('https://events.do', env)).toBe('https://events.do')
      expect(getAllowedOrigin('https://other.com', env)).toBeNull()
    })
  })

  describe('invalid origin handling', () => {
    it('handles invalid URL origins gracefully', () => {
      const env = { ALLOWED_ORIGINS: '*.do' }
      expect(getAllowedOrigin('not-a-valid-url', env)).toBeNull()
      expect(getAllowedOrigin('://invalid', env)).toBeNull()
    })

    it('handles malformed origins', () => {
      expect(getAllowedOrigin('javascript:alert(1)')).toBeNull()
      expect(getAllowedOrigin('data:text/html,<script>')).toBeNull()
    })
  })

  describe('security edge cases', () => {
    it('prevents subdomain takeover attempts', () => {
      // Attacker controls evil.events.do.attacker.com
      expect(getAllowedOrigin('https://evil.events.do.attacker.com')).toBeNull()
    })

    it('prevents port-based bypass attempts', () => {
      // Port numbers should not affect domain matching for default pattern
      expect(getAllowedOrigin('https://events.do:8080')).toBeNull() // Default regex doesn't allow ports
    })

    it('handles case sensitivity', () => {
      // URLs are case-sensitive for the scheme and case-insensitive for hostname
      // Our regex is case-sensitive, so uppercase won't match
      expect(getAllowedOrigin('HTTPS://events.do')).toBeNull()
    })

    it('rejects null-byte injection in origin', () => {
      expect(getAllowedOrigin('https://events.do\0.evil.com')).toBeNull()
    })

    it('rejects origins with whitespace', () => {
      expect(getAllowedOrigin(' https://events.do')).toBeNull()
      expect(getAllowedOrigin('https://events.do ')).toBeNull()
    })
  })

  describe('no env provided', () => {
    it('uses default pattern when env is undefined', () => {
      expect(getAllowedOrigin('https://events.do', undefined)).toBe('https://events.do')
    })

    it('uses default pattern when ALLOWED_ORIGINS is not set', () => {
      expect(getAllowedOrigin('https://events.do', {})).toBe('https://events.do')
    })
  })
})

// ============================================================================
// Integration Tests - Combined Security Scenarios
// ============================================================================

describe('Security Integration Tests', () => {
  describe('complete attack scenarios', () => {
    it('prevents directory escape to read sensitive files', () => {
      // Attacker tries to read /etc/passwd
      expect(() => sanitizeR2Path('events/../../../etc/passwd')).toThrow(InvalidR2PathError)
      expect(() => buildSafeR2Path('events', '..', '..', 'etc', 'passwd')).toThrow(InvalidR2PathError)
    })

    it('prevents access to hidden configuration files', () => {
      expect(() => sanitizeR2Path('events/.env')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('.git/config')).toThrow(InvalidR2PathError)
      expect(() => buildSafeR2Path('events', '.aws', 'credentials')).toThrow(InvalidR2PathError)
    })

    it('prevents null-byte truncation attacks', () => {
      // Attacker tries: events/safe.txt\0.exe -> events/safe.txt
      expect(() => sanitizeR2Path('events/safe.txt\0.exe')).toThrow(InvalidR2PathError)
      expect(() => buildSafeR2Path('events', 'safe.txt\0.exe')).toThrow(InvalidR2PathError)
    })

    it('prevents log injection via path', () => {
      // Attacker tries to inject newlines into logs
      expect(() => sanitizeR2Path('events/file\n[CRITICAL] Fake log entry')).toThrow(
        InvalidR2PathError
      )
    })

    it('prevents command injection setup', () => {
      // Paths that might be used in shell commands
      expect(() => sanitizeR2Path('events/file;rm -rf /')).not.toThrow() // Semicolon is OK for path
      expect(() => sanitizeR2Path('events/file\x00;rm -rf /')).toThrow(InvalidR2PathError) // But null byte isn't
    })
  })

  describe('real-world path patterns', () => {
    it('handles typical event storage paths', () => {
      expect(sanitizeR2Path('events/2024/01/15/batch-001.jsonl')).toBe(
        'events/2024/01/15/batch-001.jsonl'
      )
      expect(buildSafeR2Path('events', '2024', '01', '15', 'batch-001.jsonl')).toBe(
        'events/2024/01/15/batch-001.jsonl'
      )
    })

    it('handles deduplication paths', () => {
      const batchId = '550e8400-e29b-41d4-a716-446655440000'
      expect(buildSafeR2Path('dedup', batchId)).toBe(`dedup/${batchId}`)
    })

    it('handles parquet file paths', () => {
      expect(
        buildSafeR2Path('events', '2024', '01', 'compacted', 'events-001.parquet')
      ).toBe('events/2024/01/compacted/events-001.parquet')
    })
  })
})

// ============================================================================
// simpleHash() Tests
// ============================================================================

describe('simpleHash', () => {
  describe('basic functionality', () => {
    it('returns a non-negative integer', () => {
      const hash = simpleHash('test')
      expect(hash).toBeGreaterThanOrEqual(0)
      expect(Number.isInteger(hash)).toBe(true)
    })

    it('returns consistent values for same input', () => {
      const input = 'consistent-input-string'
      expect(simpleHash(input)).toBe(simpleHash(input))
      expect(simpleHash(input)).toBe(simpleHash(input))
    })

    it('returns different values for different inputs', () => {
      const hash1 = simpleHash('input-a')
      const hash2 = simpleHash('input-b')
      expect(hash1).not.toBe(hash2)
    })
  })

  describe('edge cases', () => {
    it('handles empty string', () => {
      const hash = simpleHash('')
      expect(hash).toBe(0)
    })

    it('handles single character', () => {
      const hash = simpleHash('x')
      expect(hash).toBeGreaterThan(0)
    })

    it('handles long strings', () => {
      const longString = 'a'.repeat(10000)
      const hash = simpleHash(longString)
      expect(hash).toBeGreaterThanOrEqual(0)
      expect(Number.isInteger(hash)).toBe(true)
    })

    it('handles special characters', () => {
      expect(simpleHash('!@#$%^&*()')).toBeGreaterThanOrEqual(0)
      expect(simpleHash('emoji: 🎉')).toBeGreaterThanOrEqual(0)
      expect(simpleHash('newline\nand\ttab')).toBeGreaterThanOrEqual(0)
    })

    it('handles unicode', () => {
      expect(simpleHash('日本語')).toBeGreaterThanOrEqual(0)
      expect(simpleHash('العربية')).toBeGreaterThanOrEqual(0)
      expect(simpleHash('🔥🔥🔥')).toBeGreaterThanOrEqual(0)
    })
  })

  describe('distribution for sharding', () => {
    it('produces varied values for sequential inputs', () => {
      const hashes = new Set<number>()
      for (let i = 0; i < 100; i++) {
        hashes.add(simpleHash(`key-${i}`))
      }
      // Should have good distribution (at least 90% unique values)
      expect(hashes.size).toBeGreaterThan(90)
    })

    it('distributes evenly across shard counts', () => {
      const shardCount = 4
      const shardCounts = [0, 0, 0, 0]
      for (let i = 0; i < 1000; i++) {
        const shard = simpleHash(`event-${i}-${Date.now()}`) % shardCount
        shardCounts[shard]++
      }
      // Each shard should get roughly 25% (±10%)
      for (const count of shardCounts) {
        expect(count).toBeGreaterThan(150)
        expect(count).toBeLessThan(350)
      }
    })
  })
})

// ============================================================================
// Response Builders Tests
// ============================================================================

describe('Response Builders', () => {
  describe('successResponse', () => {
    it('returns 200 status by default', async () => {
      const response = successResponse({ message: 'ok' })
      expect(response.status).toBe(200)
    })

    it('returns success envelope with data', async () => {
      const data = { items: [1, 2, 3], total: 3 }
      const response = successResponse(data)
      const body = await response.json()
      expect(body).toEqual({ success: true, data })
    })

    it('accepts custom status', async () => {
      const response = successResponse({ created: true }, { status: 201 })
      expect(response.status).toBe(201)
    })

    it('accepts custom headers', async () => {
      const response = successResponse({ ok: true }, {
        headers: { 'X-Custom': 'value' }
      })
      expect(response.headers.get('X-Custom')).toBe('value')
    })

    it('sets content-type to application/json', async () => {
      const response = successResponse({ ok: true })
      expect(response.headers.get('content-type')).toContain('application/json')
    })
  })

  describe('errorResponse', () => {
    it('returns error envelope with code and message', async () => {
      const response = errorResponse('TEST_ERROR', 'Something went wrong', 400)
      const body = await response.json()
      expect(body).toEqual({
        success: false,
        error: { code: 'TEST_ERROR', message: 'Something went wrong' }
      })
    })

    it('includes details when provided', async () => {
      const response = errorResponse('VALIDATION', 'Invalid input', 400, {
        details: { field: 'email', reason: 'invalid format' }
      })
      const body = await response.json()
      expect(body.error.details).toEqual({ field: 'email', reason: 'invalid format' })
    })

    it('accepts custom headers', async () => {
      const response = errorResponse('ERROR', 'msg', 500, {
        headers: { 'X-Request-Id': 'abc123' }
      })
      expect(response.headers.get('X-Request-Id')).toBe('abc123')
    })
  })

  describe('badRequest', () => {
    it('returns 400 status', () => {
      const response = badRequest('Invalid input')
      expect(response.status).toBe(400)
    })

    it('uses INVALID_REQUEST code by default', async () => {
      const response = badRequest('Invalid input')
      const body = await response.json()
      expect(body.error.code).toBe(ErrorCodes.INVALID_REQUEST)
    })

    it('accepts custom code', async () => {
      const response = badRequest('Invalid JSON', ErrorCodes.INVALID_JSON)
      const body = await response.json()
      expect(body.error.code).toBe(ErrorCodes.INVALID_JSON)
    })
  })

  describe('unauthorized', () => {
    it('returns 401 status', () => {
      const response = unauthorized()
      expect(response.status).toBe(401)
    })

    it('uses default message', async () => {
      const response = unauthorized()
      const body = await response.json()
      expect(body.error.message).toBe('Authentication required')
    })

    it('accepts custom message', async () => {
      const response = unauthorized('Token expired')
      const body = await response.json()
      expect(body.error.message).toBe('Token expired')
    })
  })

  describe('forbidden', () => {
    it('returns 403 status', () => {
      const response = forbidden()
      expect(response.status).toBe(403)
    })

    it('uses FORBIDDEN code', async () => {
      const response = forbidden()
      const body = await response.json()
      expect(body.error.code).toBe(ErrorCodes.FORBIDDEN)
    })
  })

  describe('notFound', () => {
    it('returns 404 status', () => {
      const response = notFound()
      expect(response.status).toBe(404)
    })

    it('uses NOT_FOUND code', async () => {
      const response = notFound('User not found')
      const body = await response.json()
      expect(body.error.code).toBe(ErrorCodes.NOT_FOUND)
      expect(body.error.message).toBe('User not found')
    })
  })

  describe('methodNotAllowed', () => {
    it('returns 405 status', () => {
      const response = methodNotAllowed()
      expect(response.status).toBe(405)
    })
  })

  describe('rateLimited', () => {
    it('returns 429 status', () => {
      const response = rateLimited()
      expect(response.status).toBe(429)
    })

    it('sets Retry-After header when provided', () => {
      const response = rateLimited('Too many requests', 60)
      expect(response.headers.get('Retry-After')).toBe('60')
    })

    it('includes retryAfter in details', async () => {
      const response = rateLimited('Too many requests', 30)
      const body = await response.json()
      expect(body.error.details?.retryAfter).toBe(30)
    })

    it('does not set Retry-After when not provided', () => {
      const response = rateLimited()
      expect(response.headers.get('Retry-After')).toBeNull()
    })
  })

  describe('internalError', () => {
    it('returns 500 status', () => {
      const response = internalError()
      expect(response.status).toBe(500)
    })

    it('uses INTERNAL_ERROR code', async () => {
      const response = internalError()
      const body = await response.json()
      expect(body.error.code).toBe(ErrorCodes.INTERNAL_ERROR)
    })
  })

  describe('notConfigured', () => {
    it('returns 501 status', () => {
      const response = notConfigured()
      expect(response.status).toBe(501)
    })

    it('uses NOT_CONFIGURED code', async () => {
      const response = notConfigured('CDC not enabled')
      const body = await response.json()
      expect(body.error.code).toBe(ErrorCodes.NOT_CONFIGURED)
    })
  })

  describe('serviceUnavailable', () => {
    it('returns 503 status', () => {
      const response = serviceUnavailable()
      expect(response.status).toBe(503)
    })
  })
})

// ============================================================================
// Timeout Utilities Tests
// ============================================================================

describe('withTimeout', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('successful operations', () => {
    it('returns result when promise resolves before timeout', async () => {
      const promise = Promise.resolve('success')
      const result = await withTimeout(promise, 1000, 'Operation timed out')
      expect(result).toBe('success')
    })

    it('handles async functions that complete quickly', async () => {
      const asyncFn = async () => {
        await Promise.resolve()
        return 42
      }
      const result = await withTimeout(asyncFn(), 1000, 'Timed out')
      expect(result).toBe(42)
    })
  })

  describe('timeout behavior', () => {
    it('throws TimeoutError when promise exceeds timeout', async () => {
      const slowPromise = new Promise(resolve => setTimeout(resolve, 5000))
      const resultPromise = withTimeout(slowPromise, 100, 'Too slow')

      vi.advanceTimersByTime(100)

      await expect(resultPromise).rejects.toThrow(TimeoutError)
      await expect(resultPromise).rejects.toThrow('Too slow')
    })

    it('includes timeout duration in TimeoutError', async () => {
      const slowPromise = new Promise(resolve => setTimeout(resolve, 5000))
      const resultPromise = withTimeout(slowPromise, 250, 'Timed out')

      vi.advanceTimersByTime(250)

      try {
        await resultPromise
      } catch (err) {
        expect(err).toBeInstanceOf(TimeoutError)
        expect((err as TimeoutError).timeoutMs).toBe(250)
      }
    })
  })

  describe('edge cases', () => {
    it('returns immediately when timeout is 0 or negative', async () => {
      const promise = Promise.resolve('instant')
      const result = await withTimeout(promise, 0, 'Should not timeout')
      expect(result).toBe('instant')
    })

    it('propagates errors from the original promise', async () => {
      const failingPromise = Promise.reject(new Error('Original error'))
      await expect(withTimeout(failingPromise, 1000, 'Timeout')).rejects.toThrow('Original error')
    })

    it('clears timeout when promise resolves', async () => {
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout')
      const promise = Promise.resolve('done')

      await withTimeout(promise, 1000, 'Timeout')

      expect(clearTimeoutSpy).toHaveBeenCalled()
      clearTimeoutSpy.mockRestore()
    })
  })
})

describe('TimeoutError', () => {
  it('has correct name', () => {
    const error = new TimeoutError('Test timeout', 5000)
    expect(error.name).toBe('TimeoutError')
  })

  it('stores timeout duration', () => {
    const error = new TimeoutError('Test timeout', 3000)
    expect(error.timeoutMs).toBe(3000)
  })

  it('is instance of Error', () => {
    const error = new TimeoutError('Test', 100)
    expect(error).toBeInstanceOf(Error)
  })
})

// ============================================================================
// Circuit Breaker Tests
// ============================================================================

describe('CircuitBreaker', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('initial state', () => {
    it('starts in closed state', () => {
      const breaker = new CircuitBreaker('test')
      expect(breaker.getState()).toBe('closed')
    })

    it('allows execution in closed state', async () => {
      const breaker = new CircuitBreaker('test')
      const result = await breaker.execute(async () => 'success')
      expect(result).toBe('success')
    })
  })

  describe('failure tracking', () => {
    it('opens circuit after reaching failure threshold', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 3 })

      for (let i = 0; i < 3; i++) {
        await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()
      }

      expect(breaker.getState()).toBe('open')
    })

    it('resets failure count on success', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 3 })

      // 2 failures
      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()
      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()

      // 1 success resets
      await breaker.execute(async () => 'ok')

      // 2 more failures don't open (need 3)
      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()
      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()

      expect(breaker.getState()).toBe('closed')
    })
  })

  describe('open state', () => {
    it('throws CircuitBreakerOpenError when open', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })

      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()

      await expect(breaker.execute(async () => 'should not run')).rejects.toThrow(CircuitBreakerOpenError)
    })

    it('includes circuit name in error', async () => {
      const breaker = new CircuitBreaker('my-service', { failureThreshold: 1 })

      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()

      try {
        await breaker.execute(async () => 'x')
      } catch (err) {
        expect(err).toBeInstanceOf(CircuitBreakerOpenError)
        expect((err as CircuitBreakerOpenError).circuitName).toBe('my-service')
      }
    })

    it('includes last error in open error', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })

      await expect(breaker.execute(async () => { throw new Error('original failure') })).rejects.toThrow()

      try {
        await breaker.execute(async () => 'x')
      } catch (err) {
        expect((err as CircuitBreakerOpenError).lastError?.message).toBe('original failure')
      }
    })
  })

  describe('half-open state', () => {
    it('transitions to half-open after reset timeout', async () => {
      const breaker = new CircuitBreaker('test', {
        failureThreshold: 1,
        resetTimeoutMs: 1000
      })

      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()
      expect(breaker.getState()).toBe('open')

      vi.advanceTimersByTime(1000)

      expect(breaker.getState()).toBe('half-open')
    })

    it('closes after successful calls in half-open', async () => {
      const breaker = new CircuitBreaker('test', {
        failureThreshold: 1,
        resetTimeoutMs: 100,
        successThreshold: 2
      })

      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()

      vi.advanceTimersByTime(100)

      await breaker.execute(async () => 'success 1')
      expect(breaker.getState()).toBe('half-open')

      await breaker.execute(async () => 'success 2')
      expect(breaker.getState()).toBe('closed')
    })

    it('reopens on failure in half-open state', async () => {
      const breaker = new CircuitBreaker('test', {
        failureThreshold: 1,
        resetTimeoutMs: 100
      })

      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()

      vi.advanceTimersByTime(100)
      expect(breaker.getState()).toBe('half-open')

      await expect(breaker.execute(async () => { throw new Error('fail again') })).rejects.toThrow()
      expect(breaker.getState()).toBe('open')
    })
  })

  describe('manual controls', () => {
    it('reset() closes the circuit', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })

      await expect(breaker.execute(async () => { throw new Error('fail') })).rejects.toThrow()
      expect(breaker.getState()).toBe('open')

      breaker.reset()
      expect(breaker.getState()).toBe('closed')
    })

    it('trip() opens the circuit', () => {
      const breaker = new CircuitBreaker('test')
      expect(breaker.getState()).toBe('closed')

      breaker.trip(new Error('manual trip'))
      expect(breaker.getState()).toBe('open')
    })
  })

  describe('getStatus', () => {
    it('returns detailed status', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 3 })

      await expect(breaker.execute(async () => { throw new Error('fail1') })).rejects.toThrow()
      await expect(breaker.execute(async () => { throw new Error('fail2') })).rejects.toThrow()

      const status = breaker.getStatus()
      expect(status.state).toBe('closed')
      expect(status.failures).toBe(2)
      expect(status.lastError).toBe('fail2')
    })
  })
})

describe('CircuitBreakerOpenError', () => {
  it('has correct name', () => {
    const error = new CircuitBreakerOpenError('Circuit open', 'test-circuit')
    expect(error.name).toBe('CircuitBreakerOpenError')
  })

  it('stores circuit name', () => {
    const error = new CircuitBreakerOpenError('Circuit open', 'my-circuit')
    expect(error.circuitName).toBe('my-circuit')
  })

  it('stores last error when provided', () => {
    const lastError = new Error('Previous failure')
    const error = new CircuitBreakerOpenError('Circuit open', 'test', lastError)
    expect(error.lastError).toBe(lastError)
  })
})

// ============================================================================
// protectedCall() Tests
// ============================================================================

describe('protectedCall', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  describe('timeout protection', () => {
    it('uses default 5000ms timeout', async () => {
      const slowFn = () => new Promise(resolve => setTimeout(resolve, 10000))
      const promise = protectedCall(slowFn, { errorMsg: 'Test' })

      vi.advanceTimersByTime(5000)

      await expect(promise).rejects.toThrow(TimeoutError)
    })

    it('respects custom timeout', async () => {
      const slowFn = () => new Promise(resolve => setTimeout(resolve, 10000))
      const promise = protectedCall(slowFn, { timeoutMs: 100, errorMsg: 'Custom' })

      vi.advanceTimersByTime(100)

      await expect(promise).rejects.toThrow('Custom timed out after 100ms')
    })

    it('returns result when successful', async () => {
      const result = await protectedCall(async () => 'success')
      expect(result).toBe('success')
    })
  })

  describe('circuit breaker integration', () => {
    it('uses circuit breaker when provided', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })

      await expect(
        protectedCall(async () => { throw new Error('fail') }, { circuitBreaker: breaker })
      ).rejects.toThrow()

      await expect(
        protectedCall(async () => 'ok', { circuitBreaker: breaker })
      ).rejects.toThrow(CircuitBreakerOpenError)
    })

    it('works without circuit breaker', async () => {
      const result = await protectedCall(async () => 'no breaker')
      expect(result).toBe('no breaker')
    })
  })

  describe('combined protection', () => {
    it('timeout triggers circuit breaker failure', async () => {
      const breaker = new CircuitBreaker('test', { failureThreshold: 1 })
      const slowFn = () => new Promise(resolve => setTimeout(resolve, 10000))

      const promise = protectedCall(slowFn, {
        timeoutMs: 100,
        circuitBreaker: breaker,
        errorMsg: 'DO call'
      })

      vi.advanceTimersByTime(100)

      await expect(promise).rejects.toThrow(TimeoutError)
      expect(breaker.getState()).toBe('open')
    })
  })
})
