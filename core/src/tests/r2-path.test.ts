/**
 * Tests for R2 path sanitization utilities
 */

import { describe, it, expect } from 'vitest'
import {
  InvalidR2PathError,
  sanitizePathSegment,
  sanitizeR2Path,
  buildSafeR2Path,
  validateNamespaceOrCollection,
} from '../r2-path.js'

describe('sanitizePathSegment', () => {
  describe('valid segments', () => {
    it('accepts alphanumeric segments', () => {
      expect(sanitizePathSegment('events')).toBe('events')
      expect(sanitizePathSegment('file123')).toBe('file123')
      expect(sanitizePathSegment('2024')).toBe('2024')
    })

    it('accepts segments with hyphens and underscores', () => {
      expect(sanitizePathSegment('my-file')).toBe('my-file')
      expect(sanitizePathSegment('my_file')).toBe('my_file')
      expect(sanitizePathSegment('event-2024_01')).toBe('event-2024_01')
    })

    it('accepts segments with file extensions', () => {
      expect(sanitizePathSegment('file.parquet')).toBe('file.parquet')
      expect(sanitizePathSegment('data.json')).toBe('data.json')
      expect(sanitizePathSegment('manifest.txt')).toBe('manifest.txt')
    })
  })

  describe('path traversal prevention', () => {
    it('rejects double dot (..) sequences', () => {
      expect(() => sanitizePathSegment('..')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('..').message).toThrow('Path traversal sequences are not allowed')
    })

    it('rejects single dot (.) segments', () => {
      expect(() => sanitizePathSegment('.')).toThrow(InvalidR2PathError)
    })

    it('rejects segments starting with dot', () => {
      expect(() => sanitizePathSegment('.hidden')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('..')).toThrow(InvalidR2PathError)
    })

    it('rejects segments ending with dot', () => {
      expect(() => sanitizePathSegment('file.')).toThrow(InvalidR2PathError)
    })

    it('rejects segments containing forward slashes', () => {
      expect(() => sanitizePathSegment('path/to/file')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('../etc')).toThrow(InvalidR2PathError)
    })

    it('rejects segments containing backslashes', () => {
      expect(() => sanitizePathSegment('path\\to\\file')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('..\\etc')).toThrow(InvalidR2PathError)
    })
  })

  describe('injection prevention', () => {
    it('rejects null bytes', () => {
      expect(() => sanitizePathSegment('file\0name')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('\0')).toThrow(InvalidR2PathError)
    })

    it('rejects control characters', () => {
      expect(() => sanitizePathSegment('file\nname')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('file\rname')).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment('file\tname')).toThrow(InvalidR2PathError)
    })
  })

  describe('edge cases', () => {
    it('rejects empty segments', () => {
      expect(() => sanitizePathSegment('')).toThrow(InvalidR2PathError)
    })

    it('rejects segments exceeding max length', () => {
      const longSegment = 'a'.repeat(257)
      expect(() => sanitizePathSegment(longSegment)).toThrow(InvalidR2PathError)
      expect(() => sanitizePathSegment(longSegment).message).toThrow('exceeds maximum length')
    })

    it('accepts segments at max length', () => {
      const maxSegment = 'a'.repeat(256)
      expect(sanitizePathSegment(maxSegment)).toBe(maxSegment)
    })
  })
})

describe('sanitizeR2Path', () => {
  describe('valid paths', () => {
    it('accepts simple paths', () => {
      expect(sanitizeR2Path('events')).toBe('events')
      expect(sanitizeR2Path('events/2024')).toBe('events/2024')
    })

    it('accepts multi-level paths', () => {
      expect(sanitizeR2Path('events/2024/01/15')).toBe('events/2024/01/15')
      expect(sanitizeR2Path('ns/collection/deltas')).toBe('ns/collection/deltas')
    })

    it('normalizes leading/trailing slashes', () => {
      expect(sanitizeR2Path('/events/')).toBe('events')
      expect(sanitizeR2Path('//events//')).toBe('events')
    })

    it('normalizes multiple consecutive slashes', () => {
      expect(sanitizeR2Path('events//2024///01')).toBe('events/2024/01')
    })
  })

  describe('path traversal prevention', () => {
    it('rejects paths with .. segments', () => {
      expect(() => sanitizeR2Path('../etc/passwd')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/../secrets')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/2024/../../secrets')).toThrow(InvalidR2PathError)
    })

    it('rejects paths with . segments', () => {
      expect(() => sanitizeR2Path('./events')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/./data')).toThrow(InvalidR2PathError)
    })

    it('rejects paths with hidden file segments', () => {
      expect(() => sanitizeR2Path('.hidden/file')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('events/.secret')).toThrow(InvalidR2PathError)
    })

    it('rejects backslash paths', () => {
      expect(() => sanitizeR2Path('events\\2024\\01')).toThrow(InvalidR2PathError)
    })
  })

  describe('edge cases', () => {
    it('rejects empty paths', () => {
      expect(() => sanitizeR2Path('')).toThrow(InvalidR2PathError)
    })

    it('rejects paths with only slashes', () => {
      expect(() => sanitizeR2Path('/')).toThrow(InvalidR2PathError)
      expect(() => sanitizeR2Path('//')).toThrow(InvalidR2PathError)
    })

    it('rejects paths exceeding max length', () => {
      const longPath = 'a'.repeat(1025)
      expect(() => sanitizeR2Path(longPath)).toThrow(InvalidR2PathError)
    })

    it('accepts paths at max length with valid segments', () => {
      // Build a path that's close to max length but with valid segments (<256 chars each)
      const segments = ['events', '2024', '01', '15', 'a'.repeat(200)]
      const maxPath = segments.join('/')
      expect(sanitizeR2Path(maxPath)).toBe(maxPath)
    })

    it('rejects null bytes in paths', () => {
      expect(() => sanitizeR2Path('events\0/data')).toThrow(InvalidR2PathError)
    })

    it('rejects control characters in paths', () => {
      expect(() => sanitizeR2Path('events\n/data')).toThrow(InvalidR2PathError)
    })
  })
})

describe('buildSafeR2Path', () => {
  describe('basic usage', () => {
    it('builds path from prefix and segments', () => {
      expect(buildSafeR2Path('dedup', 'batch123')).toBe('dedup/batch123')
      expect(buildSafeR2Path('events', '2024', '01', '15')).toBe('events/2024/01/15')
    })

    it('handles prefix with existing segments', () => {
      expect(buildSafeR2Path('ns/collection', 'deltas')).toBe('ns/collection/deltas')
    })
  })

  describe('security', () => {
    it('rejects path traversal in prefix', () => {
      expect(() => buildSafeR2Path('../etc', 'passwd')).toThrow(InvalidR2PathError)
    })

    it('rejects path traversal in segments', () => {
      expect(() => buildSafeR2Path('events', '..', 'secrets')).toThrow(InvalidR2PathError)
      expect(() => buildSafeR2Path('dedup', '../etc/passwd')).toThrow(InvalidR2PathError)
    })

    it('rejects hidden files in segments', () => {
      expect(() => buildSafeR2Path('events', '.hidden')).toThrow(InvalidR2PathError)
    })
  })
})

describe('validateNamespaceOrCollection', () => {
  describe('valid names', () => {
    it('accepts alphanumeric names', () => {
      expect(validateNamespaceOrCollection('myworker')).toBe('myworker')
      expect(validateNamespaceOrCollection('worker123')).toBe('worker123')
      expect(validateNamespaceOrCollection('MyApp')).toBe('MyApp')
    })

    it('accepts names with hyphens and underscores', () => {
      expect(validateNamespaceOrCollection('my-worker')).toBe('my-worker')
      expect(validateNamespaceOrCollection('my_worker')).toBe('my_worker')
      expect(validateNamespaceOrCollection('worker-v2_prod')).toBe('worker-v2_prod')
    })
  })

  describe('invalid names', () => {
    it('rejects empty names', () => {
      expect(() => validateNamespaceOrCollection('')).toThrow(InvalidR2PathError)
    })

    it('rejects names starting with non-alphanumeric', () => {
      expect(() => validateNamespaceOrCollection('-worker')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('_worker')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('.worker')).toThrow(InvalidR2PathError)
    })

    it('rejects names with slashes', () => {
      expect(() => validateNamespaceOrCollection('ns/collection')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('../secrets')).toThrow(InvalidR2PathError)
    })

    it('rejects names with special characters', () => {
      expect(() => validateNamespaceOrCollection('worker@v2')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('worker.v2')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('worker#1')).toThrow(InvalidR2PathError)
    })

    it('rejects names exceeding max length', () => {
      const longName = 'a'.repeat(65)
      expect(() => validateNamespaceOrCollection(longName)).toThrow(InvalidR2PathError)
    })

    it('rejects reserved names', () => {
      expect(() => validateNamespaceOrCollection('.')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('..')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('CON')).toThrow(InvalidR2PathError)
      expect(() => validateNamespaceOrCollection('nul')).toThrow(InvalidR2PathError)
    })
  })

  describe('error messages', () => {
    it('includes type in error message for namespace', () => {
      try {
        validateNamespaceOrCollection('', 'namespace')
      } catch (e) {
        expect((e as Error).message).toContain('namespace')
      }
    })

    it('includes type in error message for collection', () => {
      try {
        validateNamespaceOrCollection('', 'collection')
      } catch (e) {
        expect((e as Error).message).toContain('collection')
      }
    })
  })
})

describe('InvalidR2PathError', () => {
  it('includes the invalid path in the error', () => {
    const error = new InvalidR2PathError('Test error', '../bad/path')
    expect(error.path).toBe('../bad/path')
    expect(error.message).toBe('Test error')
    expect(error.name).toBe('InvalidR2PathError')
  })
})
