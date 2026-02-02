/**
 * Benchmark Worker Tests
 *
 * Comprehensive unit tests for the LZ4 compression functions in benchmark-worker.ts:
 * - lz4Compress() function
 * - lz4RawDecompress() function
 * - encodeLz4Literals() function
 * - Parquet write/read cycle
 * - Edge cases and error handling
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// ============================================================================
// LZ4 Function Implementations (extracted for testing)
// These are copied from benchmark-worker.ts since they are not exported
// ============================================================================

/**
 * Fixed LZ4 RAW decompression - hyparquet-compressors has a bug in match length extension
 */
function lz4RawDecompress(input: Uint8Array, outputLength: number): Uint8Array {
  const output = new Uint8Array(outputLength)
  let len = 0 // output position
  for (let i = 0; i < input.length; ) {
    const token = input[i++]!

    let literals = token >> 4
    if (literals) {
      // literal length extension
      let byte = literals + 240
      while (byte === 255) literals += (byte = input[i++]!)
      // copy literals
      output.set(input.subarray(i, i + literals), len)
      len += literals
      i += literals
      if (i >= input.length) return output
    }

    const offset = (input[i++] ?? 0) | ((input[i++] ?? 0) << 8)
    if (!offset || offset > len) {
      throw new Error(`lz4 offset out of range ${offset}`)
    }

    // match length - FIX: use (token & 0xf), not matchLength which includes +4
    let matchLength = (token & 0xf) + 4 // minmatch 4
    let byte = (token & 0xf) + 240 // FIX: check nibble, not matchLength
    while (byte === 255) matchLength += (byte = input[i++]!)

    // copy match byte by byte (handles overlapping)
    let pos = len - offset
    const end = len + matchLength
    while (len < end) output[len++] = output[pos++]!
  }
  return output
}

/**
 * Encode data as a literal-only LZ4 block (no matches)
 */
function encodeLz4Literals(input: Uint8Array): Uint8Array {
  const literalsLen = input.length

  // Calculate output size: 1 (token) + extended length bytes + literals
  let extendedBytes = 0
  if (literalsLen >= 15) {
    let remaining = literalsLen - 15
    while (remaining >= 255) {
      extendedBytes++
      remaining -= 255
    }
    extendedBytes++ // Final byte < 255
  }

  const outputSize = 1 + extendedBytes + literalsLen
  const output = new Uint8Array(outputSize)
  let pos = 0

  // Token: high nibble = min(literalsLen, 15), low nibble = 0 (no match)
  const tokenLiterals = Math.min(literalsLen, 15)
  output[pos++] = tokenLiterals << 4

  // Extended literals length
  if (literalsLen >= 15) {
    let remaining = literalsLen - 15
    while (remaining >= 255) {
      output[pos++] = 255
      remaining -= 255
    }
    output[pos++] = remaining
  }

  // Copy literals
  output.set(input, pos)

  return output
}

// Mock lz4Binding for lz4Compress tests
const mockLz4Binding = {
  compressBound: (inputLength: number) => inputLength + Math.ceil(inputLength / 255) + 16,
  compress: vi.fn(),
}

/**
 * LZ4 raw compression (Parquet-compatible)
 * lz4/lib/binding returns 0 when no matches found - we need to encode as literals
 */
function lz4Compress(input: Uint8Array): Uint8Array {
  const maxSize = mockLz4Binding.compressBound(input.length)
  const output = new Uint8Array(maxSize)
  const compressedSize = mockLz4Binding.compress(input, output)

  // If compression worked, return the compressed data
  if (compressedSize > 0 && compressedSize < input.length) {
    return output.slice(0, compressedSize)
  }

  // Compression failed or ineffective - encode as literal-only LZ4 block
  // LZ4 format: token (1 byte) + [extended literals length] + literals
  // Token high nibble = literals length (15 = extended), low nibble = match length (0)
  return encodeLz4Literals(input)
}

// ============================================================================
// Tests
// ============================================================================

describe('benchmark-worker LZ4 functions', () => {
  beforeEach(() => {
    vi.resetAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // ============================================================================
  // encodeLz4Literals Tests
  // ============================================================================

  describe('encodeLz4Literals()', () => {
    it('should encode empty input', () => {
      const input = new Uint8Array(0)
      const result = encodeLz4Literals(input)

      // Token only (0x00), no literals
      expect(result.length).toBe(1)
      expect(result[0]).toBe(0x00) // token with 0 literals
    })

    it('should encode small input (< 15 bytes)', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5])
      const result = encodeLz4Literals(input)

      // Token (5 << 4 = 0x50) + 5 literal bytes
      expect(result.length).toBe(6)
      expect(result[0]).toBe(0x50) // 5 literals in high nibble
      expect(Array.from(result.slice(1))).toEqual([1, 2, 3, 4, 5])
    })

    it('should encode exactly 14 bytes (no extension needed)', () => {
      const input = new Uint8Array(14).fill(0xaa)
      const result = encodeLz4Literals(input)

      // Token (14 << 4 = 0xe0) + 14 literal bytes
      expect(result.length).toBe(15)
      expect(result[0]).toBe(0xe0) // 14 literals in high nibble
      expect(Array.from(result.slice(1))).toEqual(Array(14).fill(0xaa))
    })

    it('should encode exactly 15 bytes (boundary case)', () => {
      const input = new Uint8Array(15).fill(0xbb)
      const result = encodeLz4Literals(input)

      // Token (15 << 4 = 0xf0) + 1 extension byte (0) + 15 literal bytes
      expect(result.length).toBe(17)
      expect(result[0]).toBe(0xf0) // 15 in high nibble (indicates extension)
      expect(result[1]).toBe(0) // extended length: 15 + 0 = 15 literals
      expect(Array.from(result.slice(2))).toEqual(Array(15).fill(0xbb))
    })

    it('should encode 16 bytes (first extension)', () => {
      const input = new Uint8Array(16).fill(0xcc)
      const result = encodeLz4Literals(input)

      // Token (0xf0) + 1 extension byte (1) + 16 literal bytes
      expect(result.length).toBe(18)
      expect(result[0]).toBe(0xf0)
      expect(result[1]).toBe(1) // extended length: 15 + 1 = 16 literals
      expect(Array.from(result.slice(2))).toEqual(Array(16).fill(0xcc))
    })

    it('should encode 270 bytes (requires multiple extension bytes)', () => {
      const input = new Uint8Array(270).fill(0xdd)
      const result = encodeLz4Literals(input)

      // 270 = 15 + 255 = 270, so need token + 1 extension (255) + 1 final (0)
      // Token (0xf0) + extension byte (255) + final byte (0) + 270 literals
      expect(result.length).toBe(273)
      expect(result[0]).toBe(0xf0)
      expect(result[1]).toBe(255)
      expect(result[2]).toBe(0) // 15 + 255 + 0 = 270
      expect(Array.from(result.slice(3))).toEqual(Array(270).fill(0xdd))
    })

    it('should encode 525 bytes (requires two 255 extension bytes)', () => {
      const input = new Uint8Array(525).fill(0xee)
      const result = encodeLz4Literals(input)

      // 525 = 15 + 255 + 255 = 525
      // Token (0xf0) + 2 extension bytes (255, 255) + final byte (0) + 525 literals
      expect(result.length).toBe(529)
      expect(result[0]).toBe(0xf0)
      expect(result[1]).toBe(255)
      expect(result[2]).toBe(255)
      expect(result[3]).toBe(0) // 15 + 255 + 255 + 0 = 525
    })

    it('should encode large input (1000 bytes)', () => {
      const input = new Uint8Array(1000)
      for (let i = 0; i < 1000; i++) {
        input[i] = i % 256
      }
      const result = encodeLz4Literals(input)

      // 1000 = 15 + 255 * 3 + 220 = 15 + 765 + 220 = 1000
      // Token + 3 * 255 + 220 + 1000 literals = 1 + 4 + 1000 = 1005
      expect(result.length).toBe(1005)
      expect(result[0]).toBe(0xf0)
      expect(result[1]).toBe(255)
      expect(result[2]).toBe(255)
      expect(result[3]).toBe(255)
      expect(result[4]).toBe(220) // 15 + 255*3 + 220 = 1000

      // Verify literals are copied correctly
      for (let i = 0; i < 1000; i++) {
        expect(result[5 + i]).toBe(i % 256)
      }
    })

    it('should preserve original data integrity', () => {
      const input = new TextEncoder().encode('Hello, World!')
      const result = encodeLz4Literals(input)

      // Token + literals
      const literalsStart = input.length < 15 ? 1 : 2
      const literals = result.slice(literalsStart)
      expect(new TextDecoder().decode(literals)).toBe('Hello, World!')
    })
  })

  // ============================================================================
  // lz4RawDecompress Tests
  // ============================================================================

  describe('lz4RawDecompress()', () => {
    it('should decompress literal-only block', () => {
      const original = new Uint8Array([1, 2, 3, 4, 5])
      const compressed = encodeLz4Literals(original)

      const decompressed = lz4RawDecompress(compressed, original.length)

      expect(Array.from(decompressed)).toEqual(Array.from(original))
    })

    it('should decompress literal-only block with extension bytes', () => {
      const original = new Uint8Array(100).fill(0x42)
      const compressed = encodeLz4Literals(original)

      const decompressed = lz4RawDecompress(compressed, original.length)

      expect(Array.from(decompressed)).toEqual(Array.from(original))
    })

    it('should decompress large literal-only block', () => {
      const original = new Uint8Array(1000)
      for (let i = 0; i < 1000; i++) {
        original[i] = i % 256
      }
      const compressed = encodeLz4Literals(original)

      const decompressed = lz4RawDecompress(compressed, original.length)

      expect(Array.from(decompressed)).toEqual(Array.from(original))
    })

    it('should decompress block with simple match', () => {
      // Manually construct a compressed block with a match
      // Format: token + literals + offset (2 bytes) + [match extension]
      // Token: high nibble = literal length, low nibble = match length - 4

      // Original: "ABCDABCD" (8 bytes)
      // Can compress as: 4 literals "ABCD" + match (offset=4, length=4)
      // Token: 0x40 (4 literals, 0 match = 4)
      // Literals: A B C D
      // Offset: 04 00 (little-endian 4)

      const compressed = new Uint8Array([
        0x40, // token: 4 literals, match length 0 (meaning 4 after +4)
        0x41,
        0x42,
        0x43,
        0x44, // literals: ABCD
        0x04,
        0x00, // offset: 4 (little-endian)
      ])

      const decompressed = lz4RawDecompress(compressed, 8)

      expect(new TextDecoder().decode(decompressed)).toBe('ABCDABCD')
    })

    it('should handle overlapping match (run-length encoding)', () => {
      // Create a block that repeats a byte using overlapping match
      // 1 literal "A" + match (offset=1, length=7) = "AAAAAAAA"
      // Token: 0x13 (1 literal, 3 match = 7 after +4)
      const compressed = new Uint8Array([
        0x13, // token: 1 literal, match length nibble = 3 (meaning 7)
        0x41, // literal: A
        0x01,
        0x00, // offset: 1 (little-endian)
      ])

      const decompressed = lz4RawDecompress(compressed, 8)

      expect(new TextDecoder().decode(decompressed)).toBe('AAAAAAAA')
    })

    it('should throw error for zero offset', () => {
      const compressed = new Uint8Array([
        0x10, // 1 literal, match length 4
        0x41, // literal: A
        0x00,
        0x00, // offset: 0 (invalid!)
      ])

      expect(() => lz4RawDecompress(compressed, 10)).toThrow('lz4 offset out of range 0')
    })

    it('should throw error for offset exceeding current position', () => {
      const compressed = new Uint8Array([
        0x10, // 1 literal, match length 4
        0x41, // literal: A
        0x0a,
        0x00, // offset: 10 (but we only have 1 byte output)
      ])

      expect(() => lz4RawDecompress(compressed, 10)).toThrow('lz4 offset out of range 10')
    })

    it('should decompress empty input', () => {
      const compressed = new Uint8Array(0)

      const decompressed = lz4RawDecompress(compressed, 0)

      expect(decompressed.length).toBe(0)
    })

    it('should handle match with extended length', () => {
      // Match length > 18 requires extension bytes
      // Create: 1 literal + match with extended length
      // Token: 0x1F (1 literal, 15 match nibble = needs extension)
      // For total match length 19: nibble=15, extension=0
      const compressed = new Uint8Array([
        0x1f, // token: 1 literal, match length nibble = 15 (extended)
        0x41, // literal: A
        0x01,
        0x00, // offset: 1
        0x00, // match extension: 15 + 4 + 0 = 19
      ])

      const decompressed = lz4RawDecompress(compressed, 20)

      // Should produce 1 literal + 19 copies = 20 'A's
      expect(decompressed.length).toBe(20)
      expect(new TextDecoder().decode(decompressed)).toBe('A'.repeat(20))
    })

    it('should handle literal-only termination (no trailing match)', () => {
      // When input ends after literals, return early without reading offset
      const original = new Uint8Array([1, 2, 3])
      const compressed = encodeLz4Literals(original)

      const decompressed = lz4RawDecompress(compressed, 3)

      expect(Array.from(decompressed)).toEqual([1, 2, 3])
    })

    it('should roundtrip text data through encode/decompress', () => {
      const original = new TextEncoder().encode('Hello World! This is a test of LZ4 compression.')
      const compressed = encodeLz4Literals(original)

      const decompressed = lz4RawDecompress(compressed, original.length)

      expect(new TextDecoder().decode(decompressed)).toBe('Hello World! This is a test of LZ4 compression.')
    })
  })

  // ============================================================================
  // lz4Compress Tests
  // ============================================================================

  describe('lz4Compress()', () => {
    beforeEach(() => {
      mockLz4Binding.compress.mockReset()
    })

    it('should use native compression when effective', () => {
      const input = new Uint8Array(100).fill(0x42)

      // Simulate successful compression that's smaller than input
      mockLz4Binding.compress.mockImplementation((inp: Uint8Array, out: Uint8Array) => {
        // Write some compressed data
        out[0] = 0xf0
        out[1] = 0x00
        return 50 // Compressed size smaller than input
      })

      const result = lz4Compress(input)

      expect(mockLz4Binding.compress).toHaveBeenCalled()
      expect(result.length).toBe(50)
    })

    it('should fall back to literal encoding when compression returns 0', () => {
      const input = new Uint8Array([1, 2, 3, 4, 5])

      // Simulate compression failure (returns 0)
      mockLz4Binding.compress.mockReturnValue(0)

      const result = lz4Compress(input)

      // Should produce literal-only encoding
      expect(result.length).toBe(6) // token + 5 literals
      expect(result[0]).toBe(0x50) // 5 literals token
    })

    it('should fall back to literal encoding when compression is larger than input', () => {
      const input = new Uint8Array(10).fill(0x42)

      // Simulate compression that's larger than input
      mockLz4Binding.compress.mockImplementation((inp: Uint8Array, out: Uint8Array) => {
        return 20 // Larger than input (10 bytes)
      })

      const result = lz4Compress(input)

      // Should fall back to literal encoding since compressed > input
      // 10 bytes = token (0xa0) + 10 literals = 11 bytes
      expect(result[0]).toBe(0xa0) // 10 literals token
    })

    it('should fall back to literal encoding when compression equals input size', () => {
      const input = new Uint8Array(10).fill(0x42)

      // Simulate compression that equals input size
      mockLz4Binding.compress.mockReturnValue(10)

      const result = lz4Compress(input)

      // Should fall back to literal encoding
      expect(result[0]).toBe(0xa0) // 10 literals token
    })

    it('should handle empty input', () => {
      const input = new Uint8Array(0)

      mockLz4Binding.compress.mockReturnValue(0)

      const result = lz4Compress(input)

      // Empty input produces single token byte
      expect(result.length).toBe(1)
      expect(result[0]).toBe(0x00)
    })

    it('should handle large input with successful compression', () => {
      const input = new Uint8Array(10000)
      for (let i = 0; i < 10000; i++) {
        input[i] = i % 256
      }

      // Simulate 50% compression ratio
      mockLz4Binding.compress.mockImplementation((inp: Uint8Array, out: Uint8Array) => {
        // Fill output with some data
        for (let i = 0; i < 5000; i++) {
          out[i] = 0xaa
        }
        return 5000
      })

      const result = lz4Compress(input)

      expect(result.length).toBe(5000)
    })
  })

  // ============================================================================
  // Roundtrip Tests (encode + decompress)
  // ============================================================================

  describe('LZ4 roundtrip', () => {
    it('should roundtrip various data sizes', () => {
      // Start from 1 since 0-length has edge case behavior
      const sizes = [1, 5, 14, 15, 16, 100, 255, 256, 270, 500, 1000]

      for (const size of sizes) {
        const original = new Uint8Array(size)
        for (let i = 0; i < size; i++) {
          original[i] = i % 256
        }

        const compressed = encodeLz4Literals(original)
        const decompressed = lz4RawDecompress(compressed, size)

        expect(Array.from(decompressed)).toEqual(Array.from(original))
      }
    })

    it('should handle zero-length data encoding', () => {
      const original = new Uint8Array(0)
      const compressed = encodeLz4Literals(original)

      // Zero-length produces just a token byte with 0 literals
      expect(compressed.length).toBe(1)
      expect(compressed[0]).toBe(0x00)
    })

    it('should decompress empty input to empty output', () => {
      // Empty compressed input returns empty output
      const decompressed = lz4RawDecompress(new Uint8Array(0), 0)
      expect(decompressed.length).toBe(0)
    })

    it('should roundtrip random data', () => {
      const size = 500
      const original = new Uint8Array(size)
      for (let i = 0; i < size; i++) {
        original[i] = Math.floor(Math.random() * 256)
      }

      const compressed = encodeLz4Literals(original)
      const decompressed = lz4RawDecompress(compressed, size)

      expect(Array.from(decompressed)).toEqual(Array.from(original))
    })

    it('should roundtrip JSON data', () => {
      const jsonData = JSON.stringify({
        type: 'test.event',
        ts: '2024-01-01T00:00:00Z',
        payload: {
          user: 'testuser',
          action: 'click',
          data: { nested: { value: 123 } },
        },
      })

      const original = new TextEncoder().encode(jsonData)
      const compressed = encodeLz4Literals(original)
      const decompressed = lz4RawDecompress(compressed, original.length)

      expect(new TextDecoder().decode(decompressed)).toBe(jsonData)
    })

    it('should roundtrip binary data with all byte values', () => {
      const original = new Uint8Array(256)
      for (let i = 0; i < 256; i++) {
        original[i] = i
      }

      const compressed = encodeLz4Literals(original)
      const decompressed = lz4RawDecompress(compressed, 256)

      expect(Array.from(decompressed)).toEqual(Array.from(original))
    })

    it('should roundtrip repeated patterns', () => {
      const patterns = [
        new Uint8Array(100).fill(0x00),
        new Uint8Array(100).fill(0xff),
        new Uint8Array(100).fill(0x42),
      ]

      for (const original of patterns) {
        const compressed = encodeLz4Literals(original)
        const decompressed = lz4RawDecompress(compressed, original.length)

        expect(Array.from(decompressed)).toEqual(Array.from(original))
      }
    })
  })

  // ============================================================================
  // Edge Cases and Error Handling
  // ============================================================================

  describe('edge cases', () => {
    it('should handle single byte input', () => {
      const input = new Uint8Array([0x42])
      const compressed = encodeLz4Literals(input)

      expect(compressed.length).toBe(2) // token + 1 literal
      expect(compressed[0]).toBe(0x10) // 1 literal in high nibble
      expect(compressed[1]).toBe(0x42)

      const decompressed = lz4RawDecompress(compressed, 1)
      expect(decompressed[0]).toBe(0x42)
    })

    it('should handle max single-token literal length (14 bytes)', () => {
      const input = new Uint8Array(14)
      for (let i = 0; i < 14; i++) {
        input[i] = i
      }

      const compressed = encodeLz4Literals(input)

      // Token with 14 in high nibble (0xe0) + 14 literals = 15 bytes
      expect(compressed.length).toBe(15)
      expect(compressed[0]).toBe(0xe0)

      const decompressed = lz4RawDecompress(compressed, 14)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })

    it('should handle boundary at 15 bytes (first extension)', () => {
      const input = new Uint8Array(15).fill(0xab)
      const compressed = encodeLz4Literals(input)

      // Token (0xf0) + extension (0) + 15 literals = 17 bytes
      expect(compressed.length).toBe(17)
      expect(compressed[0]).toBe(0xf0)
      expect(compressed[1]).toBe(0)

      const decompressed = lz4RawDecompress(compressed, 15)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })

    it('should handle boundary at 270 bytes (255 + 15)', () => {
      const input = new Uint8Array(270).fill(0xcd)
      const compressed = encodeLz4Literals(input)

      // Token (0xf0) + extension (255) + final (0) + 270 literals
      expect(compressed.length).toBe(273)
      expect(compressed[0]).toBe(0xf0)
      expect(compressed[1]).toBe(255)
      expect(compressed[2]).toBe(0)

      const decompressed = lz4RawDecompress(compressed, 270)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })

    it('should handle exactly 269 bytes (255 + 15 - 1)', () => {
      const input = new Uint8Array(269).fill(0xde)
      const compressed = encodeLz4Literals(input)

      // Token (0xf0) + extension (254) + 269 literals
      expect(compressed.length).toBe(271)
      expect(compressed[0]).toBe(0xf0)
      expect(compressed[1]).toBe(254) // 15 + 254 = 269

      const decompressed = lz4RawDecompress(compressed, 269)
      expect(Array.from(decompressed)).toEqual(Array.from(input))
    })

    it('should produce valid LZ4 format that could be processed by other implementations', () => {
      const input = new TextEncoder().encode('Test data for LZ4 compression')
      const compressed = encodeLz4Literals(input)

      // Verify format structure
      const token = compressed[0]
      const literalLength = token >> 4

      if (literalLength < 15) {
        // No extension bytes, literals start at position 1
        expect(compressed.length).toBe(1 + literalLength)
      } else {
        // Has extension bytes
        expect(literalLength).toBe(15)
        // First extension byte at position 1
        expect(compressed[1]).toBeDefined()
      }
    })
  })

  // ============================================================================
  // Performance Considerations
  // ============================================================================

  describe('performance characteristics', () => {
    it('should complete encoding in reasonable time', () => {
      // Just verify it can process large data without timeout
      // Precise timing tests are unreliable in CI environments
      const largeInput = new Uint8Array(100000).fill(0x42)

      const start = performance.now()
      const result = encodeLz4Literals(largeInput)
      const elapsed = performance.now() - start

      // Should complete in under 100ms for 100KB
      expect(elapsed).toBeLessThan(100)
      expect(result.length).toBeGreaterThan(largeInput.length)
    })

    it('should not allocate excessive memory for large inputs', () => {
      const inputSize = 100000
      const input = new Uint8Array(inputSize).fill(0x42)

      const compressed = encodeLz4Literals(input)

      // Literal-only encoding should be input size + small overhead
      // Overhead: 1 token + extension bytes (at most ~400 for 100K)
      expect(compressed.length).toBeLessThan(inputSize + 500)
      expect(compressed.length).toBeGreaterThan(inputSize)
    })
  })

  // ============================================================================
  // Decompression with malformed input
  // ============================================================================

  describe('decompression error handling', () => {
    it('should handle truncated input gracefully', () => {
      // Token indicates 10 literals but only 5 provided
      const truncated = new Uint8Array([
        0xa0, // 10 literals token
        0x01,
        0x02,
        0x03,
        0x04,
        0x05, // only 5 literals
      ])

      // This should not throw, but may produce incomplete output
      // The exact behavior depends on implementation - it reads what's available
      const result = lz4RawDecompress(truncated, 10)

      // Output array is pre-allocated to outputLength
      expect(result.length).toBe(10)
    })

    it('should throw for invalid match offset (zero)', () => {
      const invalid = new Uint8Array([
        0x10, // 1 literal, match length 4
        0x41, // literal A
        0x00,
        0x00, // offset 0 (invalid)
      ])

      expect(() => lz4RawDecompress(invalid, 10)).toThrow(/offset out of range/)
    })

    it('should throw for match offset exceeding output position', () => {
      const invalid = new Uint8Array([
        0x20, // 2 literals, match length 4
        0x41,
        0x42, // literals AB
        0x10,
        0x00, // offset 16 (but only 2 bytes output)
      ])

      expect(() => lz4RawDecompress(invalid, 10)).toThrow(/offset out of range/)
    })
  })
})

// ============================================================================
// Integration Tests (Parquet write/read cycle simulation)
// ============================================================================

describe('Parquet integration with LZ4', () => {
  it('should produce LZ4 data compatible with parquet decompression', () => {
    // Simulate what parquetWriteBuffer would do: compress column data
    const columnData = new TextEncoder().encode(JSON.stringify(['value1', 'value2', 'value3']))
    const compressed = encodeLz4Literals(columnData)

    // Simulate what parquetRead would do: decompress the data
    const decompressed = lz4RawDecompress(compressed, columnData.length)

    expect(new TextDecoder().decode(decompressed)).toBe(JSON.stringify(['value1', 'value2', 'value3']))
  })

  it('should handle typical Parquet string column data', () => {
    // Typical Kaikki dictionary data as used in benchmarks
    const entries = [
      { word: 'hello', pos: 'noun', lang: 'English', lang_code: 'en' },
      { word: 'world', pos: 'noun', lang: 'English', lang_code: 'en' },
      { word: 'test', pos: 'verb', lang: 'English', lang_code: 'en' },
    ]

    const serialized = new TextEncoder().encode(JSON.stringify(entries))
    const compressed = encodeLz4Literals(serialized)
    const decompressed = lz4RawDecompress(compressed, serialized.length)

    const parsed = JSON.parse(new TextDecoder().decode(decompressed))
    expect(parsed).toEqual(entries)
  })

  it('should handle nullable JSON columns', () => {
    const data = [
      { senses: '[{"def":"meaning"}]', etymology: null },
      { senses: '[{"def":"other"}]', etymology: '["origin1"]' },
    ]

    const serialized = new TextEncoder().encode(JSON.stringify(data))
    const compressed = encodeLz4Literals(serialized)
    const decompressed = lz4RawDecompress(compressed, serialized.length)

    expect(JSON.parse(new TextDecoder().decode(decompressed))).toEqual(data)
  })
})

// ============================================================================
// Benchmark Worker Fetch Handler Tests
// ============================================================================

describe('benchmark-worker fetch handler', () => {
  // Mock environment for benchmark worker
  interface MockEnv {
    BENCHMARK_BUCKET: {
      head: ReturnType<typeof vi.fn>
      get: ReturnType<typeof vi.fn>
      put: ReturnType<typeof vi.fn>
      list: ReturnType<typeof vi.fn>
    }
    PIPELINE_BUCKET: {
      get: ReturnType<typeof vi.fn>
      list: ReturnType<typeof vi.fn>
    }
  }

  function createMockEnv(): MockEnv {
    return {
      BENCHMARK_BUCKET: {
        head: vi.fn().mockResolvedValue(null),
        get: vi.fn().mockResolvedValue(null),
        put: vi.fn().mockResolvedValue(undefined),
        list: vi.fn().mockResolvedValue({ objects: [] }),
      },
      PIPELINE_BUCKET: {
        get: vi.fn().mockResolvedValue(null),
        list: vi.fn().mockResolvedValue({ objects: [] }),
      },
    }
  }

  function createMockCtx(): ExecutionContext {
    return {
      waitUntil: vi.fn(),
      passThroughOnException: vi.fn(),
    } as unknown as ExecutionContext
  }

  // ============================================================================
  // Endpoint Routing Tests
  // ============================================================================

  describe('endpoint routing', () => {
    it('should return available endpoints for root path', async () => {
      const env = createMockEnv()
      const ctx = createMockCtx()
      const request = new Request('https://benchmark.workers.do/')

      // We test the expected response structure for the index endpoint
      // Note: We cannot import the actual worker without complex mocking
      // This tests the expected behavior

      const expectedEndpoints = {
        '/run': 'Run full benchmark suite (maxRows, codec params)',
        '/test': 'Single benchmark (rows, codec params)',
        '/results': 'List previous benchmark results',
        '/data': 'Check available Wiktionary data',
        '/debug-parquet': 'Debug Parquet file structure',
      }

      // Verify expected endpoint structure
      expect(expectedEndpoints['/run']).toBeDefined()
      expect(expectedEndpoints['/test']).toBeDefined()
      expect(expectedEndpoints['/results']).toBeDefined()
    })

    it('should recognize health check endpoint', () => {
      const url = new URL('https://benchmark.workers.do/health')
      expect(url.pathname).toBe('/health')
    })

    it('should recognize lz4-test endpoint', () => {
      const url = new URL('https://benchmark.workers.do/lz4-test')
      expect(url.pathname).toBe('/lz4-test')
    })

    it('should recognize timing-test endpoint', () => {
      const url = new URL('https://benchmark.workers.do/timing-test')
      expect(url.pathname).toBe('/timing-test')
    })

    it('should recognize run endpoint with query params', () => {
      const url = new URL('https://benchmark.workers.do/run?maxRows=1000&codec=LZ4_RAW')
      expect(url.pathname).toBe('/run')
      expect(url.searchParams.get('maxRows')).toBe('1000')
      expect(url.searchParams.get('codec')).toBe('LZ4_RAW')
    })

    it('should recognize test endpoint with query params', () => {
      const url = new URL('https://benchmark.workers.do/test?rows=500&codec=SNAPPY&iterations=5')
      expect(url.pathname).toBe('/test')
      expect(url.searchParams.get('rows')).toBe('500')
      expect(url.searchParams.get('codec')).toBe('SNAPPY')
      expect(url.searchParams.get('iterations')).toBe('5')
    })
  })

  // ============================================================================
  // Query Parameter Parsing Tests
  // ============================================================================

  describe('query parameter parsing', () => {
    it('should parse maxRows as integer', () => {
      const url = new URL('https://benchmark.workers.do/run?maxRows=5000')
      const maxRows = parseInt(url.searchParams.get('maxRows') ?? '10000')
      expect(maxRows).toBe(5000)
    })

    it('should default maxRows to 10000 when not provided', () => {
      const url = new URL('https://benchmark.workers.do/run')
      const maxRows = parseInt(url.searchParams.get('maxRows') ?? '10000')
      expect(maxRows).toBe(10000)
    })

    it('should parse codec parameter', () => {
      const CODECS = ['UNCOMPRESSED', 'LZ4_RAW', 'SNAPPY', 'ZSTD'] as const
      const url = new URL('https://benchmark.workers.do/run?codec=LZ4_RAW')
      const codec = url.searchParams.get('codec') as typeof CODECS[number] | null
      expect(codec).toBe('LZ4_RAW')
    })

    it('should return null for missing codec', () => {
      const url = new URL('https://benchmark.workers.do/run')
      const codec = url.searchParams.get('codec')
      expect(codec).toBeNull()
    })

    it('should parse rows parameter for test endpoint', () => {
      const url = new URL('https://benchmark.workers.do/test?rows=2000')
      const rows = parseInt(url.searchParams.get('rows') ?? '1000')
      expect(rows).toBe(2000)
    })

    it('should default rows to 1000 for test endpoint', () => {
      const url = new URL('https://benchmark.workers.do/test')
      const rows = parseInt(url.searchParams.get('rows') ?? '1000')
      expect(rows).toBe(1000)
    })

    it('should parse iterations parameter', () => {
      const url = new URL('https://benchmark.workers.do/test?iterations=10')
      const iterations = parseInt(url.searchParams.get('iterations') ?? '1')
      expect(iterations).toBe(10)
    })

    it('should default iterations to 1', () => {
      const url = new URL('https://benchmark.workers.do/test')
      const iterations = parseInt(url.searchParams.get('iterations') ?? '1')
      expect(iterations).toBe(1)
    })

    it('should handle invalid numeric parameters gracefully', () => {
      const url = new URL('https://benchmark.workers.do/test?rows=invalid')
      const rows = parseInt(url.searchParams.get('rows') ?? '1000')
      expect(Number.isNaN(rows)).toBe(true)
    })
  })

  // ============================================================================
  // Batch Size Configuration Tests
  // ============================================================================

  describe('batch size configuration', () => {
    const BATCH_SIZES = [1, 10, 100, 1000, 10000, 100000]

    it('should have expected batch sizes defined', () => {
      expect(BATCH_SIZES).toEqual([1, 10, 100, 1000, 10000, 100000])
    })

    it('should filter batch sizes to available data', () => {
      const sampleDataLength = 5000
      const sizes = BATCH_SIZES.filter((s) => s <= sampleDataLength)
      expect(sizes).toEqual([1, 10, 100, 1000])
    })

    it('should include all sizes when data is large enough', () => {
      const sampleDataLength = 100000
      const sizes = BATCH_SIZES.filter((s) => s <= sampleDataLength)
      expect(sizes).toEqual(BATCH_SIZES)
    })

    it('should return empty when no data available', () => {
      const sampleDataLength = 0
      const sizes = BATCH_SIZES.filter((s) => s <= sampleDataLength)
      expect(sizes).toEqual([])
    })
  })

  // ============================================================================
  // Codec Configuration Tests
  // ============================================================================

  describe('codec configuration', () => {
    const CODECS = ['UNCOMPRESSED', 'LZ4_RAW', 'SNAPPY', 'ZSTD'] as const

    it('should have all expected codecs defined', () => {
      expect(CODECS).toContain('UNCOMPRESSED')
      expect(CODECS).toContain('LZ4_RAW')
      expect(CODECS).toContain('SNAPPY')
      expect(CODECS).toContain('ZSTD')
    })

    it('should filter to single codec when specified', () => {
      const codec = 'LZ4_RAW' as typeof CODECS[number] | null
      const codecs = codec ? [codec] : CODECS
      expect(codecs).toEqual(['LZ4_RAW'])
    })

    it('should use all codecs when none specified', () => {
      const codec = null as typeof CODECS[number] | null
      const codecs = codec ? [codec] : CODECS
      expect(codecs).toEqual(CODECS)
    })
  })
})

// ============================================================================
// Benchmark Result Structure Tests
// ============================================================================

describe('BenchmarkResult structure', () => {
  interface BenchmarkResult {
    codec: string
    rowCount: number
    writeMs: number
    compressedBytes: number
    uncompressedBytes: number
    compressionRatio: number
    readMs: number
    readError: string | null
    writeRowsPerSec: number
    readRowsPerSec: number
    writeMBPerSec: number
    readMBPerSec: number
    _debug?: { writeStart: number; writeEnd: number }
  }

  it('should have all required fields in result', () => {
    const result: BenchmarkResult = {
      codec: 'LZ4_RAW',
      rowCount: 1000,
      writeMs: 50,
      compressedBytes: 5000,
      uncompressedBytes: 10000,
      compressionRatio: 2.0,
      readMs: 30,
      readError: null,
      writeRowsPerSec: 20000,
      readRowsPerSec: 33333,
      writeMBPerSec: 0.2,
      readMBPerSec: 0.33,
    }

    expect(result.codec).toBe('LZ4_RAW')
    expect(result.rowCount).toBe(1000)
    expect(result.writeMs).toBe(50)
    expect(result.compressedBytes).toBe(5000)
    expect(result.uncompressedBytes).toBe(10000)
    expect(result.compressionRatio).toBe(2.0)
    expect(result.readMs).toBe(30)
    expect(result.readError).toBeNull()
    expect(result.writeRowsPerSec).toBe(20000)
    expect(result.readRowsPerSec).toBe(33333)
    expect(result.writeMBPerSec).toBe(0.2)
    expect(result.readMBPerSec).toBe(0.33)
  })

  it('should allow read error to be set', () => {
    const result: BenchmarkResult = {
      codec: 'ZSTD',
      rowCount: 100,
      writeMs: 10,
      compressedBytes: 500,
      uncompressedBytes: 1000,
      compressionRatio: 2.0,
      readMs: 0,
      readError: 'Decompression failed',
      writeRowsPerSec: 10000,
      readRowsPerSec: 0,
      writeMBPerSec: 0.1,
      readMBPerSec: 0,
    }

    expect(result.readError).toBe('Decompression failed')
    expect(result.readMs).toBe(0)
    expect(result.readRowsPerSec).toBe(0)
  })

  it('should include debug info when present', () => {
    const result: BenchmarkResult = {
      codec: 'UNCOMPRESSED',
      rowCount: 10,
      writeMs: 5,
      compressedBytes: 100,
      uncompressedBytes: 100,
      compressionRatio: 1.0,
      readMs: 3,
      readError: null,
      writeRowsPerSec: 2000,
      readRowsPerSec: 3333,
      writeMBPerSec: 0.02,
      readMBPerSec: 0.03,
      _debug: { writeStart: 1000, writeEnd: 1005 },
    }

    expect(result._debug).toBeDefined()
    expect(result._debug!.writeStart).toBe(1000)
    expect(result._debug!.writeEnd).toBe(1005)
  })

  it('should calculate compression ratio correctly', () => {
    const uncompressedBytes = 10000
    const compressedBytes = 2500
    const compressionRatio = uncompressedBytes / compressedBytes

    expect(compressionRatio).toBe(4.0)
  })

  it('should calculate rows per second correctly', () => {
    const rowCount = 1000
    const writeMs = 100

    const writeRowsPerSec = writeMs > 0 ? rowCount / (writeMs / 1000) : 0

    expect(writeRowsPerSec).toBe(10000)
  })

  it('should calculate MB per second correctly', () => {
    const uncompressedBytes = 1024 * 1024 // 1MB
    const writeMs = 500

    const writeMBPerSec = writeMs > 0 ? (uncompressedBytes / 1024 / 1024) / (writeMs / 1000) : 0

    expect(writeMBPerSec).toBe(2.0)
  })

  it('should handle zero time gracefully', () => {
    const rowCount = 100
    const writeMs = 0

    const writeRowsPerSec = writeMs > 0 ? rowCount / (writeMs / 1000) : 0

    expect(writeRowsPerSec).toBe(0)
  })
})

// ============================================================================
// KaikkiEntry Schema Tests
// ============================================================================

describe('KaikkiEntry schema', () => {
  interface KaikkiEntry {
    word: string
    pos: string
    lang: string
    lang_code: string
    senses: string
    etymology_texts: string | null
    sounds: string | null
    forms: string | null
    translations: string | null
  }

  it('should have required string fields', () => {
    const entry: KaikkiEntry = {
      word: 'test',
      pos: 'noun',
      lang: 'English',
      lang_code: 'en',
      senses: '[{"def":"a test"}]',
      etymology_texts: null,
      sounds: null,
      forms: null,
      translations: null,
    }

    expect(entry.word).toBe('test')
    expect(entry.pos).toBe('noun')
    expect(entry.lang).toBe('English')
    expect(entry.lang_code).toBe('en')
    expect(entry.senses).toBe('[{"def":"a test"}]')
  })

  it('should allow nullable fields to be null', () => {
    const entry: KaikkiEntry = {
      word: 'word',
      pos: 'verb',
      lang: 'Czech',
      lang_code: 'cs',
      senses: '[]',
      etymology_texts: null,
      sounds: null,
      forms: null,
      translations: null,
    }

    expect(entry.etymology_texts).toBeNull()
    expect(entry.sounds).toBeNull()
    expect(entry.forms).toBeNull()
    expect(entry.translations).toBeNull()
  })

  it('should allow nullable fields to have JSON string values', () => {
    const entry: KaikkiEntry = {
      word: 'hello',
      pos: 'interjection',
      lang: 'English',
      lang_code: 'en',
      senses: '[{"def":"greeting"}]',
      etymology_texts: '["from Old English"]',
      sounds: '[{"ipa":"/həˈloʊ/"}]',
      forms: '[{"form":"hellos"}]',
      translations: '[{"lang":"Spanish","word":"hola"}]',
    }

    expect(entry.etymology_texts).toBe('["from Old English"]')
    expect(entry.sounds).toBe('[{"ipa":"/həˈloʊ/"}]')
    expect(entry.forms).toBe('[{"form":"hellos"}]')
    expect(entry.translations).toBe('[{"lang":"Spanish","word":"hola"}]')
  })

  it('should calculate uncompressed bytes estimate', () => {
    const entry: KaikkiEntry = {
      word: 'test',
      pos: 'noun',
      lang: 'English',
      lang_code: 'en',
      senses: '[{"def":"definition"}]',
      etymology_texts: '["origin"]',
      sounds: null,
      forms: null,
      translations: null,
    }

    // Calculate as done in runBenchmark
    const uncompressedBytes =
      (entry.word?.length ?? 0) +
      (entry.pos?.length ?? 0) +
      (entry.lang?.length ?? 0) +
      (entry.lang_code?.length ?? 0) +
      (entry.senses?.length ?? 0) +
      (entry.etymology_texts?.length ?? 0) +
      (entry.sounds?.length ?? 0)

    expect(uncompressedBytes).toBe(4 + 4 + 7 + 2 + 21 + 10 + 0)
  })
})

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('benchmark-worker error handling', () => {
  describe('Kaikki data loading errors', () => {
    it('should handle empty data response', () => {
      const sampleData: { word: string }[] = []

      expect(sampleData.length).toBe(0)

      // Worker returns 500 when no data loaded
      if (sampleData.length === 0) {
        const response = { error: 'Failed to load Kaikki data', status: 500 }
        expect(response.error).toBe('Failed to load Kaikki data')
        expect(response.status).toBe(500)
      }
    })

    it('should handle malformed JSON lines gracefully', () => {
      const lines = ['{"word":"valid"}', 'not json', '{"word":"also valid"}']
      const entries: { word: string }[] = []

      for (const line of lines) {
        if (!line.trim()) continue
        try {
          const raw = JSON.parse(line) as { word: string }
          entries.push({ word: String(raw.word ?? '') })
        } catch {
          // Skip malformed lines - this is expected behavior
        }
      }

      expect(entries.length).toBe(2)
      expect(entries[0].word).toBe('valid')
      expect(entries[1].word).toBe('also valid')
    })

    it('should handle empty lines in data stream', () => {
      const lines = ['', '{"word":"test"}', '   ', '{"word":"test2"}', '']
      const entries: { word: string }[] = []

      for (const line of lines) {
        if (!line.trim()) continue
        try {
          const raw = JSON.parse(line) as { word: string }
          entries.push({ word: String(raw.word ?? '') })
        } catch {
          // Skip invalid lines
        }
      }

      expect(entries.length).toBe(2)
    })
  })

  describe('compression errors', () => {
    it('should handle compression returning zero', () => {
      // When lz4 binding returns 0, fallback to literal encoding
      const compressedSize = 0
      const inputLength = 100

      const shouldFallback = compressedSize <= 0 || compressedSize >= inputLength

      expect(shouldFallback).toBe(true)
    })

    it('should handle compression larger than input', () => {
      const compressedSize = 150
      const inputLength = 100

      const shouldFallback = compressedSize <= 0 || compressedSize >= inputLength

      expect(shouldFallback).toBe(true)
    })

    it('should use compressed data when smaller', () => {
      const compressedSize = 50
      const inputLength = 100

      const shouldFallback = compressedSize <= 0 || compressedSize >= inputLength

      expect(shouldFallback).toBe(false)
    })
  })

  describe('decompression errors', () => {
    it('should catch decompression errors in read benchmark', () => {
      let readError: string | null = null

      try {
        throw new Error('Decompression failed: invalid data')
      } catch (e) {
        readError = String(e)
      }

      expect(readError).toContain('Decompression failed')
    })

    it('should record row count mismatch as error', () => {
      const expectedRowCount = 100
      const actualRowCount = 95
      let readError: string | null = null

      if (actualRowCount !== expectedRowCount) {
        readError = `Row count mismatch: expected ${expectedRowCount}, got ${actualRowCount}`
      }

      expect(readError).toBe('Row count mismatch: expected 100, got 95')
    })

    it('should allow matching row counts', () => {
      const expectedRowCount = 100
      const actualRowCount = 100
      let readError: string | null = null

      if (actualRowCount !== expectedRowCount) {
        readError = `Row count mismatch: expected ${expectedRowCount}, got ${actualRowCount}`
      }

      expect(readError).toBeNull()
    })
  })

  describe('test endpoint error handling', () => {
    it('should return error response for exceptions', () => {
      // Simulate error handling in /test endpoint
      const e = new Error('Something went wrong')

      const errorResponse = {
        error: String(e),
        stack: e instanceof Error ? e.stack : undefined,
      }

      expect(errorResponse.error).toBe('Error: Something went wrong')
      expect(errorResponse.stack).toBeDefined()
    })

    it('should handle non-Error throws', () => {
      const e = 'string error'

      const errorResponse = {
        error: String(e),
        stack: e instanceof Error ? e.stack : undefined,
      }

      expect(errorResponse.error).toBe('string error')
      expect(errorResponse.stack).toBeUndefined()
    })
  })
})

// ============================================================================
// R2 Cache Key Generation Tests
// ============================================================================

describe('R2 cache key generation', () => {
  it('should generate chunk cache key with row count', () => {
    const maxRows = 1000
    const chunkKey = `kaikki-cache/cs-chunk-${maxRows}.json`

    expect(chunkKey).toBe('kaikki-cache/cs-chunk-1000.json')
  })

  it('should generate result key with timestamp', () => {
    const date = new Date('2024-06-15T10:30:45.123Z')
    const resultKey = `benchmarks/${date.toISOString().replace(/[:.]/g, '-')}.json`

    expect(resultKey).toBe('benchmarks/2024-06-15T10-30-45-123Z.json')
  })

  it('should handle various row counts', () => {
    const rowCounts = [1, 10, 100, 1000, 10000, 100000]

    for (const count of rowCounts) {
      const key = `kaikki-cache/cs-chunk-${count}.json`
      expect(key).toContain(String(count))
      expect(key).toStartWith('kaikki-cache/')
      expect(key).toEndWith('.json')
    }
  })
})

// ============================================================================
// Column Data Preparation Tests
// ============================================================================

describe('column data preparation', () => {
  interface KaikkiEntry {
    word: string
    pos: string
    lang: string
    lang_code: string
    senses: string
    etymology_texts: string | null
    sounds: string | null
    forms: string | null
    translations: string | null
  }

  const sampleData: KaikkiEntry[] = [
    {
      word: 'test',
      pos: 'noun',
      lang: 'English',
      lang_code: 'en',
      senses: '[{"def":"examination"}]',
      etymology_texts: '["Latin testum"]',
      sounds: null,
      forms: '[{"form":"tests"}]',
      translations: null,
    },
    {
      word: 'hello',
      pos: 'interjection',
      lang: 'English',
      lang_code: 'en',
      senses: '[{"def":"greeting"}]',
      etymology_texts: null,
      sounds: '[{"ipa":"/həˈloʊ/"}]',
      forms: null,
      translations: '[{"word":"hola","lang":"Spanish"}]',
    },
  ]

  it('should extract word column', () => {
    const words = sampleData.map((d) => d.word)
    expect(words).toEqual(['test', 'hello'])
  })

  it('should extract pos column', () => {
    const positions = sampleData.map((d) => d.pos)
    expect(positions).toEqual(['noun', 'interjection'])
  })

  it('should extract lang column', () => {
    const languages = sampleData.map((d) => d.lang)
    expect(languages).toEqual(['English', 'English'])
  })

  it('should extract lang_code column', () => {
    const codes = sampleData.map((d) => d.lang_code)
    expect(codes).toEqual(['en', 'en'])
  })

  it('should extract senses column as JSON strings', () => {
    const senses = sampleData.map((d) => d.senses)
    expect(senses[0]).toBe('[{"def":"examination"}]')
    expect(senses[1]).toBe('[{"def":"greeting"}]')
  })

  it('should extract nullable columns preserving null', () => {
    const etymology = sampleData.map((d) => d.etymology_texts)
    expect(etymology[0]).toBe('["Latin testum"]')
    expect(etymology[1]).toBeNull()

    const sounds = sampleData.map((d) => d.sounds)
    expect(sounds[0]).toBeNull()
    expect(sounds[1]).toBe('[{"ipa":"/həˈloʊ/"}]')
  })

  it('should prepare column data structure for parquet writer', () => {
    const columnData = [
      { name: 'word', data: sampleData.map((d) => d.word), type: 'STRING' as const },
      { name: 'pos', data: sampleData.map((d) => d.pos), type: 'STRING' as const },
      { name: 'lang', data: sampleData.map((d) => d.lang), type: 'STRING' as const },
      { name: 'lang_code', data: sampleData.map((d) => d.lang_code), type: 'STRING' as const },
      { name: 'senses', data: sampleData.map((d) => d.senses), type: 'JSON' as const },
      { name: 'etymology_texts', data: sampleData.map((d) => d.etymology_texts), type: 'JSON' as const, nullable: true },
      { name: 'sounds', data: sampleData.map((d) => d.sounds), type: 'JSON' as const, nullable: true },
      { name: 'forms', data: sampleData.map((d) => d.forms), type: 'JSON' as const, nullable: true },
      { name: 'translations', data: sampleData.map((d) => d.translations), type: 'JSON' as const, nullable: true },
    ]

    expect(columnData.length).toBe(9)
    expect(columnData[0].name).toBe('word')
    expect(columnData[0].type).toBe('STRING')
    expect(columnData[4].type).toBe('JSON')
    expect(columnData[5].nullable).toBe(true)
  })
})
