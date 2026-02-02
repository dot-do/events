/**
 * AsyncBuffer Tests
 *
 * Tests for the async-buffer utility that wraps ArrayBuffer for hyparquet.
 */

import { describe, it, expect } from 'vitest'
import { createAsyncBuffer } from '../async-buffer.js'

describe('createAsyncBuffer', () => {
  describe('byteLength', () => {
    it('returns correct byteLength for empty buffer', () => {
      const buffer = new ArrayBuffer(0)
      const asyncBuffer = createAsyncBuffer(buffer)

      expect(asyncBuffer.byteLength).toBe(0)
    })

    it('returns correct byteLength for small buffer', () => {
      const buffer = new ArrayBuffer(100)
      const asyncBuffer = createAsyncBuffer(buffer)

      expect(asyncBuffer.byteLength).toBe(100)
    })

    it('returns correct byteLength for large buffer', () => {
      const buffer = new ArrayBuffer(1024 * 1024) // 1MB
      const asyncBuffer = createAsyncBuffer(buffer)

      expect(asyncBuffer.byteLength).toBe(1024 * 1024)
    })
  })

  describe('slice()', () => {
    it('slices from start to end', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(2, 5)
      const slicedData = new Uint8Array(sliced)

      expect(slicedData.length).toBe(3)
      expect(Array.from(slicedData)).toEqual([3, 4, 5])
    })

    it('slices from start without end (to end of buffer)', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(3)
      const slicedData = new Uint8Array(sliced)

      expect(slicedData.length).toBe(2)
      expect(Array.from(slicedData)).toEqual([4, 5])
    })

    it('slices entire buffer with slice(0)', () => {
      const data = new Uint8Array([10, 20, 30])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(0)
      const slicedData = new Uint8Array(sliced)

      expect(Array.from(slicedData)).toEqual([10, 20, 30])
    })

    it('slices entire buffer with slice(0, byteLength)', () => {
      const data = new Uint8Array([10, 20, 30])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(0, asyncBuffer.byteLength)
      const slicedData = new Uint8Array(sliced)

      expect(Array.from(slicedData)).toEqual([10, 20, 30])
    })

    it('returns empty buffer when start equals end', () => {
      const data = new Uint8Array([1, 2, 3])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(1, 1)

      expect(sliced.byteLength).toBe(0)
    })

    it('handles negative start index', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      // Negative indices work like Array.slice - from end of buffer
      const sliced = asyncBuffer.slice(-2)
      const slicedData = new Uint8Array(sliced)

      expect(Array.from(slicedData)).toEqual([4, 5])
    })

    it('handles negative end index', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(1, -1)
      const slicedData = new Uint8Array(sliced)

      expect(Array.from(slicedData)).toEqual([2, 3, 4])
    })

    it('handles out-of-bounds end index (clamps to byteLength)', () => {
      const data = new Uint8Array([1, 2, 3])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(0, 100)
      const slicedData = new Uint8Array(sliced)

      expect(Array.from(slicedData)).toEqual([1, 2, 3])
    })

    it('returns empty buffer for empty input', () => {
      const buffer = new ArrayBuffer(0)
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(0)

      expect(sliced.byteLength).toBe(0)
    })

    it('returns a copy, not a view of original data', () => {
      const data = new Uint8Array([1, 2, 3, 4, 5])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      const sliced = asyncBuffer.slice(0, 3)
      const slicedData = new Uint8Array(sliced)

      // Modify the original
      data[0] = 100

      // Sliced data should be unchanged (it's a copy)
      expect(slicedData[0]).toBe(1)
    })
  })

  describe('integration with typed data', () => {
    it('preserves binary data integrity', () => {
      // Create some binary data (could be from a Parquet file header)
      const data = new Uint8Array([
        0x50, 0x41, 0x52, 0x31, // "PAR1" magic number
        0x00, 0x00, 0x00, 0x00, // padding
        0xFF, 0xFE, 0xFD, 0xFC, // some data bytes
      ])
      const buffer = data.buffer
      const asyncBuffer = createAsyncBuffer(buffer)

      // Read magic number
      const magic = asyncBuffer.slice(0, 4)
      const magicData = new Uint8Array(magic)

      expect(magicData[0]).toBe(0x50) // 'P'
      expect(magicData[1]).toBe(0x41) // 'A'
      expect(magicData[2]).toBe(0x52) // 'R'
      expect(magicData[3]).toBe(0x31) // '1'
    })

    it('works with large file simulation', () => {
      // Simulate a larger buffer (e.g., beginning of a Parquet file)
      const size = 64 * 1024 // 64KB
      const data = new Uint8Array(size)
      for (let i = 0; i < size; i++) {
        data[i] = i % 256
      }

      const asyncBuffer = createAsyncBuffer(data.buffer)

      expect(asyncBuffer.byteLength).toBe(size)

      // Read some chunks
      const chunk1 = asyncBuffer.slice(0, 1024)
      const chunk2 = asyncBuffer.slice(1024, 2048)
      const chunk3 = asyncBuffer.slice(size - 100, size)

      expect(chunk1.byteLength).toBe(1024)
      expect(chunk2.byteLength).toBe(1024)
      expect(chunk3.byteLength).toBe(100)

      // Verify data integrity
      const chunk1Data = new Uint8Array(chunk1)
      expect(chunk1Data[0]).toBe(0)
      expect(chunk1Data[255]).toBe(255)

      const chunk2Data = new Uint8Array(chunk2)
      expect(chunk2Data[0]).toBe(0) // 1024 % 256 = 0
    })
  })

  describe('edge cases', () => {
    it('handles single byte buffer', () => {
      const data = new Uint8Array([42])
      const asyncBuffer = createAsyncBuffer(data.buffer)

      expect(asyncBuffer.byteLength).toBe(1)

      const sliced = asyncBuffer.slice(0, 1)
      const slicedData = new Uint8Array(sliced)

      expect(slicedData[0]).toBe(42)
    })

    it('works with Uint8Array backed ArrayBuffer', () => {
      const uint8 = new Uint8Array([1, 2, 3, 4, 5])
      const asyncBuffer = createAsyncBuffer(uint8.buffer)

      const sliced = asyncBuffer.slice(1, 4)
      const slicedData = new Uint8Array(sliced)

      expect(Array.from(slicedData)).toEqual([2, 3, 4])
    })

    it('works with Float64Array backed ArrayBuffer', () => {
      const float64 = new Float64Array([1.5, 2.5, 3.5])
      const asyncBuffer = createAsyncBuffer(float64.buffer)

      // Each float64 is 8 bytes
      expect(asyncBuffer.byteLength).toBe(24)

      // Slice the first float64
      const sliced = asyncBuffer.slice(0, 8)
      const slicedFloat = new Float64Array(sliced)

      expect(slicedFloat[0]).toBe(1.5)
    })
  })
})
