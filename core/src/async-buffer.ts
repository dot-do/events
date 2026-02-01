/**
 * AsyncBuffer utility for hyparquet
 *
 * Creates an AsyncBuffer wrapper around an ArrayBuffer for use
 * with hyparquet's Parquet reading functions.
 */

import type { AsyncBuffer } from 'hyparquet'

/**
 * Creates an AsyncBuffer from an ArrayBuffer for hyparquet
 */
export function createAsyncBuffer(buffer: ArrayBuffer): AsyncBuffer {
  return {
    byteLength: buffer.byteLength,
    slice(start: number, end?: number): ArrayBuffer {
      return buffer.slice(start, end)
    },
  }
}
