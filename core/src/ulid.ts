/**
 * ULID - Time-ordered unique identifier (lexicographically sortable)
 *
 * Generates Universally Unique Lexicographically Sortable Identifiers
 * using Crockford Base32 encoding.
 *
 * Format: 10 chars timestamp (48 bits) + 16 chars randomness (80 bits)
 */

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ' // Crockford Base32

export function ulid(): string {
  const now = Date.now()
  let str = ''
  // Timestamp (48 bits = 10 chars)
  let ts = now
  for (let i = 9; i >= 0; i--) {
    str = ENCODING[ts % 32] + str
    ts = Math.floor(ts / 32)
  }
  // Randomness (80 bits = 16 chars)
  for (let i = 0; i < 16; i++) {
    str += ENCODING[Math.floor(Math.random() * 32)]
  }
  return str
}
