declare module 'lz4/lib/binding' {
  export function compressBound(inputSize: number): number
  export function compress(src: Uint8Array, dst: Uint8Array, sIdx?: number, eIdx?: number): number
}
