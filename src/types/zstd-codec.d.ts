declare module 'zstd-codec' {
  interface ZstdSimple {
    compress(data: Uint8Array, level?: number): Uint8Array
  }
  export const ZstdCodec: {
    run(callback: (binding: { Simple: new () => ZstdSimple }) => void): void
  }
}
