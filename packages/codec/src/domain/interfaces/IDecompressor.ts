export interface IDecompressor {
  decompress(buffer: Buffer): Buffer;
}
