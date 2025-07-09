import snappy from "snappy";
import type { ICompressor } from "@domain/interfaces/ICompressor";

export class SnappyCompressor implements ICompressor {
  static readonly COMPRESSION_SIZE_THRESHOLD = 4_000;

  constructor(
    private sizeThreshold = SnappyCompressor.COMPRESSION_SIZE_THRESHOLD
  ) {}

  compress(data: Buffer, needCompress = false): Buffer {
    if (!needCompress || data.length < this.sizeThreshold) {
      const header = Buffer.alloc(1);
      header.writeUInt8(0x00, 0); // not compressed
      return Buffer.concat([header, data]);
    }

    const compressed = snappy.compressSync(data);
    const header = Buffer.alloc(1);
    header.writeUInt8(0x01, 0); // compressed
    return Buffer.concat([header, compressed]);
  }
}
