import snappy from "snappy";
import type { IDecompressor } from "@domain/ports/IDecompressor";

export class SnappyDecompressor implements IDecompressor {
  decompress(buffer: Buffer): Buffer {
    if (buffer.length < 1) {
      throw new Error("Buffer too small for compression flag");
    }

    const flag = buffer.readUInt8(0);
    const payload = buffer.subarray(1);

    switch (flag) {
      case 0x00:
        return payload; // not compressed
      case 0x01:
        return snappy.uncompressSync(payload) as Buffer;
      default:
        throw new Error(`Unknown compression flag: ${flag}`);
    }
  }
}
