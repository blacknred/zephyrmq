import msgpack from "@msgpack/msgpack";
import snappy from "snappy";
import type { IDecoder } from "src/domain/interfaces/IDecoder";
import type { ISchema } from "src/domain/interfaces/ISchema";

export class BinarySchemaDecoder implements IDecoder {
  decode<T>(buffer: Buffer, schema?: ISchema<T>): T {
    try {
      const decompressed = this.decompressIfApplicable(buffer);

      if (schema) return schema.deserialize(decompressed);

      const uint8Array = new Uint8Array(
        buffer.buffer,
        buffer.byteOffset,
        buffer.byteLength
      );
      return msgpack.decode(uint8Array) as T;
    } catch (e) {
      throw new Error(`Decoding failed: ${e}`);
    }
  }

  private decompressIfApplicable(buffer: Buffer): Buffer {
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
