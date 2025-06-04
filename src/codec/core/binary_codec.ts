import msgpack from "@msgpack/msgpack";
import ox from "ox";
import snappy from "snappy";
import type { ISchema } from "../schema/types";
import type { ICodec } from "./types";

export class BinaryCodec implements ICodec {
  private static readonly COMPRESSION_SIZE_THRESHOLD = 4_000; // 4 KB

  encode<T>(data: T, schema?: ISchema<T>, compress = false): Buffer {
    try {
      let buffer: Buffer;
      if (schema) buffer = schema.serialize(data);
      else if (Buffer.isBuffer(data)) buffer = data;
      else buffer = Buffer.from(msgpack.encode(data));

      return this.compressIfApplicable(buffer, compress);
    } catch (e) {
      throw new Error(`Encoding failed: ${e}`);
    }
  }

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

  private compressIfApplicable(data: Buffer, compress = false): Buffer {
    if (!compress || data.length < BinaryCodec.COMPRESSION_SIZE_THRESHOLD) {
      const header = Buffer.alloc(1);
      header.writeUInt8(0x00, 0); // not compressed
      return Buffer.concat([header, data]);
    }

    const compressed = snappy.compressSync(data);
    const header = Buffer.alloc(1);
    header.writeUInt8(0x01, 0); // compressed
    return Buffer.concat([header, compressed]);
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

  private jsonToBuffer(data: unknown): Buffer {
    const str = JSON.stringify(data);
    const buf = Buffer.allocUnsafe(Buffer.byteLength(str));
    buf.write(str, 0, "utf8");
    return buf;
  }

  private bufferToJson<T>(buffer: Buffer): T {
    return ox.Json.parse(buffer.toString("utf8")) as T;
  }
}

// example

// const codec = new BinaryCodec();

// type IUser = {
//   id: number;
//   name: string;
//   email?: string;
//   age: number;
// };
// const userJsonSchema = {
//   type: "object",
//   properties: {
//     id: { type: "integer" },
//     name: { type: "string" },
//     email: { type: "string" },
//     age: { type: "integer" },
//   },
//   required: ["id", "name", "age"],
// };
// const userSchema = compileSchemaFromJson<IUser>(userJsonSchema);
// const userBinSchema = createBinarySchema(userSchema);
// const userBinSchema1 = createBinarySchema<IUser>({
//   id: { type: "uint32" },
//   name: { type: "string" },
//   email: { type: "string", optional: true },
//   age: { type: "uint8" },
// });

// (async () => {
//   const user = {
//     id: 1,
//     name: "Alice",
//     email: "alice@example.com",
//     age: 30,
//   };
//   const buffer = await codec.encode(user, userBinSchema);
//   const decodedUser = await codec.decode(buffer, userBinSchema);
// })();
