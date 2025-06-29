import msgpack from "@msgpack/msgpack";
import type { IDeserializer } from "@domain/ports/IDeserializer";
import type { ISchema } from "@domain/ports/ISchema";

export class BinarySchemaDeserializer implements IDeserializer {
  deserialize<T>(buffer: Buffer, schema?: ISchema<T>): T {
    if (schema) return schema.deserialize(buffer);

    const uint8Array = new Uint8Array(
      buffer.buffer,
      buffer.byteOffset,
      buffer.byteLength
    );

    return msgpack.decode(uint8Array) as T;
  }
}
