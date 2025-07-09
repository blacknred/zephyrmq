import msgpack from "@msgpack/msgpack";
import type { ISchema } from "@domain/interfaces/ISchema";
import type { ISerializer } from "@domain/interfaces/ISerializer";

export class BinarySchemaSerializer implements ISerializer {
  serialize<T>(data: T, schema?: ISchema<T> | undefined): Buffer {
    if (schema) return schema.serialize(data);
    if (Buffer.isBuffer(data)) return data;
    return Buffer.from(msgpack.encode(data));
  }
}
