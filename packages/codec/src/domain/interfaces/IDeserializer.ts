import type { ISchema } from "./ISchema";

export interface IDeserializer {
  deserialize<T>(buffer: Buffer, schema?: ISchema<T>): T;
}
