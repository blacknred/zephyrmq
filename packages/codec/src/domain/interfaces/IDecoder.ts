import type { ISchema } from "./ISchema";

export interface IDecoder {
  decode<T>(buffer: Buffer, schema?: ISchema<T>): T;
}
