import type { ISchema } from "./ISchema";

export interface ISerializer {
  serialize<T>(data: T, schema?: ISchema<T>): Buffer;
}
