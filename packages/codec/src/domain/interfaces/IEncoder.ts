import type { ISchema } from "./ISchema";

export interface IEncoder {
  encode<T>(data: T, schema?: ISchema<T>, compress?: boolean): Buffer;
}
