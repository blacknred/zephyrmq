import type { ISchema } from "../schema/types";

export interface ICodec {
  encode<T>(data: T, schema?: ISchema<T>, compress?: boolean): Buffer;
  decode<T>(buffer: Buffer, schema?: ISchema<T>): T;
}
