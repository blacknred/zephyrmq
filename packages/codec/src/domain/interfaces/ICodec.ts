export interface ICodec {
  registerSchema<T>(name: string, schema: T): Promise<boolean[]>;
  removeSchema(name: string): Promise<boolean[]>;
  encode<T>(data: T, schemaRef?: string, compress?: boolean): Promise<Buffer>;
  decode<T>(buffer: Buffer, schemaRef?: string): Promise<T>;
}
