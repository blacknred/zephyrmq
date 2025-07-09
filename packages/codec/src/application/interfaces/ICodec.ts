import type { IEncryptionOptions } from "@domain/interfaces/IEncryptionOptions";

export interface ICodec {
  registerSchema<T>(name: string, schema: T): Promise<void>;
  removeSchema(name: string): Promise<void>;
  encode<T>(data: T, schemaRef?: string, compress?: boolean): Promise<Buffer>;
  decode<T>(buffer: Buffer, schemaRef?: string): Promise<T>;
}

export interface ICodecConfig<Schema> {
  threadingSizeThreshold?: number;
  workersCount?: number;
  precompiledSchemas?: Record<string, Schema>;
  compressionSizeThreshold?: number;
  encryptionOptions?: IEncryptionOptions;
}
