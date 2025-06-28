import type { ICodec } from "src/domain/interfaces/ICodec";
import type { Decode } from "./usecases/Decode";
import { Encode } from "./usecases/Encode";
import type { RegisterSchema } from "./usecases/RegisterSchema";
import type { RemoveSchema } from "./usecases/RemoveSchema";

export class Codec implements ICodec {
  constructor(
    private encoder: Encode<any>,
    private decoder: Decode,
    private registrar: RegisterSchema<any>,
    private remover: RemoveSchema
  ) {}

  async encode<T>(
    data: T,
    schemaRef?: string,
    compress = false
  ): Promise<Buffer> {
    return this.encoder.execute(data, schemaRef, compress);
  }

  async decode<T>(buffer: Buffer, schemaRef?: string): Promise<T> {
    return this.decoder.execute(buffer, schemaRef);
  }

  async registerSchema<T>(name: string, schema: T) {
    return this.registrar.execute(name, schema);
  }

  async removeSchema(name: string) {
    return this.remover.execute(name);
  }
}
