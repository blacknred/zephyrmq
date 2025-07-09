import type { ICodec } from "@app/interfaces/ICodec";
import type { Decode } from "../usecases/Decode";
import type { Encode } from "../usecases/Encode";
import type { RegisterSchema } from "../usecases/RegisterSchema";
import type { RemoveSchema } from "../usecases/RemoveSchema";

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
    return this.decoder.execute<T>(buffer, schemaRef);
  }

  async registerSchema<T>(name: string, schema: T) {
    this.registrar.execute(name, schema);
  }

  async removeSchema(name: string) {
    this.remover.execute(name);
  }
}
