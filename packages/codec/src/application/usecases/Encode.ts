import type { ICompressor } from "@domain/interfaces/ICompressor";
import type { IEncryptor } from "@domain/interfaces/IEncryptor";
import type { ISchemaRegistry } from "@domain/interfaces/ISchemaRegistry";
import type { ISerializer } from "@domain/interfaces/ISerializer";

export class Encode<T> {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private serializer: ISerializer,
    private compressor: ICompressor,
    private encryptor?: IEncryptor
  ) {}

  execute(data: T, schemaRef?: string, compress = false) {
    const schema = schemaRef
      ? this.schemaRegistry.getSchema<T>(schemaRef)
      : undefined;

    try {
      const buffer = this.serializer.serialize(data, schema);
      const compressed = this.compressor.compress(buffer, compress);
      if (!this.encryptor) return compressed;
      return this.encryptor.encrypt(compressed);
    } catch (cause) {
      throw new Error(`Encoding failed`, { cause });
    }
  }
}
