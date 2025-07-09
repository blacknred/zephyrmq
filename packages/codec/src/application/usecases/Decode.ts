import type { IDecompressor } from "@domain/interfaces/IDecompressor";
import type { IDecryptor } from "@domain/interfaces/IDecryptor";
import type { IDeserializer } from "@domain/interfaces/IDeserializer";
import type { ISchemaRegistry } from "@domain/interfaces/ISchemaRegistry";

export class Decode {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private deserializer: IDeserializer,
    private decompressor: IDecompressor,
    private decryptor?: IDecryptor
  ) {}

  execute<T>(buffer: Buffer, schemaRef?: string): T {
    try {
      const decrypted = this.decryptor?.decrypt(buffer) ?? buffer;
      const decompressed = this.decompressor.decompress(decrypted);

      if (!schemaRef) return this.deserializer.deserialize(decompressed);

      const schema = this.schemaRegistry.getSchema<T>(schemaRef);
      return this.deserializer.deserialize<T>(decompressed, schema);
    } catch (cause) {
      throw new Error(`Decoding failed`, { cause });
    }
  }
}
