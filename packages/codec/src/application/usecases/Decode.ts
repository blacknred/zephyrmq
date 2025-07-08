import type { IDecompressor } from "@domain/ports/IDecompressor";
import type { IDecryptor } from "@domain/ports/IDecryptor";
import type { IDeserializer } from "@domain/ports/IDeserializer";
import type { ISchemaRegistry } from "@domain/ports/ISchemaRegistry";
import type { WorkerPool } from "@infra/worker/WorkerPool";

export class Decode {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private workerPool: WorkerPool,
    private sizeThreshold: number,
    private deserializer: IDeserializer,
    private decompressor: IDecompressor,
    private decryptor?: IDecryptor
  ) {}

  async execute<T>(buffer: Buffer, schemaRef?: string): Promise<T> {
    if (buffer.length > this.sizeThreshold) {
      // buffer => uint8array for transferlist
      const { byteOffset, byteLength } = buffer;

      return this.workerPool.send<T>(
        "decode",
        [{ byteOffset, byteLength }, schemaRef],
        [buffer.buffer]
      );
    }

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
