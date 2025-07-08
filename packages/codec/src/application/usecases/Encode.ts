import type { ICompressor } from "@domain/ports/ICompressor";
import type { IEncryptor } from "@domain/ports/IEncryptor";
import type { ISchemaBasedSizeCalculator } from "@domain/ports/ISchemaBasedSizeCalculator";
import type { ISchemaRegistry } from "@domain/ports/ISchemaRegistry";
import type { ISerializer } from "@domain/ports/ISerializer";
import type { WorkerPool } from "@infra/worker/WorkerPool";

export class Encode<T> {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private sizeCalculator: ISchemaBasedSizeCalculator,
    private workerPool: WorkerPool,
    private sizeThreshold: number,
    private serializer: ISerializer,
    private compressor: ICompressor,
    private encryptor?: IEncryptor
  ) {}

  async execute(data: T, schemaRef?: string, compress = false) {
    const schema = schemaRef
      ? this.schemaRegistry.getSchema<T>(schemaRef)
      : undefined;

    if (schema) {
      const size = this.sizeCalculator.calculate(data, schema);
      if (size >= this.sizeThreshold) {
        return this.workerPool.send<Buffer>("encode", [
          data,
          schemaRef,
          compress,
        ]);
      }
    }

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

// infra/facades/encoder,decoder