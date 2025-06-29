import { join } from "node:path";
import { Codec } from "@app/Codec";
import { Decode } from "@app/usecases/Decode";
import { Encode } from "@app/usecases/Encode";
import { RegisterSchema } from "@app/usecases/RegisterSchema";
import { RemoveSchema } from "@app/usecases/RemoveSchema";
import { WorkerPool } from "@infra/processor/worker/WorkerPool";
import type { ICodec } from "@app/interfaces/ICodec";
import type { IEncryptionOptions } from "@domain/ports/IEncryptionOptions";
import type { ISchema } from "@domain/ports/ISchema";
import { BinarySchemaDeserializer } from "@infra/serialization/binarySchema/BinarySchemaDeserializer";
import { BinarySchemaSerializer } from "@infra/serialization/binarySchema/BinarySchemaSerializer";
import { BinarySchemaRegistrar } from "@infra/serialization/binarySchema/BinarySchemaRegistrar";
import { InMemorySchemaRegistry } from "@infra/registry/InMemorySchemaRegistry";
import { BinarySchemaRemover } from "@infra/serialization/binarySchema/BinarySchemaRemover";
import { BinarySchemaSizeCalculator } from "@infra/serialization/binarySchema/BinarySchemaSizeCalculator";

export interface BinarySchemaCodecConfig {
  sizeThreshold?: number;
  precompiledSchemas?: Record<string, ISchema<unknown>>;
  compressionSizeThreshold?: number;
  encryptionOptions?: IEncryptionOptions;
}

export class BinarySchemaCodecFactory {
  private static readonly DEFAULT_SIZE_THRESHOLD = 10_000;
  private readonly sizeThreshold: number;

  constructor(private readonly config: BinarySchemaCodecConfig) {
    this.sizeThreshold =
      config.sizeThreshold ?? BinarySchemaCodecFactory.DEFAULT_SIZE_THRESHOLD;
  }

  create(): ICodec {
    const sizeCalculator = new BinarySchemaSizeCalculator();
    const encoder = new BinarySchemaEncoder(
      this.config.compressionSizeThreshold
    );
    const decoder = new BinarySchemaDecoder();
    const schemaRegistry = new BinarySchemaRegistry(
      this.config.precompiledSchemas
    );
    const schemaRegistrar = new BinarySchemaRegistrar(schemaRegistry);
    const schemaRemover = new BinarySchemaRemover(schemaRegistry);
    const workerpool = new WorkerPool(
      join(__dirname, "../infrastructure/binarySchema", "BinarySchemaWorker"),
      this.config
    );

    return new Codec(
      new Encode(
        encoder,
        schemaRegistry,
        sizeCalculator,
        workerpool,
        this.sizeThreshold
      ),
      new Decode(decoder, schemaRegistry, workerpool, this.sizeThreshold),
      new RegisterSchema(schemaRegistrar, workerpool),
      new RemoveSchema(schemaRemover, workerpool)
    );
  }
}
