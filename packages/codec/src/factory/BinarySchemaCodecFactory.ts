import { join } from "node:path";
import { Codec } from "src/application/Codec";
import { Decode } from "src/application/usecases/Decode";
import { Encode } from "src/application/usecases/Encode";
import { RegisterSchema } from "src/application/usecases/RegisterSchema";
import { RemoveSchema } from "src/application/usecases/RemoveSchema";
import { WorkerPool } from "src/application/WorkerPool";
import type { ICodec } from "src/domain/interfaces/ICodec";
import type { IEncryptionOptions } from "src/domain/interfaces/IEncryptionOptions";
import type { ISchema } from "src/domain/interfaces/ISchema";
import { BinarySchemaDecoder } from "src/infrastructure/binarySchema/BinarySchemaDecoder";
import { BinarySchemaEncoder } from "src/infrastructure/binarySchema/BinarySchemaEncoder";
import { BinarySchemaRegistrar } from "src/infrastructure/binarySchema/BinarySchemaRegistrar";
import { BinarySchemaRegistry } from "src/infrastructure/binarySchema/BinarySchemaRegistry";
import { BinarySchemaRemover } from "src/infrastructure/binarySchema/BinarySchemaRemover";
import { BinarySchemaSizeCalculator } from "src/infrastructure/binarySchema/BinarySchemaSizeCalculator";

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
