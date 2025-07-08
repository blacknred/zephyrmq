import { Codec } from "@app/Codec";
import type { ICodec } from "@app/interfaces/ICodec";
import { Decode } from "@app/usecases/Decode";
import { Encode } from "@app/usecases/Encode";
import { RegisterSchema } from "@app/usecases/RegisterSchema";
import { RemoveSchema } from "@app/usecases/RemoveSchema";
import type { IEncryptionOptions } from "@domain/ports/IEncryptionOptions";
import type { ISchema } from "@domain/ports/ISchema";
import { SnappyCompressor } from "@infra/compression/snappy/SnappyCompressor";
import { SnappyDecompressor } from "@infra/compression/snappy/SnappyDecompessor";
import { CipherDecryptor } from "@infra/encryption/cipher/CipherDecryptor";
import { CipherEncryptor } from "@infra/encryption/cipher/CipherEncryptor";
import { BinarySchemaDeserializer } from "@infra/serialization/binarySchema/BinarySchemaDeserializer";
import { BinarySchemaRegistrar } from "@infra/serialization/binarySchema/BinarySchemaRegistrar";
import { BinarySchemaRemover } from "@infra/serialization/binarySchema/BinarySchemaRemover";
import { BinarySchemaSerializer } from "@infra/serialization/binarySchema/BinarySchemaSerializer";
import { BinarySchemaSizeCalculator } from "@infra/serialization/binarySchema/BinarySchemaSizeCalculator";
import { InMemorySchemaRegistry } from "@infra/serialization/registry/InMemorySchemaRegistry";
import { WorkerPoolFactory } from "@infra/worker/WorkerPoolFactory";

export interface BinarySchemaCodecConfig {
  sizeThreshold?: number;
  workersCount?: number;
  precompiledSchemas?: Record<string, ISchema<unknown>>;
  compressionSizeThreshold?: number;
  encryptionOptions?: IEncryptionOptions;
}

export class BinarySchemaCodecFactory {
  create(config: BinarySchemaCodecConfig): ICodec {
    const {
      compressionSizeThreshold,
      precompiledSchemas,
      workersCount,
      encryptionOptions,
      sizeThreshold = 10_000,
    } = config;
    // registry
    const schemaRegistry = new InMemorySchemaRegistry(precompiledSchemas);
    const schemaRegistrar = new BinarySchemaRegistrar(schemaRegistry);
    const schemaRemover = new BinarySchemaRemover(schemaRegistry);

    // serialization
    const serializer = new BinarySchemaSerializer();
    const deserializer = new BinarySchemaDeserializer();

    // compression
    const compressor = new SnappyCompressor(compressionSizeThreshold);
    const decompressor = new SnappyDecompressor();

    // encryption
    const encryptor =
      encryptionOptions && new CipherEncryptor(encryptionOptions);
    const decryptor =
      encryptionOptions && new CipherDecryptor(encryptionOptions);

    // workers
    const sizeCalculator = new BinarySchemaSizeCalculator();
    const workerpool = new WorkerPoolFactory().create(config, workersCount);

    return new Codec(
      new Encode(
        schemaRegistry,
        sizeCalculator,
        workerpool,
        sizeThreshold,
        serializer,
        compressor,
        encryptor
      ),
      new Decode(
        schemaRegistry,
        workerpool,
        sizeThreshold,
        deserializer,
        decompressor,
        decryptor
      ),
      new RegisterSchema(schemaRegistrar, workerpool),
      new RemoveSchema(schemaRemover, workerpool)
    );
  }
}
