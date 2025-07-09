import type { ICodec, ICodecConfig } from "@app/interfaces/ICodec";
import { SizeThresholdThreadingPolicy } from "@app/policies/SizeThresholdThreadingPolicy";
import { Codec } from "@app/services/Codec";
import { ThreadedCodec } from "@app/services/ThreadedCodec";
import { Decode } from "@app/usecases/Decode";
import { Encode } from "@app/usecases/Encode";
import { RegisterSchema } from "@app/usecases/RegisterSchema";
import { RemoveSchema } from "@app/usecases/RemoveSchema";
import type { ISchema } from "@domain/interfaces/ISchema";
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
import { WorkerPoolFactory } from "@infra/factories/WorkerPoolFactory";

export class BinarySchemaCodecFactory {
  create(config: ICodecConfig<ISchema<unknown>>): ICodec {
    const {
      compressionSizeThreshold,
      precompiledSchemas,
      workersCount,
      encryptionOptions,
      threadingSizeThreshold = 10_000,
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

    const codec = new Codec(
      new Encode(schemaRegistry, serializer, compressor, encryptor),
      new Decode(schemaRegistry, deserializer, decompressor, decryptor),
      new RegisterSchema(schemaRegistrar),
      new RemoveSchema(schemaRemover)
    );

    if (!workersCount) return codec;

    // workers
    const workerpool = new WorkerPoolFactory().create(config, workersCount);
    const threadingPolicy = new SizeThresholdThreadingPolicy(
      new BinarySchemaSizeCalculator(),
      schemaRegistry,
      threadingSizeThreshold
    );

    return new ThreadedCodec(codec, workerpool, threadingPolicy);
  }
}
