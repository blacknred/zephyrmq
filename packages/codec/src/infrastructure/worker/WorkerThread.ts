import type { ICompressor } from "@domain/ports/ICompressor";
import type { IDecompressor } from "@domain/ports/IDecompressor";
import type { IDecryptor } from "@domain/ports/IDecryptor";
import type { IDeserializer } from "@domain/ports/IDeserializer";
import type { IEncryptor } from "@domain/ports/IEncryptor";
import type { ISchemaRegistrar } from "@domain/ports/ISchemaRegistrar";
import type { ISchemaRegistry } from "@domain/ports/ISchemaRegistry";
import type { ISchemaRemover } from "@domain/ports/ISchemaRemover";
import type { ISerializer } from "@domain/ports/ISerializer";
import { SnappyCompressor } from "@infra/compression/snappy/SnappyCompressor";
import { SnappyDecompressor } from "@infra/compression/snappy/SnappyDecompessor";
import { CipherDecryptor } from "@infra/encryption/cipher/CipherDecryptor";
import { CipherEncryptor } from "@infra/encryption/cipher/CipherEncryptor";
import { BinarySchemaDeserializer } from "@infra/serialization/binarySchema/BinarySchemaDeserializer";
import { BinarySchemaRegistrar } from "@infra/serialization/binarySchema/BinarySchemaRegistrar";
import { BinarySchemaRemover } from "@infra/serialization/binarySchema/BinarySchemaRemover";
import { BinarySchemaSerializer } from "@infra/serialization/binarySchema/BinarySchemaSerializer";
import { InMemorySchemaRegistry } from "@infra/serialization/registry/InMemorySchemaRegistry";
import {
  parentPort,
  workerData,
  type TransferListItem,
} from "node:worker_threads";
import type { WorkerRequest } from "./WorkerPool";

class Encode<T> {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private serializer: ISerializer,
    private compressor: ICompressor,
    private encryptor?: IEncryptor
  ) {}

  async execute(data: T, schemaRef?: string, compress = false) {
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

class Decode {
  constructor(
    private schemaRegistry: ISchemaRegistry,
    private deserializer: IDeserializer,
    private decompressor: IDecompressor,
    private decryptor?: IDecryptor
  ) {}

  async execute<T>(buffer: Buffer, schemaRef?: string): Promise<T> {
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

class RegisterSchema<T> {
  constructor(private schemaRegistrar: ISchemaRegistrar<T>) {}
  async execute(name: string, schema: T) {
    this.schemaRegistrar.register(name, schema);
  }
}

class RemoveSchema {
  constructor(private schemaRemover: ISchemaRemover) {}
  async execute(name: string) {
    this.schemaRemover.remove(name);
  }
}

class Worker<T> {
  constructor(
    private encoder: Encode<T>,
    private decoder: Decode,
    private schemaRegister: RegisterSchema<T>,
    private schemaRemover: RemoveSchema
  ) {}

  encode(data: T, schemaRef?: string, compress?: boolean) {
    return this.encoder.execute(data, schemaRef, compress);
  }

  decode(buffer: Buffer, schemaRef?: string) {
    return this.decoder.execute(buffer, schemaRef);
  }

  registerSchema(name: string, schema: T) {
    return this.schemaRegister.execute(name, schema);
  }

  removeSchema(name: string) {
    return this.schemaRemover.execute(name);
  }
}

// registry
const schemaRegistry = new InMemorySchemaRegistry(
  workerData.precompiledSchemas
);
const schemaRegistrar = new BinarySchemaRegistrar(schemaRegistry);
const schemaRemover = new BinarySchemaRemover(schemaRegistry);
// serialization
const serializer = new BinarySchemaSerializer();
const deserializer = new BinarySchemaDeserializer();
// compression
const compressor = new SnappyCompressor(workerData.compressionSizeThreshold);
const decompressor = new SnappyDecompressor();
// encryption
const encryptor =
  workerData.encryptionOptions &&
  new CipherEncryptor(workerData.encryptionOptions);
const decryptor =
  workerData.encryptionOptions &&
  new CipherDecryptor(workerData.encryptionOptions);

const worker = new Worker(
  new Encode(schemaRegistry, serializer, compressor, encryptor),
  new Decode(schemaRegistry, deserializer, decompressor, decryptor),
  new RegisterSchema(schemaRegistrar),
  new RemoveSchema(schemaRemover)
);

parentPort?.on("message", async (msg: WorkerRequest) => {
  const { id, method, args } = msg;

  try {
    let result: any;

    switch (method) {
      case "encode":
        {
          const [data, schemaRef, compress] = args;
          const buffer = await worker.encode(data, schemaRef, compress);
          // Extract ArrayBuffer for transfer
          result = buffer.buffer;
        }
        break;
      case "decode":
        {
          const [data, schemaRef] = args;
          const { byteOffset, byteLength } = data;
          const arrayBuffer = data.arrayBuffer as ArrayBuffer;
          // Reconstruct Buffer from transferred ArrayBuffer
          const buffer = Buffer.from(arrayBuffer, byteOffset, byteLength);
          result = worker.decode(buffer, schemaRef);
        }
        break;
      case "registerSchema":
        {
          const [name, schemaDef] = args;
          worker.registerSchema(name, schemaDef);
          result = true;
        }
        break;
      case "removeSchema":
        {
          const [schemaRef] = args;
          worker.removeSchema(schemaRef);
          result = true;
        }
        break;
      default:
        throw new Error(`Unknown method: ${method}`);
    }

    // If result is an ArrayBuffer, transfer it
    if (result instanceof ArrayBuffer) {
      parentPort!.postMessage({ id, result }, [result]);
    } else if (isTransferableObject(result)) {
      const transferables = collectTransferables(result);
      parentPort!.postMessage({ id, result }, transferables);
    } else {
      parentPort!.postMessage({ id, result });
    }
  } catch (err) {
    parentPort!.postMessage({ id, error: (err as Error).message });
  }
});

// Helper: Detect if object contains transferable objects
function isTransferableObject(obj: any): boolean {
  if (!obj || typeof obj !== "object") return false;
  if (obj instanceof ArrayBuffer) return true;
  for (const key in obj) {
    if (isTransferableObject(obj[key])) return true;
  }
  return false;
}

// Helper: Collect all transferable objects
function collectTransferables(obj: any): TransferListItem[] {
  const transferables: TransferListItem[] = [];
  function walk(val: any) {
    if (val instanceof ArrayBuffer) {
      transferables.push(val);
    } else if (val && typeof val === "object") {
      for (const key in val) {
        walk(val[key]);
      }
    }
  }

  walk(obj);
  return transferables;
}
