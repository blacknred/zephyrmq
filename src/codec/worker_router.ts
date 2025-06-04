import { join } from "path";
import type { TransferListItem } from "worker_threads";
import { BinaryCodec } from "./core/binary_codec";
import {
  EncryptedBinaryCodec,
  type IEncryptedCodecOptions,
} from "./core/encrypted_binary_codec";
import type { ICodec } from "./core/types";
import { BinarySchemaRegistry } from "./schema/registry";
import { BinarySchema } from "./schema/schema";
import type { Schema } from "./schema/types";
import { WorkerPool } from "./thread/pool";
import { BinarySchemaSizer } from "./schema/sizer";

export interface ICodecWorkerRouter {
  registerSchema<T>(name: string, schema: Schema<T>): Promise<boolean[]>;
  removeSchema(name: string): Promise<boolean[]>;
  encode<T>(data: T, schemaRef?: string, compress?: boolean): Promise<Buffer>;
  decode<T>(buffer: Buffer, schemaRef?: string): Promise<T>;
  encodeSync<T>(data: T, schemaRef?: string, compress?: boolean): Buffer;
  decodeSync<T>(buffer: Buffer, schemaRef?: string): T;
}

export interface CodecManagerConfig {
  sizeThreshold?: number;
  encryptionOptions?: IEncryptedCodecOptions;
}

export class CodecWorkerRouter implements ICodecWorkerRouter {
  private static readonly DEFAULT_SIZE_THRESHOLD = 10_000;

  private readonly codec: ICodec;
  private readonly sizeThreshold: number;
  private readonly workerpool: WorkerPool;
  private readonly schemaRegistry: BinarySchemaRegistry;

  constructor({ sizeThreshold, encryptionOptions }: CodecManagerConfig = {}) {
    this.sizeThreshold =
      sizeThreshold ?? CodecWorkerRouter.DEFAULT_SIZE_THRESHOLD;

    this.schemaRegistry = new BinarySchemaRegistry();

    this.workerpool = new WorkerPool(
      join(__dirname, "thread", "worker"),
      encryptionOptions
    );

    this.codec = encryptionOptions
      ? new EncryptedBinaryCodec(encryptionOptions)
      : new BinaryCodec();
  }

  async encode<T>(
    data: T,
    schemaRef?: string,
    compress = false
  ): Promise<Buffer> {
    const schema = this.getSchema<T>(schemaRef);
    const estimated = schema ? BinarySchemaSizer.estimateSize(data, schema) : 0;

    if (schema && estimated <= this.sizeThreshold) {
      return this.workerpool.send<Buffer>("encode", [
        data,
        schemaRef,
        compress,
      ]);
    }

    return this.codec.encode(data, schema, compress);
  }

  async decode<T>(buffer: Buffer, schemaRef?: string): Promise<T> {
    if (buffer.length > this.sizeThreshold) {
      // buffer => uint8array for transferlist
      const arrayBuffer = buffer.buffer as TransferListItem;
      const byteOffset = buffer.byteOffset;
      const byteLength = buffer.byteLength;

      return this.workerpool.send<T>(
        "decode",
        [{ byteOffset, byteLength }, schemaRef],
        [arrayBuffer]
      );
    }

    const schema = this.getSchema<T>(schemaRef);
    return this.codec.decode(buffer, schema);
  }

  encodeSync<T>(data: T, schemaRef?: string, compress = false): Buffer {
    const schema = this.getSchema<T>(schemaRef);
    return this.codec.encode(data, schema, compress);
  }

  decodeSync<T>(buffer: Buffer, schemaRef?: string): T {
    const schema = this.getSchema<T>(schemaRef);
    return this.codec.decode(buffer, schema);
  }

  async registerSchema<T>(name: string, schema: Schema<T>) {
    this.schemaRegistry.addSchema(name, new BinarySchema(schema));
    return this.workerpool.sendToAll<boolean>("registerSchema", [name, schema]);
  }

  async removeSchema(name: string) {
    this.schemaRegistry.removeSchema(name);
    return this.workerpool.sendToAll<boolean>("unregisterSchema", [name]);
  }

  private getSchema<T>(schemaRef?: string) {
    return schemaRef ? this.schemaRegistry.getSchema<T>(schemaRef) : undefined;
  }
}
