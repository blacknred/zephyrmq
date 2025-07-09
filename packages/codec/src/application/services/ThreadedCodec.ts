import type { ICodec } from "@app/interfaces/ICodec";
import type { IThreadingPolicy } from "@domain/interfaces/IThreadingPolicy";
import type { IWorkerPool } from "@domain/interfaces/IWorkerPool";

export class ThreadedCodec implements ICodec {
  constructor(
    private base: ICodec,
    private workerPool: IWorkerPool,
    private threadingPolicy: IThreadingPolicy
  ) {}

  async encode<T>(
    data: T,
    schemaRef?: string,
    compress = false
  ): Promise<Buffer> {
    if (this.threadingPolicy.shouldEncodeInThread(data, schemaRef)) {
      return this.workerPool.send<Buffer>("encode", [
        data,
        schemaRef,
        compress,
      ]);
    }

    return this.base.encode(data, schemaRef, compress);
  }

  async decode<T>(buffer: Buffer, schemaRef?: string): Promise<T> {
    if (this.threadingPolicy.shouldDecodeInThread(buffer)) {
      const { byteOffset, byteLength } = buffer;
      return this.workerPool.send<T>(
        "decode",
        [{ byteOffset, byteLength }, schemaRef],
        [buffer.buffer]
      );
    }

    return this.base.decode(buffer, schemaRef);
  }

  async registerSchema<T>(name: string, schema: T) {
    await this.base.registerSchema(name, schema);
    await this.workerPool.sendToAll<void>("registerSchema", [name, schema]);
  }

  async removeSchema(name: string) {
    await this.base.removeSchema(name);
    await this.workerPool.sendToAll<void>("removeSchema", [name]);
  }
}
