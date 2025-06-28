import type { TransferListItem } from "node:worker_threads";
import type { IDecoder } from "src/domain/interfaces/IDecoder";
import type { ISchemaRegistry } from "src/domain/interfaces/ISchemaRegistry";
import type { WorkerPool } from "../WorkerPool";

export class Decode {
  constructor(
    private decoder: IDecoder,
    private schemaRegistry: ISchemaRegistry,
    private workerPool: WorkerPool,
    private sizeThreshold: number
  ) {}

  async execute<T>(buffer: Buffer, schemaRef?: string) {
    if (buffer.length > this.sizeThreshold) {
      // buffer => uint8array for transferlist
      const arrayBuffer = buffer.buffer as TransferListItem;
      const byteOffset = buffer.byteOffset;
      const byteLength = buffer.byteLength;

      return this.workerPool.send<T>(
        "decode",
        [{ byteOffset, byteLength }, schemaRef],
        [arrayBuffer]
      );
    }

    const schema = schemaRef
      ? this.schemaRegistry.getSchema<T>(schemaRef)
      : undefined;

    return this.decoder.decode<T>(buffer, schema);
  }
}
