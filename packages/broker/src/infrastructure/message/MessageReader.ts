import { BasicSchema } from "@domain/entities/BasicSchema";
import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { ILogger } from "@app/interfaces/ILogger";
import type { IMessageReader } from "@domain/interfaces/IMessageReader";
import type { ICodec } from "@zephyrmq/codec";
import type { IMap } from "@zephyrmq/mapstore";
import type { ISegmentLog, SegmentPointer } from "@zephyrmq/segmentlog";

// `ptr!${id}`, `meta!${id}`

export class MessageReader<T> implements IMessageReader<T> {
  constructor(
    private log: ISegmentLog,
    private pointers: IMap<number, Buffer>,
    private metadatas: IMap<number, Buffer>,
    private codec: ICodec,
    private schemaId?: string,
    private logger?: ILogger
  ) {}

  async read(id: number) {
    return Promise.all([this.readMessage(id), this.readMetadata(id)]);
  }

  async readMessage(id: number) {
    try {
      const pointerBuffer = await this.pointers.get(id);
      if (!pointerBuffer) return;

      const pointer = await this.codec.decode<SegmentPointer>(
        pointerBuffer,
        BasicSchema.SegmentPointer
      );

      const messageBuffer = await this.log.read(pointer);
      if (!messageBuffer) return;

      return this.codec.decode<T>(messageBuffer, this.schemaId);
    } catch (error) {
      this.logger?.log("Failed message reading", { id, error }, "error");
    }
  }

  async readMetadata(id: number) {
    try {
      const metaBuffer = await this.metadatas.get(id);
      if (!metaBuffer) return;

      return this.codec.decode<MessageMetadata>(
        metaBuffer,
        BasicSchema.MessageMetadata
      );
    } catch (error) {
      this.logger?.log("Failed metadata reading", { id, error }, "error");
    }
  }
}
