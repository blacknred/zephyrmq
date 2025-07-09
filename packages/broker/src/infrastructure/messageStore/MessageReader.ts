import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { ILogger } from "@domain/interfaces/ILogger";
import type { IMessageReader } from "@domain/interfaces/IMessageReader";
import type { IMap } from "@zephyrmq/mapstore/index";
import type { ISegmentLog, SegmentPointer } from "@zephyrmq/segmentlog/index";

// `ptr!${id}`, `meta!${id}`

export class MessageReader<T> implements IMessageReader<T> {
  constructor(
    private log: ISegmentLog,
    private pointers: IMap<string, Buffer>,
    private metadatas: IMap<string, Buffer>,
    private codec: ICodec,
    private schemaId?: string,
    private logger?: ILogger
  ) {}

  async read(id: number) {
    return Promise.all([this.readMessage(id), this.readMetadata(id)]);
  }

  async readMessage(id: number) {
    try {
      const pointerBuffer = this.pointers.get(`${id}`);
      if (!pointerBuffer) return;

      const pointer = await this.codec.decode<SegmentPointer>(
        pointerBuffer,
        BasicSchemaNames.SegmentPointerSchema
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
      const metaBuffer = this.metadatas.get(`${id}`);
      if (metaBuffer) return;

      return this.codec.decode<MessageMetadata>(
        metaBuffer,
        BasicSchemaNames.messageMetadataSchema
      );
    } catch (error) {
      this.logger?.log("Failed metadata reading", { id, error }, "error");
    }
  }
}
