import type { IMessageReader } from "@domain/ports/IMessageReader";

export class MessageReader<Data> implements IMessageReader<Data> {
  constructor(
    private log: IMessageLog,
    private db: Level<string, Buffer>,
    private codec: ICodec,
    private schemaId?: string,
    private logger?: ILogCollector
  ) {}

  async read(id: number) {
    return Promise.all([this.readMessage(id), this.readMetadata(id)]);
  }

  async readMessage(id: number) {
    try {
      const pointerBuffer = await this.db.get(`ptr!${id}`);
      if (!pointerBuffer) return;

      const pointer = await this.codec.decode<SegmentPointer>(
        pointerBuffer,
        BasicSchemaNames.SegmentPointerSchema
      );

      const messageBuffer = await this.log.read(pointer);
      if (!messageBuffer) return;

      return this.codec.decode<Data>(messageBuffer, this.schemaId);
    } catch (error) {
      this.logger?.log("Failed message reading", { id, error }, "error");
    }
  }

  async readMetadata(id: number) {
    try {
      const metaBuffer = await this.db.get(`meta!${id}`);
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
