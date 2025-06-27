import type { MessageMetadata } from "src/domain/models/MessageMetadata";
import type { IMessageWriter } from "src/domain/services/IMessageWriter";

export class MessageWriter implements IMessageWriter {
  constructor(
    private wal: IWriteAheadLog,
    private log: IMessageLog,
    private db: Level<string, Buffer>,
    private codec: ICodec,
    private logger?: ILogCollector,
    private maxMessageTTLMs = 3_600_000_000
  ) {}

  async write(
    message: Buffer,
    meta: MessageMetadata
  ): Promise<number | undefined> {
    try {
      const metaBuffer = await this.codec.encode(meta, messageMetadataSchema);

      // Add length prefixes
      const metaLengthBuffer = Buffer.alloc(4);
      metaLengthBuffer.writeUInt32BE(metaBuffer.length, 0);
      const totalLength = 4 + metaBuffer.length + message.length;
      const totalLengthBuffer = Buffer.alloc(4);
      totalLengthBuffer.writeUInt32BE(totalLength, 0);

      // Combine into one wal record
      const walRecord = Buffer.concat([
        totalLengthBuffer,
        metaLengthBuffer,
        metaBuffer,
        message,
      ]);

      // 1. Write to WAL first for durability
      const walOffset = await this.wal.append(walRecord);
      if (!walOffset) throw new Error("Failed writing to WAL");

      // 2. Write to log for long-term storage
      const pointer = await this.log.append(message);
      if (!pointer) throw new Error("Failed writing to MessageLog");

      // 3. Store metadata, pointers and ttl in db.
      const pointerBuffer = await this.codec.encode(
        pointer,
        SegmentPointerSchema
      );

      const ttl = meta.ts + (meta.ttl || this.maxMessageTTLMs);
      await this.db.batch([
        { type: "put", key: `meta!${meta.id}`, value: metaBuffer },
        { type: "put", key: `ptr!${meta.id}`, value: pointerBuffer },
        {
          type: "put",
          key: `ttl!${ttl}:${meta.id}`,
          value: Buffer.alloc(0),
        },
      ]);

      // Update last_wal_offset immediately
      const lastWalOffset = String(walOffset + 4 + totalLength);
      await this.db.put("last_wal_offset", Buffer.from(lastWalOffset));

      return meta.id;
    } catch (error) {
      this.logger?.log("Failed to write message", { ...meta, error }, "error");
    }
  }
}