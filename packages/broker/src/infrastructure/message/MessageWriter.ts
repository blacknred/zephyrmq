import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { ILogger } from "@app/interfaces/ILogger";
import type { IMessageWriter } from "@domain/interfaces/IMessageWriter";
import type { ISegmentLog } from "@zephyrmq/segmentlog";
import type { IWriteAheadLog } from "@zephyrmq/wal";
import type { IMap } from "@zephyrmq/mapstore";
import type { ICodec } from "@zephyrmq/codec";
import { BasicSchema } from "@domain/entities/BasicSchema";

// `meta!${meta.id}`, `ptr!${meta.id}`, `ttl!${ttl}:${meta.id}`, "last_wal_offset"

export class MessageWriter implements IMessageWriter {
  constructor(
    private wal: IWriteAheadLog,
    private log: ISegmentLog,
    private metadatas: IMap<number, Buffer>,
    private pointers: IMap<number, Buffer>,
    private ttls: IMap<string, Buffer>,
    private systemData: IMap<string, Buffer>,
    private codec: ICodec,
    private logger?: ILogger,
    private maxMessageTTLMs = 3_600_000_000
  ) {}

  async write(
    message: Buffer,
    meta: MessageMetadata
  ): Promise<number | undefined> {
    try {
      const metaBuffer = await this.codec.encode(
        meta,
        BasicSchema.MessageMetadata
      );

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
        BasicSchema.SegmentPointer
      );

      const ttl = meta.ts + (meta.ttl || this.maxMessageTTLMs);
      this.metadatas.set(meta.id, metaBuffer);
      this.pointers.set(meta.id, pointerBuffer);
      this.ttls.set(`${ttl}:${meta.id}`, Buffer.alloc(0));

      // Update last_wal_offset immediately
      const lastWalOffset = String(walOffset + 4 + totalLength);
      this.systemData.set("last_wal_offset", Buffer.from(lastWalOffset));

      return meta.id;
    } catch (error) {
      this.logger?.log("Failed to write message", { ...meta, error }, "error");
    }
  }
}
