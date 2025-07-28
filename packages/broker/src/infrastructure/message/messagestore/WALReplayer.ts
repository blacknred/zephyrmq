import { BasicSchema } from "@domain/entities/BasicSchema";
import type { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { IMessagePublisher } from "@domain/interfaces/message/IMessagePublisher";
import type { ILogger } from "@app/interfaces/ILogger";
import type { IWALReplayer } from "@domain/interfaces/IWALReplayer";
import type { ICodec } from "@zephyrmq/codec";
import type { IMap } from "@zephyrmq/mapstore";
import type { ISegmentLog } from "@zephyrmq/segmentlog";
import type { IWriteAheadLog } from "@zephyrmq/wal";

// "last_wal_offset", `meta!${meta.id}`, `ptr!${meta.id}`

export class WALReplayer implements IWALReplayer {
  constructor(
    private wal: IWriteAheadLog,
    private log: ISegmentLog,
    private metadatas: IMap<number, Buffer>,
    private pointers: IMap<number, Buffer>,
    private systemData: IMap<string, Buffer>,
    private codec: ICodec,
    private messagePublisher: IMessagePublisher,
    private logger?: ILogger
  ) {}

  async replay(): Promise<void> {
    let offset = 0;

    try {
      while (true) {
        const offsetBuffer = await this.systemData.get("last_wal_offset");
        if (offsetBuffer) offset = +offsetBuffer.toString();

        this.logger?.log(`Replaying WAL from offset ${offset}`, { offset });

        // 1. Read total length
        const totalLengthBytes = await this.wal.read(offset, 4);
        if (!totalLengthBytes || totalLengthBytes.length < 4) break;
        const totalLength = totalLengthBytes.readUInt32BE(0);

        // 2. Read full record body (metaLength + metadata + message)
        const recordBytes = await this.wal.read(offset + 4, totalLength);
        if (!recordBytes || recordBytes.length < totalLength) break;

        // 3. Extract metadata length
        const metaLength = recordBytes.readUInt32BE(0);
        const metaBuffer = recordBytes.slice(4, 4 + metaLength);
        const messageBuffer = recordBytes.slice(4 + metaLength);

        // 4. Decode and apply
        const meta = await this.codec.decode<MessageMetadata>(
          metaBuffer,
          BasicSchema.MessageMetadata
        );
        const pointer = await this.log.append(messageBuffer);
        if (!pointer) break;

        const pointerBuffer = await this.codec.encode(
          pointer,
          BasicSchema.SegmentPointer
        );

        this.metadatas.set(meta.id, metaBuffer);
        this.pointers.set(meta.id, pointerBuffer);

        // 5. Update WAL progress
        offset = offset + 4 + totalLength;
        this.systemData.set("last_wal_offset", Buffer.from(String(offset)));

        // 6. publish
        await this.messagePublisher.publish(meta);
      }

      if (offset === 0) return;
      await this.wal.truncate(offset);
      this.logger?.log(`Replayed from WAL`, { offset });
    } catch (error) {
      this.logger?.log("Failed WAL replay", { error }, "error");
    }
  }
}
