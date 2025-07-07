import type { ILogger } from "@domain/ports/ILogger";
import type { IWALReplayer } from "@domain/ports/IWALReplayer";
import type { IMap } from "@zephyrmq/mapstore/index";
import type { ISegmentLog } from "@zephyrmq/segmentlog/index";
import type { IWriteAheadLog } from "@zephyrmq/wal/index";

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
        const offsetBuffer = this.systemData.get("last_wal_offset");
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
        const meta = await this.codec.decode(metaBuffer, messageMetadataSchema);
        const pointer = await this.log.append(messageBuffer);
        if (!pointer) break;

        const pointerBuffer = await this.codec.encode(
          pointer,
          SegmentPointerSchema
        );

        this.metadatas.set(meta.id, metaBuffer);
        this.pointers.set(meta.id, pointerBuffer);
        // await this.db.batch([
        //   { type: "put", key: `meta!${meta.id}`, value: metaBuffer },
        //   { type: "put", key: `ptr!${meta.id}`, value: pointerBuffer },
        // ]);

        // 5. Update WAL progress
        offset = offset + 4 + totalLength;
        this.systemData.set("last_wal_offset", Buffer.from(String(offset)));
        // await this.db.put("last_wal_offset", Buffer.from(String(offset)));

        // 6. publish
        await this.messagePublisher.publish(meta);
      }

      if (offset > 0) {
        await this.wal.truncate(offset);
        this.logger?.log(`Replayed from WAL`, { offset });
      }
    } catch (error) {
      this.logger?.log("Failed WAL replay", { error }, "error");
    }
  }
}
