import type { ILogger } from "@domain/ports/ILogger";
import type { IMessageRetentionManager } from "@domain/ports/IMessageRetentionManager";
import type { IMap } from "@zephyrmq/mapstore/index";
import type { ISegmentLog, SegmentPointer } from "@zephyrmq/segmentlog/index";
import type { IWriteAheadLog } from "@zephyrmq/wal/index";

// `del!${id}`, `ptr!${id}`, `meta!${id}`, `ts!${Date.now() + this.retentionMs}`, "last_wal_offset"

export class MessageRetentionManager implements IMessageRetentionManager {
  private retentionTimer?: NodeJS.Timeout;

  constructor(
    private wal: IWriteAheadLog,
    private log: ISegmentLog,
    private metadatas: IMap<number, Buffer>,
    private pointers: IMap<number, Buffer>,
    private ttls: IMap<string, Buffer>,
    private deleted: IMap<number, Buffer>,
    private systemData: IMap<string, Buffer>,
    
    private dlqManager: IDLQManager<any>,
    private codec: ICodec,
    private logger?: ILogger,
    private retentionMs = 3_600_000
  ) {}

  start(): void {
    if (this.retentionMs === Infinity) return;
    this.retentionTimer = setInterval(
      this.retain,
      Math.min(this.retentionMs, 3_600_000)
    );
  }

  stop() {
    clearInterval(this.retentionTimer);
  }

  async markDeletable(id: number): Promise<void> {
    this.deleted.set(id, Buffer.from("1"));
  }

  async unmarkDeletable(id: number): Promise<void> {
    this.deleted.delete(id);
  }

  private async clearDeletable() {
    const pointersToDelete: SegmentPointer[] = [];
    const batch = this.db.batch();

    for await (const [key] of this.db.iterator({
      gt: `del!`,
      lt: `del~`,
    })) {
      const id = key.slice(4);

      try {
        const pointerBuffer = await this.db.get(`ptr!${id}`);
        if (pointerBuffer) {
          const pointer = await this.codec.decode(
            pointerBuffer,
            SegmentPointerSchema
          );
          pointersToDelete.push(pointer);
        }

        batch.del(`meta!${id}`);
        batch.del(`ptr!${id}`);
        batch.del(`del!${id}`);
      } catch (error) {
        this.logger?.log(
          `MessageStore cannot delete ${id}`,
          { error, id },
          "error"
        );
      }
    }

    // 1. db batch
    await batch.write();

    // 2. delete from log
    if (pointersToDelete.length) {
      await this.log.compactSegmentsByRemovingOldMessages(pointersToDelete);
    }

    // 3. truncate the wal
    const offsetBuffer = await this.db.get("last_wal_offset");
    await this.wal.truncate(+offsetBuffer.toSorted());

    return pointersToDelete.length;
  }

  private async processTtl() {
    for await (const [key] of this.db.iterator({
      lt: `ts!${Date.now() + this.retentionMs}`,
    })) {
      const id = key.split(":")[1];

      try {
        const metaBuffer = await this.db.get(`meta!${id}`);
        const meta = await this.codec.decode(metaBuffer, messageMetadataSchema);

        await this.db.del(key);
        this.dlqManager.enqueue(meta, "expired");
      } catch (error) {
        this.logger?.log(
          `MessageStore cannot send to dlq ${id}`,
          { error, id },
          "error"
        );
      }
    }
  }

  private retain = async () => {
    try {
      const [deletedCount] = await Promise.all([
        this.clearDeletable(),
        this.processTtl(),
      ]);

      this.logger?.log("MessageStore retention succeed", { deletedCount });
    } catch (error) {
      this.logger?.log("MessageStore retention failed", { error }, "error");
    }
  };
}
