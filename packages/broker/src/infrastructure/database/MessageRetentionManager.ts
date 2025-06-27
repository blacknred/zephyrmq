import type { IMessageRetentionManager } from "src/domain/services/IMessageRetentionManager";

export class MessageRetentionManager implements IMessageRetentionManager {
  private retentionTimer?: NodeJS.Timeout;

  constructor(
    private db: Level<string, Buffer>,
    private wal: IWriteAheadLog,
    private log: IMessageLog,
    private dlqManager: IDLQManager<any>,
    private codec: ICodec,
    private logger?: ILogCollector,
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
    return this.db.put(`del!${id}`, Buffer.from("1"));
  }

  async unmarkDeletable(id: number): Promise<void> {
    return this.db.del(`del!${id}`);
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