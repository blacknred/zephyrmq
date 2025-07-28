import type { ILogger } from "@app/interfaces/ILogger";
import { BasicSchema } from "@domain/entities/BasicSchema";
import { MessageMetadata } from "@domain/entities/MessageMetadata";
import type { IDLQManager } from "@domain/interfaces/dlq/IDLQManager";
import type { IMessageRetentionManager } from "@domain/interfaces/message/IMessageRetentionManager";
import type { ICodec } from "@zephyrmq/codec";
import type { IMap } from "@zephyrmq/mapstore";
import { SegmentPointer, type ISegmentLog } from "@zephyrmq/segmentlog";
import type { IWriteAheadLog } from "@zephyrmq/wal";

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

    // 1. delete from db
    for await (const id of this.deleted.keys()) {
      try {
        const pointerBuffer = await this.pointers.get(id);
        if (pointerBuffer) {
          const pointer = await this.codec.decode<SegmentPointer>(
            pointerBuffer,
            BasicSchema.SegmentPointer
          );

          pointersToDelete.push(pointer);
        }

        this.metadatas.delete(id);
        this.pointers.delete(id);
        this.deleted.delete(id);
      } catch (error) {
        this.logger?.log(
          `MessageStore cannot delete ${id}`,
          { error, id },
          "error"
        );
      }
    }

    // 2. delete from log
    if (pointersToDelete.length) {
      await this.log.remove(pointersToDelete);
    }

    // 3. truncate the wal
    const offsetBuffer = await this.systemData.get("last_wal_offset");
    if (offsetBuffer) {
      await this.wal.truncate(+offsetBuffer.toSorted());
    }

    return pointersToDelete.length;
  }

  private async processTtl() {
    // `ttl!${ttl}:${meta.id}`
    for await (const [key] of this.ttls.iterator({
      lt: `ts!${Date.now() + this.retentionMs}`,
    })) {
      const id = key.split(":")[1];

      try {
        const metaBuffer = await this.metadatas.get(id);
        if (!metaBuffer) continue;

        const meta = await this.codec.decode<MessageMetadata>(
          metaBuffer,
          BasicSchema.MessageMetadata
        );

        this.ttls.delete(key);
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
