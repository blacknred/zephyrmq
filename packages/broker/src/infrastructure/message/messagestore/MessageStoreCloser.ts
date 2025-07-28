import type { IMessageRetentionManager } from "@domain/interfaces/message/IMessageRetentionManager";
import type { IMessageStoreCloser } from "@domain/interfaces/message/IMessageStoreCloser";
import type { IMapStore } from "@zephyrmq/mapstore";
import type { ISegmentLog } from "@zephyrmq/segmentlog";
import type { IWriteAheadLog } from "@zephyrmq/wal";

export class MessageStoreService implements IMessageStoreCloser {
  constructor(
    private retentionManager: IMessageRetentionManager,
    private db: IMapStore,
    private wal: IWriteAheadLog,
    private log: ISegmentLog
  ) {}

  async close() {
    this.retentionManager.stop();
    await Promise.all([this.wal.close(), this.log.close(), this.db.close()]);
  }
}
