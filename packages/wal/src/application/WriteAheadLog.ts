import type { IWriteAheadLog } from "@app/interfaces/IWriteAheadLog";
import type { AppendRecord } from "./usecases/AppendRecord";
import type { CloseLog } from "./usecases/CloseLog";
import type { CollectMetrics } from "./usecases/CollectMetrics";
import type { ReadRecord } from "./usecases/ReadRecord";
import type { TruncateRecords } from "./usecases/TruncateRecords";

export class WriteAheadLog implements IWriteAheadLog {
  constructor(
    private appendRecord: AppendRecord,
    private readRecord: ReadRecord,
    private truncateRecords: TruncateRecords,
    private closeLog: CloseLog,
    private collectMetrics: CollectMetrics
  ) {}

  async append(data: Buffer): Promise<number | void> {
    return this.appendRecord.execute(data);
  }

  async read(offset: number, length: number): Promise<Buffer | void> {
    return this.readRecord.execute(offset, length);
  }

  async truncate(upToOffset: number): Promise<void> {
    return this.truncateRecords.execute(upToOffset);
  }

  async close() {
    await this.closeLog.execute();
  }

  async getMetrics() {
    return this.collectMetrics.execute();
  }
}
