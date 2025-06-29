import type { SegmentPointer } from "@domain/entities/SegmentPointer";
import type { ISegmentLog } from "./interfaces/ISegmentLog";
import type { AppendRecord } from "./usecases/AppendRecord";
import type { CloseLog } from "./usecases/CloseLog";
import type { CollectMetrics } from "./usecases/CollectMetrics";
import type { CompactSegments } from "./usecases/CompactSegments";
import type { ReadRecord } from "./usecases/ReadRecord";

export class SegmentLog implements ISegmentLog {
  constructor(
    private appendRecord: AppendRecord,
    private readRecord: ReadRecord,
    private compactSegments: CompactSegments,
    private closeLog: CloseLog,
    private collectMetrics: CollectMetrics
  ) {}

  async append(data: Buffer): Promise<SegmentPointer | void> {
    return this.appendRecord.execute(data);
  }

  async read(pointer: SegmentPointer): Promise<Buffer | void> {
    return this.readRecord.execute(pointer);
  }

  async remove(pointers: SegmentPointer[]): Promise<void> {
    return this.compactSegments.execute(pointers);
  }

  async close() {
    await this.closeLog.execute();
  }

  async getMetrics() {
    return this.collectMetrics.execute();
  }
}
