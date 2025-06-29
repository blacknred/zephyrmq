import type { ISegmentLog } from "@app/interfaces/ISegmentLog";
import { SegmentLog } from "@app/SegmentLog";
import { AppendRecord } from "@app/usecases/AppendRecord";
import { CloseLog } from "@app/usecases/CloseLog";
import { CollectMetrics } from "@app/usecases/CollectMetrics";
import { CompactSegments } from "@app/usecases/CompactSegments";
import { ReadRecord } from "@app/usecases/ReadRecord";
import { FileAppender } from "@infra/filesystem/FileAppender";
import { FileCompactor } from "@infra/filesystem/FileCompactor";
import { FileIndexManager } from "@infra/filesystem/FileIndexManager";
import { FileReader } from "@infra/filesystem/FileReader";
import { FileSegmentManager } from "@infra/filesystem/FileSegmentManager";
import { join } from "node:path";

export class FileSegmentLogFactory {
  constructor(
    private readonly baseDir: string,
    private readonly maxSegmentSizeBytes: number = 10 * 1024 * 1024 // 10MB
  ) {}

  create(name: string): ISegmentLog {
    const dir = join(this.baseDir, name);

    const segmentManager = new FileSegmentManager(
      dir,
      this.maxSegmentSizeBytes
    );
    const indexManager = new FileIndexManager();
    const reader = new FileReader(segmentManager);
    const appender = new FileAppender(segmentManager, indexManager);
    const compactor = new FileCompactor(dir, segmentManager);

    return new SegmentLog(
      new AppendRecord(appender),
      new ReadRecord(reader),
      new CompactSegments(compactor),
      new CloseLog(segmentManager),
      new CollectMetrics(segmentManager)
    );
  }
}
