import { join } from "path";
import { AppendRecord } from "src/application/usecases/AppendRecord";
import { CloseLog } from "src/application/usecases/CloseLog";
import { CollectMetrics } from "src/application/usecases/CollectMetrics";
import { CompactSegments } from "src/application/usecases/CompactSegments";
import { ReadRecord } from "src/application/usecases/ReadRecord";
import { SegmentLog } from "src/application/SegmentLog";
import type { ISegmentLog } from "src/domain/interfaces/ISegmentLog";
import { FileAppender } from "src/infrastructure/filesystem/FileAppender";
import { FileIndexManager } from "src/infrastructure/filesystem/FileIndexManager";
import { FileReader } from "src/infrastructure/filesystem/FileReader";
import { FileSegmentManager } from "src/infrastructure/filesystem/FileSegmentManager";
import {
  LogCollector,
  type ILogCollector,
  type ILogger,
} from "src/utils/LogCollector";
import { FileCompactor } from "../infrastructure/filesystem/FileCompactor";

export interface ISegmentLogFactory {
  create(name: string): ISegmentLog;
}

export class FileSegmentLogFactory implements ISegmentLogFactory {
  private logger?: ILogCollector;

  constructor(
    private readonly baseDir: string,
    private readonly maxSegmentSizeBytes: number = 10 * 1024 * 1024, // 10MB
    logger?: ILogger
  ) {
    if (logger) this.logger = new LogCollector(logger);
  }

  create(name: string): ISegmentLog {
    const dir = join(this.baseDir, name);

    const segmentManager = new FileSegmentManager(
      dir,
      this.maxSegmentSizeBytes,
      this.logger
    );
    const indexManager = new FileIndexManager();
    const reader = new FileReader(segmentManager, this.logger);
    const appender = new FileAppender(
      segmentManager,
      indexManager,
      this.logger
    );
    const compactor = new FileCompactor(dir, segmentManager, this.logger);

    return new SegmentLog(
      new AppendRecord(appender),
      new ReadRecord(reader),
      new CompactSegments(compactor),
      new CloseLog(segmentManager),
      new CollectMetrics(segmentManager)
    );
  }
}
