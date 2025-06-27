import { AppendRecord } from "src/application/usecases/AppendRecord";
import { CloseLog } from "src/application/usecases/CloseLog";
import { CollectMetrics } from "src/application/usecases/CollectMetrics";
import { ReadRecord } from "src/application/usecases/ReadRecord";
import { TruncateRecords } from "src/application/usecases/TruncateRecords";
import { FileWriteAheadLog } from "src/application/WriteAheadLog";
import type { ILogger } from "src/domain/ports/ILogger";
import type { IWriteAheadLog } from "src/domain/ports/IWriteAheadLog";
import { FileAppender } from "src/infrastructure/filesystem/FileAppender";
import { FileLogManager } from "src/infrastructure/filesystem/FileLogManager";
import { FileReader } from "src/infrastructure/filesystem/FileReader";
import { FileTruncator } from "src/infrastructure/filesystem/FileTruncator";

export interface IWriteAheadLogFactory {
  create(filePath: string, maxBatchSizeBytes?: number): IWriteAheadLog;
}

export class FileWriteAheadLogFactory implements IWriteAheadLogFactory {
  constructor(
    private readonly maxBatchSizeBytes: number = 10 * 1024, // 10KB
    private readonly logger?: ILogger
  ) {}

  create(filePath: string, maxBatchSizeBytes?: number): IWriteAheadLog {
    const logManager = new FileLogManager(filePath, this.logger);
    const appender = new FileAppender(
      logManager,
      maxBatchSizeBytes ?? this.maxBatchSizeBytes,
      this.logger
    );
    const reader = new FileReader(logManager, this.logger);
    const truncator = new FileTruncator(filePath, logManager, this.logger);

    return new FileWriteAheadLog(
      new AppendRecord(appender),
      new ReadRecord(reader),
      new TruncateRecords(truncator),
      new CloseLog(logManager, appender),
      new CollectMetrics(logManager, appender)
    );
  }
}
