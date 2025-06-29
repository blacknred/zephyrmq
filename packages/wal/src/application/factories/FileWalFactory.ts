import type { IWriteAheadLog } from "@app/interfaces/IWriteAheadLog";
import { AppendRecord } from "@app/usecases/AppendRecord";
import { CloseLog } from "@app/usecases/CloseLog";
import { CollectMetrics } from "@app/usecases/CollectMetrics";
import { ReadRecord } from "@app/usecases/ReadRecord";
import { TruncateRecords } from "@app/usecases/TruncateRecords";
import { WriteAheadLog } from "@app/WriteAheadLog";
import { FileAppender } from "@infra/filesystem/FileAppender";
import { FileLogManager } from "@infra/filesystem/FileLogManager";
import { FileReader } from "@infra/filesystem/FileReader";
import { FileTruncator } from "@infra/filesystem/FileTruncator";

export class FileWriteAheadLogFactory {
  constructor(
    private readonly maxBatchSizeBytes: number = 10 * 1024 // 10KB
  ) {}

  create(filePath: string, maxBatchSizeBytes?: number): IWriteAheadLog {
    const logManager = new FileLogManager(filePath);

    const appender = new FileAppender(
      logManager,
      maxBatchSizeBytes ?? this.maxBatchSizeBytes
    );
    const reader = new FileReader(logManager);
    const truncator = new FileTruncator(filePath, logManager);

    return new WriteAheadLog(
      new AppendRecord(appender),
      new ReadRecord(reader),
      new TruncateRecords(truncator),
      new CloseLog(logManager, appender),
      new CollectMetrics(logManager, appender)
    );
  }
}
