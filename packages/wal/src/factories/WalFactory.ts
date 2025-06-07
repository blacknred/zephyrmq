import type { IWriteAheadLog } from "src/domain/ports/IWriteAheadLog";
import {
  LogCollector,
  type ILogCollector,
  type ILogger,
} from "src/infrastructure/utils/LogCollector";
import { WriteAheadLog } from "src/infrastructure/wal/WriteAheadLog";

export interface IWriteAheadLogFactory {
  create(filePath: string, maxBatchSizeBytes?: number): IWriteAheadLog;
}

export class WriteAheadLogFactory implements IWriteAheadLogFactory {
  private logger?: ILogCollector;

  constructor(
    private readonly maxBatchSizeBytes: number = 10 * 1024, // 10KB
    logger?: ILogger
  ) {
    if (logger) this.logger = new LogCollector(logger);
  }

  create(filePath: string, maxBatchSizeBytes?: number): IWriteAheadLog {
    return new WriteAheadLog(
      filePath,
      maxBatchSizeBytes ?? this.maxBatchSizeBytes,
      this.logger
    );
  }
}
