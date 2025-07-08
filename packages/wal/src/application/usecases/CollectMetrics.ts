import type { FileHandle } from "fs/promises";
import type { IAppender } from "@domain/interfaces/IAppender";
import type { ILogManager } from "@domain/interfaces/ILogManager";

export class CollectMetrics {
  constructor(
    private logManager: ILogManager<FileHandle>,
    private appender: IAppender
  ) {}

  async execute() {
    const stats = await this.logManager.log?.stat();
    return {
      size: stats?.size,
      batchSize: this.appender.batchSize,
      batchCount: this.appender.batch.length,
      isFlushing: this.appender.isFlushing,
    };
  }
}
