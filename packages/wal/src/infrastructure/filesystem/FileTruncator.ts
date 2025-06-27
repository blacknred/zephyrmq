import type { FileHandle } from "fs/promises";
import fs from "fs/promises";
import type { ILogger } from "src/domain/ports/ILogger";
import type { ILogManager } from "src/domain/ports/ILogManager";
import type { ITruncator } from "src/domain/ports/ITruncator";

export class FileTruncator implements ITruncator {
  static HEADER_SIZE = 24;

  constructor(
    private filePath: string,
    private logManager: ILogManager<FileHandle>,
    private logger?: ILogger
  ) {}

  async truncate(upToOffset: number): Promise<void> {
    try {
      if (!this.logManager.log) {
        this.logManager.log = await fs.open(this.filePath, "r+");
      }

      await this.logManager.log.truncate(upToOffset);
      await this.logManager.log.sync();
    } catch (error) {
      this.logger?.error("Failed to truncate the WAL", { error });
    }
  }
}
