import type { FileHandle } from "fs/promises";
import fs from "fs/promises";
import type { ILogManager } from "@domain/ports/ILogManager";
import type { ITruncator } from "@domain/ports/ITruncator";

export class FileTruncator implements ITruncator {
  static HEADER_SIZE = 24;

  constructor(
    private filePath: string,
    private logManager: ILogManager<FileHandle>
  ) {}

  async truncate(upToOffset: number): Promise<void> {
    try {
      if (!this.logManager.log) {
        this.logManager.log = await fs.open(this.filePath, "r+");
      }

      await this.logManager.log.truncate(upToOffset);
      await this.logManager.log.sync();
    } catch (cause) {
      throw new Error("Failed to truncate the WAL", { cause });
    }
  }
}
