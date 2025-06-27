import type { FileHandle } from "fs/promises";
import type { ILogManager } from "src/domain/ports/ILogManager";
import type { ILogger } from "src/domain/ports/ILogger";
import type { IReader } from "src/domain/ports/IReader";

export class FileReader implements IReader {
  constructor(
    private logManager: ILogManager<FileHandle>,
    private logger?: ILogger
  ) {}

  async read(offset: number, length: number): Promise<Buffer | void> {
    if (!this.logManager.log) return;

    try {
      const buffer = Buffer.alloc(length);
      await this.logManager.log.read(buffer, 0, length, offset);
      return buffer;
    } catch (error) {
      this.logger?.error("Failed to read from WAL", { error });
    }
  }
}
