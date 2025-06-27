import type { FileHandle } from "fs/promises";
import fs from "fs/promises";
import path from "path";
import type { ILogger } from "src/domain/ports/ILogger";
import type { ILogManager } from "src/domain/ports/ILogManager";

export class FileLogManager implements ILogManager<FileHandle> {
  public log?: FileHandle;

  constructor(
    private filePath: string,
    private logger?: ILogger
  ) {
    this.init();
  }

  private async init() {
    try {
      await fs.mkdir(path.dirname(this.filePath), { recursive: true });

      this.log = await fs.open(this.filePath, "a+");
    } catch (error) {
      this.logger?.error("Failed to initialize WAL", { error });
    }
  }

  [Symbol.asyncDispose]() {
    return this.close();
  }

  close() {
    this.log?.close();
  }
}
