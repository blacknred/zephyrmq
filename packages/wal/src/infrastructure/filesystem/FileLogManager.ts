import type { FileHandle } from "fs/promises";
import fs from "fs/promises";
import path from "path";
import type { ILogManager } from "@domain/interfaces/ILogManager";

export class FileLogManager implements ILogManager<FileHandle> {
  public log?: FileHandle;

  constructor(private filePath: string) {
    this.init();
  }

  private async init() {
    try {
      await fs.mkdir(path.dirname(this.filePath), { recursive: true });
      this.log = await fs.open(this.filePath, "a+");
    } catch (cause) {
      throw new Error("Failed to open WAL file", { cause });
    }
  }

  [Symbol.asyncDispose]() {
    return this.close();
  }

  close() {
    this.log?.close();
  }
}
