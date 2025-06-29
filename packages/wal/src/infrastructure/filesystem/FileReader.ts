import type { FileHandle } from "fs/promises";
import type { ILogManager } from "@domain/ports/ILogManager";
import type { IReader } from "@domain/ports/IReader";

export class FileReader implements IReader {
  constructor(private logManager: ILogManager<FileHandle>) {}

  async read(offset: number, length: number): Promise<Buffer | void> {
    if (!this.logManager.log) return;

    try {
      const buffer = Buffer.alloc(length);
      await this.logManager.log.read(buffer, 0, length, offset);
      return buffer;
    } catch (cause) {
      throw new Error("Failed to read from WAL", { cause });
    }
  }
}
