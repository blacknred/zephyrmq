import type { FileHandle } from "fs/promises";
import type { IAppender } from "@domain/ports/IAppender";
import type { ILogManager } from "@domain/ports/ILogManager";

export class CloseLog {
  constructor(
    private logManager: ILogManager<FileHandle>,
    private appender: IAppender
  ) {}

  async execute() {
    await this.appender.flush();
    this.logManager.close();
  }
}
