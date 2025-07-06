import type { IDBCloser } from "@domain/ports/IDBCloser";
import type { IFlushManager } from "@domain/ports/IFlushManager";
import { Level } from "level";

export class LevelDbCloser implements IDBCloser {
  constructor(
    private readonly db: Level<string, unknown>,
    private readonly flushManager: IFlushManager
  ) {}

  async close(): Promise<void> {
    this.flushManager.close();
    await this.db.close();
  }
}
