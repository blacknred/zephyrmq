import type { IDBCloser } from "@domain/ports/IDBCloser";
import type { IDBFlushManager } from "@domain/ports/IDBFlushManager";
import { Level } from "level";

export class LevelDbCloser implements IDBCloser {
  constructor(
    private readonly db: Level<string, unknown>,
    private readonly flushManager: IDBFlushManager
  ) {
    process.on("SIGINT", async () => {
      await this.close();
    });
  }

  async close(): Promise<void> {
    this.flushManager.close();
    await this.db.close();
  }
}
