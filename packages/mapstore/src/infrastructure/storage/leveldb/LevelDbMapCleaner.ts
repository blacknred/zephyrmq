import type { ICleaner } from "../../../domain/interfaces/ICleaner";
import { Mutex } from "@infra/util/Mutex";
import type { Level } from "level";

export class LevelDbMapCleaner implements ICleaner {
  private mutex = new Mutex();

  constructor(
    private db: Level<string, unknown>,
    private name: string
  ) {}

  async clear() {
    const prefix = `${this.name}!`;
    const suffix = `${this.name}~`;

    await this.mutex.acquire();

    try {
      await this.db.clear({ gt: prefix, lt: suffix });
    } catch (cause) {
      throw new Error(`Failed to clear ${this.name}`, { cause });
    } finally {
      this.mutex.release();
    }
  }
}
