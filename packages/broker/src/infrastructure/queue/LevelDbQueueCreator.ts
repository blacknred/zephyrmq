import type { IFlushManager } from "@domain/ports/IFlushManager";
import type {
  IQueueCreator,
  IQueueOptions,
} from "@domain/ports/structs/IQueueCreator";
import type { Level } from "level";
import { LevelDbQueue } from "./structs/LevelDbQueue";

export class LevelDbQueueCreator implements IQueueCreator {
  constructor(
    private db: Level<string, unknown>,
    private flushManager: IFlushManager
  ) {}

  createQueue<T>(name: string, config: IQueueOptions<T> = {}) {
    const { maxSize, keyRetriever, valueSerializer } = config;
    return new LevelDbQueue<T>(
      this.db,
      this.flushManager,
      name,
      maxSize,
      keyRetriever,
      valueSerializer
    );
  }
}
