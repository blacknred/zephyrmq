import type { IFlushManager } from "@domain/ports/IFlushManager";
import { Level } from "level";

export class FlushTaskRegistry {
  private flushes: Array<() => Promise<void>> = [];

  register(task: () => Promise<void>) {
    this.flushes.push(task);
  }

  unregister(task: () => Promise<void>) {
    const idx = this.flushes.indexOf(task);
    if (idx !== -1) this.flushes.splice(idx, 1);
  }

  getTasks(): Array<() => Promise<void>> {
    return [...this.flushes];
  }
}

export class MemoryPressureChecker {
  constructor(private thresholdMB: number = 1024) {}

  isMemoryPressured(): boolean {
    const { rss } = process.memoryUsage();
    const usedMB = Math.round(rss / 1024 / 1024);
    return usedMB > this.thresholdMB;
  }
}

export class FlushManager implements IFlushManager {
  private pendingCounter = 0;
  private timer?: NodeJS.Timeout;

  constructor(
    private taskRegistry = new FlushTaskRegistry(),
    private memoryChecker = new MemoryPressureChecker(1024),
    private persistThresholdMs = 1000,
    private maxPendingFlushes = 100
  ) {
    this.init();
  }

  private init() {
    if (this.persistThresholdMs === Infinity) return;
    this.timer = setInterval(
      () => this.flush(),
      Math.max(this.persistThresholdMs, 100)
    );

    process.on("beforeExit", () => this.close());
  }

  close() {
    clearInterval(this.timer);
    this.timer = undefined;
    this.flush();
  }

  register(task: () => Promise<void>) {
    this.taskRegistry.register(task);
  }

  unregister(task: () => Promise<void>) {
    this.taskRegistry.unregister(task);
  }

  commit() {
    if (++this.pendingCounter < this.maxPendingFlushes) return;
    if (!this.memoryChecker.isMemoryPressured()) return;
    this.flush();
  }

  private async flush() {
    if (!this.pendingCounter) return;
    this.pendingCounter = 0;
    const tasks = this.taskRegistry.getTasks();
    await Promise.all(tasks.map((task) => task()));
  }
}
const db = new Level("./broker.db", {
      compression: false,
      // valueEncoding: {
      //   type: "custom",
      //   encode: codec.encodeSync,
      //   decode: codec.decodeSync,
      //   buffer: true,
      // },
    });
    db.sublevel