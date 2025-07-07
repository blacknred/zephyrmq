import type {
  IDBFlushManager,
  IDBFlushManagerConfig,
  IFlushTask,
} from "@domain/ports/IDBFlushManager";

export class IFlushTaskRegistry {
  private flushes: Array<IFlushTask> = [];

  register(task: IFlushTask) {
    this.flushes.push(task);
  }

  unregister(task: IFlushTask) {
    const idx = this.flushes.indexOf(task);
    if (idx !== -1) this.flushes.splice(idx, 1);
  }

  getTasks(): Array<IFlushTask> {
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

export class FlushManager implements IDBFlushManager {
  private taskRegistry: IFlushTaskRegistry;
  private memoryChecker: MemoryPressureChecker;

  private maxPendingFlushes = 100;
  private persistThresholdMs = 1000;
  private pendingCounter = 0;
  private timer?: NodeJS.Timeout;

  constructor({
    memoryUsageThresholdMB,
    persistThresholdMs,
    maxPendingFlushes,
  }: IDBFlushManagerConfig) {
    this.taskRegistry = new IFlushTaskRegistry();
    this.memoryChecker = new MemoryPressureChecker(memoryUsageThresholdMB);

    if (maxPendingFlushes) {
      this.maxPendingFlushes = maxPendingFlushes;
    }

    if (persistThresholdMs) {
      this.persistThresholdMs = persistThresholdMs;
    }

    this.init();
  }

  private init() {
    if (this.persistThresholdMs === Infinity) return;
    this.timer = setInterval(
      this.flush,
      Math.max(this.persistThresholdMs, 100)
    );

    process.on("beforeExit", this.close);
  }

  close = () => {
    clearInterval(this.timer);
    this.timer = undefined;
    this.flush();
  };

  register(task: IFlushTask) {
    this.taskRegistry.register(task);
  }

  unregister(task: IFlushTask) {
    this.taskRegistry.unregister(task);
  }

  commit() {
    if (
      ++this.pendingCounter >= this.maxPendingFlushes ||
      this.memoryChecker.isMemoryPressured()
    ) {
      this.flush();
    }
  }

  private flush = async () => {
    if (!this.pendingCounter) return;
    this.pendingCounter = 0;
    const tasks = this.taskRegistry.getTasks();
    await Promise.all(tasks.map((task) => task()));
  };
}
