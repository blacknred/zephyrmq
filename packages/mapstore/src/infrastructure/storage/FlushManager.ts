import type {
  IDBFlushManager,
  IFlushTask,
} from "@domain/interfaces/IDBFlushManager";
import type { IMemoryPressureChecker } from "@domain/interfaces/IMemoryPressureChecker";

export class FlushManager implements IDBFlushManager {
  private pendingCounter = 0;
  private timer?: NodeJS.Timeout;

  constructor(
    private memoryChecker: IMemoryPressureChecker,
    private taskRegistry: Set<IFlushTask>,
    private persistThresholdMs = 1000,
    private maxPendingFlushes = 100
  ) {
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
    this.taskRegistry.add(task);
  }

  unregister(task: IFlushTask) {
    this.taskRegistry.delete(task);
  }

  commit() {
    if (
      ++this.pendingCounter >= this.maxPendingFlushes ||
      this.memoryChecker.isPressured()
    ) {
      this.flush();
    }
  }

  private flush = async () => {
    if (!this.pendingCounter) return;
    this.pendingCounter = 0;
    const tasks = Array.from(this.taskRegistry);
    await Promise.all(tasks.map((task) => task()));
  };
}
