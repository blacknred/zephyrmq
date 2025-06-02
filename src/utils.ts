export const wait = (ms = 1000, signal?: AbortSignal) =>
  new Promise((r, j) => {
    signal?.addEventListener("abort", j);
    setTimeout(r, ms);
  });

export const uniqueIntGenerator = () => {
  return Math.floor(Date.now() * Math.random());
};


interface ITimeoutScheduler {
  schedule(task: () => Promise<void>, delayMS: number): void;
  size(): number;
  stop(): void;
}
class TimeoutScheduler implements ITimeoutScheduler {
  private nextTimeout?: NodeJS.Timeout;
  private isProcessing = false;

  constructor(private queue: IPriorityQueue<[() => Promise<void>, number]>) {}

  schedule(task: () => Promise<void>, delayMS: number) {
    const readyTs = Date.now() + delayMS;
    this.queue.enqueue([task, readyTs], readyTs);
    this.setNextTimeout();
  }

  size() {
    return this.queue.size();
  }

  stop() {
    clearTimeout(this.nextTimeout);
  }

  private setNextTimeout(): void {
    if (this.isProcessing || this.queue.isEmpty()) return;

    const record = this.queue.peek();
    if (!record) return;

    const delay = Math.max(0, record[1] - Date.now());
    clearTimeout(this.nextTimeout);
    this.nextTimeout = setTimeout(this.onTimeoutHandler, delay);
  }

  private onTimeoutHandler = async () => {
    if (this.isProcessing) return;
    this.isProcessing = true;
    const now = Date.now();
    const tasks: Array<() => Promise<void>> = [];

    try {
      while (!this.queue.isEmpty()) {
        const [task, readyTs] = this.queue.peek()!;
        if (readyTs > now) break; // second peak is not ready yet
        this.queue.dequeue();
        tasks.push(task);
      }

      await Promise.all(tasks);
    } finally {
      this.isProcessing = false;
      this.setNextTimeout();
    }
  };
}