export class Mutex {
  private queue: (() => void)[] = [];
  private isLocked = false;

  acquire(): Promise<void> {
    return new Promise((resolve) => {
      const run = () => {
        this.isLocked = true;
        resolve();
      };

      if (!this.isLocked) {
        run();
      } else {
        this.queue.push(run);
      }
    });
  }

  release(): void {
    if (this.queue.length > 0) {
      const next = this.queue.shift();
      next?.();
    } else {
      this.isLocked = false;
    }
  }
}
