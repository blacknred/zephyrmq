import type { FileHandle } from "fs/promises";
import type { IAppender } from "@domain/interfaces/IAppender";
import type { ILogManager } from "@domain/interfaces/ILogManager";

export class FileAppender implements IAppender {
  private flushPromise?: Promise<void>;
  public isFlushing = false;
  public batch: Buffer[] = [];
  public batchSize = 0;

  constructor(
    private logManager: ILogManager<FileHandle>,
    private maxBatchSizeBytes = 1 * 1024 * 1024
  ) {}

  async append(data: Buffer): Promise<number | void> {
    if (!this.logManager.log) return;

    try {
      const { size: offset } = await this.logManager.log.stat();
      this.batch.push(data);
      this.batchSize += data.length;

      if (this.batchSize > this.maxBatchSizeBytes) {
        this.scheduleFlush();
      }

      return offset;
    } catch (cause) {
      throw new Error("Failed to append to WAL", { cause });
    }
  }

  private scheduleFlush(): Promise<void> | void {
    if (!this.logManager.log) return;
    if (this.isFlushing) return this.flushPromise!;
    this.isFlushing = true;

    this.flushPromise = new Promise<void>(async (resolve, reject) => {
      setImmediate(async () => {
        if (!this.logManager.log) return;
        try {
          const toWrite = Buffer.concat(this.batch);
          await this.logManager.log.write(toWrite);
          await this.logManager.log.sync();
          this.batch = [];
          this.batchSize = 0;
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          this.isFlushing = false;
          this.flushPromise = undefined;

          if (this.batch.length > 0) {
            this.scheduleFlush();
          }
        }
      });
    });

    return this.flushPromise;
  }

  async flush() {
    if (this.batch.length > 0) {
      await this.scheduleFlush();
    }
  }
}
