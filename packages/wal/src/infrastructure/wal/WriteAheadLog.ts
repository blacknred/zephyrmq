import fs from "fs/promises";
import path from "path";
import type { IWriteAheadLog } from "src/domain/ports/IWriteAheadLog";
import type { ILogCollector } from "../utils/LogCollector";

export class WriteAheadLog implements IWriteAheadLog {
  private fileHandle?: fs.FileHandle;
  private flushPromise?: Promise<void>;
  private isFlushing = false;
  private batch: Buffer[] = [];
  private batchSize = 0;

  constructor(
    private filePath: string,
    private maxBatchSizeBytes = 1 * 1024 * 1024,
    private logger?: ILogCollector
  ) {
    this.init();
  }

  private async init() {
    try {
      await fs.mkdir(path.dirname(this.filePath), { recursive: true });

      this.fileHandle = await fs.open(this.filePath, "a+");
    } catch (error) {
      this.logger?.log("Failed to initialize WAL", { error }, "error");
    }
  }

  private scheduleFlush(): Promise<void> | void {
    if (!this.fileHandle) return;
    if (this.isFlushing) return this.flushPromise!;
    this.isFlushing = true;

    this.flushPromise = new Promise<void>(async (resolve, reject) => {
      setImmediate(async () => {
        if (!this.fileHandle) return;
        try {
          const toWrite = Buffer.concat(this.batch);
          await this.fileHandle.write(toWrite);
          await this.fileHandle.sync();
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

  [Symbol.asyncDispose]() {
    return this.close();
  }

  async append(data: Buffer): Promise<number | void> {
    if (!this.fileHandle) return;

    try {
      const { size: offset } = await this.fileHandle.stat();
      this.batch.push(data);
      this.batchSize += data.length;

      if (this.batchSize > this.maxBatchSizeBytes) {
        this.scheduleFlush();
      }

      return offset;
    } catch (error) {
      this.logger?.log("Failed to append to WAL", { error }, "error");
    }
  }

  async read(offset: number, length: number): Promise<Buffer | void> {
    if (!this.fileHandle) return;

    try {
      const buffer = Buffer.alloc(length);
      await this.fileHandle.read(buffer, 0, length, offset);
      return buffer;
    } catch (error) {
      this.logger?.log("Failed to read from WAL", { error }, "error");
    }
  }

  async truncate(upToOffset: number): Promise<void> {
    try {
      if (!this.fileHandle) {
        this.fileHandle = await fs.open(this.filePath, "r+");
      }

      await this.fileHandle.truncate(upToOffset);
      await this.fileHandle.sync();
    } catch (error) {
      this.logger?.log("Failed to truncate the WAL", { error }, "error");
    }
  }

  async close() {
    if (!this.fileHandle) return;
    if (this.batch.length > 0) {
      await this.scheduleFlush();
    }
    await this.fileHandle.close();
  }

  async getMetrics() {
    const stats = await this.fileHandle?.stat();
    return {
      fileSize: stats?.size,
      batchSize: this.batchSize,
      batchCount: this.batch.length,
      isFlushing: this.isFlushing,
    };
  }
}
