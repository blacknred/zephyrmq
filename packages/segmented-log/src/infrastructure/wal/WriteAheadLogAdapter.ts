import { promises as fs } from "fs";
import { join } from "path";
import { IWriteAheadLog } from "../../core/ports/IWriteAheadLog";

export class WriteAheadLogAdapter implements IWriteAheadLog {
  private fileHandle?: fs.FileHandle;
  private flushPromise?: Promise<void>;
  private isFlushing = false;
  private batch: Buffer[] = [];
  private batchSize = 0;

  constructor(
    private filePath: string,
    private maxBatchSizeBytes = 1 * 1024 * 1024
  ) {}

  async append(data: Buffer): Promise<number | void> {
    if (!this.fileHandle) {
      this.fileHandle = await fs.open(this.filePath, "a");
    }

    const { size: offset } = await this.fileHandle.stat();
    this.batch.push(data);
    this.batchSize += data.length;

    if (this.batchSize > this.maxBatchSizeBytes) {
      this.scheduleFlush();
    }

    return offset;
  }

  private scheduleFlush(): void {
    if (this.isFlushing) return;
    this.isFlushing = true;

    this.flushPromise = new Promise(async (resolve) => {
      try {
        for (const buffer of this.batch) {
          await this.fileHandle!.write(buffer);
        }
        await this.fileHandle!.sync();
        this.batch = [];
        this.batchSize = 0;
      } catch (error) {
        console.error("WAL Flush failed", error);
      } finally {
        this.isFlushing = false;
        resolve();
      }
    });
  }

  async read(offset: number, length: number): Promise<Buffer | void> {
    if (!this.fileHandle) return;
    const buffer = Buffer.alloc(length);
    const { bytesRead } = await this.fileHandle.read(buffer, 0, length, offset);
    return bytesRead === 0 ? undefined : buffer;
  }

  async truncate(upToOffset: number): Promise<void> {
    if (!this.fileHandle) return;
    await this.fileHandle.truncate(upToOffset);
  }

  async close(): Promise<void> {
    if (this.fileHandle) {
      await this.fileHandle.close();
    }
  }

  async getMetrics(): Promise<{
    fileSize: number | undefined;
    batchSize: number;
    batchCount: number;
    isFlushing: boolean;
  }> {
    const stats = this.fileHandle
      ? await this.fileHandle.stat()
      : { size: undefined };
    return {
      fileSize: stats.size,
      batchSize: this.batchSize,
      batchCount: this.batch.length,
      isFlushing: this.isFlushing,
    };
  }
}
