import crc from "crc-32";
import { SegmentPointer } from "src/domain/entities/SegmentPointer";
import type { IAppender } from "src/domain/interfaces/IAppender";
import type { IIndexManager } from "src/domain/interfaces/IIndexManager";
import type { ISegmentManager } from "src/domain/interfaces/ISegmentManager";
import { Mutex } from "../../utils/Mutex";
import type { ILogger } from "src/domain/interfaces/ILogger";

export class FileAppender implements IAppender {
  private mutex = new Mutex();

  constructor(
    private segmentManager: ISegmentManager,
    private indexManager: IIndexManager,
    private logger?: ILogger
  ) {}

  async append(data: Buffer): Promise<SegmentPointer | void> {
    const segment = this.segmentManager.getCurrentSegment();
    if (!segment) return;

    await this.mutex.acquire();

    try {
      const checksum = crc.buf(data);
      const lengthBuffer = Buffer.alloc(8);
      lengthBuffer.writeUInt32BE(data.length, 0);
      lengthBuffer.writeUInt32BE(checksum, 4);

      const offset = segment.size;
      await segment.fileHandle!.write(lengthBuffer, 0, 8, offset);
      await segment.fileHandle!.write(data, 0, data.length, offset + 8);

      const pointer = new SegmentPointer();
      pointer.segmentId = segment.id;
      pointer.offset = offset;
      pointer.length = data.length;
      pointer.recordOffset = segment.lastOffset + 1;

      await this.indexManager.writeIndexEntry(segment, pointer);

      segment.size += 8 + data.length;
      segment.lastOffset += 1;
      segment.recordCount += 1;

      if (segment.size >= this.segmentManager.getMaxSegmentSizeBytes()) {
        await segment.fileHandle!.sync();
        await segment.fileHandle!.close();
        segment.fileHandle = undefined;
        this.segmentManager.setCurrentSegment({ ...segment }); // rotate
      }

      return pointer;
    } catch (error) {
      this.logger?.error("Failed to append to MessageLog", { error });
    } finally {
      this.mutex.release();
    }
  }
}
