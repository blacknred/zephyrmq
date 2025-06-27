import fs from "fs/promises";
import type { SegmentPointer } from "src/domain/entities/SegmentPointer";
import type { IIndexManager } from "src/domain/interfaces/IIndexManager";
import type { ILogger } from "src/domain/interfaces/ILogger";
import type { ISegmentInfo } from "src/domain/interfaces/ISegmentInfo";

export class FileIndexManager implements IIndexManager {
  static INDEX_ENTRY_SIZE = 12;
  constructor(private logger?: ILogger) {}

  async writeIndexEntry(
    segment: ISegmentInfo,
    record: SegmentPointer
  ): Promise<void> {
    try {
      const indexEntry = Buffer.alloc(FileIndexManager.INDEX_ENTRY_SIZE);
      indexEntry.writeBigUInt64BE(BigInt(record.recordOffset), 0);
      indexEntry.writeUInt32BE(record.offset, 8);

      const indexHandle = await fs.open(segment.indexFilePath, "a");

      await indexHandle.write(
        indexEntry,
        0,
        FileIndexManager.INDEX_ENTRY_SIZE,
        segment.recordCount * FileIndexManager.INDEX_ENTRY_SIZE
      );

      await indexHandle.close();
    } catch (error) {
      this.logger?.error("Failed to write index entry", { error });
    }
  }
}
