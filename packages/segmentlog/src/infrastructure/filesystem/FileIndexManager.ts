import fs from "fs/promises";
import type { SegmentPointer } from "@domain/entities/SegmentPointer";
import type { IIndexManager } from "@domain/interfaces/IIndexManager";
import type { ISegmentInfo } from "@domain/interfaces/ISegmentInfo";

export class FileIndexManager implements IIndexManager {
  static INDEX_ENTRY_SIZE = 12;

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
    } catch (cause) {
      throw new Error(`"Failed to write index entry"`, {
        cause,
      });
    }
  }
}
