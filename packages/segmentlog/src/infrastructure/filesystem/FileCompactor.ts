import type { SegmentPointer } from "@domain/entities/SegmentPointer";
import type { ICompactor } from "@domain/interfaces/ICompactor";
import type { ISegmentInfo } from "@domain/interfaces/ISegmentInfo";
import type { ISegmentManager } from "@domain/interfaces/ISegmentManager";
import crc from "crc-32";
import fs from "fs/promises";
import path from "path";

export class FileCompactor implements ICompactor {
  static HEADER_SIZE = 24;

  constructor(
    private baseDir: string,
    private segmentManager: ISegmentManager
  ) {}

  async compact(deletedPointers: SegmentPointer[]): Promise<void> {
    const recordsBySegment = new Map<number, Set<number>>();

    for (const pointer of deletedPointers) {
      if (!recordsBySegment.has(pointer.segmentId)) {
        recordsBySegment.set(pointer.segmentId, new Set());
      }
      recordsBySegment.get(pointer.segmentId)?.add(pointer.offset);
    }

    const compactionPromises: Promise<void>[] = [];
    for (const [segmentId, offsetsToDelete] of recordsBySegment.entries()) {
      compactionPromises.push(
        this.compactSegment(segmentId, [...offsetsToDelete])
      );
    }

    try {
      await Promise.all(compactionPromises);
    } catch (cause) {
      throw new Error("Failed to compact segments", { cause });
    }
  }

  async compactSegment(
    segmentId: number,
    offsetsToDelete: number[]
  ): Promise<void> {
    const segment = this.segmentManager.getSegments().get(segmentId);
    if (!segment) return;

    try {
      const allRecords = await this.readAllFromSegment(segment);
      const setToDelete = new Set(offsetsToDelete);
      const filteredRecords = allRecords.filter(
        (r) => !setToDelete.has(r.offset)
      );

      const newSegment = await this.createNewCompactedSegment(
        segment,
        filteredRecords
      );

      await this.replaceSegment(segment, newSegment);

      this.segmentManager.getSegments().delete(segment.id);
      this.segmentManager.getSegments().set(newSegment.id, newSegment);

      if (this.segmentManager.getCurrentSegment()?.id === segment.id) {
        this.segmentManager.setCurrentSegment(newSegment);
      }
    } catch (cause) {
      throw new Error(`Failed to compact segment ${segmentId}`, {
        cause,
      });
    }
  }

  private async readAllFromSegment(
    segment: ISegmentInfo
  ): Promise<SegmentPointer[]> {
    const result: SegmentPointer[] = [];
    let pos = FileCompactor.HEADER_SIZE;

    while (true) {
      const lenBuf = Buffer.alloc(8);
      let handle = segment.fileHandle;
      if (!handle) handle = await fs.open(segment.filePath, "r");

      const { bytesRead } = await handle.read(lenBuf, 0, 8, pos);
      if (bytesRead < 8) break;

      const length = lenBuf.readUInt32BE(0);
      const checksum = lenBuf.readUInt32BE(4);
      const msgBuffer = Buffer.alloc(length);

      await handle.read(msgBuffer, 0, length, pos + 8);

      if (checksum !== crc.buf(msgBuffer)) break;

      const offsets = result.map((r) => r.recordOffset);
      const recordOffset = Math.max(...offsets, segment.baseOffset - 1) + 1;

      result.push({
        segmentId: segment.id,
        offset: pos,
        length,
        recordOffset,
      });

      pos += 8 + length;
      if (!segment.fileHandle) await handle.close();
    }

    return result;
  }

  private async createNewCompactedSegment(
    original: ISegmentInfo,
    filteredRecords: SegmentPointer[]
  ): Promise<ISegmentInfo> {
    const newFilePath = path.join(this.baseDir, `${original.id}.compacted`);
    const newFileHandle = await fs.open(newFilePath, "w+");

    let originalHandle = original.fileHandle;
    if (!originalHandle) {
      originalHandle = await fs.open(original.filePath, "r");
    }

    const header = Buffer.alloc(FileCompactor.HEADER_SIZE);
    const { bytesRead } = await originalHandle.read(
      header,
      0,
      FileCompactor.HEADER_SIZE,
      0
    );

    if (bytesRead < FileCompactor.HEADER_SIZE) {
      throw new Error("Invalid segment header");
    }

    await newFileHandle.write(header, 0, FileCompactor.HEADER_SIZE, 0);

    let currentPos = FileCompactor.HEADER_SIZE;

    for (const record of filteredRecords) {
      const buffer = Buffer.alloc(record.length);
      let handle = original.fileHandle;

      if (!handle) handle = await fs.open(original.filePath, "r");
      await handle.read(buffer, 0, record.length, record.offset);
      if (!original.fileHandle) await handle.close();

      await newFileHandle.write(buffer, 0, buffer.length, currentPos);
      currentPos += record.length;
    }

    await newFileHandle.sync();
    await newFileHandle.close();

    return {
      id: original.id,
      filePath: newFilePath,
      baseOffset: original.baseOffset,
      size: currentPos,
      recordCount: filteredRecords.length,
      fileHandle: undefined,
    } as ISegmentInfo;
  }

  private async replaceSegment(
    oldSegment: ISegmentInfo,
    newSegment: ISegmentInfo
  ): Promise<void> {
    const oldPath = oldSegment.filePath;
    const newPath = newSegment.filePath;

    await fs.unlink(oldPath).catch(() => {});
    await fs.rename(newPath, oldPath);
    const fileHandle = await fs.open(oldPath, "r+");

    newSegment.fileHandle = fileHandle;
  }
}
