import crc from "crc-32";
import fs from "node:fs/promises";
import path from "path";
import type { ISegmentInfo } from "src/domain/interfaces/ISegmentInfo";
import type { ISegmentManager } from "src/domain/interfaces/ISegmentManager";
import type { ILogCollector } from "../../utils/LogCollector";

export class FileSegmentManager implements ISegmentManager {
  static HEADER_SIZE = 24;

  private segments = new Map<number, ISegmentInfo>();
  private currentSegment?: ISegmentInfo;

  constructor(
    private baseDir: string,
    private maxSegmentSizeBytes: number,
    private logger?: ILogCollector
  ) {
    this.init();
  }

  private async init(): Promise<void> {
    await this.loadExistingSegments();
    await this.ensureCurrentSegment();
  }

  private async loadExistingSegments(): Promise<void> {
    try {
      const files = await fs.readdir(this.baseDir);

      for (const file of files.sort()) {
        if (!file.endsWith(".segment")) continue;

        const id = parseInt(file.split(".")[0], 10);
        const filePath = path.join(this.baseDir, file);
        const indexFilePath = filePath.replace(".segment", ".index");

        const fileHandle = await fs.open(filePath, "r");

        const header = Buffer.alloc(FileSegmentManager.HEADER_SIZE);
        const { bytesRead } = await fileHandle.read(
          header,
          0,
          FileSegmentManager.HEADER_SIZE,
          0
        );

        let baseOffset = 0;
        if (bytesRead >= FileSegmentManager.HEADER_SIZE) {
          baseOffset = Number(header.readBigUInt64BE(6));
        }

        const recordCount = await this.countMessagesInSegment(fileHandle);
        const stat = await fs.stat(filePath);

        this.segments.set(id, {
          id,
          filePath,
          indexFilePath,
          baseOffset,
          lastOffset: baseOffset + recordCount - 1,
          size: stat.size,
          recordCount,
          fileHandle,
        });
      }
    } catch (error) {
      this.logger?.log(
        "Failed to load MessageLog segments",
        { error },
        "error"
      );
    }
  }

  private async countMessagesInSegment(
    fileHandle: fs.FileHandle
  ): Promise<number> {
    let pos = FileSegmentManager.HEADER_SIZE;
    let count = 0;

    while (true) {
      const lenBuf = Buffer.alloc(8);
      const { bytesRead } = await fileHandle.read(lenBuf, 0, 8, pos);
      if (bytesRead < 8) break;

      const length = lenBuf.readUInt32BE(0);
      const checksum = lenBuf.readUInt32BE(4);
      const msgBuffer = Buffer.alloc(length);

      await fileHandle.read(msgBuffer, 0, length, pos + 8);

      if (checksum !== crc.buf(msgBuffer)) break;
      pos += 8 + length;
      count++;
    }

    return count;
  }

  private async ensureCurrentSegment(): Promise<void> {
    if (
      this.currentSegment &&
      this.currentSegment.size < this.maxSegmentSizeBytes
    ) {
      return;
    }

    let newId = 0;
    if (this.segments.size > 0) {
      newId = Math.max(...this.segments.keys()) + 1;
    }

    const filePath = path.join(this.baseDir, `${newId}.segment`);
    const indexFilePath = filePath.replace(".segment", ".index");
    const fileHandle = await fs.open(filePath, "w+");

    const header = Buffer.alloc(FileSegmentManager.HEADER_SIZE);
    header.writeUInt32BE(0xcafebabe, 0); // magic
    header.writeUInt16BE(1, 4); // version
    header.writeBigUInt64BE(BigInt(newId), 6); // base offset placeholder
    header.writeBigUInt64BE(BigInt(Date.now()), 14); // timestamp

    await fileHandle.write(header, 0, FileSegmentManager.HEADER_SIZE, 0);

    const indexFileHandle = await fs.open(indexFilePath, "w");
    await indexFileHandle.close();

    const segment: ISegmentInfo = {
      id: newId,
      filePath,
      indexFilePath,
      baseOffset: newId,
      lastOffset: newId,
      size: FileSegmentManager.HEADER_SIZE,
      recordCount: 0,
      fileHandle,
    };

    this.currentSegment = segment;
    this.segments.set(newId, segment);
  }

  getMaxSegmentSizeBytes() {
    return this.maxSegmentSizeBytes;
  }

  async close() {
    for (const seg of this.segments.values()) {
      await seg.fileHandle?.close();
    }

    await this.currentSegment?.fileHandle?.close();
  }

  getSegments() {
    return this.segments;
  }

  getCurrentSegment() {
    return this.currentSegment;
  }

  setCurrentSegment(segment: ISegmentInfo) {
    this.currentSegment = segment;
    this.segments.set(segment.id, segment);
  }

  getAllSegments() {
    return Array.from(this.segments.values());
  }
}
