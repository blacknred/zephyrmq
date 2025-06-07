import type { SegmentPointer } from "src/domain/entities/SegmentPointer";
import type { IReader } from "src/domain/interfaces/IReader";
import type { ISegmentManager } from "src/domain/interfaces/ISegmentManager";
import type { ILogCollector } from "../../utils/LogCollector";

export class FileReader implements IReader {
  constructor(
    private segmentManager: ISegmentManager,
    private logger?: ILogCollector
  ) {}

  async read(pointer: SegmentPointer): Promise<Buffer | void> {
    const segment = this.segmentManager.getSegments().get(pointer.segmentId);
    if (!segment) return;

    try {
      const buffer = Buffer.alloc(pointer.length);
      await segment.fileHandle!.read(
        buffer,
        0,
        pointer.length,
        pointer.offset + 8
      );

      return buffer;
    } catch (error) {
      this.logger?.log("Failed to read", { error }, "error");
    }
  }
}
