import type { SegmentPointer } from "@domain/entities/SegmentPointer";
import type { IReader } from "@domain/ports/IReader";
import type { ISegmentManager } from "@domain/ports/ISegmentManager";

export class FileReader implements IReader {
  constructor(private segmentManager: ISegmentManager) {}

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
    } catch (cause) {
      throw new Error("Failed to read", { cause });
    }
  }
}
