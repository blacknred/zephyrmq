import type { SegmentPointer } from "@domain/entities/SegmentPointer";
import type { IReader } from "@domain/ports/IReader";

export class ReadRecord {
  constructor(private reader: IReader) {}

  async execute(pointer: SegmentPointer) {
    return this.reader.read(pointer);
  }
}
