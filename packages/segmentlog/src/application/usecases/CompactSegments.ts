import type { SegmentPointer } from "@domain/entities/SegmentPointer";
import type { ICompactor } from "@domain/ports/ICompactor";

export class CompactSegments {
  constructor(private compactor: ICompactor) {}

  async execute(deletedPointers: SegmentPointer[]) {
    return this.compactor.compact(deletedPointers);
  }
}
