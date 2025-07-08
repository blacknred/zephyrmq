import type { SegmentPointer } from "@domain/entities/SegmentPointer";
import type { ICompactor } from "@domain/interfaces/ICompactor";

export class CompactSegments {
  constructor(private compactor: ICompactor) {}

  async execute(deletedPointers: SegmentPointer[]) {
    return this.compactor.compact(deletedPointers);
  }
}
