import type { SegmentPointer } from "../entities/SegmentPointer";

export interface ICompactor {
  compact(deletedPointers: SegmentPointer[]): Promise<void>;
  compactSegment(segmentId: number, offsetsToDelete: number[]): Promise<void>;
}
