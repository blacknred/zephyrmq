import type { SegmentPointer } from "@domain/entities/SegmentPointer";

export interface ISegmentLog {
  append(data: Buffer): Promise<SegmentPointer | void>;
  read(pointer: SegmentPointer): Promise<Buffer | void>;
  remove(pointers: SegmentPointer[]): Promise<void>;
  close(): Promise<void>;
  getMetrics(): Promise<{
    totalSize: number;
    recordCount: number;
    currentSegmentId: number | undefined;
    segmentCount: number;
  }>;
}
