import type { SegmentPointer } from "../entities/SegmentPointer";

export interface IReader {
  read(pointer: SegmentPointer): Promise<Buffer | void>;
}
