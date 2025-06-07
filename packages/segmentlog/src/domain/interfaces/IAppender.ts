import type { SegmentPointer } from "../entities/SegmentPointer";

export interface IAppender {
  append(data: Buffer): Promise<SegmentPointer | void>;
}
