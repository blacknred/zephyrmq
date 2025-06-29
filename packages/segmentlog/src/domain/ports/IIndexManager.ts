import type { SegmentPointer } from "../entities/SegmentPointer";
import type { ISegmentInfo } from "./ISegmentInfo";

export interface IIndexManager {
  writeIndexEntry(segment: ISegmentInfo, record: SegmentPointer): Promise<void>;
}
