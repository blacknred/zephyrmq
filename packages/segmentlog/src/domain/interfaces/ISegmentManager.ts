import type { ISegmentInfo } from "./ISegmentInfo";

export interface ISegmentManager {
  getMaxSegmentSizeBytes(): number;
  close(): Promise<void>;
  getSegments(): Map<number, ISegmentInfo>;
  getCurrentSegment(): ISegmentInfo | undefined;
  setCurrentSegment(segment: ISegmentInfo): void;
  getAllSegments(): ISegmentInfo[];
}
