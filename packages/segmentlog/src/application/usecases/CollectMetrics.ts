import type { ISegmentManager } from "@domain/ports/ISegmentManager";

export class CollectMetrics {
  constructor(private segmentManager: ISegmentManager) {}

  async execute() {
    const currentSegmentId = this.segmentManager.getCurrentSegment()?.id;
    const segments = this.segmentManager.getAllSegments();
    let totalSize = 0;
    let recordCount = 0;
    for (let segment of segments) {
      totalSize += segment.size;
      recordCount += segment.recordCount;
    }

    return {
      totalSize,
      recordCount,
      currentSegmentId,
      segmentCount: segments.length,
    };
  }
}
